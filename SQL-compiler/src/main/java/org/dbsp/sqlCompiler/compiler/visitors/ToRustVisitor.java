/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.visitors;

import org.dbsp.sqlCompiler.circuit.*;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;

/**
 * This visitor generate a Rust implementation of the program.
 */
public class ToRustVisitor extends CircuitVisitor {
    private final IndentStream builder;
    public final InnerVisitor innerVisitor;

    @SuppressWarnings("SpellCheckingInspection")
    static final String rustPreamble =
            "// Automatically-generated file\n" +
                    "#![allow(dead_code)]\n" +
                    "#![allow(non_snake_case)]\n" +
                    "#![allow(unused_imports)]\n" +
                    "#![allow(unused_parens)]\n" +
                    "#![allow(unused_variables)]\n" +
                    "\n" +
                    "use dbsp::{\n" +
                    "    algebra::{ZSet, MulByRef, F32, F64, UnimplementedSemigroup},\n" +
                    "    circuit::{Circuit, Stream},\n" +
                    "    operator::{Generator, FilterMap, Fold},\n" +
                    "    trace::ord::{OrdIndexedZSet, OrdZSet},\n" +
                    "    zset,\n" +
                    "    DBWeight,\n" +
                    "    DBData,\n" +
                    "};\n" +
                    "use genlib::*;\n" +
                    "use size_of::*;\n" +
                    "use ::serde::{Deserialize,Serialize};\n" +
                    "use compare::{Compare, Extract};\n" +
                    "use std::{\n" +
                    "    convert::identity,\n" +
                    "    fmt::{Debug, Formatter, Result as FmtResult},\n" +
                    "    cell::RefCell,\n" +
                    "    rc::Rc,\n" +
                    "};\n" +
                    "use tuple::declare_tuples;\n" +
                    "use sqllib::*;\n" +
                    "use sqlvalue::*;\n" +
                    "use hashing::*;\n" +
                    "use readers::*;\n" +
                    "type Weight = isize;\n";


    public ToRustVisitor(IndentStream builder) {
        super(true);
        this.builder = builder;
        this.innerVisitor = new ToRustInnerVisitor(builder);
    }

    public static String generatePreamble() {
        IndentStream stream = new IndentStream(new StringBuilder());
        stream.append(rustPreamble)
                .newline();

        stream.append("declare_tuples! {").increase();
        for (int i: DBSPTypeTuple.tupleSizesUsed) {
            if (i == 0)
                continue;
            stream.append("Tuple")
                    .append(i)
                    .append("<");
            for (int j = 0; j < i; j++) {
                if (j > 0)
                    stream.append(", ");
                stream.append("T")
                        .append(j);
            }
            stream.append(">,\n");
        }
        DBSPTypeTuple.clearSizesUsed();
        return stream.decrease()
                .append("}\n\n")
                .toString();
    }

    //////////////// Operators

    private void genRcCell(DBSPOperator op) {
        this.builder.append("let ")
                .append(op.getName())
                .append(" = Rc::new(RefCell::<");
        op.getNonVoidType().accept(this.innerVisitor);
        this.builder.append(">::new(Default::default()));")
                .newline();
        this.builder.append("let ")
                .append(op.getName())
                .append("_external = ")
                .append(op.getName())
                .append(".clone();")
                .newline();
        if (op instanceof DBSPSourceOperator) {
            this.builder.append("let ")
                    .append(op.getName())
                    .append(" = Generator::new(move || ")
                    .append(op.getName())
                    .append(".borrow().clone());")
                    .newline();
        }
    }

    @Override
    public boolean preorder(DBSPCircuit circuit) {
        // function prototype:
        // fn name() -> impl FnMut(T0, T1) -> (O0, O1) {
        super.preorder(circuit);
        this.builder.append("fn ")
                .append(circuit.name)
                .append("() -> impl FnMut(");

        boolean first = true;
        for (
                DBSPOperator i : circuit.inputOperators) {
            if (!first)
                this.builder.append(",");
            first = false;
            i.getNonVoidType().accept(this.innerVisitor);
        }
        this.builder.append(") -> ");
        DBSPTypeTuple tuple = new DBSPTypeRawTuple(null, Linq.map(circuit.outputOperators, DBSPOperator::getNonVoidType));
        tuple.accept(this.innerVisitor);
        this.builder.append(" {").increase();
        // For each input and output operator a corresponding Rc cell
        for (DBSPOperator i : circuit.inputOperators)
            this.genRcCell(i);

        for (DBSPOperator o : circuit.outputOperators)
            this.genRcCell(o);

        // Circuit body
        this.builder.append("let root = Circuit::build(|circuit| {")
                .increase();
        for (IDBSPInnerDeclaration decl : circuit.declarations.values()) {
            decl.accept(this.innerVisitor);
            this.builder.newline();
        }
        for (DBSPOperator i : circuit.inputOperators) {
            i.accept(this);
            this.builder.newline();
        }
        for (DBSPOperator op : circuit.operators) {
            op.accept(this);
            this.builder.newline();
        }
        for (DBSPOperator o : circuit.outputOperators) {
            o.accept(this);
            this.builder.newline();
        }

        this.builder.decrease()
                .append("})")
                .append(".unwrap();")
                .newline();

        // Create the closure and return it.
        this.builder.append("return move |")
                .joinS(", ", Linq.map(circuit.inputOperators, DBSPOperator::getName))
                .append("| {")
                .increase();

        for (DBSPOperator i : circuit.inputOperators)
            builder.append("*")
                    .append(i.getName())
                    .append("_external.borrow_mut() = ")
                    .append(i.getName())
                    .append(";")
                    .newline();
        this.builder.append("root.0.step().unwrap();")
                .newline()
                .append("return ")
                .append("(")
                .intercalateS(", ",
                        Linq.map(circuit.outputOperators, o -> o.getName() + "_external.borrow().clone()"))
                .append(")")
                .append(";")
                .newline()
                .decrease()
                .append("};")
                .newline()
                .decrease()
                .append("}")
                .newline();
        return false;
    }

    @Override
    public boolean preorder(DBSPSourceOperator operator) {
        this.builder
                .append(operator.comment != null ? "// " + operator.comment + "\n" : "")
                .append("let ")
                .append(operator.getName())
                .append(" = ")
                .append("circuit.add_source(")
                .append(operator.outputName)
                .append(");");
        return false;
    }

    @Override
    public boolean preorder(DBSPSinkOperator operator) {
        this.builder
                .append("// ")
                .append(operator.query)
                .append("\n")
                .append(operator.comment != null ? "// " + operator.comment + "\n" : "")
                .append(operator.input().getName())
                .append(".")
                .append(operator.operation) // inspect
                .append("(move |m| { *")
                .append(operator.getName())
                .append(".borrow_mut() = ")
                .append("m.clone() });");
        return false;
    }

    @Override
    public boolean preorder(DBSPOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        builder.append(operator.comment != null ? "// " + operator.comment + "\n" : "")
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            builder.append(operator.inputs.get(0).getName())
                    .append(".");
        builder.append(operator.operation)
                .append("(");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                builder.append(",");
            builder.append("&")
                    .append(operator.inputs.get(i).getName());
        }
        if (operator.function != null) {
            if (operator.inputs.size() > 1)
                builder.append(", ");
            operator.function.accept(this.innerVisitor);
        }
        builder.append(");");
        return false;
    }

    @Override
    public boolean preorder(DBSPSumOperator operator) {
        this.builder.append(operator.comment != null ? "// " + operator.comment + "\n" : "")
                    .append("let ")
                    .append(operator.getName())
                    .append(": ");
        new DBSPTypeStream(operator.outputType).accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(operator.inputs.get(0).getName())
                        .append(".");
        this.builder.append(operator.operation)
                    .append("([");
        for (int i = 1; i < operator.inputs.size(); i++) {
            if (i > 1)
                this.builder.append(", ");
            this.builder.append("&").append(operator.inputs.get(i).getName());
        }
        this.builder.append("]);");
        return false;
    }

    @Override
    public boolean preorder(DBSPConstantOperator operator) {
        assert operator.function != null;
        builder.append("let ")
                .append(operator.getName())
                .append(" = ")
                .append("circuit.add_source(Generator::new(|| ");
        operator.function.accept(this.innerVisitor);
        this.builder.append("));");
        return false;
    }

    public static String toRustString(IDBSPOuterNode node) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustVisitor visitor = new ToRustVisitor(stream);
        node.accept(visitor);
        return builder.toString();
    }

    public static String toRustString(IDBSPInnerNode node) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustVisitor visitor = new ToRustVisitor(stream);
        node.accept(visitor.innerVisitor);
        return builder.toString();
    }
}
