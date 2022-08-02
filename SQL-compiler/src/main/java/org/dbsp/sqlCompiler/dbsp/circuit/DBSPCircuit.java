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
 *
 *
 */

package org.dbsp.sqlCompiler.dbsp.circuit;

import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeTuple;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DBSPCircuit extends DBSPNode {
    static final String rustPreamble =
            "// Automatically-generated file\n" +
            "#![allow(dead_code)]\n" +
            "#![allow(non_snake_case)]\n" +
            "#![allow(unused_imports)]\n" +
            "#![allow(unused_parens)]\n" +
            "#![allow(unused_variables)]\n" +
            "\n" +
            "use dbsp::{\n" +
            "    algebra::ZSet,\n" +
            "    circuit::{Root, Stream},\n" +
            "    operator::{Generator, FilterMap},\n" +
            "    trace::ord::{OrdIndexedZSet, OrdZSet},\n" +
            "    zset,\n" +
            "};\n" +
            "use ordered_float::OrderedFloat;\n" +
            "mod sqllib;\n" +
            "use crate::test::sqllib::*;\n" +
            "use ::serde::{Deserialize,Serialize};\n" +
            "use std::{\n" +
            "    fmt::{Debug, Formatter, Result as FmtResult},\n" +
            "    cell::RefCell,\n" +
            "    rc::Rc,\n" +
            "};\n" +
            "use tuple::declare_tuples;\n" +
            "use sqllibbool::*;\n" +
            "use hashing::*;\n" +
            "type Weight = isize;\n";

    private final List<DBSPSourceOperator> inputOperators = new ArrayList<>();
    private final List<DBSPSinkOperator> outputOperators = new ArrayList<>();
    private final List<DBSPOperator> operators = new ArrayList<>();
    public final String name;
    public final String query;

    public DBSPCircuit(@Nullable Object node, String name, String query) {
        super(node);
        this.name = name;
        this.query = query;
    }

    /**
     * @return the names of the input tables.
     * The order of the tables corresponds to the inputs of the generated circuit.
     */
    public List<String> getInputTables() {
        return Linq.map(this.inputOperators, DBSPOperator::getName);
    }

    public int getOutputCount() {
        return this.outputOperators.size();
    }

    public DBSPType getOutputType(int outputNo) {
        return this.outputOperators.get(outputNo).getNonVoidType();
    }

    public void addOperator(DBSPOperator operator) {
        if (operator instanceof DBSPSourceOperator)
            this.inputOperators.add((DBSPSourceOperator)operator);
        else if (operator instanceof DBSPSinkOperator)
            this.outputOperators.add((DBSPSinkOperator)operator);
        else
            this.operators.add(operator);
    }

    private void genRcCell(IndentStringBuilder builder, DBSPOperator op) {
        builder.append("let ")
                .append(op.getName())
                .append(" = Rc::new(RefCell::<")
                .append(op.getNonVoidType())
                .append(">::new(Default::default()));")
                .newline();
        builder.append("let ")
                .append(op.getName())
                .append("_external = ")
                .append(op.getName())
                .append(".clone();")
                .newline();
        if (op instanceof DBSPSourceOperator) {
            builder.append("let ")
                    .append(op.getName())
                    .append(" = Generator::new(move || ")
                    .append(op.getName())
                    .append(".borrow().clone());")
                    .newline();
        }
    }

    public static String generatePreamble() {
        IndentStringBuilder builder = new IndentStringBuilder();
        builder.append(rustPreamble)
                .newline();

        builder.append("declare_tuples! {").increase();
        // Do not generate Tuple1
        for (int i = 2; i <= DBSPTypeTuple.maxTupleSize; i++) {
            builder.append("Tuple")
                    .append(i)
                    .append("<");
            for (int j = 0; j < i; j++) {
                if (j > 0)
                    builder.append(", ");
                builder.append("T")
                        .append(j);
            }
            builder.append(">,\n");
        }
        return builder.decrease()
                .append("}\n\n")
                .toString();
    }

    /**
     * Generates a Rust function that returns a closure which evaluates the circuit.
     * TODO: generate an IR node.
     */
    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        // function prototype:
        // fn name() -> impl FnMut(T0, T1) -> (O0, O1) {
        builder.append("fn ")
                .append(this.name)
                .append("() -> impl FnMut(");

        boolean first = true;
        for (DBSPOperator i: this.inputOperators) {
            if (!first)
                builder.append(",");
            first = false;
            builder.append(i.getNonVoidType());
        }
        builder.append(") -> ");
        DBSPTypeTuple tuple = new DBSPTypeTuple(null, Linq.map(this.outputOperators, DBSPOperator::getNonVoidType));
        builder.append(tuple)
                .append(" {")
                .increase();

        builder.append("// ")
                .append(this.query)
                .append("\n");
        // For each input and output operator a corresponding Rc cell
        for (DBSPOperator i: this.inputOperators)
            this.genRcCell(builder, i);

        for (DBSPOperator o: this.outputOperators)
            this.genRcCell(builder, o);

        // Circuit body
        builder.append("let root = Root::build(|circuit| {")
                .increase();
        for (DBSPOperator i: this.inputOperators)
            builder.append(i)
                    .newline();
        for (DBSPOperator op: this.operators)
            op.toRustString(builder)
                    .newline();
        for (DBSPOperator i: this.outputOperators)
            builder.append(i)
                    .newline();

        builder.decrease()
                .append("})")
                .append(".unwrap();")
                .newline();

        // Create the closure and return it.
        builder.append("return move |")
                .append(String.join(", ",
                        Linq.map(this.inputOperators, DBSPOperator::getName)))
                .append("| {")
                .increase();

        for (DBSPOperator i: this.inputOperators)
            builder.append("*")
                    .append(i.getName())
                    .append("_external.borrow_mut() = ")
                    .append(i.getName())
                    .append(";")
                    .newline();
        builder.append("root.step().unwrap();")
                        .newline();
        builder.append("return ");
        if (this.outputOperators.size() > 1)
            builder.append("(");
        builder.append(String.join(", ",
                Linq.map(this.outputOperators,
                        o -> o.getName() + "_external.borrow().clone()")));
        if (this.outputOperators.size() > 1)
            builder.append(")");
        builder.append(";")
                .newline()
                .decrease()
                .append("};")
                .newline()
                .decrease()
                .append("}")
                .newline();
        return builder;
    }

    @Override
    public String toString() {
        IndentStringBuilder builder = new IndentStringBuilder();
        this.toRustString(builder);
        return builder.toString();
    }
}
