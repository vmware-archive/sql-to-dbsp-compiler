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

package org.dbsp.sqlCompiler.dbsp.visitors;

import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.dbsp.circuit.IDBSPNode;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.*;
import org.dbsp.sqlCompiler.dbsp.rust.DBSPFile;
import org.dbsp.sqlCompiler.dbsp.rust.DBSPFunction;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.*;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPPath;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.*;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPStatement;
import org.dbsp.sqlCompiler.dbsp.rust.type.*;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.Map;

/**
 * This visitor generate a Rust implementation of the program.
 */
public class ToRustVisitor extends Visitor {
    private final IndentStringBuilder builder;

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
                    "    algebra::{ZSet, MulByRef, F32, F64},\n" +
                    "    circuit::{Circuit, Stream},\n" +
                    "    operator::{Generator, FilterMap, Fold},\n" +
                    "    trace::ord::{OrdIndexedZSet, OrdZSet},\n" +
                    "    zset,\n" +
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
                    "type Weight = isize;\n";


    public ToRustVisitor(IndentStringBuilder builder) {
        super(true);
        this.builder = builder;
    }

    public static String generatePreamble() {
        IndentStringBuilder builder = new IndentStringBuilder();
        builder.append(rustPreamble)
                .newline();

        builder.append("declare_tuples! {").increase();
        for (int i: DBSPTypeTuple.tupleSizesUsed) {
            if (i == 0)
                continue;
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
        DBSPTypeTuple.clearSizesUsed();
        return builder.decrease()
                .append("}\n\n")
                .toString();
    }

    @Override
    public boolean preorder(DBSPLiteral literal) {
        if (literal.isNull) {
            this.builder.append(literal.noneString());
            return false;
        }
        return true;
    }

    @Override
    public void postorder(DBSPBoolLiteral literal) {
        assert literal.value != null;
        this.builder.append(literal.wrapSome(Boolean.toString(literal.value)));
    }

    @Override
    public boolean preorder(DBSPVecLiteral literal) {
        this.builder.append("vec!(")
                .increase();
        for (DBSPExpression exp: literal.data) {
            exp.accept(this);
            this.builder.append(", ");
        }
        this.builder.decrease().append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPZSetLiteral literal) {
        this.builder.append("zset!(")
                .increase();
        for (Map.Entry<DBSPExpression, Integer> e: literal.data.entrySet()) {
            e.getKey().accept(this);
            this.builder.append(" => ")
                    .append(e.getValue())
                    .append(",\n");
        }
        this.builder.decrease().append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPFloatLiteral literal) {
        assert literal.value != null;
        String val = Float.toString(literal.value);
        if (Float.isNaN(literal.value))
            val = "std::f32::NAN";
        this.builder.append(literal.wrapSome("F32::new(" + val + ")"));
        return false;
    }

    @Override
    public boolean preorder(DBSPDoubleLiteral literal) {
        assert literal.value != null;
        String val = Double.toString(literal.value);
        if (Double.isNaN(literal.value))
            val = "std::f64::NAN";
        this.builder.append(literal.wrapSome("F64::new(" + val + ")"));
        return false;
    }

    @Override
    public boolean preorder(DBSPUSizeLiteral literal) {
        assert literal.value != null;
        String val = Long.toString(literal.value);
        this.builder.append(literal.wrapSome(val + "usize"));
        return false;
    }

    @Override
    public boolean preorder(DBSPISizeLiteral literal) {
        assert literal.value != null;
        String val = Long.toString(literal.value);
        this.builder.append(literal.wrapSome(val + "isize"));
        return false;
    }

    @Override
    public boolean preorder(DBSPIntegerLiteral literal) {
        assert literal.value != null;
        String val = Integer.toString(literal.value);
        this.builder.append(literal.wrapSome(val + "i32"));
        return false;
    }

    @Override
    public boolean preorder(DBSPLongLiteral literal) {
        assert literal.value != null;
        String val = Long.toString(literal.value);
        this.builder.append(literal.wrapSome(val + "i64"));
        return false;
    }

    @Override
    public boolean preorder(DBSPStringLiteral literal) {
        assert literal.value != null;
        this.builder.append(literal.wrapSome(
                "String::from(" + Utilities.escapeString(literal.value) + ")"));
        return false;
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        if (expression.left.getNonVoidType().mayBeNull) {
            this.builder.append("(")
                    .append("match (");
            expression.left.accept(this);
            this.builder.append(", ");
            expression.right.accept(this);
            this.builder.append(") {").increase()
                    .append("(Some(x), Some(y)) => Some(x ")
                    .append(expression.operation)
                    .append(" y),\n")
                    .append("_ => None,\n")
                    .decrease()
                    .append("}")
                    .append(")");
        } else {
            this.builder.append("(");
            expression.left.accept(this);
            this.builder.append(" ")
                    .append(expression.operation)
                    .append(" ");
            expression.right.accept(this);
            this.builder.append(")");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression expression) {
        if (expression.left.getNonVoidType().mayBeNull) {
            this.builder.append("(")
                    .append("match ");
            expression.left.accept(this);
            this.builder.append(" {").increase()
                    .append("Some(x) => Some(")
                    .append(expression.operation)
                    .append("(x)),\n")
                    .append("_ => None,\n")
                    .decrease()
                    .append("}")
                    .append(")");
        } else {
            this.builder.append("(")
                    .append(expression.operation);
            expression.left.accept(this);
            this.builder.append(")");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPVariableReference expression) {
        this.builder.append(expression.variable);
        return false;
    }

    @Override
    public boolean preorder(DBSPExpressionStatement statement) {
        statement.expression.accept(this);
        if (!statement.expression.is(DBSPBlockExpression.class))
            this.builder.append(";");
        return false;
    }

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        this.builder.append("let ")
                .append(statement.mutable ? "mut " : "")
                .append(statement.variable)
                .append(" = ");
        statement.initializer.accept(this);
        this.builder.append(";");
        return false;
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

    @Override
    public boolean preorder(DBSPCircuit circuit) {
        // function prototype:
        // fn name() -> impl FnMut(T0, T1) -> (O0, O1) {
        this.builder.append("fn ")
                .append(circuit.name)
                .append("() -> impl FnMut(");

        boolean first = true;
        for (
                DBSPOperator i : circuit.inputOperators) {
            if (!first)
                this.builder.append(",");
            first = false;
            this.builder.append(i.getNonVoidType());
        }
        this.builder.append(") -> ");
        DBSPTypeTuple tuple = new DBSPTypeRawTuple(null, Linq.map(circuit.outputOperators, DBSPOperator::getNonVoidType));
        tuple.accept(this);
        this.builder.append(" {").increase();
        this.builder.append("// ")
                .append(circuit.query)
                .append("\n");
        // For each input and output operator a corresponding Rc cell
        for (DBSPOperator i : circuit.inputOperators)
            this.genRcCell(builder, i);

        for (DBSPOperator o : circuit.outputOperators)
            this.genRcCell(builder, o);

        // Circuit body
        this.builder.append("let root = Circuit::build(|circuit| {")
                .increase();
        for (
                IDBSPDeclaration decl : circuit.declarations)
            this.builder.append(decl)
                    .newline();
        for (DBSPOperator i : circuit.inputOperators)
            this.builder.append(i)
                    .newline();
        for (DBSPOperator op : circuit.operators) {
            op.accept(this);
            this.builder.newline();
        }
        for (DBSPOperator i : circuit.outputOperators) {
            i.accept(this);
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
        this.builder.append("let ")
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
        builder.append("let ")
                .append(operator.getName())
                .append(": ")
                .append(new DBSPTypeStream(operator.outputType))
                .append(" = ");
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
            operator.function.accept(this);
        }
        builder.append(");");
        return false;
    }

    @Override
    public boolean preorder(DBSPSumOperator operator) {
        this.builder.append("let ")
                    .append(operator.getName())
                    .append(": ")
                    .append(new DBSPTypeStream(operator.outputType))
                    .append(" = ");
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
    public boolean preorder(DBSPFile file) {
        for (IDBSPDeclaration decl: file.declarations) {
            decl.accept(this);
            this.builder.append("\n\n");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPFunction.Argument argument) {
        this.builder.append(argument.name)
                .append(": ")
                .append(argument.type);
        return false;
    }

    @Override
    public boolean preorder(DBSPFunction function) {
        this.builder.intercalateS("\n", function.annotations)
                .append("pub fn ")
                .append(function.name)
                .append("(");
        for (DBSPFunction.Argument arg: function.arguments) {
            arg.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(") ");
        if (function.returnType != null) {
            builder.append("-> ");
            function.returnType.accept(this);
        }
        if (function.body.is(DBSPBlockExpression.class)) {
            function.body.accept(this);
        } else {
            this.builder.append("\n{").increase();
            function.body.accept(this);
            this.builder.decrease()
                    .append("\n}");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPConstantOperator operator) {
        assert operator.function != null;
        builder.append("let ")
                .append(operator.getName())
                .append(" = ")
                .append("circuit.add_source(Generator::new(|| ");
        operator.function.accept(this);
        this.builder.append("));");
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyExpression expression) {
        expression.function.accept(this);
        this.builder.append("(");
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(", ");
            first = false;
            arg.accept(this);
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyMethodExpression expression) {
        expression.self.accept(this);
        this.builder.append(".");
        expression.function.accept(this);
        this.builder.append("(");
        boolean first = true;
        for (DBSPExpression arg: expression.arguments) {
            if (!first)
                this.builder.append(", ");
            first = false;
            arg.accept(this);
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPAsExpression expression) {
        this.builder.append("(");
        expression.source.accept(this);
        this.builder.append(" as ")
                // TODO: remove getRustString?
                .append(expression.getNonVoidType().to(IsNumericType.class).getRustString())
                .append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPAssignmentExpression expression) {
        expression.left.accept(this);
        this.builder.append(" = ");
        expression.right.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        this.builder.append("{").increase();
        for (DBSPStatement stat: expression.contents) {
            stat.accept(this);
            this.builder.append("\n");
        }
        if (expression.lastExpression != null) {
            expression.lastExpression.accept(this);
            this.builder.append("\n");
        }
        this.builder.decrease().append("}");
        return false;
    }

    @Override
    public boolean preorder(DBSPBorrowExpression expression) {
        this.builder.append("&");
        if (expression.mut)
            this.builder.append("mut ");
        expression.expression.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression.Parameter parameter) {
        parameter.pattern.accept(this);
        if (parameter.type != null) {
            this.builder.append(": ");
            parameter.type.accept(this);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        this.builder.append("move |");
        for (DBSPClosureExpression.Parameter param: expression.parameters) {
            param.accept(this);
            this.builder.append(", ");
        }
        this.builder.append("| ");
        DBSPType resultType = expression.getResultType();
        if (resultType != null) {
            this.builder.append("-> ");
            resultType.accept(this);
            this.builder.append(" ");
        }
        if (expression.body.is(DBSPBlockExpression.class)) {
            expression.body.accept(this);
        } else {
            this.builder.append("{")
                    .increase();
            expression.body.accept(this);
            this.builder.decrease()
                    .append("\n}");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPDerefExpression expression) {
        this.builder.append("*");
        expression.expression.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPEnumValue expression) {
        this.builder.append(expression.enumName)
                .append("::")
                .append(expression.constructor);
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldExpression expression) {
        expression.expression.accept(this);
        this.builder.append(".")
                .append(expression.fieldNo);
        if (expression.getNonVoidType().is(DBSPTypeString.class))
            this.builder.append(".clone()");
        return false;
    }

    @Override
    public boolean preorder(DBSPIfExpression expression) {
        builder.append("(if ");
        expression.condition.accept(this);
        this.builder.append(" ");
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.append("{")
                    .increase();
        expression.positive.accept(this);
        if (!expression.positive.is(DBSPBlockExpression.class))
            this.builder.decrease()
                    .append("\n}");
        this.builder.append(" else ");
        if (!expression.negative.is(DBSPBlockExpression.class))
            this.builder.append("{")
                    .increase();
        expression.negative.accept(this);
        if (!expression.negative.is(DBSPBlockExpression.class))
            this.builder.decrease()
                    .append("\n}");
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPMatchExpression.Case mcase) {
        mcase.against.accept(this);
        this.builder.append(" => ");
        mcase.result.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPMatchExpression expression) {
        this.builder.append("(match ");
        expression.matched.accept(this);
        this.builder.append(" {").increase();
        for (DBSPMatchExpression.Case mcase : expression.cases) {
            mcase.accept(this);
            this.builder.append(",\n");
        }
        this.builder.decrease()
                .append("})");
        return false;
    }

    @Override
    public boolean preorder(DBSPPathExpression expression) {
        expression.path.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPQualifyTypeExpression expression) {
        expression.expression.accept(this);
        this.builder.append("::<");
        for (DBSPType type: expression.types) {
            type.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPRangeExpression expression) {
        if (expression.left != null)
            expression.left.accept(this);
        this.builder.append("..");
        if (expression.endInclusive)
            this.builder.append("=");
        if (expression.right != null)
            expression.right.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPRawTupleExpression expression) {
        this.builder.append("(");
        for (DBSPExpression field: expression.fields) {
            field.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        if (expression.size() == 0) {
            this.builder.append("()");
        } else {
            this.builder.append("Tuple")
                    .append(expression.size())
                    .append("::new(");
            boolean first = true;
            for (DBSPExpression field: expression.fields) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                field.accept(this);
            }
            this.builder.append(")");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPStructExpression expression) {
        expression.function.accept(this);
        if (expression.arguments.length > 0) {
            this.builder.append("(");
            for (DBSPExpression arg: expression.arguments) {
                arg.accept(this);
                this.builder.append(", ");
            }
            this.builder.append(")");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPPath path) {
        boolean first = true;
        for (DBSPPathSegment segment: path.components) {
            if (!first)
                this.builder.append("::");
            first = false;
            segment.accept(this);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPSimplePathSegment segment) {
        builder.append(segment.identifier);
        if (segment.genericArgs.length > 0) {
            builder.append("::<");
            boolean first = true;
            for (DBSPType arg : segment.genericArgs) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                arg.accept(this);
            }
            this.builder.append(">");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPIdentifierPattern pattern) {
        this.builder.append(pattern.mutable ? "mut " : "")
                .append(pattern.identifier);
        return false;
    }

    @Override
    public boolean preorder(DBSPLiteralPattern pattern) {
        pattern.literal.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPRefPattern pattern) {
        this.builder.append("&")
                .append(pattern.mutable ? "mut " : "");
        pattern.pattern.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPTuplePattern pattern) {
        this.builder.append("(");
        for (DBSPPattern field: pattern.fields) {
            field.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleStructPattern pattern) {
        this.builder.append(pattern.path)
                .append("(");
        for (DBSPPattern field: pattern.arguments) {
            field.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        return false;
    }

    @Override
    public boolean preorder(DBSPWildcardPattern pattern) {
        this.builder.append("_");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeAny type) {
        this.builder.append("_");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeFunction type) {
        this.builder.append("_");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeDouble type) {
        type.wrapOption(this.builder,"F64");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeFloat type) {
        type.wrapOption(this.builder,"F32");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeBool type) {
        type.wrapOption(this.builder,"bool");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeInteger type) {
        type.wrapOption(this.builder, type.getRustString());
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeISize type) {
        type.wrapOption(this.builder, "isize");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeUSize type) {
        type.wrapOption(this.builder, "usize");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeString type) {
        type.wrapOption(this.builder, "String");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeNull type) {
        type.wrapOption(this.builder, "()");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeRawTuple type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("(");
        for (DBSPType ftype: type.tupFields) {
            ftype.accept(this);
            this.builder.append(", ");
        }
        this.builder.append(")");
        if (type.mayBeNull)
            this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeRef type) {
        this.builder.append("&")
                .append(type.mutable ? "mut " : "");
        type.type.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeStream type) {
        this.builder.append("Stream<")
                .append("_, "); // Circuit type
        type.elementType.accept(this);
        this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeStruct.Field field) {
        this.builder.append(field.name)
                .append(": ");
        field.type.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeStruct type) {
        this.builder.append(type.name)
                .append("{");
        boolean first = true;
        for (DBSPTypeStruct.Field field: type.args) {
            if (!first)
                this.builder.append(", ");
            first = false;
            field.accept(this);
        }
        this.builder.append("}");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeTuple type) {
        if (type.tupFields.length == 0) {
            this.builder.append("()");
            return false;
        }
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append("Tuple")
                .append(type.tupFields.length)
                .append("<");
        boolean first = true;
        for (DBSPType ftype: type.tupFields) {
            if (!first)
                this.builder.append(", ");
            first = false;
            ftype.accept(this);
        }
        this.builder.append(">");
        if (type.mayBeNull)
            this.builder.append(">");
        return false;
    }

    @Override
    public boolean preorder(DBSPTypeUser type) {
        if (type.mayBeNull)
            this.builder.append("Option<");
        this.builder.append(type.name);
        if (type.typeArgs.length > 0) {
            this.builder.append("<");
            boolean first = true;
            for (DBSPType ftype: type.typeArgs) {
                if (!first)
                    this.builder.append(", ");
                first = false;
                ftype.accept(this);
            }
            this.builder.append(">");
        }
        if (type.mayBeNull)
            this.builder.append(">");
        return false;
    }

    public static String toRustString(IDBSPNode node) {
        IndentStringBuilder builder = new IndentStringBuilder();
        ToRustVisitor visitor = new ToRustVisitor(builder);
        node.accept(visitor);
        return builder.toString();
    }
}
