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

package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.*;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This visitor generate a Rust implementation of the program.
 */
public class ToRustVisitor extends CircuitVisitor {
    protected final IndentStream builder;
    public final InnerVisitor innerVisitor;

    public ToRustVisitor(IndentStream builder) {
        super(true);
        this.builder = builder;
        this.innerVisitor = new ToRustInnerVisitor(builder);
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

    void processNode(IDBSPNode node) {
        DBSPOperator op = node.as(DBSPOperator.class);
        if (op != null)
            this.generateOperator(op);
        IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
        if (inner != null) {
            inner.accept(this.innerVisitor);
            this.builder.newline();
        }
    }

    void generateOperator(DBSPOperator operator) {
        if (operator.getNode() != null) {
            String str = operator.getNode().toString();
            this.writeComments(str);
        }
        operator.accept(this);
        this.builder.newline();
    }

    public void generateBody(DBSPPartialCircuit circuit) {
        this.builder.append("let root = dbsp::RootCircuit::build(|circuit| {")
                .increase();
        for (IDBSPNode node : circuit.getCode())
            this.processNode(node);
        this.builder.decrease()
                .append("})")
                .append(".unwrap();")
                .newline();
    }

    @Override
    public boolean preorder(DBSPCircuit circuit) {
        this.builder.append("fn ")
                .append(circuit.name);
        circuit.circuit.accept(this);
        return false;
    }

    @Override
    public boolean preorder(DBSPPartialCircuit circuit) {
        // function prototype:
        // fn name() -> impl FnMut(T0, T1) -> (O0, O1) {
        boolean first = true;
        this.builder.append("() -> impl FnMut(");
        for (DBSPOperator i : circuit.inputOperators) {
            if (!first)
                this.builder.append(",");
            first = false;
            i.getNonVoidType().accept(this.innerVisitor);
        }
        this.builder.append(") -> ");
        DBSPTypeRawTuple tuple = new DBSPTypeRawTuple(null, Linq.map(circuit.outputOperators, DBSPOperator::getNonVoidType));
        tuple.accept(this.innerVisitor);
        this.builder.append(" {").increase();
        // For each input and output operator a corresponding Rc cell
        for (DBSPOperator i : circuit.inputOperators)
            this.genRcCell(i);

        for (DBSPOperator o : circuit.outputOperators)
            this.genRcCell(o);

        // Circuit body
        this.generateBody(circuit);

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
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(" = ")
                .append("circuit.add_source(")
                .append(operator.outputName)
                .append(");");
        return false;
    }

    @Override
    public boolean preorder(DBSPIncrementalDistinctOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ")
                .append(operator.input().getName())
                .append(".")
                .append(operator.operation)
                .append("();");
        return false;
    }

    @Override
    public boolean preorder(DBSPSinkOperator operator) {
        this.writeComments(operator.query);
        this.writeComments(operator)
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
        this.writeComments(operator)
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

    /**
     * Creates a DBSP Fold object from an Aggregate.
     */
    DBSPExpression createAggregator(@Nullable DBSPExpression function, @Nullable DBSPAggregate aggregate) {
        if (function != null)
            return function;
        Objects.requireNonNull(aggregate);
        // Example for a pair of count+sum aggregations:
        // let zero_count: isize = 0;
        // let inc_count = |acc: isize, v: &usize, w: isize| -> isize { acc + 1 * w };
        // let zero_sum: isize = 0;
        // let inc_sum = |ac: isize, v: &usize, w: isize| -> isize { acc + (*v as isize) * w) }
        // let zero = (zero_count, inc_count);
        // let inc = |acc: &mut (isize, isize), v: &usize, w: isize| {
        //     *acc = (inc_count(acc.0, v, w), inc_sum(acc.1, v, w))
        // }
        // let post_count = identity;
        // let post_sum = identity;
        // let post =  move |a: (i32, i32), | -> Tuple2<_, _> {
        //            Tuple2::new(post_count(a.0), post_sum(a.1)) };
        // let fold = Fold::with_output((zero_count, zero_sum), inc, post);
        // let count_sum = input.aggregate(fold);
        // let result = count_sum.map(|k,v|: (&(), &Tuple1<isize>|) { *v };
        int parts = aggregate.components.size();
        DBSPExpression[] zeros = new DBSPExpression[parts];
        DBSPExpression[] increments = new DBSPExpression[parts];
        DBSPExpression[] posts = new DBSPExpression[parts];
        DBSPType[] accumulatorTypes = new DBSPType[parts];
        DBSPType[] semigroups = new DBSPType[parts];
        for (int i = 0; i < parts; i++) {
            DBSPAggregate.Implementation implementation = aggregate.components.get(i);
            DBSPType incType = implementation.increment.getResultType();
            zeros[i] = implementation.zero;
            increments[i] = implementation.increment;
            accumulatorTypes[i] = Objects.requireNonNull(incType);
            semigroups[i] = implementation.semigroup;
            DBSPExpression identity = new DBSPTypeFunction(incType, incType).path(
                    new DBSPPath(new DBSPSimplePathSegment("identity", incType)));
            posts[i] = implementation.postProcess != null ? implementation.postProcess : identity;
        }

        DBSPTypeRawTuple accumulatorType = new DBSPTypeRawTuple(accumulatorTypes);
        DBSPVariablePath accumulator = accumulatorType.ref(true).var("a");
        DBSPVariablePath postAccumulator = accumulatorType.var("a");
        for (int i = 0; i < parts; i++) {
            DBSPExpression accumulatorField = accumulator.field(i);
            increments[i] = new DBSPApplyExpression(increments[i],
                    accumulatorField, aggregate.rowVar, CalciteToDBSPCompiler.weight);
            DBSPExpression postAccumulatorField = postAccumulator.field(i);
            posts[i] = new DBSPApplyExpression(posts[i], postAccumulatorField);
        }
        DBSPAssignmentExpression accumulatorBody = new DBSPAssignmentExpression(
                accumulator.deref(), new DBSPRawTupleExpression(increments));
        DBSPExpression accumFunction = accumulatorBody.closure(
                accumulator.asParameter(), aggregate.rowVar.asParameter(),
                CalciteToDBSPCompiler.weight.asParameter());
        DBSPClosureExpression postClosure = new DBSPTupleExpression(posts).closure(postAccumulator.asParameter());
        DBSPExpression constructor = DBSPTypeAny.INSTANCE.path(
                new DBSPPath(
                        new DBSPSimplePathSegment("Fold",
                                DBSPTypeAny.INSTANCE,
                                new DBSPTypeSemigroup(semigroups, accumulatorTypes),
                                DBSPTypeAny.INSTANCE,
                                DBSPTypeAny.INSTANCE),
                        new DBSPSimplePathSegment("with_output")));
        return new DBSPApplyExpression(constructor,
                new DBSPRawTupleExpression(zeros),
                accumFunction, postClosure);
    }

    @Override
    public boolean preorder(DBSPWindowAggregateOperator operator) {
        // We generate two DBSP operator calls: partitioned_rolling_aggregate
        // and map_index
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        String tmp = new NameGen("stream").toString();
        this.writeComments(operator)
                .append("let ")
                .append(tmp)
                .append(" = ")
                .append(operator.input().getName())
                .append(".partitioned_rolling_aggregate(");
        DBSPExpression aggregator = this.createAggregator(operator.function, operator.aggregate);
        aggregator.accept(this.innerVisitor);
        builder.append(", ");
        operator.window.accept(this.innerVisitor);
        builder.append(");")
                .newline();

        this.builder.append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        builder.append(" = " )
                .append(tmp)
                .append(".map_index(|(key, (ts, agg))| { ((key.clone(), *ts), agg.unwrap_or_default())});");
        return false;
    }

    @Override
    public boolean preorder(DBSPAggregateOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        builder.append(operator.input().getName())
                .append(".");
        builder.append(operator.operation)
                .append("(");
        DBSPExpression aggregator = this.createAggregator(operator.function, operator.aggregate);
        aggregator.accept(this.innerVisitor);
        builder.append(");");
        return false;
    }

    @Override
    public boolean preorder(DBSPIncrementalAggregateOperator operator) {
        DBSPType streamType = new DBSPTypeStream(operator.outputType);
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        streamType.accept(this.innerVisitor);
        this.builder.append(" = ");
        builder.append(operator.input().getName())
                    .append(".");
        builder.append(operator.operation)
                .append("(");
        DBSPExpression aggregator = this.createAggregator(operator.function, operator.aggregate);
        aggregator.accept(this.innerVisitor);
        builder.append(");");
        return false;
    }

    @Override
    public boolean preorder(DBSPSumOperator operator) {
        this.writeComments(operator)
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

    IIndentStream writeComments(@Nullable String comment) {
        if (comment == null)
            return this.builder;
        String[] parts = comment.split("\n");
        parts = Linq.map(parts, p -> "// " + p, String.class);
        return this.builder.intercalate("\n", parts);
    }

     IIndentStream writeComments(DBSPOperator operator) {
        return this.writeComments(operator.getClass().getSimpleName() + " " + operator.id +
                (operator.comment != null ? "\n" + operator.comment : ""));
    }

    @Override
    public boolean preorder(DBSPIncrementalJoinOperator operator) {
        this.writeComments(operator)
                .append("let ")
                .append(operator.getName())
                .append(": ");
        new DBSPTypeStream(operator.outputType).accept(this.innerVisitor);
        this.builder.append(" = ");
        if (!operator.inputs.isEmpty())
            this.builder.append(operator.inputs.get(0).getName())
                    .append(".");
        this.builder.append(operator.operation)
                .append("(&");
        this.builder.append(operator.inputs.get(1).getName());
        this.builder.append(", ");
        operator.getFunction().accept(this.innerVisitor);
        this.builder.append(");");
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

    public static String circuitToRustString(IDBSPOuterNode node) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToRustVisitor visitor = new ToRustVisitor(stream);
        node.accept(visitor);
        return builder.toString();
    }
}
