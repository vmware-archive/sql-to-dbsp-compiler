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

package org.dbsp.sqlCompiler.dbsp;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.DBSPTuplePattern;
import org.dbsp.sqlCompiler.dbsp.rust.type.*;
import org.dbsp.sqlCompiler.frontend.*;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

public class CalciteToDBSPCompiler extends RelVisitor {
    /**
     * Type of weight used in generated z-sets.
     */
    public static final DBSPType weightType = new DBSPTypeUser(null, "Weight", false);
    /**
     * Variable that refers to the weight of the row in the z-set.
     */
    public static final DBSPVariableReference weight = new DBSPVariableReference("w", CalciteToDBSPCompiler.weightType);

    static class Context {
        @Nullable
        final
        RelNode parent;
        final int inputNo;

        public Context(@Nullable RelNode parent, int inputNo) {
            this.parent = parent;
            this.inputNo = inputNo;
        }
    }

    @Nullable
    private DBSPCircuit circuit;
    final boolean debug = false;
    // The path in the IR tree used to reach the current node.
    final List<Context> stack;
    // Map an input or output name to the corresponding operator
    final Map<String, DBSPOperator> ioOperator;
    // Map a RelNode operator to its DBSP implementation.
    final Map<RelNode, DBSPOperator> nodeOperator;

    final TypeCompiler typeCompiler = new TypeCompiler();
    final ExpressionCompiler expressionCompiler = new ExpressionCompiler(true);

    public CalciteToDBSPCompiler() {
        this.circuit = null;
        this.stack = new ArrayList<>();
        this.ioOperator = new HashMap<>();
        this.nodeOperator = new HashMap<>();
    }

    private DBSPType convertType(RelDataType dt) {
        return this.typeCompiler.convertType(dt);
    }

    private DBSPType makeZSet(DBSPType type) {
        return TypeCompiler.makeZSet(type);
    }

    DBSPCircuit getProgram() {
        return Objects.requireNonNull(this.circuit);
    }

    <T> boolean visitIfMatches(RelNode node, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(node, clazz);
        if (value != null) {
            if (debug)
                System.out.println("Processing " + node);
            method.accept(value);
            return true;
        }
        return false;
    }

    public void visitAggregate(LogicalAggregate aggregate) {
        assert this.circuit != null;

        // A pair of count+sum aggregations:
        // let count_sum = |k: GroupKey, vw: &mut Vec<(Tuple, Weight)>| -> (GroupKey, ResultType) {
        //     let result: ResultType = vw.drain(..).fold(
        //                     (0, Default::default()),
        //                     |(r1, r2), (v, w)| (add_N_N(r1 + v.0.mul_by_weight(w), sum_N_N(r2, v.1.mul_by_weight(w))));
        //     (*k, result)
        // };
        // input.aggregate(count_sum);
        DBSPType type = this.convertType(aggregate.getRowType());
        DBSPTypeTuple tuple = type.to(DBSPTypeTuple.class);
        RelNode input = aggregate.getInput();
        DBSPOperator opInput = this.getOperator(input);
        DBSPType inputRowType = this.convertType(input.getRowType());
        List<AggregateCall> aggs = aggregate.getAggCallList();
        if (!aggs.isEmpty()) {
            // TODO: currently DBSP's aggregation interface requires grouping
            // by ().  This should be avoided if it is not necessary.
            DBSPExpression toEmpty = new DBSPClosureExpression(
                    null,
                    new DBSPRawTupleExpression(
                            DBSPTupleExpression.emptyTuple,
                            DBSPTupleExpression.flatten(
                                    new DBSPVariableReference("t", inputRowType))),
                    "t");
            DBSPIndexOperator index = new DBSPIndexOperator(
                    aggregate, toEmpty, DBSPTypeTuple.emptyTupleType, inputRowType, opInput);
            this.circuit.addOperator(index);
            DBSPVariableReference k = new DBSPVariableReference("k", DBSPTypeRawTuple.emptyTupleType);
            DBSPVariableReference vw = new DBSPVariableReference("vw", new DBSPTypeTuple(inputRowType, CalciteToDBSPCompiler.weightType));
            DBSPExpression drain = new DBSPApplyMethodExpression("drain", DBSPTypeAny.instance, vw,
                    new DBSPRangeExpression(null, null, false, DBSPTypeInteger.signed32));
            DBSPVariableReference v = new DBSPVariableReference("v", inputRowType);

            AggregateCompiler.Implementation[] impl = new AggregateCompiler.Implementation[aggs.size()];
            int aggIndex = 0;
            for (AggregateCall call: aggs) {
                DBSPType resultType = tuple.getFieldType(aggIndex);
                AggregateCompiler compiler = new AggregateCompiler(call, resultType, aggIndex, v);
                impl[aggIndex] = compiler.compile();
                aggIndex++;
            }

            DBSPExpression zero = new DBSPRawTupleExpression(Linq.map(impl, i -> i.zero, DBSPExpression.class));
            DBSPExpression aggBody = new DBSPRawTupleExpression(Linq.map(impl, i -> i.increment, DBSPExpression.class));
            DBSPClosureExpression.Parameter tupAccumulators = new DBSPClosureExpression.Parameter(
                    Linq.map(impl, i -> i.accumulator, DBSPVariableReference.class));

            DBSPExpression fold = new DBSPApplyMethodExpression(
                    "fold", DBSPTypeAny.instance, drain, zero,
                    new DBSPClosureExpression(null, aggBody,
                            tupAccumulators,
                            new DBSPClosureExpression.Parameter(
                                    new DBSPTuplePattern(v.asPattern(), weight.asPattern()), null)));
            // Must return a tuple. TODO: is not using the legal Rust grammar
            DBSPExpression tup1 = new DBSPApplyExpression(
                    new DBSPPathExpression(
                            type,tuple.toPath(), "from"),
                    DBSPTypeAny.instance, fold);
            DBSPExpression closure = new DBSPClosureExpression(aggregate, tup1,
                    new DBSPClosureExpression.Parameter(k.asPattern(), null),
                    new DBSPClosureExpression.Parameter(vw.asPattern(), null));
            DBSPAggregateOperator agg = new DBSPAggregateOperator(aggregate, closure, type, index);
            this.assignOperator(aggregate, agg);
        } else {
            DBSPOperator dist = new DBSPDistinctOperator(aggregate, type, opInput);
            this.assignOperator(aggregate, dist);
        }
    }

    public void visitScan(LogicalTableScan scan) {
        List<String> name = scan.getTable().getQualifiedName();
        String tableName = name.get(name.size() - 1);
        DBSPOperator op = Utilities.getExists(this.ioOperator, tableName);
        this.assignOperator(scan, op);
    }

    void assignOperator(RelNode rel, DBSPOperator op) {
        Utilities.putNew(this.nodeOperator, rel, op);
        if (!(op instanceof DBSPSourceOperator))
            // These are already added
            Objects.requireNonNull(this.circuit).addOperator(op);
    }

    DBSPOperator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    public void visitProject(LogicalProject project) {
        // LogicalProject is not really SQL project, it is rather map.
        RelNode input = project.getInput();
        DBSPOperator opInput = this.getOperator(input);
        DBSPType type = this.convertType(project.getRowType());
        List<DBSPExpression> resultColumns = new ArrayList<>();
        for (RexNode column : project.getProjects()) {
            DBSPExpression exp = this.expressionCompiler.compile(column);
            resultColumns.add(exp);
        }
        DBSPExpression exp = new DBSPTupleExpression(project, type, resultColumns);
        DBSPExpression closure = new DBSPClosureExpression(project, exp, "t");
        DBSPMapOperator op = new DBSPMapOperator(project, closure, type, opInput);
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    private void visitUnion(LogicalUnion union) {
        DBSPType type = this.convertType(union.getRowType());
        List<DBSPOperator> inputs = Linq.map(union.getInputs(), this::getOperator);
        DBSPSumOperator sum = new DBSPSumOperator(union, type, inputs);
        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            Objects.requireNonNull(this.circuit).addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(union, type, sum);
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        DBSPType type = this.convertType(minus.getRowType());
        boolean first = true;
        List<DBSPOperator> inputs = new ArrayList<>();
        for (RelNode input : minus.getInputs()) {
            DBSPOperator opInput = this.getOperator(input);
            if (!first) {
                DBSPNegateOperator neg = new DBSPNegateOperator(minus, type, opInput);
                Objects.requireNonNull(this.circuit).addOperator(neg);
                inputs.add(neg);
            } else {
                inputs.add(opInput);
            }
            first = false;
        }

        DBSPSumOperator sum = new DBSPSumOperator(minus, type, inputs);
        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            Objects.requireNonNull(this.circuit).addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(minus, type, sum);
            this.assignOperator(minus, d);
        }
    }

    public void visitFilter(LogicalFilter filter) {
        DBSPType type = this.convertType(filter.getRowType());
        DBSPExpression condition = this.expressionCompiler.compile(filter.getCondition());
        if (condition.getNonVoidType().mayBeNull) {
            condition = new DBSPApplyExpression("wrap_bool", condition.getNonVoidType(), condition);
        }
        condition = new DBSPClosureExpression(filter.getCondition(), condition, "t");
        DBSPOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(filter, condition, type, input);
        this.assignOperator(filter, fop);
    }

    private void visitJoin(LogicalJoin join) {
        DBSPType type = this.convertType(join.getRowType());
        if (join.getInputs().size() != 2)
            throw new TranslationException("Unexpected join with " + join.getInputs().size() + " inputs", join);
        DBSPOperator left = this.getOperator(join.getInput(0));
        DBSPOperator right = this.getOperator(join.getInput(1));
        DBSPType leftElementType = left.getNonVoidType().to(DBSPTypeZSet.class).elementType;
        DBSPType rightElementType = right.getNonVoidType().to(DBSPTypeZSet.class).elementType;

        DBSPExpression toEmptyLeft = new DBSPClosureExpression(
                null,
                new DBSPRawTupleExpression(
                        DBSPTupleExpression.emptyTuple,
                        DBSPTupleExpression.flatten(
                                new DBSPVariableReference("t", leftElementType))),
                "t");
        DBSPIndexOperator lindex = new DBSPIndexOperator(
                join, toEmptyLeft, DBSPTypeTuple.emptyTupleType, leftElementType, left);
        Objects.requireNonNull(this.circuit).addOperator(lindex);
        DBSPExpression toEmptyRight = new DBSPClosureExpression(
                null,
                new DBSPRawTupleExpression(
                        DBSPTupleExpression.emptyTuple,
                        DBSPTupleExpression.flatten(
                                new DBSPVariableReference("t", rightElementType))),
                        "t");
        DBSPIndexOperator rIndex = new DBSPIndexOperator(
                join, toEmptyRight, DBSPTypeTuple.emptyTupleType, rightElementType, right);
        this.circuit.addOperator(rIndex);

        DBSPExpression l = new DBSPVariableReference("l", leftElementType);
        DBSPExpression r = new DBSPVariableReference("r", rightElementType);
        DBSPExpression makePairs = new DBSPClosureExpression(null,
                DBSPTupleExpression.flatten(l, r),
                "k", "l", "r");
        DBSPCartesianOperator cart = new DBSPCartesianOperator(join, type, makePairs, lindex, rIndex);
        this.circuit.addOperator(cart);
        DBSPExpression condition = this.expressionCompiler.compile(join.getCondition());
        if (condition.getNonVoidType().mayBeNull) {
            condition = new DBSPApplyExpression("wrap_bool", condition.getNonVoidType(), condition);
        }
        condition = new DBSPClosureExpression(join.getCondition(), condition, "t");
        DBSPFilterOperator fop = new DBSPFilterOperator(join, condition, type, cart);
        this.assignOperator(join, fop);
    }

    /**
     * Information used to translate INSERT or DELETE SQL statements
     */
    static class DMLTranslation {
        /**
         * Type of the table that is being inserted into.
         */
        @Nullable
        public DBSPType tableType;
        /**
         * Translation result.
         */
        @Nullable
        private DBSPZSetLiteral logicalValueTranslation;

        DMLTranslation() {
            this.tableType = null;
            this.logicalValueTranslation = null;
        }

        void prepare(DBSPType type) {
            this.logicalValueTranslation = null;
            this.tableType = type;
        }

        public DBSPZSetLiteral getTranslation() {
            return Objects.requireNonNull(this.logicalValueTranslation);
        }

        public DBSPType getResultType() {
            return Objects.requireNonNull(this.tableType);
        }

        public void setResult(DBSPZSetLiteral literal) {
            if (this.logicalValueTranslation != null)
                throw new RuntimeException("Overwriting logical value translation");
            this.logicalValueTranslation = literal;
        }
    }

    DMLTranslation dmTranslation = new DMLTranslation();

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression
     */
    public void visitLogicalValues(LogicalValues values) {
        DBSPTypeTuple sourceType = this.convertType(values.getRowType()).to(DBSPTypeTuple.class);
        DBSPTypeTuple resultType = this.dmTranslation
                .getResultType()
                .to(DBSPTypeZSet.class)
                .elementType
                .to(DBSPTypeTuple.class);
        if (sourceType.size() != resultType.size())
            throw new TranslationException("Expected a tuple with " + resultType.size() +
                    " values but got " + values, values);

        DBSPZSetLiteral result = new DBSPZSetLiteral(TypeCompiler.makeZSet(resultType));
        for (List<RexLiteral> t: values.getTuples()) {
            List<DBSPExpression> exprs = new ArrayList<>();
            if (t.size() != sourceType.size())
                throw new TranslationException("Expected a tuple with " + sourceType.size() +
                        " values but got " + t, values);
            int i = 0;
            for (RexLiteral rl : t) {
                DBSPExpression expr = this.expressionCompiler.compile(rl);
                DBSPType resultFieldType = resultType.tupArgs[i];
                if (!expr.getNonVoidType().same(resultFieldType)) {
                    DBSPCastExpression cast = new DBSPCastExpression(values, resultFieldType, expr);
                    exprs.add(cast);
                } else {
                    exprs.add(expr);
                }
                i++;
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(t, resultType, exprs);
            result.add(expression);
        }
        this.dmTranslation.setResult(result);
    }

    @Override public void visit(
            RelNode node, int ordinal,
            @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
        stack.add(new Context(parent, ordinal));
        if (debug)
            System.out.println("Visiting " + node);
        // First process children
        super.visit(node, ordinal, parent);
        // Synthesize current node
        boolean success =
                this.visitIfMatches(node, LogicalTableScan.class, this::visitScan) ||
                this.visitIfMatches(node, LogicalProject.class, this::visitProject) ||
                this.visitIfMatches(node, LogicalUnion.class, this::visitUnion) ||
                this.visitIfMatches(node, LogicalMinus.class, this::visitMinus) ||
                this.visitIfMatches(node, LogicalFilter.class, this::visitFilter) ||
                this.visitIfMatches(node, LogicalValues.class, this::visitLogicalValues) ||
                this.visitIfMatches(node, LogicalAggregate.class, this::visitAggregate) ||
                this.visitIfMatches(node, LogicalJoin.class, this::visitJoin);
        if (!success)
            throw new Unimplemented(node);
        if (stack.size() == 0)
            throw new TranslationException("Empty stack", node);
        stack.remove(stack.size() - 1);
    }

    public DBSPCircuit compile(CalciteProgram program, String circuitName) {
        this.circuit = new DBSPCircuit(null, circuitName, program.query);
        for (TableDDL i: program.inputTables) {
            DBSPSourceOperator si = this.createInput(i);
            this.circuit.addOperator(si);
        }
        for (ViewDDL view: program.views) {
            RelNode rel = Objects.requireNonNull(view.compiled).rel;
            this.go(rel);
            // TODO: connect the result of the query compilation with
            // the fields of rel; for now we assume that these are 1/1
            DBSPOperator op = this.getOperator(rel);
            DBSPSinkOperator o = this.createOutput(view, op);
            this.circuit.addOperator(o);
        }
        return this.getProgram();
    }

    /**
     * Translate the specified statement and add to the given transaction.
     * @param transaction  A transaction that will contain the translation of 'statement'
     * @param statement A statement that updates a table.
     */
    public void extendTransaction(DBSPTransaction transaction, TableModifyStatement statement) {
        // The type of the data must be extracted from the modified table
        DBSPOperator op = Utilities.getExists(this.ioOperator, statement.table);
        this.dmTranslation.prepare(op.getNonVoidType());
        this.go(statement.rel);
        transaction.addSet(statement.table, this.dmTranslation.getTranslation());
    }

    private DBSPSourceOperator createInput(TableDDL i) {
        List<DBSPType> fields = new ArrayList<>();
        for (ColumnInfo col: i.columns) {
            DBSPType fType = this.convertType(col.type);
            fields.add(fType);
        }
        DBSPTypeTuple type = new DBSPTypeTuple(i, fields);
        DBSPSourceOperator result = new DBSPSourceOperator(i, this.makeZSet(type), i.name);
        return Utilities.putNew(this.ioOperator, i.name, result);
    }

    private DBSPSinkOperator createOutput(ViewDDL v, DBSPOperator source) {
        List<DBSPType> fields = new ArrayList<>();
        if (v.compiled == null)
            throw new TranslationException("Could not compile ", v.getNode());
        for (RelDataTypeField field: v.compiled.validatedRowType.getFieldList()) {
            DBSPType fType = this.convertType(field.getType());
            fields.add(fType);
        }
        DBSPTypeTuple type = new DBSPTypeTuple(v, fields);
        DBSPSinkOperator result = new DBSPSinkOperator(v, this.makeZSet(type), v.name, source);
        return Utilities.putNew(this.ioOperator, v.name, result);
    }
}
