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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
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

    private final CalciteCompiler calciteCompiler;
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

    public CalciteToDBSPCompiler(CalciteCompiler calciteCompiler) {
        this.circuit = null;
        this.calciteCompiler = calciteCompiler;
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

    /**
     * Result returned by the createFoldingFunction below.
     */
    static class FoldingDescription {
        /**
         * Expression that represents a DBSP Fold object that computes the desired aggregate.
         */
        public final DBSPExpression fold;
        /**
         * In DBSP the result of aggregating an empty collection is always an empty collection.
         * But in SQL this is not always true: this is the result that should be returned for an
         * empty input.
         */
        public final DBSPExpression defaultZero;

        FoldingDescription(DBSPExpression fold, DBSPExpression defaultZero) {
            this.fold = fold;
            this.defaultZero = defaultZero;
        }
    }

    /**
     * Helper function for creating aggregates.
     * @param aggregates   Aggregates to implement.
     * @param groupCount   Number of groupBy variables.
     * @param inputRowType Type of input row.
     * @param resultType   Type of result produced.
     */
    public FoldingDescription createFoldingFunction(
            List<AggregateCall> aggregates, DBSPTypeTuple resultType,
            DBSPType inputRowType, int groupCount) {
        assert this.circuit != null;
        DBSPVariableReference rowVar = new DBSPVariableReference("v", new DBSPTypeRef(inputRowType));
        int aggIndex = 0;
        int parts = aggregates.size();
        DBSPExpression[] zeros = new DBSPExpression[parts];
        DBSPExpression[] increments = new DBSPExpression[parts];
        DBSPExpression[] posts = new DBSPExpression[parts];
        DBSPExpression[] defaultZeros = new DBSPExpression[parts];

        DBSPType[] accumulatorTypes = new DBSPType[parts];
        for (AggregateCall call: aggregates) {
            DBSPType resultFieldType = resultType.getFieldType(aggIndex + groupCount);
            AggregateCompiler compiler = new AggregateCompiler(call, resultFieldType, rowVar);
            AggregateCompiler.FoldDescription folder = compiler.compile();
            DBSPExpression zero = this.circuit.declareLocal("zero", folder.zero)
                    .getVarReference();
            zeros[aggIndex] = zero;
            DBSPExpression increment = this.circuit.declareLocal("inc", folder.increment)
                    .getVarReference();
            increments[aggIndex] = increment;
            accumulatorTypes[aggIndex] = folder.increment.getResultType();
            DBSPExpression post = this.circuit.declareLocal("post",
                    folder.postprocess != null ? folder.postprocess :
                            new DBSPVariableReference("identity", DBSPTypeAny.instance))
                    .getVarReference();
            posts[aggIndex] = post;
            defaultZeros[aggIndex] = folder.emptySetResult;
            aggIndex++;
        }

        DBSPExpression zero = this.circuit.declareLocal("zero", new DBSPRawTupleExpression(zeros))
                .getVarReference();
        DBSPType accumulatorType = new DBSPTypeRawTuple(accumulatorTypes);
        DBSPVariableReference accumulator = new DBSPVariableReference("a",
                new DBSPTypeRef(accumulatorType, true));
        DBSPVariableReference postAccum = new DBSPVariableReference("a", accumulatorType);
        for (int i = 0; i < increments.length; i++) {
            DBSPExpression accumField = new DBSPFieldExpression(accumulator, i);
            increments[i] = new DBSPApplyExpression(increments[i],
                    accumField, rowVar, weight);
            DBSPExpression postAccumField = new DBSPFieldExpression(postAccum, i);
            posts[i] = new DBSPApplyExpression(posts[i], postAccumField);
        }
        DBSPAssignmentExpression accumBody = new DBSPAssignmentExpression(
                new DBSPDerefExpression(accumulator), new DBSPRawTupleExpression(increments));
        DBSPExpression accumFunction = new DBSPClosureExpression(accumBody,
                accumulator.asParameter(), rowVar.asParameter(), weight.asParameter());
        DBSPExpression increment = this.circuit.declareLocal("increment", accumFunction)
                .getVarReference();
        DBSPClosureExpression postClosure = new DBSPClosureExpression(new DBSPTupleExpression(posts), postAccum.asParameter());
        DBSPExpression post = this.circuit.declareLocal("post", postClosure).getVarReference();

        DBSPExpression constructor = new DBSPPathExpression(DBSPTypeAny.instance, "Fold", "with_output");
        DBSPExpression folder = new DBSPApplyExpression(constructor, zero, increment, post);
        return new FoldingDescription(this.circuit.declareLocal("folder", folder).getVarReference(),
                new DBSPTupleExpression(defaultZeros));
    }

    public void visitAggregate(LogicalAggregate aggregate) {
        assert this.circuit != null;

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
        DBSPType type = this.convertType(aggregate.getRowType());
        DBSPTypeTuple tuple = type.to(DBSPTypeTuple.class);
        RelNode input = aggregate.getInput();
        DBSPOperator opInput = this.getOperator(input);
        DBSPType inputRowType = this.convertType(input.getRowType());
        List<AggregateCall> aggregates = aggregate.getAggCallList();
        DBSPVariableReference t = new DBSPVariableReference("t", inputRowType);

        if (!aggregates.isEmpty()) {
            if (aggregate.getGroupType() != Aggregate.Group.SIMPLE)
                throw new Unimplemented(aggregate);
            DBSPExpression[] groups = new DBSPExpression[aggregate.getGroupCount()];
            int next = 0;
            for (int index: aggregate.getGroupSet()) {
                DBSPExpression field = new DBSPFieldExpression(t, index);
                groups[next] = field;
                next++;
            }
            DBSPExpression keyExpression = new DBSPRawTupleExpression(groups);
            DBSPType[] aggTypes = Utilities.arraySlice(tuple.tupFields, aggregate.getGroupCount());
            DBSPTypeTuple aggType = new DBSPTypeTuple(aggTypes);

            DBSPExpression groupKeys = new DBSPClosureExpression(
                    new DBSPRawTupleExpression(
                            keyExpression,
                            DBSPTupleExpression.flatten(t)),
                    t.asRefParameter());
            DBSPIndexOperator index = new DBSPIndexOperator(
                    aggregate, this.circuit.declareLocal("index", groupKeys).getVarReference(),
                    keyExpression.getNonVoidType(), inputRowType, opInput);
            this.circuit.addOperator(index);
            DBSPType groupType = keyExpression.getNonVoidType();
            FoldingDescription fd = this.createFoldingFunction(aggregates, tuple, inputRowType, aggregate.getGroupCount());
            // The aggregate operator will not return a stream of type aggType, but a stream
            // with a type given by fd.defaultZero.
            DBSPTypeTuple typeFromAggregate = fd.defaultZero.getNonVoidType().to(DBSPTypeTuple.class);
            DBSPAggregateOperator agg = new DBSPAggregateOperator(aggregate, fd.fold, groupType,
                    /*aggType*/ typeFromAggregate, index);

            // Flatten the resulting set
            DBSPVariableReference kResult = new DBSPVariableReference("k", new DBSPTypeRef(groupType));
            DBSPVariableReference vResult = new DBSPVariableReference("v", new DBSPTypeRef(typeFromAggregate));
            DBSPExpression[] flattenFields = new DBSPExpression[aggregate.getGroupCount() + aggType.size()];
            for (int i = 0; i < aggregate.getGroupCount(); i++)
                flattenFields[i] = new DBSPFieldExpression(kResult, i);
            for (int i = 0; i < aggType.size(); i++) {
                DBSPExpression flattenField = new DBSPFieldExpression(vResult, i);
                // Here we correct from the type produced by the Folder (typeFromAggregate) to the
                // actual expected type aggType (which is the tuple of aggTypes).
                flattenFields[aggregate.getGroupCount() + i] = ExpressionCompiler.makeCast(flattenField, aggTypes[i]);
            }
            DBSPExpression mapper = new DBSPClosureExpression(new DBSPTupleExpression(flattenFields),
                    new DBSPClosureExpression.Parameter(kResult, vResult));
            this.circuit.addOperator(agg);
            DBSPMapOperator map = new DBSPMapOperator(aggregate,
                    this.circuit.declareLocal("flatten", mapper).getVarReference(), tuple, agg);
            if (aggregate.getGroupCount() == 0) {
                // This almost works, but we have a problem with empty input collections
                // for aggregates without grouping.
                // aggregate_stream returns empty collections for empty input collections -- the fold
                // method is never invoked.
                // So we need to do some postprocessing step for this case.
                // The current result is a zset like {}/{c->1}: either the empty set (for an empty input)
                // or the correct count with a weight of 1.
                // We need to produce {z->1}/{c->1}, where z is the actual zero of the fold above.
                // For this we synthesize the following graph:
                // {}/{c->1}------------------------
                //    | map (|x| x -> z}           |
                // {}/{z->1}                       |
                //    | -                          |
                // {} {z->-1}   {z->1} (constant)  |
                //          \  /                  /
                //           +                   /
                //         {z->1}/{}  -----------
                //                 \ /
                //                  +
                //              {z->1}/{c->1}
                this.circuit.addOperator(map);
                DBSPVariableReference _t = new DBSPVariableReference("_t", DBSPTypeAny.instance);
                DBSPExpression toZero = new DBSPClosureExpression(fd.defaultZero, _t.asParameter());
                DBSPOperator map1 = new DBSPMapOperator(aggregate, toZero, type, map);
                this.circuit.addOperator(map1);
                DBSPOperator neg = new DBSPNegateOperator(aggregate, type, map1);
                this.circuit.addOperator(neg);
                DBSPOperator constant = new DBSPConstantOperator(
                        aggregate, new DBSPZSetLiteral(weightType, fd.defaultZero));
                this.circuit.addOperator(constant);
                DBSPOperator sum = new DBSPSumOperator(aggregate, type, Linq.list(constant, neg, map));
                this.assignOperator(aggregate, sum);
            } else {
                this.assignOperator(aggregate, map);
            }
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
        assert this.circuit != null;
        RelNode input = project.getInput();
        DBSPOperator opInput = this.getOperator(input);
        DBSPType outputType = this.convertType(project.getRowType());
        DBSPTypeTuple tuple = outputType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType());
        DBSPVariableReference row = new DBSPVariableReference("t", inputType);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(row, this.calciteCompiler.getRexBuilder());

        List<DBSPExpression> resultColumns = new ArrayList<>();
        int index = 0;
        for (RexNode column : project.getProjects()) {
            DBSPExpression exp = expressionCompiler.compile(column);
            DBSPType expectedType = tuple.getFieldType(index);
            if (!exp.getNonVoidType().same(expectedType)) {
                // Calcite's optimizations do not preserve types!
                exp = ExpressionCompiler.makeCast(exp, expectedType);
            }
            resultColumns.add(exp);
            index++;
        }
        DBSPExpression exp = new DBSPTupleExpression(project, outputType, resultColumns);
        DBSPExpression closure = new DBSPClosureExpression(project, exp, row.asRefParameter());
        DBSPExpression mapFunc = this.circuit.declareLocal("map", closure).getVarReference();
        DBSPMapOperator op = new DBSPMapOperator(project, mapFunc, outputType, opInput);
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    @SuppressWarnings("DuplicatedCode")
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
        assert this.circuit != null;
        DBSPType type = this.convertType(filter.getRowType());
        DBSPVariableReference t = new DBSPVariableReference("t", type);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.calciteCompiler.getRexBuilder());
        DBSPExpression condition = expressionCompiler.compile(filter.getCondition());
        condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
        condition = new DBSPClosureExpression(filter.getCondition(), condition, t.asRefParameter());
        DBSPOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(
                filter, this.circuit.declareLocal("cond", condition).getVarReference(), type, input);
        this.assignOperator(filter, fop);
    }

    private void visitJoin(LogicalJoin join) {
        assert this.circuit != null;
        DBSPType type = this.convertType(join.getRowType());
        if (join.getInputs().size() != 2)
            throw new TranslationException("Unexpected join with " + join.getInputs().size() + " inputs", join);
        DBSPOperator left = this.getOperator(join.getInput(0));
        DBSPOperator right = this.getOperator(join.getInput(1));
        DBSPType leftElementType = left.getNonVoidType().to(DBSPTypeZSet.class).elementType;
        DBSPType rightElementType = right.getNonVoidType().to(DBSPTypeZSet.class).elementType;
        DBSPVariableReference l = new DBSPVariableReference("l", new DBSPTypeRef(leftElementType));
        DBSPVariableReference r = new DBSPVariableReference("r", new DBSPTypeRef(rightElementType));
        // TODO: use key information
        DBSPVariableReference k = new DBSPVariableReference("k", DBSPTypeRawTuple.emptyTupleType);

        DBSPExpression toEmptyLeft = new DBSPClosureExpression(
                new DBSPRawTupleExpression(
                        DBSPTupleExpression.emptyTuple,
                        DBSPTupleExpression.flatten(l)),
                l.asParameter());
        DBSPIndexOperator lindex = new DBSPIndexOperator(
                join, this.circuit.declareLocal("index", toEmptyLeft).getVarReference(),
                DBSPTypeTuple.emptyTupleType, leftElementType, left);
        Objects.requireNonNull(this.circuit).addOperator(lindex);

        DBSPExpression toEmptyRight = new DBSPClosureExpression(
                new DBSPRawTupleExpression(
                        DBSPTupleExpression.emptyTuple,
                        DBSPTupleExpression.flatten(r)),
                r.asParameter());
        DBSPIndexOperator rIndex = new DBSPIndexOperator(
                join, this.circuit.declareLocal("index", toEmptyRight).getVarReference(),
                DBSPTypeTuple.emptyTupleType, rightElementType, right);
        this.circuit.addOperator(rIndex);

        DBSPClosureExpression makePairs = new DBSPClosureExpression(null,
                DBSPTupleExpression.flatten(l, r),
                k.asRefParameter(), l.asParameter(), r.asParameter());
        DBSPCartesianOperator cart = new DBSPCartesianOperator(join, type, makePairs, lindex, rIndex);
        this.circuit.addOperator(cart);
        DBSPVariableReference t = new DBSPVariableReference("t", makePairs.getNonVoidType());
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.calciteCompiler.getRexBuilder());
        DBSPExpression condition = expressionCompiler.compile(join.getCondition());
        if (condition.getNonVoidType().mayBeNull) {
            condition = new DBSPApplyExpression("wrap_bool", condition.getNonVoidType(), condition);
        }
        condition = new DBSPClosureExpression(join.getCondition(), condition, t.asParameter());
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

        public boolean prepared() {
            return this.tableType != null;
        }
    }

    final DMLTranslation dmTranslation = new DMLTranslation();

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression.
     * This can be invoked by a DDM statement, or by a SQL query that computes a constant result.
     */
    public void visitLogicalValues(LogicalValues values) {
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, this.calciteCompiler.getRexBuilder());
        DBSPTypeTuple sourceType = this.convertType(values.getRowType()).to(DBSPTypeTuple.class);
        boolean ddmTranslation = this.dmTranslation.prepared();

        DBSPTypeTuple resultType;
        if (ddmTranslation) {
            resultType = this.dmTranslation
                    .getResultType()
                    .to(DBSPTypeZSet.class)
                    .elementType
                    .to(DBSPTypeTuple.class);
            if (sourceType.size() != resultType.size())
                throw new TranslationException("Expected a tuple with " + resultType.size() +
                        " values but got " + values, values);
        } else {
            resultType = sourceType;
        }

        DBSPZSetLiteral result = new DBSPZSetLiteral(TypeCompiler.makeZSet(resultType));
        for (List<RexLiteral> t : values.getTuples()) {
            List<DBSPExpression> exprs = new ArrayList<>();
            if (t.size() != sourceType.size())
                throw new TranslationException("Expected a tuple with " + sourceType.size() +
                        " values but got " + t, values);
            int i = 0;
            for (RexLiteral rl : t) {
                DBSPExpression expr = expressionCompiler.compile(rl);
                DBSPType resultFieldType = resultType.tupFields[i];
                if (!expr.getNonVoidType().same(resultFieldType)) {
                    DBSPExpression cast = ExpressionCompiler.makeCast(expr, resultFieldType);
                    exprs.add(cast);
                } else {
                    exprs.add(expr);
                }
                i++;
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(t, resultType, exprs);
            result.add(expression);
        }

        if (ddmTranslation) {
            this.dmTranslation.setResult(result);
        } else {
            DBSPOperator constant = new DBSPConstantOperator(values, result);
            this.assignOperator(values, constant);
        }
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
            // Dump plan
            if (this.debug)
                System.out.println(
                        RelOptUtil.dumpPlan("[Logical plan]", rel,
                                SqlExplainFormat.TEXT,
                                SqlExplainLevel.NON_COST_ATTRIBUTES));
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
