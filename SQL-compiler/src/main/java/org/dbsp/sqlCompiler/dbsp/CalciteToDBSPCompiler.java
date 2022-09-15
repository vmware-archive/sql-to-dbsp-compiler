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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.*;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPPath;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.*;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.dbsp.rust.type.*;
import org.dbsp.sqlCompiler.frontend.*;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

public class CalciteToDBSPCompiler extends RelVisitor {
    final boolean debug = false;

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

    /**
     * A project does not apply distinct -- except it must if it
     * is a subquer.
     */
    private DBSPOperator getInputAs(RelNode input, boolean asMultiset) {
        assert this.circuit != null;
        DBSPOperator op = this.getOperator(input);
        if (op.isMultiset && !asMultiset) {
            op = new DBSPDistinctOperator(input, op);
            this.circuit.addOperator(op);
        }
        return op;
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

        DBSPExpression constructor = new DBSPPathExpression(DBSPTypeAny.instance,
                new DBSPPath("Fold", "with_output"));
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
        DBSPOperator opInput = this.getInputAs(input, true);
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
                    keyExpression.getNonVoidType(), inputRowType, false, opInput);
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
                DBSPOperator neg = new DBSPNegateOperator(aggregate, map1);
                this.circuit.addOperator(neg);
                DBSPOperator constant = new DBSPConstantOperator(
                        aggregate, new DBSPZSetLiteral(weightType, fd.defaultZero), false);
                this.circuit.addOperator(constant);
                DBSPOperator sum = new DBSPSumOperator(aggregate, Linq.list(constant, neg, map));
                this.assignOperator(aggregate, sum);
            } else {
                this.assignOperator(aggregate, map);
            }
        } else {
            DBSPOperator dist = new DBSPDistinctOperator(aggregate, opInput);
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
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType outputType = this.convertType(project.getRowType());
        DBSPTypeTuple tuple = outputType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType());
        DBSPVariableReference row = new DBSPVariableReference("t", inputType);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(row, this.calciteCompiler);

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
        DBSPExpression exp = new DBSPTupleExpression(project, resultColumns);
        DBSPExpression closure = new DBSPClosureExpression(project, exp, row.asRefParameter());
        DBSPExpression mapFunc = this.circuit.declareLocal("map", closure).getVarReference();
        DBSPMapOperator op = new DBSPMapOperator(project, mapFunc, outputType, opInput);
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    @SuppressWarnings("DuplicatedCode")
    private void visitUnion(LogicalUnion union) {
        assert this.circuit != null;
        List<DBSPOperator> inputs = Linq.map(union.getInputs(), this::getOperator);
        DBSPSumOperator sum = new DBSPSumOperator(union, inputs);
        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(union, sum);
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        boolean first = true;
        List<DBSPOperator> inputs = new ArrayList<>();
        for (RelNode input : minus.getInputs()) {
            DBSPOperator opInput = this.getInputAs(input, false);
            if (!first) {
                DBSPNegateOperator neg = new DBSPNegateOperator(minus, opInput);
                Objects.requireNonNull(this.circuit).addOperator(neg);
                inputs.add(neg);
            } else {
                inputs.add(opInput);
            }
            first = false;
        }

        DBSPSumOperator sum = new DBSPSumOperator(minus, inputs);
        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            Objects.requireNonNull(this.circuit).addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(minus, sum);
            this.assignOperator(minus, d);
        }
    }

    public void visitFilter(LogicalFilter filter) {
        assert this.circuit != null;
        DBSPType type = this.convertType(filter.getRowType());
        DBSPVariableReference t = new DBSPVariableReference("t", type);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.calciteCompiler);
        DBSPExpression condition = expressionCompiler.compile(filter.getCondition());
        condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
        condition = new DBSPClosureExpression(filter.getCondition(), condition, t.asRefParameter());
        DBSPOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(
                filter, this.circuit.declareLocal("cond", condition).getVarReference(), type, input);
        this.assignOperator(filter, fop);
    }

    private DBSPOperator filterNonNullKeys(LogicalJoin join,
            List<Integer> keyFields, DBSPOperator input) {
        assert this.circuit != null;
        DBSPTypeTuple rowType = input.getNonVoidType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        boolean shouldFilter = Linq.any(keyFields, i -> rowType.tupFields[i].mayBeNull);
        if (!shouldFilter) return input;

        DBSPVariableReference var = new DBSPVariableReference("r", rowType);
        List<DBSPMatchExpression.Case> cases = new ArrayList<>();
        DBSPPattern[] patterns = new DBSPPattern[rowType.size()];
        for (int i = 0; i < rowType.size(); i++) {
            if (keyFields.contains(i)) {
                patterns[i] = DBSPTupleStructPattern.somePattern(DBSPWildcardPattern.instance);
            } else {
                patterns[i] = DBSPWildcardPattern.instance;
            }
        }
        DBSPTupleStructPattern tup = new DBSPTupleStructPattern(rowType.toPath(), patterns);
        DBSPSomeExpression some = new DBSPSomeExpression(
                new DBSPApplyMethodExpression("clone", var.getNonVoidType(), var));
        cases.add(new DBSPMatchExpression.Case(tup, some));
        cases.add(new DBSPMatchExpression.Case(
                DBSPWildcardPattern.instance, new DBSPLiteral(some.getNonVoidType())));
        DBSPMatchExpression match = new DBSPMatchExpression(var, cases, some.getNonVoidType());
        DBSPClosureExpression filterFunc = new DBSPClosureExpression(match, var.asRefParameter());
        DBSPOperator filter = new DBSPFlatMapOperator(join, filterFunc, rowType, input);
        this.circuit.addOperator(filter);
        return filter;
    }

    private void visitJoin(LogicalJoin join) {
        assert this.circuit != null;
        JoinRelType joinType = join.getJoinType();
        if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI)
            throw new Unimplemented(join);

        DBSPTypeTuple resultType = this.convertType(join.getRowType()).to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new TranslationException("Unexpected join with " + join.getInputs().size() + " inputs", join);
        DBSPOperator left = this.getInputAs(join.getInput(0), false);
        DBSPOperator right = this.getInputAs(join.getInput(1), false);
        DBSPTypeTuple leftElementType = left.getNonVoidType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
                leftElementType.to(DBSPTypeTuple.class).size());
        JoinConditionAnalyzer.ConditionDecomposition decomposition = analyzer.analyze(join.getCondition());
        // If any key field is nullable we need to filter the inputs; this will make key columns non-nullable
        DBSPOperator filteredLeft = this.filterNonNullKeys(join, Linq.map(decomposition.comparisons, c -> c.leftColumn), left);
        DBSPOperator filteredRight = this.filterNonNullKeys(join, Linq.map(decomposition.comparisons, c -> c.rightColumn), right);

        leftElementType = filteredLeft.getNonVoidType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        DBSPTypeTuple rightElementType = filteredRight.getNonVoidType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        int leftColumns = leftElementType.size();
        int rightColumns = rightElementType.size();
        int totalColumns = leftColumns + rightColumns;
        DBSPTypeTuple leftResultType = resultType.slice(0, leftColumns);
        DBSPTypeTuple rightResultType = resultType.slice(leftColumns, leftColumns + rightColumns);

        DBSPVariableReference l = new DBSPVariableReference("l", new DBSPTypeRef(leftElementType));
        DBSPVariableReference r = new DBSPVariableReference("r", new DBSPTypeRef(rightElementType));
        DBSPTupleExpression lr = DBSPTupleExpression.flatten(l, r);
        DBSPVariableReference t = new DBSPVariableReference("t", Objects.requireNonNull(lr.getNonVoidType()));
        List<DBSPExpression> leftKeyFields = Linq.map(
                decomposition.comparisons,
                c -> new DBSPFieldExpression(l, c.leftColumn));
        leftKeyFields = Linq.map(leftKeyFields,
                c -> ExpressionCompiler.makeCast(c, c.getNonVoidType().setMayBeNull(false)));
        List<DBSPExpression> rightKeyFields = Linq.map(
                decomposition.comparisons,
                c -> new DBSPFieldExpression(r, c.rightColumn));
        rightKeyFields = Linq.map(rightKeyFields,
                c -> ExpressionCompiler.makeCast(c, c.getNonVoidType().setMayBeNull(false)));
        DBSPExpression leftKey = new DBSPRawTupleExpression(leftKeyFields);
        DBSPExpression rightKey = new DBSPRawTupleExpression(rightKeyFields);

        @Nullable
        RexNode leftOver = decomposition.getLeftOver();
        DBSPExpression condition = null;
        if (leftOver != null) {
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.calciteCompiler);
            condition = expressionCompiler.compile(leftOver);
            if (condition.getNonVoidType().mayBeNull) {
                condition = new DBSPApplyExpression("wrap_bool", condition.getNonVoidType().setMayBeNull(false), condition);
            }
            condition = new DBSPClosureExpression(join.getCondition(), condition, t.asRefParameter());
            condition = this.circuit.declareLocal("cond", condition).getVarReference();
        }
        DBSPVariableReference k = new DBSPVariableReference("k", leftKey.getNonVoidType());

        DBSPClosureExpression toLeftKey = new DBSPClosureExpression(
                new DBSPRawTupleExpression(leftKey, DBSPTupleExpression.flatten(l)),
                l.asParameter());
        DBSPIndexOperator lindex = new DBSPIndexOperator(
                join, this.circuit.declareLocal("index", toLeftKey).getVarReference(),
                leftKey.getNonVoidType(), leftElementType, false, filteredLeft);
        this.circuit.addOperator(lindex);

        DBSPClosureExpression toRightKey = new DBSPClosureExpression(
                new DBSPRawTupleExpression(rightKey, DBSPTupleExpression.flatten(r)),
                r.asParameter());
        DBSPIndexOperator rIndex = new DBSPIndexOperator(
                join, this.circuit.declareLocal("index", toRightKey).getVarReference(),
                rightKey.getNonVoidType(), rightElementType, false, filteredRight);
        this.circuit.addOperator(rIndex);

        // For outer joins additional columns may become nullable.
        DBSPTupleExpression allFields = lr.pointwiseCast(resultType);
        DBSPClosureExpression makeTuple = new DBSPClosureExpression(
                allFields, k.asRefParameter(), l.asParameter(), r.asParameter());
        DBSPJoinOperator joinResult = new DBSPJoinOperator(join, resultType,
                this.circuit.declareLocal("pair", makeTuple).getVarReference(),
                left.isMultiset || right.isMultiset, lindex, rIndex);

        DBSPOperator inner = joinResult;
        if (condition != null) {
            DBSPFilterOperator fop = new DBSPFilterOperator(join, condition, resultType, joinResult);
            this.circuit.addOperator(joinResult);
            inner = fop;
        }

        // Handle outer joins
        DBSPOperator result = inner;
        DBSPVariableReference joinVar = new DBSPVariableReference("j", resultType);
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            DBSPVariableReference lCasted = new DBSPVariableReference("l", leftResultType);
            this.circuit.addOperator(result);
            // project the join on the left columns
            DBSPClosureExpression toLeftColumns = new DBSPClosureExpression(
                    DBSPTupleExpression.flatten(joinVar)
                            .slice(0, leftColumns)
                            .pointwiseCast(leftResultType),
                    joinVar.asRefParameter());
            DBSPOperator joinLeftColumns = new DBSPMapOperator(
                    join, this.circuit.declareLocal("proj", toLeftColumns).getVarReference(),
                    leftResultType, inner);
            this.circuit.addOperator(joinLeftColumns);
            DBSPOperator distJoin = new DBSPDistinctOperator(join, joinLeftColumns);
            this.circuit.addOperator(distJoin);

            // subtract from left relation
            DBSPOperator leftCast = left;
            if (!leftResultType.same(leftElementType)) {
                DBSPClosureExpression castLeft = new DBSPClosureExpression(
                    DBSPTupleExpression.flatten(l).pointwiseCast(leftResultType),
                    l.asParameter()
                );
                leftCast = new DBSPMapOperator(join, castLeft, leftResultType, left);
                this.circuit.addOperator(leftCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(join, leftCast, distJoin);
            this.circuit.addOperator(sub);
            DBSPDistinctOperator dist = new DBSPDistinctOperator(join, sub);
            this.circuit.addOperator(dist);

            // fill nulls in the right relation fields
            DBSPTupleExpression rEmpty = new DBSPTupleExpression(
                    Linq.map(rightElementType.tupFields,
                             et -> new DBSPLiteral(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPClosureExpression leftRow = new DBSPClosureExpression(
                    DBSPTupleExpression.flatten(lCasted, rEmpty),
                    lCasted.asRefParameter());
            DBSPOperator expand = new DBSPMapOperator(join,
                    this.circuit.declareLocal("expand", leftRow).getVarReference(), resultType, dist);
            this.circuit.addOperator(expand);
            result = new DBSPSumOperator(join, result, expand);
        }
        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            DBSPVariableReference rCasted = new DBSPVariableReference("r", rightResultType);
            this.circuit.addOperator(result);

            // project the join on the right columns
            DBSPClosureExpression toRightColumns = new DBSPClosureExpression(
                    DBSPTupleExpression.flatten(joinVar)
                            .slice(leftColumns, totalColumns)
                            .pointwiseCast(rightResultType),
                    joinVar.asRefParameter());
            DBSPOperator joinRightColumns = new DBSPMapOperator(
                    join, this.circuit.declareLocal("proj", toRightColumns).getVarReference(),
                    rightResultType, inner);
            this.circuit.addOperator(joinRightColumns);
            DBSPOperator distJoin = new DBSPDistinctOperator(join, joinRightColumns);
            this.circuit.addOperator(distJoin);

            // subtract from right relation
            DBSPOperator rightCast = right;
            if (!rightResultType.same(rightElementType)) {
                DBSPClosureExpression castRight = new DBSPClosureExpression(
                        DBSPTupleExpression.flatten(r).pointwiseCast(rightResultType),
                        r.asParameter()
                );
                rightCast = new DBSPMapOperator(join, castRight, rightResultType, right);
                this.circuit.addOperator(rightCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(join, rightCast, distJoin);
            this.circuit.addOperator(sub);
            DBSPDistinctOperator dist = new DBSPDistinctOperator(join, sub);
            this.circuit.addOperator(dist);

            // fill nulls in the left relation fields
            DBSPTupleExpression lEmpty = new DBSPTupleExpression(
                    Linq.map(leftElementType.tupFields,
                            et -> new DBSPLiteral(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPClosureExpression rightRow = new DBSPClosureExpression(
                    DBSPTupleExpression.flatten(lEmpty, rCasted),
                    rCasted.asRefParameter());
            DBSPOperator expand = new DBSPMapOperator(join,
                    this.circuit.declareLocal("expand", rightRow).getVarReference(), resultType, dist);
            this.circuit.addOperator(expand);
            result = new DBSPSumOperator(join, result, expand);
        }

        this.assignOperator(join, Objects.requireNonNull(result));
    }

    /**
     * Information used to translate INSERT or DELETE SQL statements
     */
    static class DMLTranslation {
        /**
         * Translation result.
         */
        @Nullable
        private DBSPZSetLiteral logicalValueTranslation;
        /**
         * Maps each column index to the actual destination column index.
         * This handles SQL statements such as
         * INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104);
         * which specify explicitly the order of columns.
         */
        @Nullable
        private HashMap<Integer, Integer> columnPermutation;
        /**
         * Circuit input operator representing the table that is being written.
         */
        @Nullable
        public DBSPSourceOperator source;
        @Nullable
        DBSPTypeTuple resultType;

        DMLTranslation() {
            this.logicalValueTranslation = null;
            this.source = null;
        }

        void prepare(SqlNode rootNode, DBSPSourceOperator source, @Nullable SqlNodeList columnList) {
            this.logicalValueTranslation = null;
            this.source = source;
            DBSPTypeTuple sourceType = this.source
                    .getNonVoidType()
                    .to(DBSPTypeZSet.class)
                    .elementType
                    .to(DBSPTypeTuple.class);
            if (columnList != null) {
                // The column list specifies an order for the columns that are assigned,
                // which may not be the order of the columns in the table.  We need to
                // compute a permutation.
                this.columnPermutation = new HashMap<>();
                int index = 0;
                DBSPType[] columnTypes = new DBSPType[columnList.size()];
                for (SqlNode node : columnList) {
                    if (!(node instanceof SqlIdentifier))
                        throw new Unimplemented(rootNode);
                    SqlIdentifier id = (SqlIdentifier) node;
                    int actualIndex = this.source.ddlDef.getColumnIndex(id);
                    // This must be a permutation
                    for (int value: this.columnPermutation.values())
                        if (value == actualIndex)
                            throw new TranslationException("Not a column permutation " +
                                    this.columnPermutation, rootNode);
                    Utilities.putNew(this.columnPermutation, index, actualIndex);
                    columnTypes[index] = sourceType.getFieldType(actualIndex);
                    index++;
                }
                this.resultType = new DBSPTypeTuple(columnTypes);
            } else {
                this.resultType = sourceType;
            }
        }

        public DBSPZSetLiteral getTranslation() {
            return Objects.requireNonNull(this.logicalValueTranslation);
        }

        public DBSPTypeTuple getResultType() {
            return Objects.requireNonNull(this.resultType);
        }

        DBSPExpression permuteColumns(DBSPExpression expression) {
            assert this.columnPermutation != null;
            DBSPExpression[] columns = new DBSPExpression[this.columnPermutation.size()];
            DBSPTupleExpression tuple = expression.to(DBSPTupleExpression.class);
            for (Map.Entry<Integer, Integer> e: this.columnPermutation.entrySet()) {
                columns[e.getValue()] = tuple.get(e.getKey());
            }
            return new DBSPTupleExpression(columns);
        }

        DBSPZSetLiteral permuteColumns(DBSPZSetLiteral source) {
            assert this.resultType != null;
            if (this.columnPermutation == null)
                return source;
            DBSPZSetLiteral result = new DBSPZSetLiteral(new DBSPTypeZSet(
                    this.resultType, CalciteToDBSPCompiler.weightType));
            for (Map.Entry<DBSPExpression, Integer> e: source.data.entrySet()) {
                DBSPExpression perm = this.permuteColumns(e.getKey());
                result.add(perm, e.getValue());
            }
            return result;
        }

        public void setResult(DBSPZSetLiteral literal) {
            if (this.logicalValueTranslation != null)
                throw new RuntimeException("Overwriting logical value translation");
            this.logicalValueTranslation = this.permuteColumns(literal);
        }

        public boolean prepared() {
            return this.source != null;
        }
    }

    final DMLTranslation dmTranslation = new DMLTranslation();

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression.
     * This can be invoked by a DDM statement, or by a SQL query that computes a constant result.
     */
    public void visitLogicalValues(LogicalValues values) {
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, this.calciteCompiler);
        DBSPTypeTuple sourceType = this.convertType(values.getRowType()).to(DBSPTypeTuple.class);
        boolean ddmTranslation = this.dmTranslation.prepared();

        DBSPTypeTuple resultType;
        if (ddmTranslation) {
            resultType = this.dmTranslation.getResultType();
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
                DBSPType resultFieldType = resultType.tupFields[i];
                DBSPExpression expr = expressionCompiler.compile(rl);
                if (expr.is(DBSPLiteral.class)) {
                    // The expression compiler does not actually have type information
                    // so the nulls produced will have the wrong type.
                    DBSPLiteral lit = expr.to(DBSPLiteral.class);
                    if (lit.isNull)
                        expr = new DBSPLiteral(resultFieldType);
                }
                if (!expr.getNonVoidType().same(resultFieldType)) {
                    DBSPExpression cast = ExpressionCompiler.makeCast(expr, resultFieldType);
                    exprs.add(cast);
                } else {
                    exprs.add(expr);
                }
                i++;
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(t, exprs);
            result.add(expression);
        }

        if (ddmTranslation) {
            this.dmTranslation.setResult(result);
        } else {
            DBSPOperator constant = new DBSPConstantOperator(values, result, false);
            this.assignOperator(values, constant);
        }
    }

    public void visitIntersect(LogicalIntersect intersect) {
        // Intersect is a special case of join.
        assert this.circuit != null;
        List<RelNode> inputs = intersect.getInputs();
        RelNode input = intersect.getInput(0);
        DBSPOperator previous = this.getInputAs(input, false);

        if (inputs.size() == 0)
            throw new UnsupportedException(intersect);
        if (inputs.size() == 1) {
            Utilities.putNew(this.nodeOperator, intersect, previous);
            return;
        }

        DBSPType inputRowType = this.convertType(input.getRowType());
        DBSPTypeTuple resultType = this.convertType(intersect.getRowType()).to(DBSPTypeTuple.class);
        DBSPVariableReference t = new DBSPVariableReference("t", inputRowType);
        DBSPExpression entireKey = new DBSPClosureExpression(
                new DBSPRawTupleExpression(
                        t.applyClone(),
                        new DBSPRawTupleExpression()),
                t.asRefParameter());
        DBSPVariableReference l = new DBSPVariableReference(
                "l", new DBSPTypeRef(DBSPTypeRawTuple.emptyTupleType));
        DBSPVariableReference r = new DBSPVariableReference(
                "r", new DBSPTypeRef(DBSPTypeRawTuple.emptyTupleType));
        DBSPVariableReference k = new DBSPVariableReference(
                "k", inputRowType);

        DBSPClosureExpression closure = new DBSPClosureExpression(
                k.applyClone(),
                k.asRefParameter(), l.asParameter(), r.asParameter());
        for (int i = 1; i < inputs.size(); i++) {
            DBSPOperator previousIndex = new DBSPIndexOperator(
                    intersect,
                    this.circuit.declareLocal("index", entireKey).getVarReference(),
                    inputRowType, new DBSPTypeRawTuple(), previous.isMultiset, previous);
            this.circuit.addOperator(previousIndex);
            DBSPOperator inputI = this.getInputAs(intersect.getInput(i), false);
            DBSPOperator index = new DBSPIndexOperator(
                    intersect,
                    this.circuit.declareLocal("index", entireKey).getVarReference(),
                    inputRowType, new DBSPTypeRawTuple(), inputI.isMultiset, inputI);
            this.circuit.addOperator(index);
            previous = new DBSPJoinOperator(intersect, resultType, closure, false,
                    previousIndex, index);
            this.circuit.addOperator(previous);
        }
        Utilities.putNew(this.nodeOperator, intersect, previous);
    }

    public void visitSort(LogicalSort sort) {
        assert this.circuit != null;
        // Aggregate in a single group.
        // TODO: make this more efficient?
        RelNode input = sort.getInput();
        DBSPType inputRowType = this.convertType(input.getRowType());
        DBSPOperator opInput = this.getOperator(input);

        DBSPVariableReference t = new DBSPVariableReference("t", inputRowType);
        DBSPExpression emptyGroupKeys = new DBSPClosureExpression(
                new DBSPRawTupleExpression(
                        new DBSPRawTupleExpression(),
                        DBSPTupleExpression.flatten(t)),
                t.asRefParameter());
        DBSPIndexOperator index = new DBSPIndexOperator(
                sort, this.circuit.declareLocal("index", emptyGroupKeys).getVarReference(),
                new DBSPTypeRawTuple(), inputRowType, opInput.isMultiset, opInput);
        this.circuit.addOperator(index);
        // apply an aggregation function that just creates a vector.
        DBSPTypeVec vecType = new DBSPTypeVec(inputRowType);
        DBSPExpression constructor = new DBSPPathExpression(DBSPTypeAny.instance,
                new DBSPPath("Fold", "new"));
        DBSPExpression zero = new DBSPApplyExpression(new DBSPPathExpression(DBSPTypeAny.instance,
                new DBSPPath(vecType.name, "new")));
        DBSPVariableReference accum = new DBSPVariableReference("a", vecType);
        DBSPVariableReference row = new DBSPVariableReference("v", inputRowType);
        // An element with weight 'w' is pushed 'w' times into the vector
        DBSPExpression wPush = new DBSPApplyExpression("weighted_push", null, accum, row, weight);
        DBSPExpression push = new DBSPClosureExpression(wPush,
                accum.asRefParameter(true), row.asRefParameter(), CalciteToDBSPCompiler.weight.asParameter());
        DBSPExpression folder = new DBSPApplyExpression(constructor, zero, push);
        DBSPAggregateOperator agg = new DBSPAggregateOperator(sort,
                this.circuit.declareLocal("tovec", folder).getVarReference(),
                new DBSPTypeRawTuple(), new DBSPTypeVec(inputRowType), index);
        this.circuit.addOperator(agg);

        // Generate comparison function for sorting the vector
        DBSPExpression comparators = null;
        for (RelFieldCollation collation: sort.getCollation().getFieldCollations()) {
            int field = collation.getFieldIndex();
            RelFieldCollation.Direction direction = collation.getDirection();
            DBSPExpression comparator = new DBSPApplyExpression(
                    new DBSPPathExpression(DBSPTypeAny.instance,
                            new DBSPPath("Extract", "new")),
                    new DBSPClosureExpression(new DBSPFieldExpression(row, field), row.asRefParameter()));
            switch (direction) {
                case ASCENDING:
                    break;
                case DESCENDING:
                    comparator = new DBSPApplyMethodExpression("rev", DBSPTypeAny.instance, comparator);
                    break;
                case STRICTLY_ASCENDING:
                case STRICTLY_DESCENDING:
                case CLUSTERED:
                    throw new Unimplemented(sort);
            }
            if (comparators == null)
                comparators = comparator;
            else
                comparators = new DBSPApplyMethodExpression("then", DBSPTypeAny.instance, comparators, comparator);
        }
        if (comparators == null)
            throw new TranslationException("ORDER BY without order?", sort);
        DBSPLetStatement comp = this.circuit.declareLocal("comp", comparators);
        DBSPVariableReference k = new DBSPVariableReference("k", new DBSPTypeRef(new DBSPTypeRawTuple()));
        DBSPVariableReference v = new DBSPVariableReference("v", new DBSPTypeRef(vecType));
        DBSPVariableReference v1 = new DBSPVariableReference("v1", vecType);

        DBSPVariableReference a = new DBSPVariableReference("a", inputRowType);
        DBSPVariableReference b = new DBSPVariableReference("b", inputRowType);
        DBSPExpression sorter = new DBSPClosureExpression(
                new DBSPBlockExpression(
                    Linq.list(
                            new DBSPLetStatement(v1.variable,
                                    v.applyClone(),true),
                            new DBSPExpressionStatement(
                                    new DBSPApplyMethodExpression("sort_by", vecType, v1,
                                            new DBSPClosureExpression(
                                                    new DBSPApplyMethodExpression("compare", DBSPTypeAny.instance, comp.getVarReference(), a, b),
                                                    a.asRefParameter(), b.asRefParameter())))),
                    v1),
                new DBSPClosureExpression.Parameter(k, v));
        DBSPOperator sortElement = new DBSPMapOperator(sort,
                this.circuit.declareLocal("sort", sorter).getVarReference(), vecType, agg);
        this.assignOperator(sort, sortElement);
    }

    @Override
    public void visit(
            RelNode node, int ordinal,
            @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
        stack.add(new Context(parent, ordinal));
        if (debug)
            System.out.println("Visiting " + node);
        if (this.nodeOperator.containsKey(node))
            // We have already done this one.  This can happen because the
            // plan can be a DAG, not just a tree.
            return;
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
                this.visitIfMatches(node, LogicalJoin.class, this::visitJoin) ||
                this.visitIfMatches(node, LogicalIntersect.class, this::visitIntersect) ||
                this.visitIfMatches(node, LogicalSort.class, this::visitSort);
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
            RelNode rel = view.getRelNode();
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
        DBSPSourceOperator op = Utilities.getExists(this.ioOperator, statement.table).to(DBSPSourceOperator.class);
        if (!(statement.node instanceof SqlInsert))
            throw new Unimplemented(statement);
        SqlInsert insert = (SqlInsert) statement.node;
        this.dmTranslation.prepare(
                statement.node, op, insert.getTargetColumnList());
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
        DBSPSinkOperator result = new DBSPSinkOperator(v, source.getNonVoidType(), v.name,
                source.isMultiset, source);
        return Utilities.putNew(this.ioOperator, v.name, result);
    }
}
