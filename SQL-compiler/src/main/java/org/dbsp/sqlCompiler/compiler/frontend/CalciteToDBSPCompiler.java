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

package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.frontend.statements.*;
import org.dbsp.sqlCompiler.compiler.sqlparser.*;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.DBSPPattern;
import org.dbsp.sqlCompiler.ir.pattern.DBSPTupleStructPattern;
import org.dbsp.sqlCompiler.ir.pattern.DBSPWildcardPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

/**
 * The compiler is stateful: it compiles a sequence of SQL statements
 * defining tables and views.  The views must be defined in terms of
 * the previously defined tables and views.  Multiple views can be
 * compiled.  The result is a circuit which has an input for each table
 * and an output for each view.
 * The function generateOutputForNextView can be used to prevent
 * some views from generating outputs.
 */
public class CalciteToDBSPCompiler extends RelVisitor implements IModule {
    /**
     * If true, the inputs to the circuit are generated from the CREATE TABLE
     * statements.  Otherwise they are generated from the LogicalTableScan
     * operations in a view plan.
     */
    boolean generateInputsFromTables = false;

    /**
     * If true the inputs to the circuit are generated from the CREATE TABLE
     * statements.
     */
    public void setGenerateInputsFromTables(boolean generateInputsFromTables) {
        this.generateInputsFromTables = generateInputsFromTables;
    }

    /**
     * Variable that refers to the weight of the row in the z-set.
     */
    public static final DBSPVariablePath weight = DBSPTypeZSet.defaultWeightType.var("w");

    // Result is deposited here
    @Nullable
    private DBSPCircuit circuit;
    // Occasionally we need to invoke some services of the calcite compiler.
    public final CalciteCompiler calciteCompiler;
    // Map each compiled RelNode operator to its DBSP implementation.
    final Map<RelNode, DBSPOperator> nodeOperator;
    final TypeCompiler typeCompiler;
    final TableContents tableContents;
    final CompilerOptions options;

    /**
     * Create a compiler that translated from calcite to DBSP circuits.
     * @param calciteCompiler     Calcite compiler.
     * @param trackTableContents  If true this compiler will track INSERT and DELETE statements.
     * @param options             Options for compilation.
     */
    public CalciteToDBSPCompiler(CalciteCompiler calciteCompiler, boolean trackTableContents, CompilerOptions options) {
        this.circuit = null;
        this.typeCompiler = new TypeCompiler();
        this.calciteCompiler = calciteCompiler;
        this.nodeOperator = new HashMap<>();
        this.tableContents = new TableContents(trackTableContents);
        this.options = options;
    }

    /**
     * Deposit the result of the compilation in a new circuit.
     * @param circuitName  Name of the function that will generate the circuit.
     */
    public void newCircuit(String circuitName) {
        this.circuit = new DBSPCircuit(circuitName);
    }

    private DBSPType convertType(RelDataType dt) {
        return this.typeCompiler.convertType(dt);
    }

    private DBSPType makeZSet(DBSPType type) {
        return TypeCompiler.makeZSet(type);
    }

    public DBSPCircuit getCircuit() {
        return Objects.requireNonNull(this.circuit);
    }

    /**
     * This retrieves the operator that is an input.  If the operator may
     * produce multiset results and this is not desired (asMultiset = false),
     * a distinct operator is introduced in the getCircuit().
     */
    private DBSPOperator getInputAs(RelNode input, boolean asMultiset) {
        DBSPOperator op = this.getOperator(input);
        if (op.isMultiset && !asMultiset) {
            op = new DBSPDistinctOperator(input, op);
            this.getCircuit().addOperator(op);
        }
        return op;
    }

    <T> boolean visitIfMatches(RelNode node, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(node, clazz);
        if (value != null) {
            Logger.instance.from(this, 4)
                    .append("Processing ")
                    .append(node.toString())
                    .newline();
            method.accept(value);
            return true;
        }
        return false;
    }

    private boolean generateOutputForNextView = true;

    /**
     * @param generate
     * If 'false' the next "create view" statements will not generate
     * an output for the circuit.  This is sticky, it has to be
     * explicitly reset.
     */
    public void generateOutputForNextView(boolean generate) {
         this.generateOutputForNextView = generate;
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
        DBSPVariablePath rowVar = inputRowType.ref().var("v");
        int aggIndex = 0;
        int parts = aggregates.size();
        DBSPExpression[] zeros = new DBSPExpression[parts];
        DBSPExpression[] increments = new DBSPExpression[parts];
        DBSPExpression[] posts = new DBSPExpression[parts];
        DBSPExpression[] defaultZeros = new DBSPExpression[parts];

        DBSPType[] accumulatorTypes = new DBSPType[parts];
        DBSPType[] semigroups = new DBSPType[parts];
        for (AggregateCall call: aggregates) {
            DBSPType resultFieldType = resultType.getFieldType(aggIndex + groupCount);
            AggregateCompiler compiler = new AggregateCompiler(call, resultFieldType, rowVar);
            AggregateCompiler.AggregateImplementation folder = compiler.compile();
            DBSPExpression zero = this.declare("zero", folder.zero);
            zeros[aggIndex] = zero;
            DBSPExpression increment = this.declare("inc", folder.increment);
            increments[aggIndex] = increment;
            DBSPType incType = folder.increment.getResultType();
            accumulatorTypes[aggIndex] = incType;
            semigroups[aggIndex] = folder.semigroup;
            DBSPExpression identity = new DBSPTypeFunction(incType, incType).path(
                        new DBSPPath(new DBSPSimplePathSegment("identity", incType)));
            DBSPExpression post = this.declare("post",
                    folder.postprocess != null ? folder.postprocess : identity);
            posts[aggIndex] = post;
            defaultZeros[aggIndex] = folder.emptySetResult;
            aggIndex++;
        }

        DBSPExpression zero = this.declare("zero", new DBSPRawTupleExpression(zeros));
        DBSPTypeRawTuple accumulatorType = new DBSPTypeRawTuple(accumulatorTypes);
        DBSPVariablePath accumulator = accumulatorType.ref(true).var("a");
        DBSPVariablePath postAccum = accumulatorType.var("a");
        for (int i = 0; i < increments.length; i++) {
            DBSPExpression accumField = accumulator.field(i);
            increments[i] = new DBSPApplyExpression(increments[i],
                    accumField, rowVar, weight);
            DBSPExpression postAccumField = postAccum.field(i);
            posts[i] = new DBSPApplyExpression(posts[i], postAccumField);
        }
        DBSPAssignmentExpression accumBody = new DBSPAssignmentExpression(
                accumulator.deref(), new DBSPRawTupleExpression(increments));
        DBSPExpression accumFunction = accumBody.closure(
                accumulator.asParameter(), rowVar.asParameter(), weight.asParameter());
        DBSPExpression increment = this.declare("increment", accumFunction);
        DBSPClosureExpression postClosure = new DBSPTupleExpression(posts).closure(postAccum.asParameter());
        DBSPExpression post = this.declare("post", postClosure);
        DBSPExpression constructor = DBSPTypeAny.instance.path(
                new DBSPPath(
                        new DBSPSimplePathSegment("Fold",
                                DBSPTypeAny.instance,
                                new DBSPSemigroupType(semigroups, accumulatorTypes),
                                DBSPTypeAny.instance,
                                DBSPTypeAny.instance),
                        new DBSPSimplePathSegment("with_output")));
        DBSPExpression folder = new DBSPApplyExpression(constructor, zero, increment, post);
        return new FoldingDescription(this.declare("folder", folder),
                new DBSPTupleExpression(defaultZeros));
    }

    public void visitAggregate(LogicalAggregate aggregate) {
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
        DBSPVariablePath t = inputRowType.ref().var("t");

        if (!aggregates.isEmpty()) {
            if (aggregate.getGroupType() != Aggregate.Group.SIMPLE)
                throw new Unimplemented(aggregate);
            DBSPExpression[] groups = new DBSPExpression[aggregate.getGroupCount()];
            int next = 0;
            for (int index: aggregate.getGroupSet()) {
                groups[next] = t.field(index);
                next++;
            }
            DBSPExpression keyExpression = new DBSPRawTupleExpression(groups);
            DBSPType[] aggTypes = Utilities.arraySlice(tuple.tupFields, aggregate.getGroupCount());
            DBSPTypeTuple aggType = new DBSPTypeTuple(aggTypes);

            DBSPExpression groupKeys =
                    new DBSPRawTupleExpression(
                            keyExpression,
                            DBSPTupleExpression.flatten(t)).closure(
                    t.asParameter());
            DBSPIndexOperator index = new DBSPIndexOperator(
                    aggregate, this.declare("index", groupKeys),
                    keyExpression.getNonVoidType(), inputRowType, false, opInput);
            this.getCircuit().addOperator(index);
            DBSPType groupType = keyExpression.getNonVoidType();
            FoldingDescription fd = this.createFoldingFunction(aggregates, tuple, inputRowType, aggregate.getGroupCount());
            // The aggregate operator will not return a stream of type aggType, but a stream
            // with a type given by fd.defaultZero.
            DBSPTypeTuple typeFromAggregate = fd.defaultZero.getNonVoidType().to(DBSPTypeTuple.class);
            DBSPAggregateOperator agg = new DBSPAggregateOperator(aggregate, fd.fold, groupType,
                    typeFromAggregate, index);

            // Flatten the resulting set
            DBSPVariablePath kResult = groupType.ref().var("k");
            DBSPVariablePath vResult = typeFromAggregate.ref().var("v");
            DBSPExpression[] flattenFields = new DBSPExpression[aggregate.getGroupCount() + aggType.size()];
            for (int i = 0; i < aggregate.getGroupCount(); i++)
                flattenFields[i] = kResult.field(i);
            for (int i = 0; i < aggType.size(); i++) {
                DBSPExpression flattenField = vResult.field(i);
                // Here we correct from the type produced by the Folder (typeFromAggregate) to the
                // actual expected type aggType (which is the tuple of aggTypes).
                flattenFields[aggregate.getGroupCount() + i] = ExpressionCompiler.makeCast(flattenField, aggTypes[i]);
            }
            DBSPExpression mapper = new DBSPTupleExpression(flattenFields).closure(
                    new DBSPParameter(kResult, vResult));
            this.getCircuit().addOperator(agg);
            DBSPMapOperator map = new DBSPMapOperator(aggregate,
                    this.declare("flatten", mapper), tuple, agg);
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
                this.getCircuit().addOperator(map);
                DBSPVariablePath _t = DBSPTypeAny.instance.var("_t");
                DBSPExpression toZero = fd.defaultZero.closure(_t.asParameter());
                DBSPOperator map1 = new DBSPMapOperator(aggregate, toZero, type, map);
                this.getCircuit().addOperator(map1);
                DBSPOperator neg = new DBSPNegateOperator(aggregate, map1);
                this.getCircuit().addOperator(neg);
                DBSPOperator constant = new DBSPConstantOperator(
                        aggregate, new DBSPZSetLiteral(fd.defaultZero), false);
                this.getCircuit().addOperator(constant);
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
        @Nullable
        DBSPOperator source = this.getCircuit().getOperator(tableName);
        if (source != null) {
            if (source.is(DBSPSinkOperator.class))
                // We do this because sink operators do not have outputs.
                // A table scan for a sink operator can appear because of
                // a VIEW that is an input to a query.
                Utilities.putNew(this.nodeOperator, scan, source.to(DBSPSinkOperator.class).input());
            else
                // Multiple queries can share an input.
                // Or the input may have been created by a CREATE TABLE statement.
                Utilities.putNew(this.nodeOperator, scan, source);
            return;
        }

        if (this.generateInputsFromTables)
            throw new RuntimeException("Could not find input for table " + tableName);
        @Nullable String comment = null;
        if (scan.getTable() instanceof RelOptTableImpl) {
            RelOptTableImpl impl = (RelOptTableImpl) scan.getTable();
            CreateRelationStatement.EmulatedTable et = impl.unwrap(CreateRelationStatement.EmulatedTable.class);
            if (et != null)
                comment = et.getStatement();
        }
        DBSPType rowType = this.convertType(scan.getRowType());
        DBSPSourceOperator result = new DBSPSourceOperator(scan, this.makeZSet(rowType), comment, tableName);
        this.assignOperator(scan, result);
    }

    void assignOperator(RelNode rel, DBSPOperator op) {
        Utilities.putNew(this.nodeOperator, rel, op);
        this.getCircuit().addOperator(op);
    }

    DBSPOperator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    public void visitProject(LogicalProject project) {
        // LogicalProject is not really SQL project, it is rather map.
        RelNode input = project.getInput();
        DBSPOperator opInput = this.getInputAs(input, true);
        DBSPType outputType = this.convertType(project.getRowType());
        DBSPTypeTuple tuple = outputType.to(DBSPTypeTuple.class);
        DBSPType inputType = this.convertType(project.getInput().getRowType());
        DBSPVariablePath row = inputType.ref().var("t");
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(row, this.calciteCompiler);

        List<DBSPExpression> resultColumns = new ArrayList<>();
        int index = 0;
        for (RexNode column : project.getProjects()) {
            if (column instanceof RexOver) {
                throw new UnsupportedException("Optimizer should have removed OVER expressions", column);
            } else {
                DBSPExpression exp = expressionCompiler.compile(column);
                DBSPType expectedType = tuple.getFieldType(index);
                if (!exp.getNonVoidType().sameType(expectedType)) {
                    // Calcite's optimizations do not preserve types!
                    exp = ExpressionCompiler.makeCast(exp, expectedType);
                }
                resultColumns.add(exp);
                index++;
            }
        }
        DBSPExpression exp = new DBSPTupleExpression(project, resultColumns);
        DBSPExpression closure = new DBSPClosureExpression(project, exp, row.asParameter());
        DBSPExpression mapFunc = this.declare("map", closure);
        DBSPMapOperator op = new DBSPMapOperator(project, mapFunc, outputType, opInput);
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    private void visitUnion(LogicalUnion union) {
        List<DBSPOperator> inputs = Linq.map(union.getInputs(), this::getOperator);
        DBSPSumOperator sum = new DBSPSumOperator(union, inputs);
        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.getCircuit().addOperator(sum);
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
                this.getCircuit().addOperator(neg);
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
            this.getCircuit().addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(minus, sum);
            this.assignOperator(minus, d);
        }
    }

    public DBSPExpression declare(String prefix, DBSPExpression closure) {
        return this.getCircuit().declareLocal(prefix, closure).getVarReference();
    }

    public void visitFilter(LogicalFilter filter) {
        DBSPType type = this.convertType(filter.getRowType());
        DBSPVariablePath t = type.ref().var("t");
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.calciteCompiler);
        DBSPExpression condition = expressionCompiler.compile(filter.getCondition());
        condition = ExpressionCompiler.wrapBoolIfNeeded(condition);
        condition = new DBSPClosureExpression(filter.getCondition(), condition, t.asParameter());
        DBSPOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(
                filter, this.declare("cond", condition), input);
        this.assignOperator(filter, fop);
    }

    private DBSPOperator filterNonNullKeys(LogicalJoin join,
            List<Integer> keyFields, DBSPOperator input) {
        DBSPTypeTuple rowType = input.getNonVoidType().to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class);
        boolean shouldFilter = Linq.any(keyFields, i -> rowType.tupFields[i].mayBeNull);
        if (!shouldFilter) return input;

        DBSPVariablePath var = rowType.var("r");
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
                DBSPWildcardPattern.instance, DBSPLiteral.none(some.getNonVoidType())));
        DBSPMatchExpression match = new DBSPMatchExpression(var, cases, some.getNonVoidType());
        DBSPClosureExpression filterFunc = match.closure(var.asRefParameter());
        DBSPOperator filter = new DBSPFlatMapOperator(join, filterFunc, TypeCompiler.makeZSet(rowType), input);
        this.getCircuit().addOperator(filter);
        return filter;
    }

    private void visitJoin(LogicalJoin join) {
        JoinRelType joinType = join.getJoinType();
        if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI)
            throw new Unimplemented(join);

        DBSPTypeTuple resultType = this.convertType(join.getRowType()).to(DBSPTypeTuple.class);
        if (join.getInputs().size() != 2)
            throw new TranslationException("Unexpected join with " + join.getInputs().size() + " inputs", join);
        DBSPOperator left = this.getInputAs(join.getInput(0), true);
        DBSPOperator right = this.getInputAs(join.getInput(1), true);
        DBSPTypeTuple leftElementType = left.getNonVoidType().to(DBSPTypeZSet.class).elementType
                .to(DBSPTypeTuple.class);

        JoinConditionAnalyzer analyzer = new JoinConditionAnalyzer(
                leftElementType.to(DBSPTypeTuple.class).size(), this.typeCompiler);
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

        DBSPVariablePath l = leftElementType.ref().var("l");
        DBSPVariablePath r = rightElementType.ref().var("r");
        DBSPTupleExpression lr = DBSPTupleExpression.flatten(l, r);
        DBSPVariablePath t = lr.getNonVoidType().ref().var("t");
        List<DBSPExpression> leftKeyFields = Linq.map(
                decomposition.comparisons,
                c -> ExpressionCompiler.makeCast(l.field(c.leftColumn), c.resultType));
        List<DBSPExpression> rightKeyFields = Linq.map(
                decomposition.comparisons,
                c -> ExpressionCompiler.makeCast(r.field(c.rightColumn), c.resultType));
        DBSPExpression leftKey = new DBSPRawTupleExpression(leftKeyFields);
        DBSPExpression rightKey = new DBSPRawTupleExpression(rightKeyFields);

        @Nullable
        RexNode leftOver = decomposition.getLeftOver();
        DBSPExpression condition = null;
        if (leftOver != null) {
            t = resultType.ref().var("t");
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(t, this.calciteCompiler);
            condition = expressionCompiler.compile(leftOver);
            if (condition.getNonVoidType().mayBeNull) {
                condition = new DBSPApplyExpression("wrap_bool", condition.getNonVoidType().setMayBeNull(false), condition);
            }
            condition = new DBSPClosureExpression(join.getCondition(), condition, t.asParameter());
            condition = this.declare("cond", condition);
        }
        DBSPVariablePath k = leftKey.getNonVoidType().var("k");

        DBSPClosureExpression toLeftKey = new DBSPRawTupleExpression(leftKey, DBSPTupleExpression.flatten(l))
                .closure(l.asParameter());
        DBSPIndexOperator lindex = new DBSPIndexOperator(
                join, this.declare("index", toLeftKey),
                leftKey.getNonVoidType(), leftElementType, false, filteredLeft);
        this.getCircuit().addOperator(lindex);

        DBSPClosureExpression toRightKey = new DBSPRawTupleExpression(rightKey, DBSPTupleExpression.flatten(r))
                .closure(r.asParameter());
        DBSPIndexOperator rIndex = new DBSPIndexOperator(
                join, this.declare("index", toRightKey),
                rightKey.getNonVoidType(), rightElementType, false, filteredRight);
        this.getCircuit().addOperator(rIndex);

        // For outer joins additional columns may become nullable.
        DBSPTupleExpression allFields = lr.pointwiseCast(resultType);
        DBSPClosureExpression makeTuple = allFields.closure(k.asRefParameter(), l.asParameter(), r.asParameter());
        DBSPJoinOperator joinResult = new DBSPJoinOperator(join, resultType,
                this.declare("pair", makeTuple),
                left.isMultiset || right.isMultiset, lindex, rIndex);

        DBSPOperator inner = joinResult;
        if (condition != null) {
            DBSPBoolLiteral blit = condition.as(DBSPBoolLiteral.class);
            if (blit == null || blit.value == null || !blit.value) {
                // Technically if blit.value == null or !blit.value then
                // the filter is false, and the result is empty.  But hopefully
                // the calcite optimizer won't allow that.
                DBSPFilterOperator fop = new DBSPFilterOperator(join, condition, joinResult);
                this.getCircuit().addOperator(joinResult);
                inner = fop;
            }
            // if blit it true we don't need to filter.
        }

        // Handle outer joins
        DBSPOperator result = inner;
        DBSPVariablePath joinVar = resultType.var("j");
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            DBSPVariablePath lCasted = leftResultType.var("l");
            this.getCircuit().addOperator(result);
            // project the join on the left columns
            DBSPClosureExpression toLeftColumns =
                    DBSPTupleExpression.flatten(joinVar)
                            .slice(0, leftColumns)
                            .pointwiseCast(leftResultType).closure(joinVar.asRefParameter());
            DBSPOperator joinLeftColumns = new DBSPMapOperator(
                    join, this.declare("proj", toLeftColumns),
                    leftResultType, inner);
            this.getCircuit().addOperator(joinLeftColumns);
            DBSPOperator distJoin = new DBSPDistinctOperator(join, joinLeftColumns);
            this.getCircuit().addOperator(distJoin);

            // subtract from left relation
            DBSPOperator leftCast = left;
            if (!leftResultType.sameType(leftElementType)) {
                DBSPClosureExpression castLeft =
                    DBSPTupleExpression.flatten(l).pointwiseCast(leftResultType).closure(l.asParameter()
                );
                leftCast = new DBSPMapOperator(join, castLeft, leftResultType, left);
                this.getCircuit().addOperator(leftCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(join, leftCast, distJoin);
            this.getCircuit().addOperator(sub);
            DBSPDistinctOperator dist = new DBSPDistinctOperator(join, sub);
            this.getCircuit().addOperator(dist);

            // fill nulls in the right relation fields
            DBSPTupleExpression rEmpty = new DBSPTupleExpression(
                    Linq.map(rightElementType.tupFields,
                             et -> DBSPLiteral.none(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPClosureExpression leftRow = DBSPTupleExpression.flatten(lCasted, rEmpty).closure(
                    lCasted.asRefParameter());
            DBSPOperator expand = new DBSPMapOperator(join,
                    this.declare("expand", leftRow), resultType, dist);
            this.getCircuit().addOperator(expand);
            result = new DBSPSumOperator(join, result, expand);
        }
        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            DBSPVariablePath rCasted = rightResultType.var("r");
            this.getCircuit().addOperator(result);

            // project the join on the right columns
            DBSPClosureExpression toRightColumns =
                    DBSPTupleExpression.flatten(joinVar)
                            .slice(leftColumns, totalColumns)
                            .pointwiseCast(rightResultType).closure(
                    joinVar.asRefParameter());
            DBSPOperator joinRightColumns = new DBSPMapOperator(
                    join, this.declare("proj", toRightColumns),
                    rightResultType, inner);
            this.getCircuit().addOperator(joinRightColumns);
            DBSPOperator distJoin = new DBSPDistinctOperator(join, joinRightColumns);
            this.getCircuit().addOperator(distJoin);

            // subtract from right relation
            DBSPOperator rightCast = right;
            if (!rightResultType.sameType(rightElementType)) {
                DBSPClosureExpression castRight =
                        DBSPTupleExpression.flatten(r).pointwiseCast(rightResultType).closure(
                        r.asParameter());
                rightCast = new DBSPMapOperator(join, castRight, rightResultType, right);
                this.getCircuit().addOperator(rightCast);
            }
            DBSPOperator sub = new DBSPSubtractOperator(join, rightCast, distJoin);
            this.getCircuit().addOperator(sub);
            DBSPDistinctOperator dist = new DBSPDistinctOperator(join, sub);
            this.getCircuit().addOperator(dist);

            // fill nulls in the left relation fields
            DBSPTupleExpression lEmpty = new DBSPTupleExpression(
                    Linq.map(leftElementType.tupFields,
                            et -> DBSPLiteral.none(et.setMayBeNull(true)), DBSPExpression.class));
            DBSPClosureExpression rightRow =
                    DBSPTupleExpression.flatten(lEmpty, rCasted).closure(
                    rCasted.asRefParameter());
            DBSPOperator expand = new DBSPMapOperator(join,
                    this.declare("expand", rightRow), resultType, dist);
            this.getCircuit().addOperator(expand);
            result = new DBSPSumOperator(join, result, expand);
        }

        this.assignOperator(join, Objects.requireNonNull(result));
    }

    @Nullable
    ModifyTableTranslation modifyTableTranslation;

    /**
     * Visit a LogicalValue: a SQL literal, as produced by a VALUES expression.
     * This can be invoked by a DDM statement, or by a SQL query that computes a constant result.
     */
    public void visitLogicalValues(LogicalValues values) {
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(null, this.calciteCompiler);
        DBSPTypeTuple sourceType = this.convertType(values.getRowType()).to(DBSPTypeTuple.class);
        DBSPTypeTuple resultType;
        if (this.modifyTableTranslation != null) {
            resultType = this.modifyTableTranslation.getResultType();
            if (sourceType.size() != resultType.size())
                throw new TranslationException("Expected a tuple with " + resultType.size() +
                        " values but got " + values, values);
        } else {
            resultType = sourceType;
        }

        DBSPZSetLiteral result = new DBSPZSetLiteral(TypeCompiler.makeZSet(resultType));
        for (List<RexLiteral> t : values.getTuples()) {
            List<DBSPExpression> expressions = new ArrayList<>();
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
                        expr = DBSPLiteral.none(resultFieldType);
                }
                if (!expr.getNonVoidType().sameType(resultFieldType)) {
                    DBSPExpression cast = ExpressionCompiler.makeCast(expr, resultFieldType);
                    expressions.add(cast);
                } else {
                    expressions.add(expr);
                }
                i++;
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(t, expressions);
            result.add(expression);
        }

        if (this.modifyTableTranslation != null) {
            this.modifyTableTranslation.setResult(result);
        } else {
            DBSPOperator constant = new DBSPConstantOperator(values, result, false);
            this.assignOperator(values, constant);
        }
    }

    public void visitIntersect(LogicalIntersect intersect) {
        // Intersect is a special case of join.
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
        DBSPVariablePath t = inputRowType.ref().var("t");
        DBSPExpression entireKey =
                new DBSPRawTupleExpression(
                        t.applyClone(),
                        new DBSPRawTupleExpression()).closure(
                t.asParameter());
        DBSPVariablePath l = DBSPTypeRawTuple.emptyTupleType.ref().var("l");
        DBSPVariablePath r = DBSPTypeRawTuple.emptyTupleType.ref().var("r");
        DBSPVariablePath k = inputRowType.ref().var("k");

        DBSPClosureExpression closure = k.applyClone().closure(
                k.asParameter(), l.asParameter(), r.asParameter());
        for (int i = 1; i < inputs.size(); i++) {
            DBSPOperator previousIndex = new DBSPIndexOperator(
                    intersect,
                    this.declare("index", entireKey),
                    inputRowType, new DBSPTypeRawTuple(), previous.isMultiset, previous);
            this.getCircuit().addOperator(previousIndex);
            DBSPOperator inputI = this.getInputAs(intersect.getInput(i), false);
            DBSPOperator index = new DBSPIndexOperator(
                    intersect,
                    this.declare("index", entireKey),
                    inputRowType, new DBSPTypeRawTuple(), inputI.isMultiset, inputI);
            this.getCircuit().addOperator(index);
            previous = new DBSPJoinOperator(intersect, resultType, closure, false,
                    previousIndex, index);
            this.getCircuit().addOperator(previous);
        }
        Utilities.putNew(this.nodeOperator, intersect, previous);
    }

    DBSPExpression compileWindowBound(RexWindowBound bound, DBSPType boundType, ExpressionCompiler eComp) {
        IsNumericType numType = boundType.to(IsNumericType.class);
        DBSPExpression numericBound;
        if (bound.isUnbounded())
            numericBound = numType.getMaxValue();
        else if (bound.isCurrentRow())
            numericBound = numType.getZero();
        else {
            DBSPExpression value = eComp.compile(Objects.requireNonNull(bound.getOffset()));
            numericBound = ExpressionCompiler.makeCast(value, boundType);
        }
        String beforeAfter = bound.isPreceding() ? "Before" : "After";
        return new DBSPStructExpression(DBSPTypeAny.instance.path(
                new DBSPPath("RelOffset", beforeAfter)),
                DBSPTypeAny.instance, numericBound);
    }

    public void visitWindow(LogicalWindow window) {
        DBSPTypeTuple windowResultType = this.convertType(window.getRowType()).to(DBSPTypeTuple.class);
        RelNode inputNode = window.getInput();
        DBSPOperator input = this.getInputAs(window.getInput(0), true);
        DBSPTypeTuple inputRowType = this.convertType(inputNode.getRowType()).to(DBSPTypeTuple.class);
        DBSPVariablePath inputRowRefVar = inputRowType.ref().var("t");
        ExpressionCompiler eComp = new ExpressionCompiler(inputRowRefVar, window.constants, this.calciteCompiler);
        int windowFieldIndex = inputRowType.size();
        DBSPVariablePath previousRowRefVar = inputRowRefVar;

        DBSPTypeTuple currentTupleType = inputRowType;
        DBSPOperator lastOperator = input;
        for (Window.Group group: window.groups) {
            if (lastOperator != input)
                this.getCircuit().addOperator(lastOperator);
            List<RelFieldCollation> orderKeys = group.orderKeys.getFieldCollations();
            // Sanity checks
            if (orderKeys.size() > 1)
                throw new Unimplemented("ORDER BY not yet supported with multiple columns", window);
            RelFieldCollation collation = orderKeys.get(0);
            if (collation.getDirection() != RelFieldCollation.Direction.ASCENDING)
                throw new Unimplemented("OVER only supports ascending sorting", window);
            int orderColumnIndex = collation.getFieldIndex();
            DBSPExpression orderField = inputRowRefVar.field(orderColumnIndex);
            DBSPType sortType = inputRowType.tupFields[orderColumnIndex];
            if (!sortType.is(DBSPTypeInteger.class) &&
                    !sortType.is(DBSPTypeTimestamp.class))
                throw new Unimplemented("OVER currently requires an integer type for ordering ", window);
            if (sortType.mayBeNull)
                throw new Unimplemented("OVER currently does not support sorting on nullable column ", window);

            // Create window description
            DBSPExpression lb = this.compileWindowBound(group.lowerBound, sortType, eComp);
            DBSPExpression ub = this.compileWindowBound(group.upperBound, sortType, eComp);
            DBSPExpression windowExpr = new DBSPStructExpression(
                    DBSPTypeAny.instance.path(
                            new DBSPPath("RelRange", "new")),
                    DBSPTypeAny.instance, lb, ub);
            DBSPExpression windowExprVar = this.declare("window", windowExpr);

            // Map each row to an expression of the form: |t| (partition, (order, t.clone()))
            List<Integer> partitionKeys = group.keys.toList();
            List<DBSPExpression> exprs = Linq.map(partitionKeys, inputRowRefVar::field);
            DBSPTupleExpression partition = new DBSPTupleExpression(window, exprs);
            DBSPExpression orderAndRow = new DBSPRawTupleExpression(orderField, inputRowRefVar.applyClone());
            DBSPExpression mapExpr = new DBSPRawTupleExpression(partition, orderAndRow);
            DBSPClosureExpression mapClo = mapExpr.closure(inputRowRefVar.asParameter());
            DBSPExpression mapCloVar = this.declare("map", mapClo);
            DBSPOperator mapIndex = new DBSPMapIndexOperator(window, mapCloVar,
                    partition.getNonVoidType(), orderAndRow.getNonVoidType(), input);
            this.getCircuit().addOperator(mapIndex);

            List<AggregateCall> aggregateCalls = group.getAggregateCalls(window);
            List<DBSPType> types = Linq.map(aggregateCalls, c -> this.convertType(c.type));
            DBSPTypeTuple tuple = new DBSPTypeTuple(types);
            FoldingDescription fd = this.createFoldingFunction(aggregateCalls, tuple, inputRowType, 0);

            // Compute aggregates for the window
            DBSPTypeTuple aggResultType = fd.defaultZero.getNonVoidType().to(DBSPTypeTuple.class);
            // This operator is always incremental, so create the non-incremental version
            // of it by adding a D and an I around it.
            DBSPDifferentialOperator diff = new DBSPDifferentialOperator(window, mapIndex);
            this.getCircuit().addOperator(diff);
            DBSPWindowAggregateOperator windowAgg = new DBSPWindowAggregateOperator(
                    group, fd.fold,
                    windowExprVar, partition.getNonVoidType(), sortType,
                    aggResultType, diff);
            this.getCircuit().addOperator(windowAgg);
            DBSPIntegralOperator integ = new DBSPIntegralOperator(window, windowAgg);
            this.getCircuit().addOperator(integ);

            // Join the previous result with the aggregate
            // First index the aggregate.
            DBSPExpression partAndOrder = new DBSPRawTupleExpression(partition, orderField);
            DBSPExpression indexedInput = new DBSPRawTupleExpression(partAndOrder, previousRowRefVar.applyClone());
            DBSPExpression partAndOrderClo = indexedInput.closure(previousRowRefVar.asParameter());
            DBSPOperator indexInput = new DBSPIndexOperator(window,
                    this.declare("index", partAndOrderClo),
                    partAndOrder.getNonVoidType(), previousRowRefVar.getNonVoidType().deref(),
                    lastOperator.isMultiset, lastOperator);
            this.getCircuit().addOperator(indexInput);

            DBSPVariablePath key = partAndOrder.getNonVoidType().var("k");
            DBSPVariablePath left = currentTupleType.var("l");
            DBSPVariablePath right = aggResultType.ref().var("r");
            DBSPExpression[] allFields = new DBSPExpression[
                    currentTupleType.size() + aggResultType.size()];
            for (int i = 0; i < currentTupleType.size(); i++)
                allFields[i] = left.field(i);
            for (int i = 0; i < aggResultType.size(); i++) {
                // Calcite is very smart and sometimes infers non-nullable result types
                // for these aggregates.  So we have to cast the results to whatever
                // Calcite says they will be.
                allFields[i + currentTupleType.size()] = ExpressionCompiler.makeCast(
                        right.field(i), windowResultType.getFieldType(windowFieldIndex));
                windowFieldIndex++;
            }
            DBSPTupleExpression addExtraFieldBody = new DBSPTupleExpression(allFields);
            DBSPClosureExpression addExtraField =
                    addExtraFieldBody.closure(key.asRefParameter(), left.asRefParameter(), right.asParameter());
            lastOperator = new DBSPJoinOperator(window, addExtraFieldBody.getNonVoidType(), this.declare("join", addExtraField),
                    indexInput.isMultiset || windowAgg.isMultiset, indexInput, integ);
            currentTupleType = addExtraFieldBody.getNonVoidType().to(DBSPTypeTuple.class);
            previousRowRefVar = currentTupleType.ref().var("t");
        }
        this.assignOperator(window, lastOperator);
    }

    public void visitSort(LogicalSort sort) {
        // Aggregate in a single group.
        // TODO: make this more efficient?
        RelNode input = sort.getInput();
        DBSPType inputRowType = this.convertType(input.getRowType());
        DBSPOperator opInput = this.getOperator(input);

        DBSPVariablePath t = inputRowType.var("t");
        DBSPExpression emptyGroupKeys =
                new DBSPRawTupleExpression(
                        new DBSPRawTupleExpression(),
                        DBSPTupleExpression.flatten(t)).closure(t.asRefParameter());
        DBSPIndexOperator index = new DBSPIndexOperator(
                sort, this.declare("index", emptyGroupKeys),
                new DBSPTypeRawTuple(), inputRowType, opInput.isMultiset, opInput);
        this.getCircuit().addOperator(index);
        // apply an aggregation function that just creates a vector.
        DBSPTypeVec vecType = new DBSPTypeVec(inputRowType);
        DBSPExpression zero = new DBSPApplyExpression(DBSPTypeAny.instance.path(
                new DBSPPath(vecType.name, "new")));
        DBSPVariablePath accum = vecType.var("a");
        DBSPVariablePath row = inputRowType.var("v");
        // An element with weight 'w' is pushed 'w' times into the vector
        DBSPExpression wPush = new DBSPApplyExpression("weighted_push", null, accum, row, weight);
        DBSPExpression push = wPush.closure(
                accum.asRefParameter(true), row.asRefParameter(), CalciteToDBSPCompiler.weight.asParameter());
        DBSPExpression constructor = DBSPTypeAny.instance.path(
            new DBSPPath(
                    new DBSPSimplePathSegment("Fold",
                            DBSPTypeAny.instance,
                        new DBSPTypeUser(null, "UnimplementedSemigroup",
                                false, DBSPTypeAny.instance),
                        DBSPTypeAny.instance,
                        DBSPTypeAny.instance),
                    new DBSPSimplePathSegment("new")));

        DBSPExpression folder = new DBSPApplyExpression(constructor, zero, push);
        DBSPAggregateOperator agg = new DBSPAggregateOperator(sort,
                this.declare("toVec", folder),
                new DBSPTypeRawTuple(), new DBSPTypeVec(inputRowType), index);
        this.getCircuit().addOperator(agg);

        // Generate comparison function for sorting the vector
        DBSPExpression comparators = null;
        for (RelFieldCollation collation: sort.getCollation().getFieldCollations()) {
            int field = collation.getFieldIndex();
            RelFieldCollation.Direction direction = collation.getDirection();
            DBSPExpression comparator = new DBSPApplyExpression(
                    DBSPTypeAny.instance.path(
                            new DBSPPath("Extract", "new")),
                    row.field(field).closure(row.asRefParameter()));
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
        DBSPExpression comp = this.declare("comp", comparators);
        DBSPVariablePath k = new DBSPTypeRawTuple().ref().var("k");
        DBSPVariablePath v = vecType.ref().var("v");
        DBSPVariablePath v1 = vecType.var("v1");

        DBSPVariablePath a = inputRowType.var("a");
        DBSPVariablePath b = inputRowType.var("b");
        DBSPExpression sorter =
                new DBSPBlockExpression(
                    Linq.list(
                            new DBSPLetStatement(v1.variable,
                                    v.applyClone(),true),
                            new DBSPExpressionStatement(
                                    new DBSPApplyMethodExpression("sort_unstable_by", vecType, v1,
                                                    new DBSPApplyMethodExpression("compare", DBSPTypeAny.instance, comp, a, b).closure(
                                                    a.asRefParameter(), b.asRefParameter())))),
                    v1).closure(new DBSPParameter(k, v));
        DBSPOperator sortElement = new DBSPMapOperator(sort,
                this.declare("sort", sorter), vecType, agg);
        this.assignOperator(sort, sortElement);
    }

    @Override
    public void visit(
            RelNode node, int ordinal,
            @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
        Logger.instance.from(this, 3)
                .append("Visiting ")
                .append(node.toString())
                .newline();
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
                this.visitIfMatches(node, LogicalWindow.class, this::visitWindow) ||
                this.visitIfMatches(node, LogicalSort.class, this::visitSort);
        if (!success)
            throw new Unimplemented(node);
    }

    @Nullable
    public DBSPNode compile(FrontEndStatement statement) {
        if (statement.is(CreateViewStatement.class)) {
            CreateViewStatement view = statement.to(CreateViewStatement.class);
            RelNode rel = view.getRelNode();
            Logger.instance.from(this, 2)
                    .append(CalciteCompiler.getPlan(rel))
                    .newline();
            this.go(rel);
            // TODO: connect the result of the query compilation with
            // the fields of rel; for now we assume that these are 1/1
            DBSPOperator op = this.getOperator(rel);
            DBSPOperator o;
            if (this.generateOutputForNextView) {
                o = new DBSPSinkOperator(
                        view, view.tableName, view.statement, statement.comment, op);
            } else {
                // We may already have a node for this output
                DBSPOperator previous = this.getCircuit().getOperator(view.tableName);
                if (previous != null)
                    return previous;
                o = new DBSPNoopOperator(view, op, statement.comment, view.tableName);
            }
            this.getCircuit().addOperator(o);
            return o;
        } else if (statement.is(CreateTableStatement.class) ||
                statement.is(DropTableStatement.class)) {
            this.tableContents.execute(statement);
            CreateTableStatement create = statement.as(CreateTableStatement.class);
            if (create != null && this.generateInputsFromTables) {
                // We create an input for the circuit.  The inputs
                // could be created by visiting LogicalTableScan, but if a table
                // is *not* used in a view, it won't have a corresponding input
                // in the circuit.
                String tableName = create.tableName;
                CreateTableStatement def = this.tableContents.getTableDefinition(tableName);
                DBSPType rowType = def.getRowType();
                DBSPSourceOperator result = new DBSPSourceOperator(
                        create, this.makeZSet(rowType), def.statement, tableName);
                this.getCircuit().addOperator(result);
            }
            return null;
        } else if (statement.is(TableModifyStatement.class)) {
            TableModifyStatement modify = statement.to(TableModifyStatement.class);
            // The type of the data must be extracted from the modified table
            if (!(modify.node instanceof SqlInsert))
                throw new Unimplemented(statement);
            SqlInsert insert = (SqlInsert) statement.node;
            assert insert != null;
            CreateTableStatement def = this.tableContents.getTableDefinition(modify.tableName);
            this.modifyTableTranslation = new ModifyTableTranslation(
                    modify, def, insert.getTargetColumnList());
            if (modify.rel instanceof LogicalTableScan) {
                // Support for INSERT INTO table (SELECT * FROM otherTable)
                LogicalTableScan scan = (LogicalTableScan) modify.rel;
                List<String> name = scan.getTable().getQualifiedName();
                String sourceTable = name.get(name.size() - 1);
                DBSPZSetLiteral data = this.tableContents.getTableContents(sourceTable);
                this.tableContents.addToTable(modify.tableName, data);
                this.modifyTableTranslation = null;
                return data;
            } else if (modify.rel instanceof LogicalValues) {
                this.go(modify.rel);
                DBSPZSetLiteral result = this.modifyTableTranslation.getTranslation();
                this.tableContents.addToTable(modify.tableName, result);
                this.modifyTableTranslation = null;
                return result;
            }
        }
        assert statement.node != null;
        throw new Unimplemented(statement.node);
    }

    public TableContents getTableContents() {
        return this.tableContents;
    }
}
