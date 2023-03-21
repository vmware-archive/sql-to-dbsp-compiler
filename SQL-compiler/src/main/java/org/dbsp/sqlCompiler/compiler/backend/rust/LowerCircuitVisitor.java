package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowAggregateOperator;
import org.dbsp.sqlCompiler.compiler.backend.optimize.BetaReduction;
import org.dbsp.sqlCompiler.compiler.backend.visitors.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeSemigroup;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Lowers a circuit's representation.
 */
public class LowerCircuitVisitor extends CircuitCloneVisitor {
    public LowerCircuitVisitor() {
        super(false);
    }

    /**
     * Creates a DBSP Fold object from an Aggregate.
     */
    DBSPExpression createAggregator(@Nullable DBSPAggregate aggregate) {
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
        int parts = aggregate.components.length;
        DBSPExpression[] zeros = new DBSPExpression[parts];
        DBSPExpression[] increments = new DBSPExpression[parts];
        DBSPExpression[] posts = new DBSPExpression[parts];
        DBSPType[] accumulatorTypes = new DBSPType[parts];
        DBSPType[] semigroups = new DBSPType[parts];
        for (int i = 0; i < parts; i++) {
            DBSPAggregate.Implementation implementation = aggregate.components[i];
            DBSPType incType = implementation.increment.getResultType();
            zeros[i] = implementation.zero;
            increments[i] = implementation.increment;
            accumulatorTypes[i] = Objects.requireNonNull(incType);
            semigroups[i] = implementation.semigroup;
            posts[i] = implementation.getPostprocessing();
        }

        DBSPTypeRawTuple accumulatorType = new DBSPTypeRawTuple(accumulatorTypes);
        DBSPVariablePath accumulator = accumulatorType.ref(true).var("a");
        DBSPVariablePath postAccumulator = accumulatorType.var("a");

        BetaReduction reducer = new BetaReduction();
        for (int i = 0; i < parts; i++) {
            DBSPExpression accumulatorField = accumulator.field(i);
            DBSPExpression expr = increments[i].call(
                    accumulatorField, aggregate.rowVar, CalciteToDBSPCompiler.WEIGHT_VAR);
            increments[i] = Objects.requireNonNull(reducer.apply(expr)).to(DBSPExpression.class);
            DBSPExpression postAccumulatorField = postAccumulator.field(i);
            expr = posts[i].call(postAccumulatorField);
            posts[i] = Objects.requireNonNull(reducer.apply(expr)).to(DBSPExpression.class);
        }
        DBSPAssignmentExpression accumulatorBody = new DBSPAssignmentExpression(
                accumulator.deref(), new DBSPRawTupleExpression(increments));
        DBSPExpression accumFunction = accumulatorBody.closure(
                accumulator.asParameter(), aggregate.rowVar.asParameter(),
                CalciteToDBSPCompiler.WEIGHT_VAR.asParameter());
        DBSPClosureExpression postClosure = new DBSPTupleExpression(posts).closure(postAccumulator.asParameter());
        DBSPExpression constructor = DBSPTypeAny.INSTANCE.path(
                new DBSPPath(
                        new DBSPSimplePathSegment("Fold",
                                DBSPTypeAny.INSTANCE,
                                new DBSPTypeSemigroup(semigroups, accumulatorTypes),
                                DBSPTypeAny.INSTANCE,
                                DBSPTypeAny.INSTANCE),
                        new DBSPSimplePathSegment("with_output")));
        DBSPExpression fold = constructor.call(
                new DBSPRawTupleExpression(zeros),
                accumFunction, postClosure);
        DBSPLetStatement result = this.getResult().declareLocal("fold", fold);
        return result.getVarReference();
    }

    @Override
    public void postorder(DBSPAggregateOperator node) {
        if (node.function != null) {
            // OrderBy
            super.postorder(node);
            return;
        }
        DBSPExpression function = this.createAggregator(node.getAggregate());
        DBSPOperator result = new DBSPAggregateOperator(node.getNode(), node.keyType, node.outputElementType,
                function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPIncrementalAggregateOperator node) {
        if (node.function != null) {
            // OrderBy
            super.postorder(node);
            return;
        }
        DBSPExpression function = this.createAggregator(node.getAggregate());
        DBSPOperator result = new DBSPIncrementalAggregateOperator(node.getNode(), node.keyType, node.outputElementType,
                function, null, this.mapped(node.input()));
        this.map(node, result);
    }

    @Override
    public void postorder(DBSPWindowAggregateOperator node) {
        DBSPExpression function = this.createAggregator(node.getAggregate());
        DBSPOperator result = new DBSPWindowAggregateOperator(node.getNode(),
                function, null, node.window,
                node.partitionKeyType, node.timestampType, node.aggregateType,
                this.mapped(node.input()));
        this.map(node, result);
    }
}
