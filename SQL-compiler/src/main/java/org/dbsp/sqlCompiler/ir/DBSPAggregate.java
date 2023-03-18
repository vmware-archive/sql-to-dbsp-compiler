package org.dbsp.sqlCompiler.ir;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperator;
import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.Objects;


/**
 * Description of an aggregate.
 * In general an aggregate performs multiple simple aggregates simultaneously.
 * These are the components.
 */
public class DBSPAggregate extends DBSPNode implements IDBSPInnerNode {
    public final DBSPVariablePath rowVar;
    public final Implementation[] components;

    public DBSPAggregate(RelNode node, DBSPVariablePath rowVar, int size) {
        super(node);
        this.rowVar = rowVar;
        this.components = new Implementation[size];
    }

    public void set(int i, Implementation implementation) {
        this.components[i] = implementation;
    }

    public DBSPTypeTuple defaultZeroType() {
        return this.defaultZero().getNonVoidType().to(DBSPTypeTuple.class);
    }

    public DBSPExpression defaultZero() {
        return new DBSPTupleExpression(Linq.map(this.components, c -> c.emptySetResult, DBSPExpression.class));
    }

    public DBSPExpression getZero() {
        return new DBSPTupleExpression(Linq.map(this.components, c -> c.zero, DBSPExpression.class));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (Implementation impl: this.components) {
            impl.accept(visitor);
        }
        visitor.postorder(this);
    }

    public DBSPClosureExpression getIncrement() {
        DBSPClosureExpression[] closures = Linq.map(this.components, c -> c.increment, DBSPClosureExpression.class);
        return DBSPClosureExpression.parallelClosure(closures);
    }

    public DBSPClosureExpression getPostprocessing() {
        DBSPClosureExpression[] closures = Linq.map(this.components, c -> c.postProcess, DBSPClosureExpression.class);
        return DBSPClosureExpression.parallelClosure(closures);
    }

    /**
     * An aggregate is compiled as functional fold operation,
     * described by a zero (initial value), an increment
     * function, and a postprocessing step that makes any necessary conversions.
     * For example, AVG has a zero of (0,0), an increment of (1, value),
     * and a postprocessing step of |a| a.1/a.0.
     * Notice that the DBSP `Fold` structure has a slightly different signature
     * for the increment.
     */
    public static class Implementation extends DBSPNode implements IDBSPInnerNode {
        @Nullable
        public final SqlOperator operator;
        /**
         * Zero of the fold function.
         */
        public final DBSPExpression zero;
        /**
         * A closure with signature |accumulator, value, weight| -> accumulator
         */
        public final DBSPClosureExpression increment;
        /**
         * Function that may post-process the accumulator to produce the final result.
         */
        @Nullable
        public final DBSPClosureExpression postProcess;
        /**
         * Result produced for an empty set (DBSP produces no result in this case).
         */
        public final DBSPExpression emptySetResult;
        /**
         * Name of the Type that implements the semigroup for this operation.
         */
        public final DBSPType semigroup;

        public Implementation(
                @Nullable SqlOperator operator,
                DBSPExpression zero,
                DBSPClosureExpression increment,
                @Nullable
                DBSPClosureExpression postProcess,
                DBSPExpression emptySetResult,
                DBSPType semigroup) {
            super(operator);
            this.operator = operator;
            this.zero = zero;
            this.increment = increment;
            this.postProcess = postProcess;
            this.emptySetResult = emptySetResult;
            this.semigroup = semigroup;
            this.validate();
        }

        public Implementation(
                @Nullable SqlOperator operator,
                DBSPExpression zero,
                DBSPClosureExpression increment,
                DBSPExpression emptySetResult,
                DBSPType semigroup) {
            this(operator, zero, increment, null, emptySetResult, semigroup);
        }

        void validate() {
            if (true)
                return;
            // These validation rules actually don't apply for window-based aggregates.
            // TODO: check them for standard aggregates.
            if (this.postProcess != null) {
                if (!this.emptySetResult.getNonVoidType().sameType(this.postProcess.getResultType()))
                    throw new RuntimeException("Post-process result type " + this.postProcess.getResultType() +
                            " different from empty set type " + this.emptySetResult.getNonVoidType());
            } else {
                if (!this.emptySetResult.getNonVoidType().sameType(this.increment.getResultType())) {
                    throw new RuntimeException("Increment result type " + this.increment.getResultType() +
                            " different from empty set type " + this.emptySetResult.getNonVoidType());
                }
            }
        }

        @Override
        public void accept(InnerVisitor visitor) {
            if (!visitor.preorder(this)) return;
            this.semigroup.accept(visitor);
            this.zero.accept(visitor);
            this.increment.accept(visitor);
            if (this.postProcess != null)
                this.postProcess.accept(visitor);
            this.emptySetResult.accept(visitor);
            visitor.postorder(this);
        }

        public DBSPClosureExpression getPostprocessing() {
            if (this.postProcess != null)
                return this.postProcess;
            DBSPVariablePath var = new DBSPVariablePath("x", Objects.requireNonNull(this.increment.getResultType()));
            return var.closure(var.asParameter());
        }
    }
}
