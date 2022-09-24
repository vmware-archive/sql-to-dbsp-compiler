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

package org.dbsp.sqlCompiler.dbsp;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPLongLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.dbsp.rust.type.IsNumericType;
import org.dbsp.util.ICastable;
import org.dbsp.util.Linq;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Compiles SQL aggregate functions.
 */
public class AggregateCompiler {
    /**
     * An aggregate is compiled as a Rust Fold structure (see the DBSP aggregate).
     * The fold is described by a zero (initial value), an increment
     * function, and a postprocessing step that makes any necessary conversions.
     * For example, AVG has a zero of (0,0), an increment of (1, value),
     * and a postprocessing step of |a| a.1/a.0.
     */
    public static class FoldDescription {
        /**
         * Zero of the fold function.
         */
        public final DBSPExpression zero;
        /**
         * A closure with signature |accum, value, weight| -> accum
         */
        public final DBSPClosureExpression increment;
        /**
         * Function that may postprocess the accumulator to produce the final result.
         */
        @Nullable
        public final DBSPClosureExpression postprocess;
        /**
         * Result produced for an empty set (DBSP produces no result in this case).
         */
        public final DBSPExpression emptySetResult;

        public FoldDescription(
                DBSPExpression zero,
                DBSPClosureExpression increment,
                @Nullable
                DBSPClosureExpression postprocess,
                DBSPExpression emptySetResult) {
            this.zero = zero;
            this.increment = increment;
            this.postprocess = postprocess;
            this.emptySetResult = emptySetResult;
            this.validate();
        }

        public FoldDescription(
                DBSPExpression zero,
                DBSPClosureExpression increment,
                DBSPExpression emptySetResult) {
            this(zero, increment, null, emptySetResult);
        }

        void validate() {
            if (this.postprocess != null) {
                if (!this.emptySetResult.getNonVoidType().same(this.postprocess.getResultType()))
                    throw new RuntimeException("Postprocess result type " + this.postprocess.getResultType() +
                            " different from empty set type " + this.emptySetResult.getNonVoidType());
            } else {
                if (!this.emptySetResult.getNonVoidType().same(this.increment.getResultType())) {
                    throw new RuntimeException("Increment result type " + this.increment.getResultType() +
                            " different from empty set type " + this.emptySetResult.getNonVoidType());
                }
            }
        }
    }

    /**
     * Aggregate that is being compiled.
     */
    public final AggregateCall call;
    /**
     * Type of result expected.
     */
    public final DBSPType resultType;
    /**
     * Almost all aggregates may return nullable results, even if Calcite pretends it's not true.
     */
    public final DBSPType nullableResultType;
    // Deposit compilation result here
    @Nullable
    private FoldDescription foldingFunction;

    /**
     * Expression that stands for the a whole input row in the input zset.
     */
    private final DBSPVariableReference v;

    public AggregateCompiler(
            AggregateCall call, DBSPType resultType,
            DBSPVariableReference v) {
        this.resultType = resultType;
        this.nullableResultType = resultType.setMayBeNull(true);
        this.call = call;
        this.foldingFunction = null;
        this.v = v;
    }

    <T> boolean process(AggregateCall call, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(call.getAggregation(), clazz);
        if (value != null) {
            method.accept(value);
            return true;
        }
        return false;
    }

    /**
     * Given the body of a closure, make a closure with arguments accum, row, weight
     */
    DBSPClosureExpression makeRowClosure(DBSPExpression body, DBSPVariableReference accum) {
        return new DBSPClosureExpression(body,
                accum.asParameter(), this.v.asParameter(), CalciteToDBSPCompiler.weight.asParameter());
    }

    void processCount(SqlCountAggFunction function) {
        // This can never be null.
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression increment;
        DBSPExpression argument;
        DBSPExpression one = this.resultType.to(DBSPTypeInteger.class).getOne();
        if (this.call.getArgList().size() == 0) {
            // COUNT(*)
            argument = one;
        } else {
            DBSPExpression agg = this.getAggregatedValue();
            if (agg.getNonVoidType().mayBeNull)
                argument = new DBSPApplyExpression("indicator", this.resultType.setMayBeNull(false), agg);
            else
                argument = one;
        }

        DBSPVariableReference accum = new DBSPVariableReference("a", this.resultType);
        if (this.call.isDistinct()) {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType, accum, argument);
        } else {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType,
                    accum, new DBSPApplyMethodExpression("mul_by_ref",
                            DBSPTypeInteger.signed64,
                            argument,
                            new DBSPBorrowExpression(CalciteToDBSPCompiler.weight)));
        }
        this.foldingFunction = new FoldDescription(zero, this.makeRowClosure(increment, accum), zero);
    }

    private DBSPExpression getAggregatedValue() {
        if (this.call.getArgList().size() != 1)
            throw new Unimplemented(this.call);
        int fieldNumber = this.call.getArgList().get(0);
        return new DBSPFieldExpression(this.v, fieldNumber);
    }

    private DBSPType getAggregatedValueType() {
        return this.getAggregatedValue().getNonVoidType();
    }

    void processMinMax(SqlMinMaxAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        String call;
        switch (function.getKind()) {
            case MIN:
                call = "min";
                break;
            case MAX:
                call = "max";
                break;
            default:
                throw new Unimplemented(this.call);
        }
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariableReference accum = new DBSPVariableReference("a", this.nullableResultType);
        DBSPExpression increment = ExpressionCompiler.aggregateOperation(
                call, this.nullableResultType, accum, aggregatedValue);
        this.foldingFunction = new FoldDescription(zero, this.makeRowClosure(increment, accum), zero);
    }

    void processSum(SqlSumAggFunction function) {
        DBSPExpression zero = DBSPLiteral.none(this.nullableResultType);
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariableReference accum = new DBSPVariableReference("a", this.nullableResultType);

        if (call.isDistinct()) {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.nullableResultType, accum, aggregatedValue);
        } else {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.nullableResultType,
                    accum, new DBSPApplyMethodExpression("mul_by_ref",
                            aggregatedValue.getNonVoidType(),
                            aggregatedValue,
                            new DBSPBorrowExpression(CalciteToDBSPCompiler.weight)));
        }
        this.foldingFunction = new FoldDescription(zero, this.makeRowClosure(increment, accum), zero);
    }

    void processSumZero(SqlSumEmptyIsZeroAggFunction function) {
        DBSPExpression zero = this.resultType.to(IsNumericType.class).getZero();
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariableReference accum = new DBSPVariableReference("a", this.resultType);

        if (call.isDistinct()) {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType, accum, aggregatedValue);
        } else {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType,
                    accum, new DBSPApplyMethodExpression("mul_by_ref",
                            aggregatedValue.getNonVoidType(),
                            aggregatedValue,
                            new DBSPBorrowExpression(CalciteToDBSPCompiler.weight)));
        }
        this.foldingFunction = new FoldDescription(zero, this.makeRowClosure(increment, accum), zero);
    }

    void processAvg(SqlAvgAggFunction function) {
        DBSPType aggregatedValueType = this.getAggregatedValueType();
        DBSPType i64 = DBSPTypeInteger.signed64.setMayBeNull(true);
        DBSPExpression zero = new DBSPRawTupleExpression(
                DBSPLiteral.none(i64), DBSPLiteral.none(i64));
        DBSPType pairType = zero.getNonVoidType();
        DBSPExpression count, sum;
        DBSPVariableReference accum = new DBSPVariableReference("a", pairType);
        final int sumIndex = 0;
        final int countIndex = 1;
        DBSPExpression countAccumulator = new DBSPFieldExpression(accum, countIndex);
        DBSPExpression sumAccumulator = new DBSPFieldExpression(accum, sumIndex);
        DBSPExpression aggregatedValue = ExpressionCompiler.makeCast(this.getAggregatedValue(), i64);
        DBSPExpression plusOne = new DBSPLongLiteral(1L);
        if (aggregatedValueType.mayBeNull)
            plusOne = new DBSPApplyExpression("indicator", DBSPTypeInteger.signed64, aggregatedValue);
        if (call.isDistinct()) {
            count = ExpressionCompiler.aggregateOperation(
                    "+", i64, countAccumulator, plusOne);
            sum = ExpressionCompiler.aggregateOperation(
                    "+", i64, sumAccumulator, aggregatedValue);
        } else {
            count = ExpressionCompiler.aggregateOperation(
                    "+", i64,
                    countAccumulator, new DBSPApplyMethodExpression("mul_by_ref",
                            DBSPTypeInteger.signed64.setMayBeNull(plusOne.getNonVoidType().mayBeNull),
                            plusOne,
                            new DBSPBorrowExpression(CalciteToDBSPCompiler.weight)));
            sum = ExpressionCompiler.aggregateOperation(
                    "+", i64,
                    sumAccumulator, new DBSPApplyMethodExpression("mul_by_ref",
                            i64,
                            aggregatedValue,
                            new DBSPBorrowExpression(CalciteToDBSPCompiler.weight)));
        }

        DBSPExpression increment = new DBSPRawTupleExpression(sum, count);
        DBSPVariableReference a = new DBSPVariableReference("a", pairType);
        DBSPExpression divide = ExpressionCompiler.makeBinaryExpression(
                function, this.resultType, "/",
                Linq.list(new DBSPFieldExpression(a, sumIndex),
                        new DBSPFieldExpression(a, countIndex)));
        divide = ExpressionCompiler.makeCast(divide, this.nullableResultType);
        DBSPClosureExpression post = new DBSPClosureExpression(
                null, divide, a.asParameter());
        DBSPExpression postZero = DBSPLiteral.none(this.nullableResultType);
        this.foldingFunction = new FoldDescription(zero,
                this.makeRowClosure(increment, accum), post, postZero);
    }

    public FoldDescription compile() {
        boolean success =
                this.process(this.call, SqlCountAggFunction.class, this::processCount) ||
                this.process(this.call, SqlMinMaxAggFunction.class, this::processMinMax) ||
                this.process(this.call, SqlSumAggFunction.class, this::processSum) ||
                this.process(this.call, SqlSumEmptyIsZeroAggFunction.class, this::processSumZero) ||
                this.process(this.call, SqlAvgAggFunction.class, this::processAvg);
        if (!success || this.foldingFunction == null)
            throw new Unimplemented(this.call);
        return this.foldingFunction;
    }
}
