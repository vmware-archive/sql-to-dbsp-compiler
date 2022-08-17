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
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeInteger;
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
     * An aggregate is compiled as a fold followed by postporocessing.
     * The fold is described by a zero (initial value), and an increment
     * function.  The postprocessing step makes any necessary conversions.
     * For example, AVG has a zero of (0,0), an increment of (1, value),
     * and a postprocessing step of sum/count.
     */
    public static class Implementation {
        public final DBSPExpression zero;
        public final DBSPExpression increment;
        public final DBSPVariableReference accumulator;
        @Nullable
        public final DBSPClosureExpression postprocess;
        /**
         * Zero produced by postprocessing for an empty set.
         */
        public final DBSPExpression postZero;

        public Implementation(
                DBSPExpression zero,
                DBSPExpression increment,
                DBSPVariableReference accumulator,
                @Nullable
                DBSPClosureExpression postprocess,
                @Nullable
                DBSPExpression postZero) {
            this.accumulator = accumulator;
            this.zero = zero;
            this.increment = increment;
            this.postprocess = postprocess;
            if (postZero != null)
                this.postZero = postZero;
            else
                this.postZero = zero;
        }

        public Implementation(
                DBSPExpression zero,
                DBSPExpression increment,
                DBSPVariableReference accumulator) {
            this(zero, increment, accumulator, null, null);
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
     * The AggregateCompiler may be invoked multiple times
     * for SQL queries that contain multiple aggregates.
     * This is an index which helps the compiler generate unique names.
     * For example SELECT COUNT(*), SUM(t.f) will cause
     * one invocation for COUNT and one for SUM, with different indexes.
     */
    public final int aggIndex;

    // Deposit compilation result here
    @Nullable
    private Implementation implementation;

    /**
     * Expression that stands for the a whole input row in the input zset.
     */
    private final DBSPVariableReference v;

    public AggregateCompiler(AggregateCall call, DBSPType resultType, int aggIndex, DBSPVariableReference v) {
        this.resultType = resultType;
        this.call = call;
        this.implementation = null;
        this.aggIndex = aggIndex;
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

    void processCount(SqlCountAggFunction function) {
        // This can never be null.
        DBSPExpression zero = new DBSPLiteral(null, this.resultType, "0");
        DBSPExpression increment;
        DBSPExpression argument;
        if (this.call.getArgList().size() == 0) {
            // COUNT(*)
            argument = new DBSPLiteral(1);
        } else {
            DBSPExpression agg = this.getAggregatedValue();
            if (agg.getNonVoidType().mayBeNull)
                argument = new DBSPApplyExpression("indicator", this.resultType.setMayBeNull(false), agg);
            else
                argument = new DBSPLiteral(1);
        }

        DBSPVariableReference accumulator= new DBSPVariableReference("r" + aggIndex, this.resultType);
        if (this.call.isDistinct()) {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType, accumulator, argument);
        } else {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType,
                    accumulator, new DBSPApplyMethodExpression("mul_by_ref",
                            DBSPTypeInteger.signed64,
                            argument,
                            new DBSPRefExpression(CalciteToDBSPCompiler.weight)));
        }
        this.implementation = new Implementation(zero, increment, accumulator);
    }

    private DBSPExpression getAggregatedValue() {
        if (this.call.getArgList().size() != 1)
            throw new Unimplemented(this.call);
        int fieldNumber = this.call.getArgList().get(0);
        return new DBSPFieldExpression(null, this.v, fieldNumber);
    }

    private DBSPType getAggregatedValueType() {
        return this.getAggregatedValue().getNonVoidType();
    }

    void processMinMax(SqlMinMaxAggFunction function) {
        DBSPExpression zero = new DBSPLiteral(this.resultType.setMayBeNull(true));
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
        DBSPVariableReference accumulator= new DBSPVariableReference("r" + aggIndex, this.resultType);
        DBSPExpression increment = ExpressionCompiler.aggregateOperation(
                call, this.resultType, accumulator, aggregatedValue);
        this.implementation = new Implementation(zero, increment, accumulator);
    }

    void processSum(SqlSumAggFunction function) {
        DBSPExpression zero = new DBSPLiteral(this.resultType.setMayBeNull(true));
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPVariableReference accumulator= new DBSPVariableReference("r" + aggIndex, this.resultType);
        if (call.isDistinct()) {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", resultType, accumulator, aggregatedValue);
        } else {
            increment = ExpressionCompiler.aggregateOperation(
                    "+", resultType,
                    accumulator, new DBSPApplyMethodExpression("mul_by_ref",
                            aggregatedValue.getNonVoidType(),
                            aggregatedValue,
                            new DBSPRefExpression(CalciteToDBSPCompiler.weight)));
        }
        this.implementation = new Implementation(zero, increment, accumulator);
    }

    void processAvg(SqlAvgAggFunction function) {
        DBSPType aggregatedValueType = this.getAggregatedValueType();
        DBSPType i64 = DBSPTypeInteger.signed64.setMayBeNull(true);
        DBSPExpression zero = new DBSPRawTupleExpression(new DBSPLiteral(i64), new DBSPLiteral(i64));
        DBSPType pairType = zero.getNonVoidType();
        DBSPExpression count, sum;
        DBSPVariableReference accumulator = new DBSPVariableReference("r" + aggIndex, zero.getNonVoidType());
        DBSPExpression countAccumulator = new DBSPFieldExpression(null, accumulator, 0);
        DBSPExpression sumAccumulator = new DBSPFieldExpression(null, accumulator, 1);
        DBSPExpression aggregatedValue = ExpressionCompiler.makeCast(this.getAggregatedValue(), i64);
        DBSPExpression plusOne = new DBSPLiteral(1L);
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
                            new DBSPRefExpression(CalciteToDBSPCompiler.weight)));
            sum = ExpressionCompiler.aggregateOperation(
                    "+", i64,
                    sumAccumulator, new DBSPApplyMethodExpression("mul_by_ref",
                            i64,
                            aggregatedValue,
                            new DBSPRefExpression(CalciteToDBSPCompiler.weight)));
        }

        DBSPExpression increment = new DBSPRawTupleExpression(sum, count);
        DBSPExpression a = new DBSPVariableReference("a", pairType);
        DBSPExpression divide = ExpressionCompiler.makeBinaryExpression(
                function, this.resultType, "/",
                Linq.list(new DBSPFieldExpression(null, a, 0),
                        new DBSPFieldExpression(null, a, 1)));
        divide = ExpressionCompiler.makeCast(divide, this.resultType);
        DBSPClosureExpression closure = new DBSPClosureExpression(
                null, divide, new DBSPClosureExpression.Parameter("a", pairType));
        DBSPExpression postZero = new DBSPLiteral(this.resultType.setMayBeNull(true));
        this.implementation = new Implementation(zero, increment, accumulator, closure, postZero);
    }

    public Implementation compile() {
        boolean success =
                this.process(this.call, SqlCountAggFunction.class, this::processCount) ||
                this.process(this.call, SqlMinMaxAggFunction.class, this::processMinMax) ||
                this.process(this.call, SqlSumAggFunction.class, this::processSum) ||
                this.process(this.call, SqlAvgAggFunction.class, this::processAvg);
        if (!success || this.implementation == null)
            throw new Unimplemented(this.call);
        return this.implementation;
    }
}
