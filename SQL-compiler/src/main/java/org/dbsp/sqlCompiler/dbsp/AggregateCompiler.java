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
    public class Implementation {
        public final DBSPExpression zero;
        public final DBSPExpression increment;
        public final DBSPExpression postprocess;
        public final DBSPVariableReference accumulator;

        public Implementation(
                DBSPExpression zero,
                DBSPExpression increment,
                DBSPExpression postprocess) {
            this.zero = zero;
            this.increment = increment;
            this.postprocess = postprocess;
            this.accumulator = AggregateCompiler.this.accumulator;
        }
    }

    public final DBSPVariableReference accumulator;
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
        this.accumulator = new DBSPVariableReference("r" + aggIndex, this.resultType);
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
        // This can never be null.l
        DBSPExpression zero = new DBSPLiteral(null, this.resultType, "0");
        DBSPExpression increment;
        DBSPExpression argument;
        if (this.call.getArgList().size() == 0) {
            // COUNT(*)
            argument = new DBSPLiteral(null, DBSPTypeInteger.signed64, "1i64");
        } else {
            DBSPExpression agg = this.getAggregatedValue();
            argument = new DBSPApplyExpression("indicator", DBSPTypeInteger.signed32, agg);
        }

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
        this.implementation = new Implementation(zero, increment, ExpressionCompiler.id);
    }

    private DBSPExpression getAggregatedValue() {
        if (this.call.getArgList().size() != 1)
            throw new Unimplemented(this.call);
        int fieldNumber = this.call.getArgList().get(0);
        return new DBSPFieldExpression(null, v, fieldNumber);
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
        DBSPExpression increment = ExpressionCompiler.aggregateOperation(
                call, this.resultType, accumulator, aggregatedValue);
        this.implementation = new Implementation(zero, increment, ExpressionCompiler.id);
    }

    void processSum(SqlSumAggFunction function) {
        DBSPExpression zero = new DBSPLiteral(this.resultType.setMayBeNull(true));
        DBSPExpression increment;
        DBSPExpression aggregatedValue = this.getAggregatedValue();
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
        this.implementation = new Implementation(zero, increment, ExpressionCompiler.id);
    }

    void processAvg(SqlAvgAggFunction function) {
        DBSPExpression zero = new DBSPRawTupleExpression(
                new DBSPLiteral(this.resultType.setMayBeNull(true)),
                new DBSPLiteral(this.resultType.setMayBeNull(true)));
        DBSPType pairType = zero.getNonVoidType();
        DBSPExpression count, sum;

        DBSPExpression aggregatedValue = this.getAggregatedValue();
        DBSPExpression plusOne = new DBSPApplyExpression("indicator", DBSPTypeInteger.signed32, aggregatedValue);
        if (call.isDistinct()) {
            count = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType, accumulator, plusOne);
            sum = ExpressionCompiler.aggregateOperation(
                    "+", resultType, accumulator, aggregatedValue);
        } else {
            count = ExpressionCompiler.aggregateOperation(
                    "+", this.resultType,
                    accumulator, new DBSPApplyMethodExpression("mul_by_ref",
                            DBSPTypeInteger.signed64,
                            plusOne,
                            new DBSPRefExpression(CalciteToDBSPCompiler.weight)));
            sum = ExpressionCompiler.aggregateOperation(
                    "+", resultType,
                    accumulator, new DBSPApplyMethodExpression("mul_by_ref",
                            aggregatedValue.getNonVoidType(),
                            aggregatedValue,
                            new DBSPRefExpression(CalciteToDBSPCompiler.weight)));
        }

        DBSPExpression increment = new DBSPRawTupleExpression(sum, count);
        DBSPExpression a = new DBSPVariableReference("a", pairType);
        DBSPExpression divide = ExpressionCompiler.makeBinaryExpression(
                function, this.resultType, "/",
                Linq.list(new DBSPFieldExpression(null, a, 0),
                        new DBSPFieldExpression(null, a, 1)));
        DBSPExpression closure = new DBSPClosureExpression(null, divide, "a");
        this.implementation = new Implementation(zero, increment, closure);
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
