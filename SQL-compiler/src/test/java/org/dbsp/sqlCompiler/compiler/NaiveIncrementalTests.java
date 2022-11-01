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

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.expression.DBSPSomeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Test;

// Runs the EndToEnd tests but on an input stream with 3 elements each and
// using an incremental non-optimized circuit.
public class NaiveIncrementalTests extends EndToEndTests {
    public void invokeTestQueryBase(String query, InputOutputPair... streams) {
        super.testQueryBase(query, true, false, streams);
    }

    @Override
    void testQuery(String query,
                   DBSPZSetLiteral firstOutput) {
        DBSPZSetLiteral input = this.createInput();
        DBSPZSetLiteral secondOutput = new DBSPZSetLiteral(firstOutput.getNonVoidType());
        DBSPZSetLiteral thirdOutput = secondOutput.minus(firstOutput);
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, firstOutput),
                // Add an empty input
                new InputOutputPair(empty, secondOutput),
                // Subtract the first input
                new InputOutputPair(input.negate(), thirdOutput)
        );
    }

    void testConstantOutput(String query,
                            DBSPZSetLiteral output) {
        DBSPZSetLiteral input = this.createInput();
        DBSPZSetLiteral e = new DBSPZSetLiteral(output.getNonVoidType());
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, output),
                // Add an empty input
                new InputOutputPair(NaiveIncrementalTests.empty, e),
                // Subtract the first input
                new InputOutputPair(input.negate(), e)
        );
    }

    void testAggregate(String query,
                       DBSPZSetLiteral firstOutput,
                       DBSPZSetLiteral outputForEmptyInput) {
        DBSPZSetLiteral input = this.createInput();
        DBSPZSetLiteral secondOutput = new DBSPZSetLiteral(firstOutput.getNonVoidType());
        DBSPZSetLiteral thirdOutput = outputForEmptyInput.minus(firstOutput);
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, firstOutput),
                // Add an empty input
                new InputOutputPair(empty, secondOutput),
                // Subtract the first input
                new InputOutputPair(input.negate(), thirdOutput)
        );
    }

    @Test @Override
    public void zero() {
        String query = "SELECT 0";
        DBSPZSetLiteral result = new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPIntegerLiteral(0)));
        this.testConstantOutput(query, result);
    }

    @Test @Override
    public void customDivisionTest() {
        // Use a custom division operator.
        String query = "SELECT DIVISION(1, 0)";
        this.testConstantOutput(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test @Override
    public void inTest() {
        String query = "SELECT 3 in (SELECT COL5 FROM T)";
        this.testAggregate(query,
                new DBSPZSetLiteral(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeBool.instance.setMayBeNull(true)))),
                new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPBoolLiteral(false, true)))
        );
    }

    @Test
    public void divZeroTest() {
        String query = "SELECT 1 / 0";
        this.testConstantOutput(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test @Override
    public void geoPointTest() {
        String query = "SELECT ST_POINT(0, 0)";
        this.testConstantOutput(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(
                        new DBSPSomeExpression(new DBSPGeoPointLiteral(null,
                                new DBSPDoubleLiteral(0), new DBSPDoubleLiteral(0))))));
    }

    @Override @Test
    public void geoDistanceTest() {
        String query = "SELECT ST_DISTANCE(ST_POINT(0, 0), ST_POINT(0,1))";
        this.testConstantOutput(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(
                        new DBSPSomeExpression(new DBSPDoubleLiteral(1)))));
    }

    @Test @Override
    public void foldTest() {
        String query = "SELECT + 91 + NULLIF ( + 93, + 38 )";
        DBSPZSetLiteral result = new DBSPZSetLiteral(
                new DBSPTupleExpression(
                new DBSPIntegerLiteral(184, true)));
        this.testConstantOutput(query, result);
    }

    @Test @Override
    public void aggregateFalseTest() {
        String query = "SELECT SUM(T.COL1) FROM T WHERE FALSE";
        DBSPZSetLiteral result = new DBSPZSetLiteral(
                new DBSPTupleExpression(
                DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true))));
        this.testConstantOutput(query, result);
    }

    @Test @Override
    public void constAggregateDoubleExpression() {
        String query = "SELECT 34 / SUM (1), 20 / SUM(2) FROM T GROUP BY COL1";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(
                                new DBSPIntegerLiteral(17, true),
                                new DBSPIntegerLiteral(5, true))));
    }

    @Test @Override
    public void constAggregateExpression2() {
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPIntegerLiteral(34, true))));
    }

    @Test @Override
    public void maxConst() {
        String query = "SELECT MAX(6) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPIntegerLiteral(6, true))),
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test @Override
    public void maxTest() {
        String query = "SELECT MAX(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPIntegerLiteral(10, true))),
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test @Override
    public void averageTest() {
        String query = "SELECT AVG(T.COL1) FROM T";
        DBSPZSetLiteral output = new DBSPZSetLiteral(
                new DBSPTupleExpression(
                new DBSPIntegerLiteral(10, true)));
        this.testAggregate(query, output, new DBSPZSetLiteral(
                output.zsetType.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test @Override
    public void aggregateFloatTest() {
        String query = "SELECT SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeDouble.instance.setMayBeNull(true)))));
    }

    @Test @Override
    public void optionAggregateTest() {
        String query = "SELECT SUM(T.COL5) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPIntegerLiteral(1, true))),
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test @Override
    public void aggregateTest() {
        String query = "SELECT SUM(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPIntegerLiteral(20, true))),
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test @Override
    public void aggregateDistinctTest() {
        String query = "SELECT SUM(DISTINCT T.COL1), SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(
                                new DBSPIntegerLiteral(10, true),
                                new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(
                                DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)),
                                DBSPLiteral.none(DBSPTypeDouble.instance.setMayBeNull(true)))));
    }
}
