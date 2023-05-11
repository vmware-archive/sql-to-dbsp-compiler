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

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.junit.Test;

/**
 * Test end-to-end by compiling some DDL statements and view
 * queries by compiling them to rust and executing them
 * by inserting data in the input tables and reading data
 * from the declared views.
 */
public class EndToEndTests extends BaseSQLTests {
    /*
     * T is
     *  col1  col2   col3   col4  col5      col6
     * -------------------------------------------
     * | 10 | 12.0 | true  | Hi | NULL    | NULL |
     * | 10 |  1.0 | false | Hi | Some[1] |  0.0 |
     * -------------------------------------------
     *
     * INSERT INTO T VALUES (10, 12, true, 'Hi', NULL, NULL);
     * INSERT INTO T VALUES (10, 1.0, false, 'Hi', 1, 0.0);
     */

    void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        DBSPZSetLiteral input = this.createInput();
        super.testQueryBase(query, false, false, new InputOutputPair(input, expectedOutput));
    }

    @Test
    public void testNullableBoolean() {
        String query = "SELECT T.COL5 > 10 AND T.COL3 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPBoolLiteral.NONE),
                new DBSPTupleExpression(DBSPBoolLiteral.NULLABLE_FALSE)));
    }

    @Test
    public void overTest() {
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(2));
        String query = "SELECT T.COL1, COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(t, t));
    }

    @Test
    public void overSumTest() {
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(13.0));
        String query = "SELECT T.COL1, SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(t, t));
    }

    @Test
    public void testConcat() {
        String query = "SELECT T.COL4 || ' ' || T.COL4 FROM T";
        DBSPExpression lit = new DBSPTupleExpression(new DBSPStringLiteral("Hi Hi"));
        this.testQuery(query, new DBSPZSetLiteral(lit, lit));
    }

    @Test
    public void testArray() {
        String query = "SELECT ELEMENT(ARRAY [2])";
        DBSPZSetLiteral result = new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPI32Literal(2)));
        this.testQuery(query, result);
    }

    @Test
    public void testArrayIndex() {
        String query = "SELECT (ARRAY [2])[1]";
        DBSPZSetLiteral result = new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPI32Literal(2, true)));
        this.testQuery(query, result);
    }

    @Test
    public void testArrayIndexOutOfBounds() {
        String query = "SELECT (ARRAY [2])[3]";
        DBSPZSetLiteral result = new DBSPZSetLiteral(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32)));
        this.testQuery(query, result);
    }

    //@Test
    // Calcite's semantics requires this to crash at runtime;
    // we should change the semantics of ELEMENT
    // to return NULL when the array has more than 1 element.
    public void testArrayElement() {
        String query = "SELECT ELEMENT(ARRAY [2, 3])";
        DBSPZSetLiteral result =
                new DBSPZSetLiteral(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32)));
        this.testQuery(query, result);
    }

    @Test
    public void testConcatNull() {
        String query = "SELECT T.COL4 || NULL FROM T";
        DBSPExpression lit = new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeString.NULLABLE_INSTANCE));
        this.testQuery(query, new DBSPZSetLiteral(lit, lit));
    }

    @Test
    public void overTwiceTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING), " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(t, t));
    }

    @Test
    public void overConstantWindowTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(t, t));
    }

    @Test
    public void overTwiceDifferentTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING), " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(t, t));
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Test
    public void correlatedAggregate() {
        // From: Efficient Incrementialization of Correlated Nested Aggregate
        // Queries using Relative Partial Aggregate Indexes (RPAI)
        // Supun Abeysinghe, Qiyang He, Tiark Rompf, SIGMOD 22
        String query = "SELECT Sum(r.COL1 * r.COL5) FROM T r\n" +
                "WHERE\n" +
                "0.5 * (SELECT Sum(r1.COL5) FROM T r1) =\n" +
                "(SELECT Sum(r2.COL5) FROM T r2 WHERE r2.COL1 = r.COL1)";
        this.testQuery(query, new DBSPZSetLiteral(new DBSPTupleExpression(
                DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));

        // TODO
        query = "SELECT Sum(b.price * b.volume) FROM bids b\n" +
                "WHERE\n" +
                "0.75 * (SELECT Sum(b1.volume) FROM bids b1)\n" +
                "< (SELECT Sum(b2.volume) FROM bids b2\n" +
                "WHERE b2.price <= b.price)";
    }

    @Test
    public void projectTest() {
        String query = "SELECT T.COL3 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(DBSPBoolLiteral.TRUE),
                        new DBSPTupleExpression(DBSPBoolLiteral.FALSE)));
    }

    @Test
    public void intersectTest() {
        String query = "SELECT * FROM T INTERSECT (SELECT * FROM T)";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void plusNullTest() {
        String query = "SELECT T.COL1 + T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPI32Literal(11, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test
    public void negateNullTest() {
        String query = "SELECT -T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPI32Literal(-1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test
    public void projectNullTest() {
        String query = "SELECT T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    // @Test
    // TODO: Crashes the NaiveIncrementalTests.
    // https://github.com/vmware/database-stream-processor/issues/390
    public void idTest() {
        String query = "SELECT * FROM T";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void unionTest() {
        String query = "(SELECT * FROM T) UNION (SELECT * FROM T)";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void unionAllTest() {
        String query = "(SELECT * FROM T) UNION ALL (SELECT * FROM T)";
        DBSPZSetLiteral output = this.createInput();
        output.add(output);
        this.testQuery(query, output);
    }

    @Test
    public void joinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPBoolLiteral.FALSE, DBSPBoolLiteral.FALSE),
                new DBSPTupleExpression(DBSPBoolLiteral.FALSE, DBSPBoolLiteral.TRUE),
                new DBSPTupleExpression(DBSPBoolLiteral.TRUE,  DBSPBoolLiteral.FALSE),
                new DBSPTupleExpression(DBSPBoolLiteral.TRUE,  DBSPBoolLiteral.TRUE)));
    }

    @Test
    public void joinNullableTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, empty);
    }

    @Test
    public void zero() {
        String query = "SELECT 0";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(0))));
    }

    @Test
    public void geoPointTest() {
        String query = "SELECT ST_POINT(0, 0)";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(
                        new DBSPGeoPointLiteral(null,
                                new DBSPDoubleLiteral(0), new DBSPDoubleLiteral(0)).some())));
    }

    @Test
    public void geoDistanceTest() {
        String query = "SELECT ST_DISTANCE(ST_POINT(0, 0), ST_POINT(0,1))";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPDoubleLiteral(1).some())));
    }

    @Test
    public void leftOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 LEFT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPBoolLiteral.FALSE, DBSPBoolLiteral.NONE),
                new DBSPTupleExpression(DBSPBoolLiteral.TRUE, DBSPBoolLiteral.NONE)
        ));
    }

    @Test
    public void rightOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 RIGHT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPBoolLiteral.NONE, DBSPBoolLiteral.FALSE),
                new DBSPTupleExpression(DBSPBoolLiteral.NONE, DBSPBoolLiteral.TRUE)
        ));
    }

    @Test
    public void fullOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 FULL OUTER JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPBoolLiteral.NULLABLE_FALSE, DBSPBoolLiteral.NONE),
                new DBSPTupleExpression(DBSPBoolLiteral.NULLABLE_TRUE, DBSPBoolLiteral.NONE),
                new DBSPTupleExpression(DBSPBoolLiteral.NONE, DBSPBoolLiteral.NULLABLE_FALSE),
                new DBSPTupleExpression(DBSPBoolLiteral.NONE, DBSPBoolLiteral.NULLABLE_TRUE)
        ));
    }

    @Test
    public void emptyWhereTest() {
        String query = "SELECT * FROM T WHERE FALSE";
        this.testQuery(query, empty);
    }

    @Test
    public void whereTest() {
        String query = "SELECT * FROM T WHERE COL3";
        this.testQuery(query, z0);
    }

    @Test
    public void whereImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < COL1";
        this.testQuery(query, z1);
    }

    @Test
    public void whereExplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS DOUBLE)";
        this.testQuery(query, z1);
    }

    @Test
    public void whereExplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS DOUBLE)";
        this.testQuery(query, empty);
    }

    @Test
    public void whereExplicitImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS FLOAT)";
        this.testQuery(query, z1);
    }

    @Test
    public void whereExplicitImplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS FLOAT)";
        this.testQuery(query, empty);
    }

    @Test
    public void whereExpressionTest() {
        String query = "SELECT * FROM T WHERE COL2 < 0";
        this.testQuery(query, DBSPZSetLiteral.emptyWithType(z0.getNonVoidType()));
    }

    @Test
    public void exceptTest() {
        String query = "SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.testQuery(query, z1);
    }

    @Test
    public void constantFoldTest() {
        String query = "SELECT 1 + 2";
        this.testQuery(query, new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPI32Literal(3))));
    }

    @Test
    public void groupByTest() {
        String query = "SELECT COL1 FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(10))));
    }

    @Test
    public void groupByCountTest() {
        String query = "SELECT COL1, COUNT(COL2) FROM T GROUP BY COL1, COL3";
        DBSPExpression row =  new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(1));
        this.testQuery(query, new DBSPZSetLiteral( row, row));
    }

    @Test
    public void groupBySumTest() {
        String query = "SELECT COL1, SUM(COL2) FROM T GROUP BY COL1, COL3";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(1)),
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(12))));
    }

    @Test
    public void divTest() {
        String query = "SELECT T.COL1 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPI32Literal(10, true))));
    }

    @Test
    public void divIntTest() {
        String query = "SELECT T.COL5 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPI32Literal(1, true))));
    }

    @Test
    public void divZeroTest() {
        String query = "SELECT 1 / 0";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test
    public void nestedDivTest() {
        String query = "SELECT 2 / (1 / 0)";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test
    public void customDivisionTest() {
        // Use a custom division operator.
        String query = "SELECT DIVISION(1, 0)";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test
    public void floatDivTest() {
        String query = "SELECT T.COL6 / T.COL6 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeDouble.NULLABLE_INSTANCE)),
                new DBSPTupleExpression(new DBSPDoubleLiteral(Double.NaN, true))));
    }

    @Test
    public void aggregateDistinctTest() {
        String query = "SELECT SUM(DISTINCT T.COL1), SUM(T.COL2) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(10, true), new DBSPDoubleLiteral(13.0, true))));
    }

    @Test
    public void aggregateTest() {
        String query = "SELECT SUM(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(20, true))));
    }

    @Test
    public void maxTest() {
        String query = "SELECT MAX(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(10, true))));
    }

    @Test
    public void maxConst() {
        String query = "SELECT MAX(6) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(6, true))));
    }

    @Test
    public void constAggregateExpression() {
        String query = "SELECT 34 / SUM (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(17, true))));
    }

    @Test
    public void inTest() {
        String query = "SELECT 3 in (SELECT COL5 FROM T)";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeBool.NULLABLE_INSTANCE))));
    }

    @Test
    public void constAggregateExpression2() {
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(34, true))));
    }

    @Test
    public void constAggregateDoubleExpression() {
        String query = "SELECT 34 / SUM (1), 20 / SUM(2) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(17, true), new DBSPI32Literal(5, true))));
    }

    @Test
    public void aggregateFloatTest() {
        String query = "SELECT SUM(T.COL2) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPDoubleLiteral(13.0, true))));
    }

    @Test
    public void optionAggregateTest() {
        String query = "SELECT SUM(T.COL5) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(1, true))));
    }

    @Test
    public void aggregateFalseTest() {
        String query = "SELECT SUM(T.COL1) FROM T WHERE FALSE";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test
    public void averageTest() {
        String query = "SELECT AVG(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(10, true))));
    }

    @Test
    public void cartesianTest() {
        String query = "SELECT * FROM T, T AS X";
        DBSPExpression inResult = DBSPTupleExpression.flatten(e0, e0);
        DBSPZSetLiteral result = new DBSPZSetLiteral( inResult);
        result.add(DBSPTupleExpression.flatten(e0, e1));
        result.add(DBSPTupleExpression.flatten(e1, e0));
        result.add(DBSPTupleExpression.flatten(e1, e1));
        this.testQuery(query, result);
    }

    @Test
    public void foldTest() {
        String query = "SELECT + 91 + NULLIF ( + 93, + 38 )";
        this.testQuery(query, new DBSPZSetLiteral(
                 new DBSPTupleExpression(
                new DBSPI32Literal(184, true))));
    }

    @Test
    public void orderbyTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPVecLiteral(e1, e0)
        ));
    }

    @Test
    public void orderbyDescendingTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 DESC";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPVecLiteral(e0, e1)
        ));
    }

    @Test
    public void orderby2Test() {
        String query = "SELECT * FROM T ORDER BY T.COL2, T.COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPVecLiteral(e1, e0)
        ));
    }
}
