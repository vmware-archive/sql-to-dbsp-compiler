package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.midend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeInteger;
import org.junit.Test;

/**
 * Test end-to-end by compiling some DDL statements and view
 * queries by compiling them to rust and executing them
 * by inserting data in the input tables and reading data
 * from the declared views.
 */
public class EndToEndTests extends BaseSQLTests {
    void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        DBSPZSetLiteral input = this.createInput();
        super.testQueryBase(query, false, new InputOutputPair(input, expectedOutput));
    }

    @Test
    public void projectTest() {
        String query = "SELECT T.COL3 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(DBSPBoolLiteral.True),
                        new DBSPTupleExpression(DBSPBoolLiteral.False)));
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
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPIntegerLiteral(11, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void negateNullTest() {
        String query = "SELECT -T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPIntegerLiteral(-1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void projectNullTest() {
        String query = "SELECT T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPIntegerLiteral(1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
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
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.False, DBSPBoolLiteral.False),
                new DBSPTupleExpression(DBSPBoolLiteral.False, DBSPBoolLiteral.True),
                new DBSPTupleExpression(DBSPBoolLiteral.True,  DBSPBoolLiteral.False),
                new DBSPTupleExpression(DBSPBoolLiteral.True,  DBSPBoolLiteral.True)));
    }

    @Test
    public void joinNullableTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, this.empty);
    }

    @Test
    public void leftOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 LEFT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.False, DBSPBoolLiteral.None),
                new DBSPTupleExpression(DBSPBoolLiteral.True, DBSPBoolLiteral.None)
        ));
    }

    @Test
    public void rightOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 RIGHT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.False),
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.True)
        ));
    }

    @Test
    public void fullOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 FULL OUTER JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.NullableFalse, DBSPBoolLiteral.None),
                new DBSPTupleExpression(DBSPBoolLiteral.NullableTrue, DBSPBoolLiteral.None),
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.NullableFalse),
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.NullableTrue)
        ));
    }

    @Test
    public void emptyWhereTest() {
        String query = "SELECT * FROM T WHERE FALSE";
        this.testQuery(query, this.empty);
    }

    @Test
    public void whereTest() {
        String query = "SELECT * FROM T WHERE COL3";
        this.testQuery(query, this.z0);
    }

    @Test
    public void whereImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < COL1";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS DOUBLE)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS DOUBLE)";
        this.testQuery(query, this.empty);
    }

    @Test
    public void whereExplicitImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS FLOAT)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitImplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS FLOAT)";
        this.testQuery(query, this.empty);
    }

    @Test
    public void whereExpressionTest() {
        String query = "SELECT * FROM T WHERE COL2 < 0";
        this.testQuery(query, new DBSPZSetLiteral(this.z0.getNonVoidType()));
    }

    @Test
    public void exceptTest() {
        String query = "SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void groupByTest() {
        String query = "SELECT COL1 FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(new DBSPIntegerLiteral(10))));
    }

    @Test
    public void groupByCountTest() {
        String query = "SELECT COL1, COUNT(col2) FROM T GROUP BY COL1, COL3";
        DBSPExpression row =  new DBSPTupleExpression(new DBSPIntegerLiteral(10), new DBSPLongLiteral(1));
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, row, row));
    }

    @Test
    public void divTest() {
        String query = "SELECT T.COL1 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPIntegerLiteral(10, true))));
    }

    @Test
    public void divIntTest() {
        String query = "SELECT T.COL5 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPIntegerLiteral(1, true))));
    }

    // Calcite seems to handle this query incorrectly, since
    // it claims that 1 / 0 is an integer instead of NULL
    //@Test
    @SuppressWarnings("unused")
    public void divZeroTest() {
        String query = "SELECT 1 / 0";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void floatDivTest() {
        String query = "SELECT T.COL6 / T.COL6 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeDouble.instance.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPDoubleLiteral(Double.NaN, true))));
    }

    @Test
    public void aggregateDistinctTest() {
        String query = "SELECT SUM(DISTINCT T.COL1), SUM(T.COL2) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(10, true), new DBSPDoubleLiteral(13.0, true))));
    }

    @Test
    public void aggregateTest() {
        String query = "SELECT SUM(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(20, true))));
    }

    @Test
    public void maxTest() {
        String query = "SELECT MAX(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(10, true))));
    }

    @Test
    public void maxConst() {
        String query = "SELECT MAX(6) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(6, true))));
    }

    @Test
    public void constAggregateExpression() {
        String query = "SELECT 34 / SUM (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(17))));
    }

    @Test
    public void constAggregateExpression2() {
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(34))));
    }

    @Test
    public void constAggregateDoubleExpression() {
        String query = "SELECT 34 / SUM (1), 20 / SUM(2) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(17), new DBSPIntegerLiteral(5))));
    }

    @Test
    public void aggregateFloatTest() {
        String query = "SELECT SUM(T.COL2) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPDoubleLiteral(13.0, true))));
    }

    @Test
    public void optionAggregateTest() {
        String query = "SELECT SUM(T.COL5) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(1, true))));
    }

    @Test
    public void aggregateFalseTest() {
        String query = "SELECT SUM(T.COL1) FROM T WHERE FALSE";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void averageTest() {
        String query = "SELECT AVG(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(10, true))));
    }

    @Test
    public void cartesianTest() {
        String query = "SELECT * FROM T, T AS X";
        DBSPExpression inResult = DBSPTupleExpression.flatten(e0, e0);
        DBSPZSetLiteral result = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, inResult);
        result.add(DBSPTupleExpression.flatten(e0, e1));
        result.add(DBSPTupleExpression.flatten(e1, e0));
        result.add(DBSPTupleExpression.flatten(e1, e1));
        this.testQuery(query, result);
    }

    @Test
    public void foldTest() {
        String query = "SELECT + 91 + NULLIF ( + 93, + 38 )";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                new DBSPIntegerLiteral(184, true))));
    }

    @Test
    public void orderbyTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPVecLiteral(this.e1, this.e0)
        ));
    }

    @Test
    public void orderbyDescendingTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 DESC";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPVecLiteral(this.e0, this.e1)
        ));
    }

    @Test
    public void orderby2Test() {
        String query = "SELECT * FROM T ORDER BY T.COL2, T.COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPVecLiteral(this.e1, this.e0)
        ));
    }
}
