/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.postgres;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.util.Linq;
import org.junit.Test;

/**
 * Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/date.out
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresDateTests extends BaseSQLTests {
    static DBSPType nullableDate = DBSPTypeDate.instance.setMayBeNull(true);

    public DBSPCompiler compileQuery(String query, boolean optimize) throws SqlParseException {
        String data = "CREATE TABLE DATE_TBL (f1 date);\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-04-09');\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-06-13');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-28');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-29');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-02-28');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-02-29');\n" +    // illegal date
                "INSERT INTO DATE_TBL VALUES ('1997-03-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-03-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-03');\n" +
                "INSERT INTO DATE_TBL VALUES ('2038-04-08');\n" +
                "INSERT INTO DATE_TBL VALUES ('2039-04-09');\n" +
                "INSERT INTO DATE_TBL VALUES ('2040-04-10');\n";
                // Calcite does not seem to support dates BC
                //"INSERT INTO DATE_TBL VALUES ('2040-04-10 BC');";
        CompilerOptions options = new CompilerOptions();
        options.optimizerOptions.noOptimizations = !optimize;
        DBSPCompiler compiler = new DBSPCompiler(options);
        // So that queries that do not depend on the input still
        // have circuits with inputs.
        compiler.setGenerateInputsFromTables(true);
        compiler.compileStatements(data);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String query, DBSPZSetLiteral expectedOutput, boolean optimize) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCompiler compiler = this.compileQuery(query, optimize);
            DBSPCircuit circuit = getCircuit(compiler);
            DBSPZSetLiteral input = compiler.getTableContents().getTableContents("DATE_TBL");
            InputOutputPair streams = new InputOutputPair(input, expectedOutput);
            this.addRustTestCase(circuit, streams);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    void testQueryTwice(String query, long expectedValue) {
        // by running with optimizations disabled we test runtime evaluation
        DBSPZSetLiteral zset = new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPI64Literal(expectedValue)));
        this.testQuery(query, zset, false);
        this.testQuery(query, zset, true);
    }

    static DBSPZSetLiteral makeSet(String[] data, boolean nullable) {
        DBSPType type = nullable ? nullableDate : DBSPTypeDate.instance;
        DBSPZSetLiteral result = new DBSPZSetLiteral(
                TypeCompiler.makeZSet(
                        new DBSPTypeTuple(type)));
        for (String s: data) {
            DBSPExpression value;
            if (s == null)
                value = DBSPLiteral.none(type);
            else
                value = new DBSPDateLiteral(s, nullable);
            result.add(new DBSPTupleExpression(value));
        }
        return result;
    }

    @Test
    public void testSelect() {
        String query = "SELECT f1 FROM DATE_TBL";
        String[] results = {
                "1957-04-09",
                "1957-06-13",
                "1996-02-28",
                "1996-02-29",
                "1996-03-01",
                "1996-03-02",
                "1997-02-28",
                "1997-03-01",
                "1997-03-02",
                "2000-04-01",
                "2000-04-02",
                "2000-04-03",
                "2038-04-08",
                "2039-04-09",
                "2040-04-10",
                null
                //"2040-04-10 BC"
        };
        DBSPZSetLiteral result = makeSet(results, true);
        this.testQuery(query, result, true);
    }

    @Test
    public void testLt() {
        String query = "SELECT f1 FROM DATE_TBL WHERE f1 < '2000-01-01'";
        String[] results = {
                "1957-04-09",
                "1957-06-13",
                "1996-02-28",
                "1996-02-29",
                "1996-03-01",
                "1996-03-02",
                "1997-02-28",
                "1997-03-01",
                "1997-03-02"
        };
        DBSPZSetLiteral result = makeSet(results, true);
        this.testQuery(query, result, true);
    }

    @Test
    public void testBetween() {
        String query = "SELECT f1 FROM DATE_TBL\n" +
                "  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01'";
        String[] results = {
                "2000-04-01",
                "2000-04-02",
                "2000-04-03"
        };
        DBSPZSetLiteral result = makeSet(results, true);
        this.testQuery(query, result, true);
    }

    @Test
    public void testConstant() {
        String query = "SELECT date '1999-01-08'";
        String[] results = new String[] { "1999-01-08" };
        DBSPZSetLiteral result = makeSet(results, false);
        this.testQuery(query, result, false);
        this.testQuery(query, result, true);
    }

    @Test
    public void testDiff() {
        String query = "SELECT (f1 - date '2000-01-01') day AS \"Days From 2K\" FROM DATE_TBL";
        Long[] results = {
                -15607L,
                -15542L,
                -1403L,
                -1402L,
                -1401L,
                -1400L,
                -1037L,
                -1036L,
                -1035L,
                91L,
                92L,
                93L,
                13977L,
                14343L,
                14710L,
                //-1475115L
        };
        DBSPZSetLiteral result =
                new DBSPZSetLiteral(Linq.map(results,
                        l -> new DBSPTupleExpression(new DBSPIntervalMillisLiteral(
                                l * 86400 * 1000, true)), DBSPExpression.class));
        result.add(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeMillisInterval.instance.setMayBeNull(true))));
        this.testQuery(query, result, true);
    }

    int dow(int value) {
        // This is an adjustment from the Postgres values
        return value + CalciteToDBSPCompiler.firstDOW;
    }

    @Test
    public void testParts() {
        Object[][] data = {
                { "1957-04-09"   ,  1957,     4,   9,       2,    195,      20,          2,    1957,   15,   dow(2),      2,  99,    -401760000L },
                { "1957-06-13"   ,  1957,     6,  13,       2,    195,      20,          2,    1957,   24,   dow(4),      4, 164,    -396144000L },
                { "1996-02-28"   ,  1996,     2,  28,       1,    199,      20,          2,    1996,    9,   dow(3),      3,  59,     825465600L },
                { "1996-02-29"   ,  1996,     2,  29,       1,    199,      20,          2,    1996,    9,   dow(4),      4,  60,     825552000L },
                { "1996-03-01"   ,  1996,     3,   1,       1,    199,      20,          2,    1996,    9,   dow(5),      5,  61,     825638400L },
                { "1996-03-02"   ,  1996,     3,   2,       1,    199,      20,          2,    1996,    9,   dow(6),      6,  62,     825724800L },
                { "1997-02-28"   ,  1997,     2,  28,       1,    199,      20,          2,    1997,    9,   dow(5),      5,  59,     857088000L },
                { "1997-03-01"   ,  1997,     3,   1,       1,    199,      20,          2,    1997,    9,   dow(6),      6,  60,     857174400L },
                { "1997-03-02"   ,  1997,     3,   2,       1,    199,      20,          2,    1997,    9,   dow(0),      7,  61,     857260800L },
                { "2000-04-01"   ,  2000,     4,   1,       2,    200,      20,          2,    2000,   13,   dow(6),      6,  92,     954547200L },
                { "2000-04-02"   ,  2000,     4,   2,       2,    200,      20,          2,    2000,   13,   dow(0),      7,  93,     954633600L },
                { "2000-04-03"   ,  2000,     4,   3,       2,    200,      20,          2,    2000,   14,   dow(1),      1,  94,     954720000L },
                { "2038-04-08"   ,  2038,     4,   8,       2,    203,      21,          3,    2038,   14,   dow(4),      4,  98,    2154297600L },
                { "2039-04-09"   ,  2039,     4,   9,       2,    203,      21,          3,    2039,   14,   dow(6),      6,  99,    2185920000L },
                { "2040-04-10"   ,  2040,     4,  10,       2,    204,      21,          3,    2040,   15,   dow(2),      2, 101,    2217628800L },
              //{ "04-10-2040 BC", -2040,     4,  10,       2,   -204,     -21,         -3,   -2040,   15,   dow(1),      1, 100, -126503251200L }
        };
        String query = "SELECT f1 as \"date\",\n" +
                "    EXTRACT(YEAR from f1) AS 'year',\n" +
                "    EXTRACT(month from f1) AS 'month',\n" +
                "    EXTRACT(day from f1) AS 'day',\n" +
                "    EXTRACT(quarter from f1) AS 'quarter',\n" +
                "    EXTRACT(decade from f1) AS 'decade',\n" +
                "    EXTRACT(century from f1) AS 'century',\n" +
                "    EXTRACT(millennium from f1) AS 'millennium',\n" +
                "    EXTRACT(isoyear from f1) AS 'isoyear',\n" +
                "    EXTRACT(week from f1) AS 'week',\n" +
                "    EXTRACT(dow from f1) AS 'dow',\n" +
                "    EXTRACT(isodow from f1) AS 'isodow',\n" +
                "    EXTRACT(doy from f1) AS 'doy',\n" +
                "    EXTRACT(epoch from f1) AS 'epoch'\n" +
                "    FROM DATE_TBL";
        DBSPExpression[] exprs = new DBSPExpression[data.length + 1];
        for (int i = 0; i < data.length; i++) {
            Object[] row = data[i];
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPDateLiteral((String)row[0], true),
                    new DBSPI64Literal((Integer)row[1], true),
                    new DBSPI64Literal((Integer)row[2], true),
                    new DBSPI64Literal((Integer)row[3], true),
                    new DBSPI64Literal((Integer)row[4], true),
                    new DBSPI64Literal((Integer)row[5], true),
                    new DBSPI64Literal((Integer)row[6], true),
                    new DBSPI64Literal((Integer)row[7], true),
                    new DBSPI64Literal((Integer)row[8], true),
                    new DBSPI64Literal((Integer)row[9], true),
                    new DBSPI64Literal((Integer)row[10], true),
                    new DBSPI64Literal((Integer)row[11], true),
                    new DBSPI64Literal((Integer)row[12], true),
                    new DBSPI64Literal((Long)row[13], true)
            );
            exprs[i] = tuple;
        }
        DBSPExpression ni = DBSPLiteral.none(DBSPTypeInteger.signed64.setMayBeNull(true));
        exprs[data.length] = new DBSPTupleExpression(DBSPLiteral.none(nullableDate), ni, ni, ni, ni, ni, ni, ni, ni, ni, ni, ni, ni, ni);
        this.testQuery(query, new DBSPZSetLiteral(exprs), true);
    }

    @Test
    public void testEpoch() {
        String query = "SELECT EXTRACT(EPOCH FROM DATE '1970-01-01')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testCentury() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '0001-01-01')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testCentury1() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '1900-12-31')";
        this.testQueryTwice(query, 19);
    }

    @Test
    public void testCentury2() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '1901-01-01')";
        this.testQueryTwice(query, 20);
    }

    @Test
    public void testCentury3() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '2000-12-31')";
        this.testQueryTwice(query, 20);
    }

    @Test
    public void testCentury4() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '2001-01-01')";
        this.testQueryTwice(query, 21);
    }

    @Test
    public void testMillennium() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '0001-01-01')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testMillenium1() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '1000-12-31')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testMillennium2() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '2000-12-31')";
        this.testQueryTwice(query, 2);
    }

    @Test
    public void testMillenium3() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '2001-01-01')";
        this.testQueryTwice(query, 3);
    }

    @Test
    public void testDecade() {
        String query = "SELECT EXTRACT(DECADE FROM DATE '1994-12-25')";
        this.testQueryTwice(query, 199);
    }

    @Test
    public void testDecade1() {
        String query = "SELECT EXTRACT(DECADE FROM DATE '0010-01-01')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testDecade2() {
        String query = "SELECT EXTRACT(DECADE FROM DATE '0009-12-31')";
        this.testQueryTwice(query, 0);
    }

    @Test(expected = RuntimeException.class)
    public void testMicroseconds() {
        String query = "SELECT EXTRACT(MICROSECONDS  FROM DATE '2020-08-11')";
        this.testQuery(query, new DBSPZSetLiteral(TypeCompiler.makeZSet(new DBSPTypeTuple())), true);
    }

    @Test(expected = RuntimeException.class)
    public void testMillseconds() {
        String query = "SELECT EXTRACT(MILLISECONDS  FROM DATE '2020-08-11')";
        this.testQuery(query, new DBSPZSetLiteral(TypeCompiler.makeZSet(new DBSPTypeTuple())), true);
    }

    @Test
    public void testSeconds() {
        // TODO: This fails in Postgres
        String query = "SELECT EXTRACT(SECOND        FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testSeconds0() {
        String query = "SELECT SECOND(DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testMinutes() {
        // TODO: This fails in Postgres
        String query = "SELECT EXTRACT(MINUTE FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testMinutes1() {
        // TODO: This fails in Postgres
        String query = "SELECT MINUTE(DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testHour() {
        // TODO: This fails in Postgres
        String query = "SELECT EXTRACT(HOUR          FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testHour1() {
        String query = "SELECT HOUR(DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testDay() {
        String query = "SELECT EXTRACT(DAY           FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 11);
    }

    @Test
    public void testDay1() {
        String query = "SELECT DAYOFMONTH(DATE '2020-08-11')";
        this.testQueryTwice(query, 11);
    }

    @Test
    public void testMonth() {
        String query = "SELECT EXTRACT(MONTH         FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 8);
    }

    @Test
    public void testMonth1() {
        String query = "SELECT MONTH(DATE '2020-08-11')";
        this.testQueryTwice(query, 8);
    }

    @Test
    public void testYear() {
        String query = "SELECT EXTRACT(YEAR          FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 2020);
    }

    @Test
    public void testYear1() {
        String query = "SELECT YEAR(DATE '2020-08-11')";
        this.testQueryTwice(query, 2020);
    }

    @Test
    public void testDecade5() {
        String query = "SELECT EXTRACT(DECADE        FROM DATE '2020-08-11')";
        this.testQueryTwice(query,202);
    }

    @Test
    public void testCentury5() {
        String query = "SELECT EXTRACT(CENTURY       FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 21);
    }

    @Test
    public void testMillennium5() {
        String query = "SELECT EXTRACT(MILLENNIUM    FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 3);
    }

    @Test
    public void testIsoYear() {
        String query = "SELECT EXTRACT(ISOYEAR       FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 2020);
    }

    @Test
    public void testQuarter() {
        String query = "SELECT EXTRACT(QUARTER       FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 3);
    }

    @Test
    public void testWeek() {
        String query = "SELECT EXTRACT(WEEK          FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 33);
    }

    @Test
    public void testDow() {
        String query = "SELECT EXTRACT(DOW           FROM DATE '2020-08-11')";
        this.testQueryTwice(query, dow(2));
    }

    @Test
    public void testDow2() {
        String query = "SELECT DAYOFWEEK(DATE '2020-08-11')";
        this.testQueryTwice(query, dow(2));
    }

    /*
     * TODO Postgres actually returns 0 for this query, but the Calcite optimizer
     * folds this to 1. Same for previous test.
     */
    @Test
    public void testDow1() {
        // Sunday
        String query = "SELECT EXTRACT(DOW FROM DATE '2020-08-16')";
        this.testQueryTwice(query, dow(0));
    }

    @Test
    public void testIsoDow() {
        String query = "SELECT EXTRACT(ISODOW FROM DATE '2020-08-16')";
        this.testQueryTwice(query, 7);
    }

    @Test
    public void testDoy() {
        String query = "SELECT EXTRACT(DOY           FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 224);
    }

    @Test(expected = RuntimeException.class)
    // Calcite does not seem to know about DATE_TRUNC
    public void testDateTrunc() {
        String query = "SELECT DATE_TRUNC('MILLENNIUM', DATE '1970-03-20')";
        this.testQueryTwice(query, 0);
    }

    @Test(expected = RuntimeException.class)
    // Calcite does not seem to support infinity dates
    public void testInfinity() {
        String query = "SELECT DATE 'infinity'";
        this.testQueryTwice(query, 0);
    }
}
