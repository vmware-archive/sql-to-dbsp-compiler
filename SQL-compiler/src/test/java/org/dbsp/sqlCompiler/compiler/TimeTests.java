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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.junit.Test;

public class TimeTests extends BaseSQLTests {
    @Override
    public DBSPCompiler compileQuery(String query) {
        DBSPCompiler compiler = testCompiler();
        compiler.setGenerateInputsFromTables(true);
        String ddl = "CREATE TABLE T (\n" +
                "COL1 TIMESTAMP NOT NULL" +
                ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String query, DBSPExpression... fields) {
        // T contains a date with timestamp '100'.
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query);
        compiler.throwOnError();
        DBSPCircuit circuit = getCircuit(compiler);
        DBSPZSetLiteral expectedOutput = new DBSPZSetLiteral(new DBSPTupleExpression(fields));
        InputOutputPair streams = new InputOutputPair(this.createInput(), expectedOutput);
        this.addRustTestCase(circuit, streams);
    }

    @Override
    DBSPZSetLiteral createInput() {
        return new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPTimestampLiteral(100)));
    }

    @Test
    public void timestampTableTest() {
        String query = "SELECT COL1 FROM T";
        this.testQuery(query, new DBSPTimestampLiteral(100));
    }

    @Test
    public void castTimestampToString() {
        String query = "SELECT CAST(T.COL1 AS STRING) FROM T";
        this.testQuery(query, new DBSPStringLiteral("1970-01-01 00:00:00"));
    }

    @Test
    public void castTimestampToStringToTimestamp() {
        String query = "SELECT CAST(CAST(T.COL1 AS STRING) AS Timestamp) FROM T";
        this.testQuery(query, new DBSPTimestampLiteral(0));
    }

    @Test
    public void secondTest() {
        String query = "SELECT SECOND(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(0));
    }

    @Test
    public void minuteTest() {
        String query = "SELECT MINUTE(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(0));
    }

    @Test
    public void hourTest() {
        String query = "SELECT HOUR(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(0));
    }

    @Test
    public void dayTest() {
        String query = "SELECT DAYOFMONTH(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(1));
    }

    @Test
    public void dayOfWeekTest() {
        String query = "SELECT DAYOFWEEK(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(5));
    }

    @Test
    public void monthTest() {
        String query = "SELECT MONTH(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(1));
    }

    @Test
    public void yearTest() {
        String query = "SELECT YEAR(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(1970));
    }

    @Test
    public void dowTest() {
        String query = "SELECT extract (ISODOW from COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(4));
        // We know that the next date was a Thursday
        query = "SELECT extract (ISODOW from TIMESTAMP '2022-12-15')";
        this.testQuery(query, new DBSPI64Literal(4));
    }

    @Test
    public void timestampAddTableTest() {
        String query =
                "SELECT TIMESTAMPADD(SECOND, 10, COL1), " +
                " TIMESTAMPADD(HOUR, 1, COL1), " +
                " TIMESTAMPADD(MINUTE, 10, COL1) FROM T";
        this.testQuery(query,
                        new DBSPTimestampLiteral(10100),
                        new DBSPTimestampLiteral(3600100),
                        new DBSPTimestampLiteral(600100)
                        );
    }

    @Test
    public void timestampParse() {
        String query = "SELECT TIMESTAMP '2020-04-30 12:25:13.45'";
        this.testQuery(query, new DBSPTimestampLiteral(1588249513450L));
    }

    @Test(expected = RuntimeException.class)
    public void timestampParseIllegal() {
        String query = "SELECT DATE '1997-02-29'";
        this.testQuery(query, new DBSPDateLiteral("1997-02-29"));
    }

    @Test
    public void timestampDiffTest() {
        String query =
                "SELECT timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'), " +
                "timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00'), " +
                "timestampdiff(YEAR, DATE'2021-01-01', DATE'1900-03-28')";
        this.testQuery(query,
                new DBSPI32Literal(0), new DBSPI32Literal(1), new DBSPI32Literal(-120)
                );
    }
}
