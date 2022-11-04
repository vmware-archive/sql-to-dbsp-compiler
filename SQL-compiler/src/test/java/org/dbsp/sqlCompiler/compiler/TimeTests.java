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

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntegerLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Utilities;
import org.junit.Test;

import java.io.PrintWriter;

public class TimeTests extends BaseSQLTests {
    @Override
    DBSPCompiler compileQuery(String query) throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler(options).newCircuit("circuit");
        compiler.setGenerateInputsFromTables(true);
        String ddl = "CREATE TABLE T (\n" +
                "COL1 TIMESTAMP NOT NULL" +
                ")";
        compiler.compileStatement(ddl, null);
        compiler.compileStatement(query, null);
        return compiler;
    }

    void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCompiler compiler = this.compileQuery(query);
            PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
            writer.println(ToRustVisitor.generatePreamble());
            DBSPCircuit circuit = compiler.getResult();
            writer.println(ToRustVisitor.toRustString(circuit));
            InputOutputPair streams = new InputOutputPair(this.createInput(), expectedOutput);
            this.createTester(writer, circuit, streams);
            writer.close();
            Utilities.compileAndTestRust(rustDirectory, false);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    DBSPZSetLiteral createInput() {
        return new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPTimestampLiteral(100)));
    }

    @Test
    public void timestampTableTest() {
        String query = "SELECT COL1 FROM T";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void timestampAddTableTest() {
        String query =
                "SELECT " +
                        "TIMESTAMPADD(SECOND, 10, COL1), " +
                        "TIMESTAMPADD(HOUR, 1, COL1), " +
                        "TIMESTAMPADD(MINUTE, 10, COL1) " +
                        "FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(
                        new DBSPTimestampLiteral(10100),
                        new DBSPTimestampLiteral(3600100),
                        new DBSPTimestampLiteral(600100)
                        )));
    }

    @Test
    public void timestampParse() {
        String query = "SELECT TIMESTAMP '2020-04-30 12:25:13.45'";
        this.testQuery(query, new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPTimestampLiteral(1588249513450L))));
    }

    @Test
    public void timestampDiffTest() {
        String query =
                "SELECT timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'), " +
                "timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00'), " +
                "timestampdiff(YEAR, DATE'2021-01-01', DATE'1900-03-28')";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(
                        new DBSPIntegerLiteral(0), new DBSPIntegerLiteral(1), new DBSPIntegerLiteral(-120)
                )));
    }
}
