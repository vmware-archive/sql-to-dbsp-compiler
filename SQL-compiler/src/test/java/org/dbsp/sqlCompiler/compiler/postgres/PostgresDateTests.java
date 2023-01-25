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
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.util.Linq;
import org.junit.Test;

public class PostgresDateTests extends BaseSQLTests {
    @Override
    public DBSPCompiler compileQuery(String query) throws SqlParseException {
        String data = "CREATE TABLE DATE_TBL (f1 date);\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-04-09');\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-06-13');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-28');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-29');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-02-28');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-02-29');\n" +    // error
                "INSERT INTO DATE_TBL VALUES ('1997-03-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-03-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-03');\n" +
                "INSERT INTO DATE_TBL VALUES ('2038-04-08');\n" +
                "INSERT INTO DATE_TBL VALUES ('2039-04-09');\n" +
                "INSERT INTO DATE_TBL VALUES ('2040-04-10');\n";
                // Calcie does not seem to support dates BC
                //"INSERT INTO DATE_TBL VALUES ('2040-04-10 BC');";
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatements(data);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCompiler compiler = this.compileQuery(query);
            DBSPCircuit circuit = getCircuit(compiler);
            DBSPZSetLiteral input = compiler.getTableContents().getTableContents("DATE_TBL");
            InputOutputPair streams = new InputOutputPair(input, expectedOutput);
            this.addRustTestCase(circuit, streams);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void test0() {
        String[] dates = new String[] {
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
                //"2040-04-10 BC"
        };
        String query = "SELECT f1 FROM DATE_TBL";
        DBSPZSetLiteral result = new DBSPZSetLiteral(
                Linq.map(dates, d -> new DBSPTupleExpression(new DBSPDateLiteral(d, true)), DBSPExpression.class));
        result.add(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeDate.instance.setMayBeNull(true))));
        this.testQuery(query, result);
    }
}
