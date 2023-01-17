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

package org.dbsp.sqlCompiler.compiler;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;
import org.dbsp.util.IModule;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;
import org.dbsp.zetalexer;
import org.dbsp.zetatest;

import java.io.IOException;
import java.io.PrintWriter;

public class zsetasqlTests extends BaseSQLTests implements IModule {
    @Test
    public void simpleParseTest() {
        String test = "[name=current_date_1]\n" +
                "select current_date() = current_date,\n" +
                "       current_date() >= '2015-03-17',\n" +
                "       current_date() >= date_from_unix_date(0),\n" +
                "       current_date() >= date_from_unix_date(-719162),\n" +
                "       current_date() <= date_from_unix_date(2932896)\n" +
                "--\n" +
                "ARRAY<STRUCT<BOOL, BOOL, BOOL, BOOL, BOOL>>[{true, true, true, true, true}]\n" +
                "==\n" +
                "\n" +
                "[name=date_from_unix_date_1]\n" +
                "select date_from_unix_date(-719162),\n" +
                "       date_from_unix_date(-365),\n" +
                "       date_from_unix_date(0),\n" +
                "       date_from_unix_date(16071),\n" +
                "       date_from_unix_date(2932896)\n" +
                "--\n" +
                "ARRAY<STRUCT<DATE, DATE, DATE, DATE, DATE>>[\n" +
                "  {0001-01-01, 1969-01-01, 1970-01-01, 2014-01-01, 9999-12-31}\n" +
                "]";
        zetalexer lexer = new zetalexer(CharStreams.fromString(test));
        zetatest parser = new zetatest(new CommonTokenStream(lexer));
        zetatest.TestsContext tests = parser.tests();
        Assert.assertEquals(4, tests.getChildCount());
        for (int i = 0; i < tests.getChildCount(); i++) {
            System.out.println(i + " " + tests.getChild(i));
        }
    }

    @Test
    public void DDLZetaOverTest() throws SqlParseException, IOException, InterruptedException {
        String query = "CREATE TABLE TestTable AS\n" +
                "SELECT cast(1 as int64) as row_id,\n" +
                "       cast(null as bool) as bool_val,\n" +
                "       cast(1 as int64) as int64_val,\n" +
                "       cast(null as uint64) as uint64_val,\n" +
                "       cast(null as double) as double_val,\n" +
                "       cast(null as string) as str_val UNION ALL\n" +
                "  SELECT 2,  true,  2,    3,    1.5,  \"A\"   UNION ALL\n" +
                "  SELECT 3,  false, 1,    6,    1.5,  \"A\"   UNION ALL\n" +
                "  SELECT 4,  null,  2,    2,    2.5,  \"B\"   UNION ALL\n" +
                "  SELECT 5,  false, 1,    null, 3.5,  \"A\"   UNION ALL\n" +
                "  SELECT 6,  true,  2,    2,    null, \"C\"   UNION ALL\n" +
                "  SELECT 7,  null,  1,    5,    -0.5,  null UNION ALL\n" +
                "  SELECT 8,  true,  4,    2,    -1.5,  \"A\"  UNION ALL\n" +
                "  SELECT 9,  false, 2,    3,    1.5,   \"B\"  UNION ALL\n" +
                "  SELECT 10, true,  3,    1,    2.5,   \"B\"\n";
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.dialect = Lex.BIG_QUERY;
        DBSPCompiler compiler = new DBSPCompiler(options).newCircuit("circuit");
        compiler.compileStatement(query, null);
        compiler.compileStatement("CREATE VIEW V AS SELECT int64_val, COUNT(*) OVER " +
                "(ORDER BY int64_val RANGE UNBOUNDED PRECEDING)\n" +
                "FROM (SELECT int64_val FROM TestTable)\n", null);
        DBSPCircuit circuit = compiler.getResult();
        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());
        writer.println(ToRustVisitor.circuitToRustString(circuit));
        writer.close();
        Utilities.compileAndTestRust(rustDirectory, false);
    }

    @Test
    public void DDLZetaSyntaxTest() throws SqlParseException {
        // ZetaSQL query syntax - BIG_QUERY dialect
        String query = "CREATE TABLE R AS\n" +
                "SELECT cast(1 as int64) as primary_key,\n" +
                "       cast(1 as int64) as id, cast(\"a1\" as string) as a UNION ALL\n" +
                "  SELECT 2, 2, \"a2\"";
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.dialect = Lex.BIG_QUERY;
        DBSPCompiler compiler = new DBSPCompiler(options).newCircuit("circuit");
        compiler.compileStatement(query, null);
        DBSPCircuit circuit = compiler.getResult();
        String rust = ToRustVisitor.circuitToRustString(circuit);
        Assert.assertNotNull(rust);
    }
}
