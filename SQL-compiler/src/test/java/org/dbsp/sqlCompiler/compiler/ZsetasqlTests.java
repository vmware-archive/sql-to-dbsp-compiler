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
import org.dbsp.Zetatest;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.IModule;
import org.dbsp.zetasqltest.ZetaSQLTest;
import org.dbsp.zetasqltest.ZetatestVisitor;
import org.junit.Assert;
import org.junit.Test;
import org.dbsp.Zetalexer;

import java.util.Objects;

public class ZsetasqlTests extends BaseSQLTests implements IModule {
    static final String sample = "[name=current_date_1]\n" +
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

    @Test
    public void simpleParseTest() {
        Zetalexer lexer = new Zetalexer(CharStreams.fromString(sample));
        Zetatest parser = new Zetatest(new CommonTokenStream(lexer));
        Zetatest.TestsContext tests = parser.tests();
        Assert.assertEquals(4, tests.getChildCount());
    }

    @Test
    public void visitorParseTest() {
        Zetalexer lexer = new Zetalexer(CharStreams.fromString(sample));
        Zetatest parser = new Zetatest(new CommonTokenStream(lexer));
        ZetatestVisitor visitor = new ZetatestVisitor();
        parser.tests().accept(visitor);
        Assert.assertEquals(2, visitor.tests.size());
        ZetaSQLTest test = visitor.tests.get(0);
        Assert.assertEquals(
                "select current_date() = current_date," +
                "       current_date() >= '2015-03-17'," +
                "       current_date() >= date_from_unix_date(0)," +
                "       current_date() >= date_from_unix_date(-719162)," +
                "       current_date() <= date_from_unix_date(2932896)", test.statement);
        Assert.assertEquals(1, test.results.size());
        ZetaSQLTest.TestResult result = test.results.get(0);
        Assert.assertNotNull(result.type);
        Assert.assertTrue(result.type.is(DBSPTypeZSet.class));
        DBSPTypeZSet zset = result.type.to(DBSPTypeZSet.class);
        Assert.assertTrue(zset.elementType.is(DBSPTypeTuple.class));
        DBSPTypeTuple tuple = zset.elementType.to(DBSPTypeTuple.class);
        Assert.assertEquals(5, tuple.size());
        for (int i = 0; i < 5; i++)
            Assert.assertTrue(tuple.getFieldType(i).is(DBSPTypeBool.class));
        Assert.assertEquals("{ Tuple5::new(Some(true), Some(true), Some(true), Some(true), Some(true)) => 1}",
                Objects.requireNonNull(result.result).toString());
    }

    @Test
    public void DDLZetaOverTest() {
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
        options.ioOptions.lexicalRules = Lex.BIG_QUERY;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(query);
        compiler.compileStatement("CREATE VIEW V AS SELECT int64_val, COUNT(*) OVER " +
                "(ORDER BY int64_val RANGE UNBOUNDED PRECEDING)\n" +
                "FROM (SELECT int64_val FROM TestTable)\n");
        this.addRustTestCase(getCircuit(compiler));
    }

    //@Test
    // Calcite does not seem to be able to parse this.
    // In fact, it does not seem to be defined in standard sql, which cannot mix
    // intervals of different types.
    public void testIntervals() {
        String query = "CREATE TABLE Intervals AS\n" +
                //"SELECT 1 id, CAST(NULL AS INTERVAL) value UNION ALL\n" +
                "SELECT 2, interval 0 year UNION ALL\n" +
                "SELECT 3, interval '0.000001' second UNION ALL\n" +
                "SELECT 4, interval -1 second UNION ALL\n" +
                "SELECT 5, interval 1 month UNION ALL\n" +
                "SELECT 6, interval 30 day UNION ALL\n" +
                "SELECT 7, interval 720 hour UNION ALL\n" +
                "SELECT 8, interval 10000 year UNION ALL\n" +
                //"SELECT 9, interval '1-2 3 4:5:6.789' year to second UNION ALL\n" +
                "SELECT 10, interval 2 hour\n"
                //"SELECT 11, interval '1:59:59.999999' hour to second UNION ALL\n" +
                //"SELECT 12, interval '1:00:00.000001' hour to second\n"
                ;
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.lexicalRules = Lex.BIG_QUERY;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(query);
        this.addRustTestCase(getCircuit(compiler));
    }

    @Test
    public void DDLZetaSyntaxTest() {
        // ZetaSQL query syntax - BIG_QUERY dialect
        String query = "CREATE TABLE R AS\n" +
                "SELECT cast(1 as int64) as primary_key,\n" +
                "       cast(1 as int64) as id, cast(\"a1\" as string) as a UNION ALL\n" +
                "  SELECT 2, 2, \"a2\"";
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.lexicalRules = Lex.BIG_QUERY;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(query);
        this.addRustTestCase(getCircuit(compiler));
    }
}
