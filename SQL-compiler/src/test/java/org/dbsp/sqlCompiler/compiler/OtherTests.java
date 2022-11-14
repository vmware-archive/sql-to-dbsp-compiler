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

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntegerLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class OtherTests implements IModule {
    static final CompilerOptions options = new CompilerOptions();

    private DBSPCompiler compileDef() throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler(options).newCircuit("circuit");
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ", COL5 INT" +
                ", COL6 DOUBLE" +
                ")";

        compiler.compileStatement(ddl, null);
        return compiler;
    }

    private DBSPCircuit compileQuery(String query) throws SqlParseException {
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query, null);
        return compiler.getResult();
    }

    private void testQuery(String query) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCircuit circuit = this.compileQuery(query);
            String rust = ToRustVisitor.toRustString(circuit);
            Assert.assertNotNull(rust);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void toRustTest() {
        String query = "SELECT T.COL3 FROM T";
        this.testQuery(query);
    }

    @Test
    public void DDLZetaSyntaxTest() throws SqlParseException {
        // ZetaSQL query syntax - BIG_QUERY dialect
        String query = "CREATE TABLE R AS\n" +
                "SELECT cast(1 as int64) as primary_key,\n" +
                "       cast(1 as int64) as id, cast(\"a1\" as string) as a UNION ALL\n" +
                "  SELECT 2, 2, \"a2\"";
        CompilerOptions options = new CompilerOptions();
        options.dialect = Lex.BIG_QUERY;
        DBSPCompiler compiler = new DBSPCompiler(options).newCircuit("circuit");
        compiler.compileStatement(query, null);
        DBSPCircuit circuit = compiler.getResult();
        String rust = ToRustVisitor.toRustString(circuit);
        Assert.assertNotNull(rust);
    }

    @Test
    public void DDLZetaOverTest() throws SqlParseException {
        String query = "CREATE TABLE TestTable AS\n" +
                "SELECT cast(1 as int64) as row_id,\n" +
                "       cast(null as bool) as bool_val,\n" +
                "       cast(null as int64) as int64_val,\n" +
                "       cast(null as uint64) as uint64_val,\n" +
                "       cast(null as double) as double_val,\n" +
                "       cast(null as string) as str_val UNION ALL\n" +
                "  SELECT 2,  true,  2,    3,    1.5,  \"A\"   UNION ALL\n" +
                "  SELECT 3,  false, 1,    6,    1.5,  \"A\"   UNION ALL\n" +
                "  SELECT 4,  null,  null, 2,    2.5,  \"B\"   UNION ALL\n" +
                "  SELECT 5,  false, 1,    null, 3.5,  \"A\"   UNION ALL\n" +
                "  SELECT 6,  true,  2,    2,    null, \"C\"   UNION ALL\n" +
                "  SELECT 7,  null,  1,    5,    -0.5,  null UNION ALL\n" +
                "  SELECT 8,  true,  4,    2,    -1.5,  \"A\"  UNION ALL\n" +
                "  SELECT 9,  false, 2,    3,    1.5,   \"B\"  UNION ALL\n" +
                "  SELECT 10, true,  3,    1,    2.5,   \"B\"\n";
        CompilerOptions options = new CompilerOptions();
        options.dialect = Lex.BIG_QUERY;
        DBSPCompiler compiler = new DBSPCompiler(options).newCircuit("circuit");
        compiler.compileStatement(query, null);
        compiler.compileStatement("SELECT int64_val, COUNT(*) OVER " +
                "(ORDER BY int64_val RANGE UNBOUNDED PRECEDING)\n" +
                "FROM (SELECT int64_val FROM TestTable)\n", null);
        DBSPCircuit circuit = compiler.getResult();
        String rust = ToRustVisitor.toRustString(circuit);
        Assert.assertNotNull(rust);
        System.out.println(rust);
    }

    @Test
    public void loggerTest() {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.instance.setDebugStream(builder);
        Logger.instance.setDebugLevel(this.getModule(), 1);
        Assert.assertEquals("OtherTests", this.getModule());
        Logger.instance.from(this, 1)
                .append("Logging one statement")
                .newline();
        Logger.instance.setDebugLevel(this.getModule(), 0);
        Logger.instance.from(this, 1)
                .append("This one is not logged")
                .newline();
        Logger.instance.setDebugStream(save);
        Assert.assertEquals("Logging one statement\n", builder.toString());
        Logger.instance.setDebugLevel(this.getModule(), 0);
    }

    @Test
    public void toCsvTest() {
        DBSPZSetLiteral s = new DBSPZSetLiteral(BaseSQLTests.e0, BaseSQLTests.e1);
        StringBuilder builder = new StringBuilder();
        ToCsvVisitor visitor = new ToCsvVisitor(builder, () -> "");
        visitor.traverse(s);
        String[] lines = builder.toString().split("\n");
        Arrays.sort(lines);
        Assert.assertEquals(
                "10,1.0,false,\"Hi\",1,0.0,\n" +
                "10,12.0,true,\"Hi\",,,",
                String.join("\n", lines));
    }

    @Test
    public void rustCsvTest() throws IOException, InterruptedException {
        DBSPZSetLiteral data = new DBSPZSetLiteral(BaseSQLTests.e0, BaseSQLTests.e1);
        String fileName = BaseSQLTests.rustDirectory + "/" + "test.csv";
        File file = Solutions.toCsv(fileName, data);
        List<DBSPStatement> list = new ArrayList<>();
        // let src = csv_source::<Tuple3<bool, Option<String>, Option<u32>>, isize>("src/test.csv");
        DBSPLetStatement src = new DBSPLetStatement("src",
                new DBSPApplyExpression("read_csv", data.getNonVoidType(),
                        new DBSPStrLiteral(fileName)));
        list.add(src);
        list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                "assert_eq!", null, src.getVarReference(),
                data)));
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(), null, body)
                .addAnnotation("#[test]");

        PrintWriter rustWriter = new PrintWriter(BaseSQLTests.testFilePath, "UTF-8");
        rustWriter.println(ToRustVisitor.generatePreamble());
        rustWriter.println(ToRustVisitor.toRustString(tester));
        rustWriter.close();

        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
        boolean success = file.delete();
        Assert.assertTrue(success);
    }

    @Test
    public void rustCsvTest2() throws IOException, InterruptedException {
        DBSPZSetLiteral data = new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPIntegerLiteral(1, true)),
                new DBSPTupleExpression(new DBSPIntegerLiteral(2, true)),
                new DBSPTupleExpression(DBSPIntegerLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))
        );
        String fileName = BaseSQLTests.rustDirectory + "/" + "test.csv";
        File file = Solutions.toCsv(fileName, data);
        List<DBSPStatement> list = new ArrayList<>();
        DBSPLetStatement src = new DBSPLetStatement("src",
                new DBSPApplyExpression("read_csv", data.getNonVoidType(),
                        new DBSPStrLiteral(fileName)));
        list.add(src);
        list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                "assert_eq!", null, src.getVarReference(),
                data)));
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(), null, body)
                .addAnnotation("#[test]");

        PrintWriter rustWriter = new PrintWriter(BaseSQLTests.testFilePath, "UTF-8");
        rustWriter.println(ToRustVisitor.generatePreamble());
        rustWriter.println(ToRustVisitor.toRustString(tester));
        rustWriter.close();

        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
        boolean success = file.delete();
        Assert.assertTrue(success);
    }
}
