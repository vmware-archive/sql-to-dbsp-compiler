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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsp.sqlCompiler.compiler.backend.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToRustVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;
import sun.awt.image.FileImageSource;
import sun.awt.image.ImageFormatException;
import sun.awt.image.JPEGImageDecoder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class OtherTests extends BaseSQLTests implements IModule {
    static final CompilerOptions options = new CompilerOptions();

    private DBSPCompiler compileDef() {
        DBSPCompiler compiler = new DBSPCompiler(options);
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ", COL5 INT" +
                ", COL6 DOUBLE" +
                ")";

        compiler.compileStatement(ddl);
        return compiler;
    }

    public DBSPCircuit queryToCircuit(String query) {
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query);
        return getCircuit(compiler);
    }

    private void testQuery(String query) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCircuit circuit = this.queryToCircuit(query);
            String rust = ToRustVisitor.circuitToRustString(circuit);
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
    public void loggerTest() {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.INSTANCE.setDebugStream(builder);
        Logger.INSTANCE.setDebugLevel(this.getModule(), 1);
        Assert.assertEquals("OtherTests", this.getModule());
        Logger.INSTANCE.from(this, 1)
                .append("Logging one statement")
                .newline();
        Logger.INSTANCE.setDebugLevel(this.getModule(), 0);
        Logger.INSTANCE.from(this, 1)
                .append("This one is not logged")
                .newline();
        Logger.INSTANCE.setDebugStream(save);
        Assert.assertEquals("Logging one statement\n", builder.toString());
        Logger.INSTANCE.setDebugLevel(this.getModule(), 0);
    }

    @Test
    public void testJoin() throws IOException, InterruptedException {
        String statement0 = "CREATE TABLE demographics (\n" +
                "    cc_num FLOAT64,\n" +
                "    first STRING,\n" +
                "    gender STRING,\n" +
                "    street STRING,\n" +
                "    city STRING,\n" +
                "    state STRING,\n" +
                "    zip INTEGER,\n" +
                "    lat FLOAT64,\n" +
                "    long FLOAT64,\n" +
                "    city_pop INTEGER,\n" +
                "    job STRING,\n" +
                "    dob DATE\n" +
                ")\n";
        String statement1 =
                "CREATE TABLE transactions (\n" +
                "    trans_date_trans_time TIMESTAMP NOT NULL,\n" +
                "    cc_num FLOAT64,\n" +
                "    merchant STRING,\n" +
                "    category STRING,\n" +
                "    amt FLOAT64,\n" +
                "    trans_num STRING,\n" +
                "    unix_time INTEGER,\n" +
                "    merch_lat FLOAT64,\n" +
                "    merch_long FLOAT64,\n" +
                "    is_fraud INTEGER\n" +
                ")\n";
        String statement2 =
                "CREATE VIEW transactions_with_demographics as \n" +
                "    SELECT transactions.*, demographics.first, demographics.city\n" +
                "    FROM\n" +
                "        transactions JOIN demographics\n" +
                "        ON transactions.cc_num = demographics.cc_num";
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(statement0);
        compiler.compileStatement(statement1);
        compiler.compileStatement(statement2);
        DBSPCircuit circuit = compiler.getFinalCircuit("circuit");
        RustFileWriter writer = new RustFileWriter(testFilePath);
        writer.add(circuit);
        writer.writeAndClose();
        Utilities.compileAndTestRust(rustDirectory, false);
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
        File file = ToCsvVisitor.toCsv(fileName, data);
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

        RustFileWriter writer = new RustFileWriter(BaseSQLTests.testFilePath);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
        boolean success = file.delete();
        Assert.assertTrue(success);
    }

    @SuppressWarnings("SqlDialectInspection")
    @Test
    public void rustSqlTest() throws IOException, InterruptedException, SQLException {
        String filepath = BaseSQLTests.rustDirectory + "/" + "test.db";
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + filepath);
        Statement statement = connection.createStatement();
        statement.executeUpdate("drop table if exists t1");
        statement.executeUpdate("create table t1(c1 integer not null, c2 bool not null, " +
                                "c3 varcharnot null , c4 integer)");
        statement.executeUpdate("insert into t1 values(10, true, 'Hi', null)"); // e0NoDouble
        statement.executeUpdate("insert into t1 values(10, false, 'Hi', 1)"); // e1NoDouble
        connection.close();

        DBSPZSetLiteral data = new DBSPZSetLiteral(BaseSQLTests.e0NoDouble, BaseSQLTests.e1NoDouble);
        List<DBSPStatement> list = new ArrayList<>();

        String connectionString = "sqlite://" + filepath;
        // Generates a read_table(<conn>, <table_name>, <mapper from |AnyRow| -> Tuple type>) invocation
        DBSPTypeUser sqliteRowType = new DBSPTypeUser(null, "AnyRow", false);
        DBSPVariablePath rowVariable = new DBSPVariablePath("row", sqliteRowType);
        DBSPExpression[] fields = BaseSQLTests.e0NoDouble.fields; // Should be the same for e1NoDouble too
        final List<DBSPExpression> rowGets = new ArrayList<>(fields.length);
        for (int i = 0; i < fields.length; i++) {
            DBSPApplyMethodExpression rowGet =
                    new DBSPApplyMethodExpression("get",
                            fields[i].getNonVoidType(),
                            rowVariable, new DBSPUSizeLiteral(i));
            rowGets.add(rowGet);
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(rowGets, false);
        DBSPClosureExpression mapClosure = new DBSPClosureExpression(null, tuple,
               rowVariable.asRefParameter());
        DBSPApplyExpression readDb = new DBSPApplyExpression("read_db", data.getNonVoidType(),
                new DBSPStrLiteral(connectionString), new DBSPStrLiteral("t1"), mapClosure);

        DBSPLetStatement src = new DBSPLetStatement("src", readDb);
        list.add(src);
        list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                "assert_eq!", null, src.getVarReference(),
                data)));
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction tester = new DBSPFunction("test", new ArrayList<>(), null, body)
                .addAnnotation("#[test]");

        RustFileWriter writer = new RustFileWriter(BaseSQLTests.testFilePath);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
        boolean success = new File(filepath).delete();
        Assert.assertTrue(success);
    }

    @Test
    public void rustCsvTest2() throws IOException, InterruptedException {
        DBSPZSetLiteral data = new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                new DBSPTupleExpression(new DBSPI32Literal(2, true)),
                new DBSPTupleExpression(DBSPI32Literal.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))
        );
        String fileName = BaseSQLTests.rustDirectory + "/" + "test.csv";
        File file = ToCsvVisitor.toCsv(fileName, data);
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

        PrintStream outputStream = new PrintStream(BaseSQLTests.testFilePath, "UTF-8");
        RustFileWriter writer = new RustFileWriter(outputStream);
        writer.add(tester);
        writer.writeAndClose();
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
        boolean success = file.delete();
        Assert.assertTrue(success);
    }

    File createInputScript(String... contents) throws FileNotFoundException, UnsupportedEncodingException {
        String inputScript = rustDirectory + "/script.sql";
        PrintWriter script = new PrintWriter(inputScript, "UTF-8");
        script.println(String.join(";\n", contents));
        script.close();
        return new File(inputScript);
    }

    @Test
    public void testRustCompiler() throws IOException, InterruptedException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T"
        };
        File file = this.createInputScript(statements);
        CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Utilities.compileAndTestRust(BaseSQLTests.rustDirectory, false);
        boolean success = file.delete();
        Assert.assertTrue(success);
    }

    @Test
    public void testSchema() throws IOException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T"
        };
        File file = this.createInputScript(statements);
        File json = File.createTempFile("out", ".json", new File("."));
        CompilerMessages message = CompilerMain.execute("-js", json.getPath(), file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
        boolean success = file.delete();
        Assert.assertTrue(success);
        success = json.delete();
        Assert.assertTrue(success);
    }

    @Test
    public void testCompilerToJson() throws IOException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T WHERE COL1 > 5"
        };
        File file = this.createInputScript(statements);
        File json = File.createTempFile("out", ".json", new File("."));
        CompilerMessages message = CompilerMain.execute("-j", "-o", json.getPath(), file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        Assert.assertNotNull(parsed);
        boolean success = file.delete();
        Assert.assertTrue(success);
        success = json.delete();
        Assert.assertTrue(success);
    }

    @Test
    public void testCompilerToJpeg() throws IOException, ImageFormatException {
        String[] statements = new String[]{
                "CREATE TABLE T (\n" +
                        "COL1 INT NOT NULL" +
                        ", COL2 DOUBLE NOT NULL" +
                        ")",
                "CREATE VIEW V AS SELECT COL1 FROM T WHERE COL1 > 5"
        };
        File file = this.createInputScript(statements);
        File jpg = File.createTempFile("out", ".jpg", new File("."));
        CompilerMessages message = CompilerMain.execute("-jpg", "-o", jpg.getPath(), file.getPath());
        Assert.assertEquals(message.exitCode, 0);
        Assert.assertTrue(file.exists());
        JPEGImageDecoder decoder = new JPEGImageDecoder(
                new FileImageSource(jpg.getPath()),
                Files.newInputStream(Paths.get(jpg.getPath())));
        decoder.produceImage();
        boolean success = file.delete();
        Assert.assertTrue(success);
        success = jpg.delete();
        Assert.assertTrue(success);
    }

    @Test
    public void compilerError() throws FileNotFoundException, UnsupportedEncodingException {
        String statement = "CREATE TABLE T (\n" +
                "  COL1 INT NOT NULL" +
                ", COL2 GARBAGE";
        File file = this.createInputScript(statement);
        CompilerMessages messages = CompilerMain.execute(file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        CompilerMessages.Error error = messages.messages.get(0);
        Assert.assertTrue(error.message.startsWith("Encountered \"<EOF>\""));
    }

    @Test
    public void errorTest() throws FileNotFoundException, UnsupportedEncodingException {
        String[] statements = new String[]{
                "This is not SQL"
        };
        File file = this.createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        CompilerMessages.Error msg = messages.getError(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals(msg.message, "Non-query expression encountered in illegal context");

        statements = new String[] {
                "CREATE VIEW V AS SELECT * FROM T"
        };
        file = this.createInputScript(statements);
        messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        msg = messages.getError(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals(msg.message, "Object 'T' not found");

        statements = new String[] {
                "CREATE VIEW V AS SELECT ST_MAKELINE(ST_POINT(0,0), ST_POINT(0, 0))"
        };
        file = this.createInputScript(statements);
        messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        msg = messages.getError(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals(msg.message, "Not yet implemented: cannot convert GEOMETRY literal to class org.locationtech.jts.geom.Point\n" +
                "LINESTRING (0 0, 0 0):GEOMETRY");

        boolean success = file.delete();
        Assert.assertTrue(success);
    }

    @Test
    public void jsonErrorTest() throws FileNotFoundException, UnsupportedEncodingException, JsonProcessingException {
        String[] statements = new String[] {
                "CREATE VIEW V AS SELECT * FROM T"
        };
        File file = this.createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute("-je", file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        String json = messages.toString();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(json);
        Assert.assertNotNull(jsonNode);
    }
}
