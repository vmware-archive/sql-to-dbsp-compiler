package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.dbsp.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPLiteral;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeDouble;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.frontend.CalciteProgram;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.*;

/**
 * Test end-to-end by compiling some DDL statements and view
 * queries by compiling them to rust and executing them
 * by inserting data in the input tables and reading data
 * from the declared views.
 */
public class EndToEndTests {
    static String rustDirectory = "../temp";
    static String testFilePath = rustDirectory + "/src/test.rs";

    @Before
    public void generateLib() throws IOException {
        SqlRuntimeLibrary.instance.writeSqlLibrary(rustDirectory + "/src/test/sqllib.rs");
    }

    private CalciteCompiler compileDef() throws SqlParseException {
        CalciteCompiler calcite = new CalciteCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ", COL5 INT" +
                ", COL6 DOUBLE" +
                ")";

        calcite.compile(ddl);
        return calcite;
    }

    private String compileQuery(String query) throws SqlParseException {
        CalciteCompiler calcite = this.compileDef();
        calcite.compile(query);
        CalciteProgram program = calcite.getProgram();

        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler();
        DBSPCircuit dbsp = compiler.compile(program);
        return dbsp.toRustString();
    }

    private PrintWriter writeToFile(String file, String contents) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.print(contents);
        return writer;
    }

    private void compileAndTestRust(String directory) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("cargo", "test");
        processBuilder.directory(new File(directory));
        processBuilder.inheritIO();
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        assert exitCode == 0 : "Rust process failed with exit code " + exitCode;
    }

    private final DBSPExpression e0 = new DBSPTupleExpression(
            new DBSPLiteral(10),
            new DBSPLiteral(12.0),
            new DBSPLiteral(true),
            new DBSPLiteral("Hi"),
            new DBSPLiteral(DBSPTypeInteger.signed32.setMayBeNull(true)),
            new DBSPLiteral(DBSPTypeDouble.instance.setMayBeNull(true))
    );
    private final DBSPExpression e1 = new DBSPTupleExpression(
            new DBSPLiteral(10),
            new DBSPLiteral(1.0),
            new DBSPLiteral(false),
            new DBSPLiteral("Hi"),
            new DBSPLiteral(1, true),
            new DBSPLiteral(0.0, true)
    );
    private final DBSPZSetLiteral z0 = new DBSPZSetLiteral(e0);
    private final DBSPZSetLiteral z1 = new DBSPZSetLiteral(e1);
    private final DBSPZSetLiteral empty = new DBSPZSetLiteral(z0.getType());

    private DBSPExpression createInput() {
        return new DBSPZSetLiteral(e0, e1);
    }

    private void createTester(PrintWriter writer, @Nullable DBSPExpression expectedOutput) {
        DBSPExpression input = this.createInput();
        writer.println("#[test]");
        writer.println("fn tester() {");
        writer.print("   let data = ");
        writer.print(input.toRustString());
        writer.println(";\n");
        writer.println("   let mut circuit = circuit_generator();");
        writer.println("   let _output = circuit(data);");
        if (expectedOutput != null) {
            writer.print("    assert_eq!(");
            writer.print(expectedOutput.toRustString());
            writer.println(", _output);");
        }
        writer.println("}\n");
    }

    private void testQuery(String query, @Nullable DBSPExpression expectedOutput) {
        try {
            String rust = this.compileQuery(query);
            PrintWriter writer = this.writeToFile(testFilePath, rust);
            this.createTester(writer, expectedOutput);
            writer.close();
            this.compileAndTestRust(rustDirectory);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void projectTest() {
        String query = "CREATE VIEW V AS SELECT T.COL3 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPLiteral(true)),
                        new DBSPTupleExpression(new DBSPLiteral(false))));
    }

    @Test
    public void plusNullTest() {
        String query = "CREATE VIEW V AS SELECT T.COL1 + T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPLiteral(11, true)),
                        new DBSPTupleExpression(new DBSPLiteral(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void negateNullTest() {
        String query = "CREATE VIEW V AS SELECT -T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPLiteral(-1, true)),
                        new DBSPTupleExpression(new DBSPLiteral(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void projectNullTest() {
        String query = "CREATE VIEW V AS SELECT T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(
                        new DBSPTupleExpression(new DBSPLiteral(1, true)),
                        new DBSPTupleExpression(new DBSPLiteral(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void unionTest() {
        String query = "CREATE VIEW V AS (SELECT * FROM T) UNION (SELECT * FROM T)";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void whereTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL3";
        this.testQuery(query, this.z0);
    }

    @Test
    public void whereImplicitCastTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL2 < COL1";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitCastTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL2 < CAST(COL1 AS DOUBLE)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitCastTestNull() {
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL2 < CAST(COL5 AS DOUBLE)";
        this.testQuery(query, this.empty);
    }

    @Test
    public void whereExplicitImplicitCastTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL2 < CAST(COL1 AS FLOAT)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitImplicitCastTestNull() {
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL2 < CAST(COL5 AS FLOAT)";
        this.testQuery(query, this.empty);
    }

    @Test
    public void whereExpressionTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T WHERE COL2 < 0";
        this.testQuery(query, new DBSPZSetLiteral(this.z0.getType()));
    }

    @Test
    public void exceptTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.testQuery(query, this.z1);
    }
}
