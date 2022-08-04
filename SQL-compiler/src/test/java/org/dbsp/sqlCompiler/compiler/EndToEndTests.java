package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.dbsp.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.dbsp.DBSPTransaction;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.*;
import org.dbsp.sqlCompiler.dbsp.circuit.type.*;
import org.dbsp.sqlCompiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.frontend.CalciteProgram;
import org.dbsp.sqllogictest.SqlTestOutputDescription;
import org.dbsp.util.Utilities;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

/**
 * Test end-to-end by compiling some DDL statements and view
 * queries by compiling them to rust and executing them
 * by inserting data in the input tables and reading data
 * from the declared views.
 */
public class EndToEndTests {
    static final String rustDirectory = "../temp";
    static final String testFilePath = rustDirectory + "/src/test.rs";

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

        calcite.startCompilation();
        calcite.compile(ddl);
        return calcite;
    }

    private DBSPCircuit compileQuery(String query) throws SqlParseException {
        CalciteCompiler calcite = this.compileDef();
        calcite.compile(query);
        CalciteProgram program = calcite.getProgram();

        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler();
        return compiler.compile(program, "circuit");
    }

    private final DBSPTupleExpression e0 = new DBSPTupleExpression(
            new DBSPLiteral(10),
            new DBSPLiteral(12.0),
            new DBSPLiteral(true),
            new DBSPLiteral("Hi"),
            new DBSPLiteral(DBSPTypeInteger.signed32.setMayBeNull(true)),
            new DBSPLiteral(DBSPTypeDouble.instance.setMayBeNull(true))
    );
    private final DBSPTupleExpression e1 = new DBSPTupleExpression(
            new DBSPLiteral(10),
            new DBSPLiteral(1.0),
            new DBSPLiteral(false),
            new DBSPLiteral("Hi"),
            new DBSPLiteral(1, true),
            new DBSPLiteral(0.0, true)
    );
    private final DBSPZSetLiteral z0 = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e0);
    private final DBSPZSetLiteral z1 = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e1);
    private final DBSPZSetLiteral empty = new DBSPZSetLiteral(z0.getNonVoidType());

    private DBSPZSetLiteral createInput() {
        return new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e0, e1);
    }

    @SuppressWarnings("unused")
    private String getStringFormat(DBSPTypeTuple type) {
        StringBuilder result = new StringBuilder();
        for (DBSPType field: type.tupArgs) {
            if (field.is(DBSPTypeInteger.class))
                result.append("I");
            else if (field.is(DBSPTypeFP.class))
                result.append("R");
            else if (field.is(DBSPTypeString.class))
                result.append("T");
            else result.append("T");
        }
        return result.toString();
    }

    private void createTester(PrintWriter writer, DBSPCircuit circuit, DBSPZSetLiteral expectedOutput) {
        DBSPZSetLiteral input = this.createInput();
        DBSPTransaction transaction = new DBSPTransaction();
        transaction.addSet("T", input);
        DBSPFunction inputGen = transaction.inputGeneratingFunction("input", circuit);
        writer.println(inputGen.toRustString());
        SqlTestOutputDescription description = new SqlTestOutputDescription();
        description.columnTypes = null;
        description.setValueCount(expectedOutput.size());
        description.order = SqlTestOutputDescription.SortOrder.Row;
        DBSPFunction tester = SqlRuntimeLibrary.createTesterCode(
                "tester", "input",
                circuit, expectedOutput, description);
        writer.println("#[test]");
        writer.println(tester.toRustString());
    }

    private void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        try {
            DBSPCircuit circuit = this.compileQuery(query);
            PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
            writer.println(DBSPCircuit.generatePreamble());
            writer.println(circuit.toRustString());
            this.createTester(writer, circuit, expectedOutput);
            writer.close();
            Utilities.compileAndTestRust(rustDirectory);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void projectTest() {
        String query = "CREATE VIEW V AS SELECT T.COL3 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPLiteral(true)),
                        new DBSPTupleExpression(new DBSPLiteral(false))));
    }

    @Test
    public void plusNullTest() {
        String query = "CREATE VIEW V AS SELECT T.COL1 + T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPLiteral(11, true)),
                        new DBSPTupleExpression(new DBSPLiteral(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void negateNullTest() {
        String query = "CREATE VIEW V AS SELECT -T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPLiteral(-1, true)),
                        new DBSPTupleExpression(new DBSPLiteral(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void projectNullTest() {
        String query = "CREATE VIEW V AS SELECT T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
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
        this.testQuery(query, new DBSPZSetLiteral(this.z0.getNonVoidType()));
    }

    @Test
    public void exceptTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void cartesianTest() {
        String query = "CREATE VIEW V AS SELECT * FROM T, T AS X";
        DBSPExpression inResult = DBSPTupleExpression.flatten(e0, e0);
        DBSPZSetLiteral result = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, inResult);
        result.add(DBSPTupleExpression.flatten(e0, e1));
        result.add(DBSPTupleExpression.flatten(e1, e0));
        result.add(DBSPTupleExpression.flatten(e1, e1));
        this.testQuery(query, result);
    }
}
