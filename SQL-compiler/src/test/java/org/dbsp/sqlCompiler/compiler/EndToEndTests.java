package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.dbsp.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.dbsp.DBSPTransaction;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.rust.DBSPFunction;
import org.dbsp.sqlCompiler.dbsp.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.*;
import org.dbsp.sqlCompiler.dbsp.rust.type.*;
import org.dbsp.sqlCompiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.frontend.CalciteProgram;
import org.dbsp.sqllogictest.RustTestGenerator;
import org.dbsp.sqllogictest.SqlTestOutputDescription;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;

/**
 * Test end-to-end by compiling some DDL statements and view
 * queries by compiling them to rust and executing them
 * by inserting data in the input tables and reading data
 * from the declared views.
 */
public class EndToEndTests {
    static final String rustDirectory = "../temp/src";
    static final String testFilePath = rustDirectory + "/test0.rs";

    @BeforeClass
    public static void generateLib() throws IOException {
        SqlRuntimeLibrary.instance.writeSqlLibrary( "../lib/genlib/src/lib.rs");
        Utilities.writeRustMain(rustDirectory + "/main.rs",
                Linq.list("test0"));
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

        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler(calcite);
        return compiler.compile(program, "circuit");
    }

    private final DBSPTupleExpression e0 = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            new DBSPDoubleLiteral(12.0),
            DBSPBoolLiteral.True,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)),
            DBSPLiteral.none(DBSPTypeDouble.instance.setMayBeNull(true))
    );
    private final DBSPTupleExpression e1 = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            new DBSPDoubleLiteral(1.0),
            DBSPBoolLiteral.False,
            new DBSPStringLiteral("Hi"),
            new DBSPIntegerLiteral(1, true),
            new DBSPDoubleLiteral(0.0, true)
    );
    private final DBSPZSetLiteral z0 = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e0);
    private final DBSPZSetLiteral z1 = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e1);
    private final DBSPZSetLiteral empty = new DBSPZSetLiteral(this.z0.getNonVoidType());

    /**
     * Returns the table containing:
     * -------------------------------------------
     * | 10 | 12.0 | true  | Hi | NULL    | NULL |
     * | 10 |  1.0 | false | Hi | Some[1] |  0.0 |
     * -------------------------------------------
     */
    private DBSPZSetLiteral createInput() {
        return new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e0, e1);
    }

    @SuppressWarnings("unused")
    private String getStringFormat(DBSPTypeTuple type) {
        StringBuilder result = new StringBuilder();
        for (DBSPType field: type.tupFields) {
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
        DBSPFunction tester = RustTestGenerator.createTesterCode(
                "tester", "input",
                circuit, expectedOutput, description);
        writer.println("#[test]");
        writer.println(tester.toRustString());
    }

    private void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        try {
            query = "CREATE VIEW V AS " + query;
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
        String query = "SELECT T.COL3 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(DBSPBoolLiteral.True),
                        new DBSPTupleExpression(DBSPBoolLiteral.False)));
    }

    @Test
    public void intersectTest() {
        String query = "SELECT * FROM T INTERSECT (SELECT * FROM T)";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void plusNullTest() {
        String query = "SELECT T.COL1 + T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPIntegerLiteral(11, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void negateNullTest() {
        String query = "SELECT -T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPIntegerLiteral(-1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void projectNullTest() {
        String query = "SELECT T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                        new DBSPTupleExpression(new DBSPIntegerLiteral(1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void unionTest() {
        String query = "(SELECT * FROM T) UNION (SELECT * FROM T)";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void unionAllTest() {
        String query = "(SELECT * FROM T) UNION ALL (SELECT * FROM T)";
        DBSPZSetLiteral output = this.createInput();
        output.add(output);
        this.testQuery(query, output);
    }

    @Test
    public void joinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL1";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.False, DBSPBoolLiteral.False),
                new DBSPTupleExpression(DBSPBoolLiteral.False, DBSPBoolLiteral.True),
                new DBSPTupleExpression(DBSPBoolLiteral.True,  DBSPBoolLiteral.False),
                new DBSPTupleExpression(DBSPBoolLiteral.True,  DBSPBoolLiteral.True)));
    }

    @Test
    public void joinNullableTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, this.empty);
    }

    @Test
    public void leftOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 LEFT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.False, DBSPBoolLiteral.None),
                new DBSPTupleExpression(DBSPBoolLiteral.True, DBSPBoolLiteral.None)
        ));
    }

    @Test
    public void rightOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 RIGHT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.False),
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.True)
        ));
    }

    @Test
    public void fullOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 FROM T AS T1 FULL OUTER JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPBoolLiteral.NullableFalse, DBSPBoolLiteral.None),
                new DBSPTupleExpression(DBSPBoolLiteral.NullableTrue, DBSPBoolLiteral.None),
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.NullableFalse),
                new DBSPTupleExpression(DBSPBoolLiteral.None, DBSPBoolLiteral.NullableTrue)
        ));
    }

    @Test
    public void whereTest() {
        String query = "SELECT * FROM T WHERE COL3";
        this.testQuery(query, this.z0);
    }

    @Test
    public void whereImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < COL1";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS DOUBLE)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS DOUBLE)";
        this.testQuery(query, this.empty);
    }

    @Test
    public void whereExplicitImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS FLOAT)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void whereExplicitImplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS FLOAT)";
        this.testQuery(query, this.empty);
    }

    @Test
    public void whereExpressionTest() {
        String query = "SELECT * FROM T WHERE COL2 < 0";
        this.testQuery(query, new DBSPZSetLiteral(this.z0.getNonVoidType()));
    }

    @Test
    public void exceptTest() {
        String query = "SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.testQuery(query, this.z1);
    }

    @Test
    public void groupByTest() {
        String query = "SELECT COL1 FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(new DBSPIntegerLiteral(10))));
    }

    @Test
    public void groupByCountTest() {
        String query = "SELECT COL1, COUNT(col2) FROM T GROUP BY COL1, COL3";
        DBSPExpression row =  new DBSPTupleExpression(new DBSPIntegerLiteral(10), new DBSPLongLiteral(1));
        this.testQuery(query, new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, row, row));
    }

    @Test
    public void divTest() {
        String query = "SELECT T.COL1 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPIntegerLiteral(10, true))));
    }

    @Test
    public void divIntTest() {
        String query = "SELECT T.COL5 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPIntegerLiteral(1, true))));
    }

    // Calcite seems to handle this query incorrectly, since
    // it claims that 1 / 0 is an integer instead of NULL
    //@Test
    public void divZeroTest() {
        String query = "SELECT 1 / 0";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void floatDivTest() {
        String query = "SELECT T.COL6 / T.COL6 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeDouble.instance.setMayBeNull(true))),
                new DBSPTupleExpression(new DBSPDoubleLiteral(Double.NaN, true))));
    }

    @Test
    public void aggregateDistinctTest() {
        String query = "SELECT SUM(DISTINCT T.COL1), SUM(T.COL2) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(10, true), new DBSPDoubleLiteral(13.0, true))));
    }

    @Test
    public void aggregateTest() {
        String query = "SELECT SUM(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(20, true))));
    }

    @Test
    public void maxTest() {
        String query = "SELECT MAX(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(10, true))));
    }

    @Test
    public void maxConst() {
        String query = "SELECT MAX(6) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(6, true))));
    }

    @Test
    public void constAggregateExpression() {
        String query = "SELECT 34 / SUM (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(17))));
    }

    @Test
    public void constAggregateExpression2() {
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(34))));
    }

    @Test
    public void constAggregateDoubleExpression() {
        String query = "SELECT 34 / SUM (1), 20 / SUM(2) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(17), new DBSPIntegerLiteral(5))));
    }

    @Test
    public void aggregateFloatTest() {
        String query = "SELECT SUM(T.COL2) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPDoubleLiteral(13.0, true))));
    }

    @Test
    public void optionAggregateTest() {
        String query = "SELECT SUM(T.COL5) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(1, true))));
    }

    @Test
    public void aggregateFalseTest() {
        String query = "SELECT SUM(T.COL1) FROM T WHERE FALSE";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)))));
    }

    @Test
    public void averageTest() {
        String query = "SELECT AVG(T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                        new DBSPIntegerLiteral(10, true))));
    }

    @Test
    public void cartesianTest() {
        String query = "SELECT * FROM T, T AS X";
        DBSPExpression inResult = DBSPTupleExpression.flatten(e0, e0);
        DBSPZSetLiteral result = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, inResult);
        result.add(DBSPTupleExpression.flatten(e0, e1));
        result.add(DBSPTupleExpression.flatten(e1, e0));
        result.add(DBSPTupleExpression.flatten(e1, e1));
        this.testQuery(query, result);
    }

    @Test
    public void foldTest() {
        String query = "SELECT + 91 + NULLIF ( + 93, + 38 )";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, new DBSPTupleExpression(
                new DBSPIntegerLiteral(184, true))));
    }

    @Test
    public void orderbyTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPVecLiteral(this.e1, this.e0)
        ));
    }

    @Test
    public void orderbyDescendingTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 DESC";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPVecLiteral(this.e0, this.e1)
        ));
    }

    @Test
    public void orderby2Test() {
        String query = "SELECT * FROM T ORDER BY T.COL2, T.COL1";
        this.testQuery(query, new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType,
                new DBSPVecLiteral(this.e1, this.e0)
        ));
    }
}
