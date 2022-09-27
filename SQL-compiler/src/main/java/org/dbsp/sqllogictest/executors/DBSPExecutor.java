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

package org.dbsp.sqllogictest.executors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.dbsp.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.dbsp.DBSPTransaction;
import org.dbsp.sqlCompiler.dbsp.ExpressionCompiler;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.rust.DBSPFunction;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.*;
import org.dbsp.sqlCompiler.dbsp.rust.type.*;
import org.dbsp.sqlCompiler.dbsp.visitors.ToRustVisitor;
import org.dbsp.sqlCompiler.frontend.*;
import org.dbsp.sqllogictest.*;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sql test executor that uses DBSP as a SQL runtime.
 * Does not support arbitrary tests: only tests that can be recast as a standing query will work.
 */
public class DBSPExecutor implements ISqlTestExecutor {
    /**
     * A pair of a Rust circuit representation and a tester function that can
     * exercise it.
     */
    static class ProgramAndTester {
        public final String program;
        public final DBSPFunction tester;

        ProgramAndTester(String program, DBSPFunction tester) {
            this.program = program;
            this.tester = tester;
        }
    }

    @SuppressWarnings("FieldCanBeLocal")
    private final boolean debug = false;
    static final String rustDirectory = "../temp/src/";
    static final String testFileName = "test";
    private static final String inputFunctionName = "input";
    private final boolean execute;
    private int batchSize;  // Number of queries to execute together
    private int skip;       // Number of queries to skip in each test file.
    SqlTestPrepareInput inputPreparation;
    SqlTestPrepareTables tablePreparation;
    private final Set<String> calciteBugs;
    private final List<SqlTestQuery> queriesToRun;

    public void setBatchSize(int batchSize, int skip) {
        this.batchSize = batchSize;
        this.skip = skip;
    }

    public void avoid(HashSet<String> calciteBugs) {
        this.calciteBugs.addAll(calciteBugs);
    }

    /**
     * Create an executor that executes SqlLogicTest queries directly compiling to
     * Rust and using the DBSP library.
     * @param execute  If true the tests are executed, otherwise they are only compiled to Rust.
     */
    public DBSPExecutor(boolean execute) {
        this.execute = execute;
        this.inputPreparation = new SqlTestPrepareInput();
        this.tablePreparation = new SqlTestPrepareTables();
        this.calciteBugs = new HashSet<>();
        this.batchSize = 10;
        this.queriesToRun = new ArrayList<>();
    }

    DBSPFunction createInputFunction(CalciteToDBSPCompiler compiler, DBSPTransaction transaction) throws SqlParseException {
        for (SqlStatement statement : this.inputPreparation.statements) {
            SimulatorResult sim = compiler.calciteCompiler.compile(statement.statement);
            TableModifyStatement stat = (TableModifyStatement) sim;
            compiler.extendTransaction(transaction, stat);
        }
        return transaction.inputGeneratingFunction(inputFunctionName);
    }

    static long startTime = -1;
    static int totalTests = 0;

    void runBatch(TestStatistics result) throws SqlParseException, IOException, InterruptedException {
        CalciteCompiler calcite = new CalciteCompiler();
        calcite.startCompilation();
        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler(calcite);

        final List<ProgramAndTester> codeGenerated = new ArrayList<>();
        DBSPTransaction transaction = new DBSPTransaction();
        // Create input tables
        this.createTables(calcite, transaction);
        // Create function which generates inputs for all tests in this batch.
        // We know that all these tests consume the same input tables.
        DBSPFunction inputFunction = this.createInputFunction(compiler, transaction);

        // Generate a function and a tester for each query.
        int queryNo = 0;
        for (SqlTestQuery testQuery : this.queriesToRun) {
            try {
                ProgramAndTester pc = this.generateTestCase(compiler, testQuery, transaction, queryNo);
                codeGenerated.add(pc);
            } catch (Throwable ex) {
                System.err.println("Error while compiling " + testQuery.query);
                throw ex;
            }
            queryNo++;
        }

        // Write the code to Rust files on the filesystem.
        List<String> filesGenerated = this.writeCodeToFiles(inputFunction, codeGenerated);
        Utilities.writeRustMain(rustDirectory + "/main.rs", filesGenerated);
        long start = System.nanoTime();
        if (startTime == -1)
            startTime = start;
        if (this.execute) {
            Utilities.compileAndTestRust(rustDirectory);
        }
        this.queriesToRun.clear();
        long end = System.nanoTime();
        totalTests += queryNo;
        System.out.println(queryNo + " tests took " + seconds(end, start) + "s, "
                + totalTests + " took " + seconds(end, startTime) + "s");
        this.clenupFilesystem();
        result.passed += queryNo;  // This is not entirely correct, but I am not parsing the rust output
    }

    ProgramAndTester generateTestCase(
            CalciteToDBSPCompiler compiler, SqlTestQuery testQuery,
            DBSPTransaction transaction, int suffix)
            throws SqlParseException {
        String origQuery = testQuery.query;
        String dbspQuery = "CREATE VIEW V AS (" + origQuery + ")";
        if (this.debug)
            System.out.println("Query " + suffix + ":\n" + dbspQuery);
        compiler.calciteCompiler.startCompilation();
        compiler.calciteCompiler.compile(dbspQuery);
        CalciteProgram program = compiler.calciteCompiler.getProgram();
        DBSPCircuit dbsp = compiler.compile(program, "c" + suffix);
        DBSPZSetLiteral expectedOutput = null;
        if (testQuery.outputDescription.queryResults != null) {
            IDBSPContainer container;
            if (dbsp.getOutputCount() != 1)
                throw new RuntimeException(
                        "Didn't expect a query to have " + dbsp.getOutputCount() + " outputs");
            DBSPTypeZSet outputType = dbsp.getOutputType(0).to(DBSPTypeZSet.class);
            DBSPType elementType = outputType.elementType;
            if (elementType.is(DBSPTypeVec.class)) {
                elementType = elementType.to(DBSPTypeVec.class).getElementType();
                DBSPVecLiteral vec = new DBSPVecLiteral(elementType);
                container = vec;
                expectedOutput = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, vec);
            } else {
                expectedOutput = new DBSPZSetLiteral(outputType);
                container = expectedOutput;
            }
            DBSPTypeTuple outputElementType = elementType.to(DBSPTypeTuple.class);

            List<DBSPExpression> fields = new ArrayList<>();
            int col = 0;
            DBSPExpression field;
            for (String s: testQuery.outputDescription.queryResults) {
                DBSPType colType = outputElementType.tupFields[col];
                if (s.equalsIgnoreCase("null"))
                    field = DBSPLiteral.none(colType);
                else if (colType.is(DBSPTypeInteger.class))
                    field = new DBSPIntegerLiteral(Integer.parseInt(s));
                else if (colType.is(DBSPTypeDouble.class))
                    field = new DBSPDoubleLiteral(Double.parseDouble(s));
                else if (colType.is(DBSPTypeFloat.class))
                    field = new DBSPFloatLiteral(Float.parseFloat(s));
                else if (colType.is(DBSPTypeString.class))
                    field = new DBSPStringLiteral(s);
                else
                    throw new RuntimeException("Unexpected type " + colType);
                if (!colType.same(field.getNonVoidType()))
                    field = ExpressionCompiler.makeCast(field, colType);
                fields.add(field);
                col++;
                if (col == outputElementType.size()) {
                    container.add(new DBSPTupleExpression(outputElementType, fields));
                    fields = new ArrayList<>();
                    col = 0;
                }
            }
            if (col != 0) {
                throw new RuntimeException("Could not assign all query output values to rows. " +
                        "I have " + col + " leftover values in the last row");
            }
        } else {
            if (testQuery.outputDescription.hash == null)
                throw new RuntimeException("No hash or outputs specified");
        }

        String rust = ToRustVisitor.toRustString(dbsp);
        DBSPFunction func = RustTestGenerator.createTesterCode(
                "tester" + suffix, inputFunctionName, transaction,
                dbsp, expectedOutput, testQuery.outputDescription);
        return new ProgramAndTester(rust, func);
    }

    void clenupFilesystem() {
        File directory = new File(rustDirectory);
        FilenameFilter filter = (dir, name) -> name.startsWith(testFileName);
        File[] files = directory.listFiles(filter);
        if (files == null)
            return;
        for (File file: files) {
            boolean deleted = file.delete();
            if (!deleted)
                throw new RuntimeException("Cannot delete file " + file);
        }
    }

    void createTables(CalciteCompiler compiler, DBSPTransaction transaction) throws SqlParseException {
        for (SqlStatement statement : this.tablePreparation.statements) {
            SimulatorResult result = compiler.compile(statement.statement);
            transaction.addTable(result.to(TableDDL.class));
        }
    }

    @Override
    public TestStatistics execute(SqlTestFile file)
            throws SqlParseException, IOException, InterruptedException {
        TestStatistics result = new TestStatistics();

        boolean seenQueries = false;
        int remainingInBatch = this.batchSize;
        int toSkip = this.skip;
        for (ISqlTestOperation operation: file.fileContents) {
            SqlStatement stat = operation.as(SqlStatement.class);
            if (stat != null) {
                if (seenQueries) {
                    this.runBatch(result);
                    remainingInBatch = this.batchSize;
                }
                boolean status = this.statement(stat);
                if (status != stat.shouldPass)
                    throw new RuntimeException("Statement failed " + stat.statement);
            } else {
                SqlTestQuery query = operation.to(SqlTestQuery.class);
                if (toSkip > 0) {
                    toSkip--;
                    result.ignored++;
                    continue;
                }
                if (this.calciteBugs.contains(query.query)) {
                    System.err.println("Skipping " + query.query);
                    result.passed++;
                    continue;
                }
                seenQueries = true;
                this.queriesToRun.add(query);
                remainingInBatch--;
                if (remainingInBatch == 0) {
                    this.runBatch(result);
                    remainingInBatch = this.batchSize;
                }
            }
        }
        if (remainingInBatch != this.batchSize)
            this.runBatch(result);
        // Make sure there are no left-overs if this executor
        // is invoked to process a new file.
        this.reset();
        return result;
    }

    long seconds(long end, long start) {
        return (end - start) / 1000000000;
    }

    public boolean statement(SqlStatement statement) {
        String command = statement.statement.toLowerCase();
        if (command.startsWith("create index"))
            return true;
        if (command.startsWith("create distinct index"))
            return false;
        if (command.toLowerCase().contains("create table")) {
            this.tablePreparation.add(statement);
        } else {
            this.inputPreparation.add(statement);
        }
        return true;
    }

    void reset() {
        this.inputPreparation.clear();
        this.tablePreparation.clear();
        this.queriesToRun.clear();
    }

    public List<String> writeCodeToFiles(
            DBSPFunction inputFunction,
            List<ProgramAndTester> functions
    ) throws FileNotFoundException, UnsupportedEncodingException {
        String genFileName = testFileName + ".rs";
        String testFilePath = rustDirectory + "/" + genFileName;
        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());

        writer.println(ToRustVisitor.toRustString(inputFunction));
        for (ProgramAndTester pt: functions) {
            writer.println(pt.program);
            writer.println("#[test]");
            writer.println(ToRustVisitor.toRustString(pt.tester));
        }
        writer.close();
        return Linq.list(testFileName);
    }
}
