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
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.Solutions;
import org.dbsp.sqlCompiler.compiler.optimizer.CircuitOptimizer;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.midend.ExpressionCompiler;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.midend.TableContents;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.sqllogictest.*;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Sql test executor that uses DBSP as a SQL runtime.
 * Does not support arbitrary tests: only tests that can be recast as a standing query will work.
 */
public class DBSPExecutor extends SqlTestExecutor {
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

    static final String rustDirectory = "../temp/src/";
    static final String testFileName = "test";
    static final String csvBaseName = "data";
    private final boolean execute;
    private int batchSize;  // Number of queries to execute together
    private int skip;       // Number of queries to skip in each test file.
    public final boolean incrementalMode;  // If true test an incremental streaming version of the circuit.
    final SqlTestPrepareInput inputPreparation;
    final SqlTestPrepareTables tablePreparation;
    final SqlTestPrepareViews viewPreparation;
    private final List<SqlTestQuery> queriesToRun;

    public void setBatchSize(int batchSize, int skip) {
        this.batchSize = batchSize;
        this.skip = skip;
    }

    /**
     * Create an executor that executes SqlLogicTest queries directly compiling to
     * Rust and using the DBSP library.
     * @param execute  If true the tests are executed, otherwise they are only compiled to Rust.
     * @param incremental  If true the executor generates and tests incremental circuit.
     */
    public DBSPExecutor(boolean execute, boolean incremental) {
        this.execute = execute;
        this.incrementalMode = incremental;
        this.inputPreparation = new SqlTestPrepareInput();
        this.tablePreparation = new SqlTestPrepareTables();
        this.viewPreparation = new SqlTestPrepareViews();
        this.batchSize = 10;
        this.queriesToRun = new ArrayList<>();
    }

    public DBSPZSetLiteral[] getInputSets(DBSPCompiler compiler) throws SQLException, SqlParseException {
        for (SqlStatement statement : this.inputPreparation.statements)
            compiler.compileStatement(statement.statement, null);
        TableContents tables = compiler.getTableContents();
        DBSPZSetLiteral[] tuple = new DBSPZSetLiteral[tables.tablesCreated.size()];
        for (int i = 0; i < tuple.length; i++) {
            String table = tables.tablesCreated.get(i);
            tuple[i] = tables.getTableContents(table);
        }
        return tuple;
    }

    DBSPFunction createInputFunction(DBSPZSetLiteral[] tuple) throws IOException {
        DBSPExpression[] fields = new DBSPExpression[tuple.length];
        int totalSize = 0;
        for (int i = 0; i < tuple.length; i++) {
            totalSize += tuple[i].size();
            fields[i] = tuple[i];
        }

        // If the data is large write it to a set of CSV files and read it at runtime.
        if (totalSize > 10) {
            for (int i = 0; i < tuple.length; i++) {
                String fileName = (rustDirectory + csvBaseName) + i + ".csv";
                Solutions.toCsv(fileName, tuple[i]);
                fields[i] = new DBSPApplyExpression("read_csv",
                        tuple[i].getNonVoidType(),
                        new DBSPStrLiteral(fileName));
            }
        }

        DBSPRawTupleExpression result = new DBSPRawTupleExpression(fields);
        return new DBSPFunction("input", new ArrayList<>(),
                result.getType(), result);
    }

    /**
     * Example generated code for the function body:
     *     let mut vec = Vec::new();
     *     vec.push((data.0, zset!(), zset!(), zset!()));
     *     vec.push((zset!(), data.1, zset!(), zset!()));
     *     vec.push((zset!(), zset!(), data.2, zset!()));
     *     vec.push((zset!(), zset!(), zset!(), data.3));
     *     vec
     */
    DBSPFunction createStreamInputFunction(
            DBSPFunction inputGeneratingFunction) {
        DBSPTypeRawTuple inputType = Objects.requireNonNull(inputGeneratingFunction.returnType).to(DBSPTypeRawTuple.class);
        DBSPType returnType = new DBSPTypeVec(inputType);
        DBSPVariableReference vec = new DBSPVariableReference("vec", returnType);
        DBSPLetStatement input = new DBSPLetStatement("data", inputGeneratingFunction.call());
        List<DBSPStatement> statements = new ArrayList<>();
        statements.add(input);
        DBSPLetStatement let = new DBSPLetStatement(vec.variable,
                new DBSPApplyExpression(new DBSPPathExpression(
                        DBSPTypeAny.instance,
                        new DBSPPath("Vec", "new"))),
                true);
        statements.add(let);
        if (this.incrementalMode) {
            for (int i = 0; i < inputType.tupFields.length; i++) {
                DBSPExpression[] fields = new DBSPExpression[inputType.tupFields.length];
                for (int j = 0; j < inputType.tupFields.length; j++) {
                    DBSPType fieldType = inputType.tupFields[j];
                    if (i == j) {
                        fields[j] = new DBSPFieldExpression(input.getVarReference(), i);
                    } else {
                        fields[j] = new DBSPApplyExpression("zset!", fieldType);
                    }
                }
                DBSPExpression projected = new DBSPRawTupleExpression(fields);
                DBSPExpression expr = new DBSPApplyMethodExpression(
                        "push", null, vec, projected);
                DBSPStatement statement = new DBSPExpressionStatement(expr);
                statements.add(statement);
            }
        } else {
            DBSPExpression expr = new DBSPApplyMethodExpression(
                    "push", null, vec, input.getVarReference());
            DBSPStatement statement = new DBSPExpressionStatement(expr);
            statements.add(statement);
        }
        DBSPBlockExpression block = new DBSPBlockExpression(statements, vec);
        return new DBSPFunction("stream_input", Linq.list(), returnType, block);
    }

    void runBatch(TestStatistics result) throws SqlParseException, IOException, InterruptedException, SQLException {
        DBSPCompiler compiler = new DBSPCompiler();
        final List<ProgramAndTester> codeGenerated = new ArrayList<>();
        // Create input tables
        this.createTables(compiler);
        // Create function which generates inputs for all tests in this batch.
        // We know that all these tests consume the same input tables.
        DBSPZSetLiteral[] inputSets = this.getInputSets(compiler);
        DBSPFunction inputFunction = this.createInputFunction(inputSets);
        DBSPFunction streamInputFunction = this.createStreamInputFunction(inputFunction);

        // Generate a function and a tester for each query.
        int queryNo = 0;
        for (SqlTestQuery testQuery : this.queriesToRun) {
            try {
                ProgramAndTester pc = this.generateTestCase(
                        compiler, streamInputFunction, this.viewPreparation, testQuery, queryNo);
                codeGenerated.add(pc);
                this.queriesExecuted++;
            } catch (Throwable ex) {
                System.err.println("Error while compiling " + testQuery.query);
                throw ex;
            }
            queryNo++;
        }

        // Write the code to Rust files on the filesystem.
        List<String> filesGenerated = this.writeCodeToFiles(
                Linq.list(inputFunction, streamInputFunction), codeGenerated);
        Utilities.writeRustMain(rustDirectory + "/main.rs", filesGenerated);
        this.startTest();
        if (this.execute) {
            Utilities.compileAndTestRust(rustDirectory, true);
        }
        this.queriesToRun.clear();
        this.reportTime(queryNo);
        this.cleanupFilesystem();
        result.passed += queryNo;  // This is not entirely correct, but I am not parsing the rust output
    }

    ProgramAndTester generateTestCase(
            DBSPCompiler compiler,
            DBSPFunction inputGeneratingFunction,
            SqlTestPrepareViews viewPreparation,
            SqlTestQuery testQuery, int suffix)
            throws SqlParseException {
        String origQuery = testQuery.query;
        String dbspQuery = origQuery;
        if (!dbspQuery.toLowerCase().contains("create view"))
            dbspQuery = "CREATE VIEW V AS (" + origQuery + ")";
        if (this.getDebugLevel() > 0)
            Logger.instance.append("Query ")
                    .append(suffix)
                    .append(":\n")
                    .append(dbspQuery)
                    .append("\n");
        compiler.newCircuit("gen" + suffix);
        compiler.generateOutputForNextView(false);
        for (SqlStatement view: viewPreparation.definitions()) {
            compiler.compileStatement(view.statement, view.statement);
        }
        compiler.generateOutputForNextView(true);
        compiler.compileStatement(dbspQuery, testQuery.name);
        DBSPCircuit dbsp = compiler.getResult();
        CompilerOptions options = new CompilerOptions();
        options.incrementalize = this.incrementalMode;
        CircuitOptimizer optimizer = new CircuitOptimizer(options);
        dbsp = optimizer.optimize(dbsp);
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
                expectedOutput = new DBSPZSetLiteral(vec);
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
                if (!colType.sameType(field.getNonVoidType()))
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
        DBSPFunction func = createTesterCode(
                "tester" + suffix, dbsp,
                inputGeneratingFunction,
                compiler.getTableContents(),
                expectedOutput, testQuery.outputDescription);
        return new ProgramAndTester(rust, func);
    }

    void cleanupFilesystem() {
        File directory = new File(rustDirectory);
        FilenameFilter filter = (dir, name) -> name.startsWith(testFileName) ||
                (name.startsWith(csvBaseName) && name.endsWith("csv"));
        File[] files = directory.listFiles(filter);
        if (files == null)
            return;
        for (File file: files) {
            boolean deleted = file.delete();
            if (!deleted)
                throw new RuntimeException("Cannot delete file " + file);
        }
    }

    void createTables(DBSPCompiler compiler) throws SqlParseException {
        for (SqlStatement statement : this.tablePreparation.statements) {
            String stat = statement.statement;
            compiler.compileStatement(stat, stat);
        }
    }

    @Override
    public TestStatistics execute(SqlTestFile file)
            throws SqlParseException, IOException, InterruptedException, SQLException {
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
                    seenQueries = false;
                }
                boolean status = this.statement(stat);
                this.statementsExecuted++;
                if (status != stat.shouldPass)
                    throw new RuntimeException("Statement failed " + stat.statement);
            } else {
                SqlTestQuery query = operation.to(SqlTestQuery.class);
                if (toSkip > 0) {
                    toSkip--;
                    result.ignored++;
                    continue;
                }
                if (this.buggyQueries.contains(query.query)) {
                    System.err.println("Skipping " + query.query);
                    result.ignored++;
                    continue;
                }
                seenQueries = true;
                this.queriesToRun.add(query);
                remainingInBatch--;
                if (remainingInBatch == 0) {
                    this.runBatch(result);
                    remainingInBatch = this.batchSize;
                    seenQueries = false;
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

    /**
     * Generates a Rust function which tests a DBSP circuit.
     * @param name          Name of the generated function.
     * @param circuit       DBSP circuit that will be tested.
     * @param output        Expected data from the circuit.
     * @param description   Description of the expected outputs.
     * @return              The code for a function that runs the circuit with the specified
     *                      input and tests the produced output.
     */
    static DBSPFunction createTesterCode(
            String name,
            DBSPCircuit circuit,
            DBSPFunction inputGeneratingFunction,
            TableContents contents,
            @Nullable DBSPZSetLiteral output,
            SqlTestQueryOutputDescription description) {
        List<DBSPStatement> list = new ArrayList<>();
        DBSPLetStatement circ = new DBSPLetStatement("circ",
                new DBSPApplyExpression(circuit.name, DBSPTypeAny.instance), true);
        list.add(circ);
        DBSPType circuitOutputType = circuit.getOutputType(0);
        // the following may not be the same, since SqlLogicTest sometimes lies about the output type
        DBSPTypeRawTuple outputType = new DBSPTypeRawTuple(output != null ? output.getNonVoidType() : circuitOutputType);
        DBSPExpression[] arguments = new DBSPExpression[circuit.getInputTables().size()];
        // True if the output is a zset of vectors (generated for orderby queries)
        boolean isVector = circuitOutputType.to(DBSPTypeZSet.class).elementType.is(DBSPTypeVec.class);

        DBSPLetStatement inputStream = new DBSPLetStatement("_in_stream",
                inputGeneratingFunction.call());
        list.add(inputStream);
        for (int i = 0; i < arguments.length; i++) {
            String inputI = circuit.getInputTables().get(i);
            int index = contents.getTableIndex(inputI);
            arguments[i] = new DBSPFieldExpression(null,
                    new DBSPVariableReference("_in", DBSPTypeAny.instance), index);
        }
        DBSPLetStatement createOutput = new DBSPLetStatement(
                "output",
                new DBSPRawTupleExpression(
                        new DBSPApplyExpression("zset!", outputType.getFieldType(0))), true);
        list.add(createOutput);
        DBSPForExpression loop = new DBSPForExpression(
                new DBSPIdentifierPattern("_in"),
                inputStream.getVarReference(),
                new DBSPBlockExpression(
                        Linq.list(),
                                new DBSPAssignmentExpression(createOutput.getVarReference(),
                                        new DBSPApplyExpression("add_zset_tuple", outputType,
                                                createOutput.getVarReference(),
                                                new DBSPApplyExpression("circ", outputType, arguments)))
                )
        );
        list.add(new DBSPExpressionStatement(loop));
        DBSPExpression sort = new DBSPEnumValue("SortOrder", description.order.toString());
        DBSPExpression output0 = new DBSPFieldExpression(null,
                createOutput.getVarReference(), 0);

        if (description.getExpectedOutputSize() >= 0) {
            DBSPExpression count;
            if (isVector) {
                count = new DBSPApplyExpression("weighted_vector_count",
                        DBSPTypeUSize.instance,
                        new DBSPBorrowExpression(output0));
            } else {
                count = new DBSPApplyMethodExpression("weighted_count",
                        DBSPTypeUSize.instance,
                        output0);
            }
            list.add(new DBSPExpressionStatement(
                    new DBSPApplyExpression("assert_eq!", null,
                            count, new DBSPISizeLiteral(description.getExpectedOutputSize()))));
        }if (output != null) {
            if (description.columnTypes != null) {
                DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
                DBSPTypeZSet oType = output.getNonVoidType().to(DBSPTypeZSet.class);
                String functionProducingStrings;
                DBSPType elementType;
                if (isVector) {
                    functionProducingStrings = "zset_of_vectors_to_strings";
                    elementType = oType.elementType.to(DBSPTypeVec.class).getElementType();
                } else {
                    functionProducingStrings = "zset_to_strings";
                    elementType = oType.elementType;
                }
                DBSPExpression zset_to_strings = new DBSPQualifyTypeExpression(
                        new DBSPVariableReference(functionProducingStrings, DBSPTypeAny.instance),
                        elementType,
                        oType.weightType
                );
                list.add(new DBSPExpressionStatement(
                        new DBSPApplyExpression("assert_eq!", null,
                                new DBSPApplyExpression(functionProducingStrings, DBSPTypeAny.instance,
                                        new DBSPBorrowExpression(output0),
                                        columnTypes,
                                        sort),
                                new DBSPApplyExpression(zset_to_strings,
                                        new DBSPBorrowExpression(output),
                                        columnTypes,
                                        sort))));
            } else {
                list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                        "assert_eq!", null, output0, output)));
            }
        } else {
            if (description.columnTypes == null)
                throw new RuntimeException("Expected column types to be supplied");
            DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
            if (description.hash == null)
                throw new RuntimeException("Expected hash to be supplied");
            String hash = isVector ? "hash_vectors" : "hash";
            list.add(new DBSPLetStatement("_hash",
                    new DBSPApplyExpression(hash, DBSPTypeString.instance,
                            new DBSPBorrowExpression(output0),
                            columnTypes,
                            sort)));
            list.add(
                    new DBSPExpressionStatement(
                            new DBSPApplyExpression("assert_eq!", null,
                                    new DBSPVariableReference("_hash", DBSPTypeString.instance),
                                    new DBSPStringLiteral(description.hash))));
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction(name, new ArrayList<>(), null, body)
                .addAnnotation("#[test]");
    }

    public boolean statement(SqlStatement statement) throws SQLException {
        String command = statement.statement.toLowerCase();
        if (command.startsWith("create index"))
            return true;
        if (command.startsWith("create distinct index"))
            return false;
        if (command.contains("create table") || command.contains("drop table")) {
            this.tablePreparation.add(statement);
        } else if (command.contains("create view")) {
            this.viewPreparation.add(statement);
        } else if (command.contains("drop view")) {
            this.viewPreparation.remove(statement);
        } else
            this.inputPreparation.add(statement);
        return true;
    }

    void reset() {
        this.inputPreparation.clear();
        this.tablePreparation.clear();
        this.viewPreparation.clear();
        this.queriesToRun.clear();
    }

    public List<String> writeCodeToFiles(
            List<DBSPFunction> inputFunctions,
            List<ProgramAndTester> functions
    ) throws FileNotFoundException, UnsupportedEncodingException {
        String genFileName = testFileName + ".rs";
        String testFilePath = rustDirectory + "/" + genFileName;
        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());

        for (DBSPFunction function: inputFunctions)
            writer.println(ToRustVisitor.toRustString(function));
        for (ProgramAndTester pt: functions) {
            writer.println(pt.program);
            writer.println(ToRustVisitor.toRustString(pt.tester));
        }
        writer.close();
        return Linq.list(testFileName);
    }
}
