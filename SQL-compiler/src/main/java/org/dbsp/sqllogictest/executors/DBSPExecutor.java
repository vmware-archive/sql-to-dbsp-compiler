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
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.backend.*;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.sqllogictest.*;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.io.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

/**
 * Sql test executor that uses DBSP as a SQL runtime.
 * Does not support arbitrary tests: only tests that can be recast as a standing query will work.
 */
public class DBSPExecutor extends SqlSLTTestExecutor {
    /**
     * A pair of a Rust circuit representation and a tester function that can
     * exercise it.
     */
    static class ProgramAndTester {
        public final DBSPCircuit program;
        public final DBSPFunction tester;

        ProgramAndTester(DBSPCircuit program, DBSPFunction tester) {
            this.program = program;
            this.tester = tester;
        }
    }

    static final String rustDirectory = "../temp/src/";
    static final String testFileName = "test";
    private final boolean execute;
    private final boolean validateJson;
    private int batchSize;  // Number of queries to execute together
    private int skip;       // Number of queries to skip in each test file.
    public final CompilerOptions options;

    private final String connectionString; // either csv or a valid sqlx connection string
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
     * @param validateJson If true validate the JSON IR representation for each query.
     * @param connectionString Connection string to use to get ground truth from database.
     * @param execute  If true the tests are executed, otherwise they are only compiled to Rust.
     * @param options  Options to use for compilation.
     */
    public DBSPExecutor(boolean execute, boolean validateJson, CompilerOptions options, String connectionString) {
        this.execute = execute;
        this.validateJson = validateJson;
        this.inputPreparation = new SqlTestPrepareInput();
        this.tablePreparation = new SqlTestPrepareTables();
        this.viewPreparation = new SqlTestPrepareViews();
        this.batchSize = 10;
        this.options = options;
        this.queriesToRun = new ArrayList<>();
        this.connectionString = connectionString;
    }

    public static class TableValue {
        public final String tableName;
        public final DBSPZSetLiteral contents;

        public TableValue(String tableName, DBSPZSetLiteral contents) {
            this.tableName = tableName;
            this.contents = contents;
        }
    }

    public TableValue[] getInputSets(DBSPCompiler compiler) throws SQLException {
        for (SqlStatement statement : this.inputPreparation.statements)
            compiler.compileStatement(statement.statement, null);
        TableContents tables = compiler.getTableContents();
        TableValue[] tableValues = new TableValue[tables.tablesCreated.size()];
        for (int i = 0; i < tableValues.length; i++) {
            String table = tables.tablesCreated.get(i);
            tableValues[i] = new TableValue(table, tables.getTableContents(table));
        }
        return tableValues;
    }

    DBSPFunction createInputFunction(TableValue[] tables) throws IOException {
        DBSPExpression[] fields = new DBSPExpression[tables.length];
        int totalSize = 0;
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < tables.length; i++) {
            totalSize += tables[i].contents.size();
            fields[i] = tables[i].contents;
            if (seen.contains(tables[i].tableName))
                throw new RuntimeException("Table " + tables[i].tableName + " already in input");
            seen.add(tables[i].tableName);
        }

        if (totalSize > 10) {
            if (connectionString.equals("csv")) {
                // If the data is large write, it to a set of CSV files and read it at runtime.
                for (int i = 0; i < tables.length; i++) {
                    String fileName = (rustDirectory + tables[i].tableName) + ".csv";
                    ToCsvVisitor.toCsv(fileName, tables[i].contents);
                    fields[i] = new DBSPApplyExpression("read_csv",
                            tables[i].contents.getNonVoidType(),
                            new DBSPStrLiteral(fileName));
                }
            } else {
                // read from DB
                for (int i = 0; i < tables.length; i++) {
                    fields[i] = generateReadDbCall(tables[i]);
                }
            }
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(fields);
        return new DBSPFunction("input", new ArrayList<>(),
                result.getType(), result);
    }

    private DBSPExpression generateReadDbCall(TableValue tableValue) {
        // Generates a read_table(<conn>, <table_name>, <mapper from |AnyRow| -> Tuple type>) invocation
        DBSPTypeUser sqliteRowType = new DBSPTypeUser(null, "AnyRow", false);
        DBSPVariablePath rowVariable = new DBSPVariablePath("row", sqliteRowType);
        DBSPTypeTuple tupleType = tableValue.contents.zsetType.elementType.to(DBSPTypeTuple.class);
        final List<DBSPExpression> rowGets = new ArrayList<>(tupleType.tupFields.length);
        for (int i = 0; i <  tupleType.tupFields.length; i++) {
            DBSPApplyMethodExpression rowGet =
                    new DBSPApplyMethodExpression("get",
                            tupleType.tupFields[i],
                            rowVariable, new DBSPUSizeLiteral(i));
            rowGets.add(rowGet);
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(rowGets, false);
        DBSPClosureExpression mapClosure = new DBSPClosureExpression(null, tuple,
                rowVariable.asRefParameter());
        return new DBSPApplyExpression("read_db", tableValue.contents.zsetType,
                new DBSPStrLiteral(connectionString), new DBSPStrLiteral(tableValue.tableName),
                mapClosure);
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
        DBSPVariablePath vec = returnType.var("vec");
        DBSPLetStatement input = new DBSPLetStatement("data", inputGeneratingFunction.call());
        List<DBSPStatement> statements = new ArrayList<>();
        statements.add(input);
        DBSPLetStatement let = new DBSPLetStatement(vec.variable,
                new DBSPApplyExpression(DBSPTypeAny.INSTANCE.path(
                        new DBSPPath("Vec", "new"))),
                true);
        statements.add(let);
        if (this.options.optimizerOptions.incrementalize) {
            for (int i = 0; i < inputType.tupFields.length; i++) {
                DBSPExpression field = input.getVarReference().field(i);
                DBSPExpression elems = new DBSPApplyExpression("to_elements",
                        DBSPTypeAny.INSTANCE, field.borrow());

                DBSPVariablePath e = DBSPTypeAny.INSTANCE.var("e");
                DBSPExpression[] fields = new DBSPExpression[inputType.tupFields.length];
                for (int j = 0; j < inputType.tupFields.length; j++) {
                    DBSPType fieldType = inputType.tupFields[j];
                    if (i == j) {
                        fields[j] = e.applyClone();
                    } else {
                        fields[j] = new DBSPApplyExpression("zset!", fieldType);
                    }
                }
                DBSPExpression projected = new DBSPRawTupleExpression(fields);
                DBSPExpression lambda = projected.closure(e.asParameter());
                DBSPExpression iter = new DBSPApplyMethodExpression(
                        "iter", DBSPTypeAny.INSTANCE, elems);
                DBSPExpression map = new DBSPApplyMethodExpression(
                        "map", DBSPTypeAny.INSTANCE, iter, lambda);
                DBSPExpression expr = new DBSPApplyMethodExpression(
                        "extend", null, vec, map);
                DBSPStatement statement = new DBSPExpressionStatement(expr);
                statements.add(statement);
            }
            if (inputType.tupFields.length == 0) {
                // This case will cause no invocation of the circuit, but we need
                // at least one.
                DBSPExpression expr = new DBSPApplyMethodExpression(
                        "push", null, vec, new DBSPRawTupleExpression());
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

    void runBatch(TestStatistics result) throws IOException, InterruptedException, SQLException {
        DBSPCompiler compiler = new DBSPCompiler(this.options);
        final List<ProgramAndTester> codeGenerated = new ArrayList<>();
        // Create input tables
        this.createTables(compiler);
        compiler.throwOnError();
        // Create function which generates inputs for all tests in this batch.
        // We know that all these tests consume the same input tables.
        TableValue[] inputSets = this.getInputSets(compiler);
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
                System.err.println("Error while compiling " + testQuery.query + ": " + ex.getMessage());
                throw ex;
            }
            queryNo++;
        }

        // Write the code to Rust files on the filesystem.
        String fileGenerated = this.writeCodeToFile(
                Linq.list(inputFunction, streamInputFunction), codeGenerated);
        Utilities.writeRustLib(rustDirectory + "/lib.rs", Linq.list(fileGenerated));
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
            SqlTestQuery testQuery, int suffix) {
        String origQuery = testQuery.query;
        String dbspQuery = origQuery;
        if (!dbspQuery.toLowerCase().contains("create view"))
            dbspQuery = "CREATE VIEW V AS (" + origQuery + ")";
        Logger.INSTANCE.from(this, 1)
                .append("Query ")
                .append(suffix)
                .append(":\n")
                .append(dbspQuery)
                .append(testQuery.name != null ? " " + testQuery.name : "")
                .append("\n");
        compiler.generateOutputForNextView(false);
        for (SqlStatement view: viewPreparation.definitions()) {
            compiler.compileStatement(view.statement, view.statement);
            compiler.throwOnError();
        }
        compiler.generateOutputForNextView(true);
        compiler.compileStatement(dbspQuery, testQuery.name);
        compiler.throwOnError();
        compiler.optimize();
        DBSPCircuit dbsp = compiler.getFinalCircuit("gen" + suffix);
        //ToDotVisitor.toDot("circuit.jpg", true, dbsp);
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
                    field = new DBSPI32Literal(Integer.parseInt(s));
                else if (colType.is(DBSPTypeDouble.class))
                    field = new DBSPDoubleLiteral(Double.parseDouble(s));
                else if (colType.is(DBSPTypeFloat.class))
                    field = new DBSPFloatLiteral(Float.parseFloat(s));
                else if (colType.is(DBSPTypeString.class))
                    field = new DBSPStringLiteral(s);
                else if (colType.is(DBSPTypeDecimal.class))
                    field = new DBSPDecimalLiteral(s, colType, new BigDecimal(s));
                else if (colType.is(DBSPTypeBool.class))
                    // Booleans are encoded as ints
                    field = new DBSPBoolLiteral(Integer.parseInt(s) != 0);
                else
                    throw new RuntimeException("Unexpected type " + colType);
                if (!colType.sameType(field.getNonVoidType()))
                    field = ExpressionCompiler.makeCast(null, field, colType);
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

        if (this.validateJson)
            ToJitVisitor.validateJson(dbsp);
        DBSPFunction func = createTesterCode(
                "tester" + suffix, dbsp,
                inputGeneratingFunction,
                compiler.getTableContents(),
                expectedOutput, testQuery.outputDescription);
        return new ProgramAndTester(dbsp, func);
    }

    void cleanupFilesystem() {
        File directory = new File(rustDirectory);
        FilenameFilter filter = (dir, name) -> name.startsWith(testFileName) || name.endsWith("csv");
        File[] files = directory.listFiles(filter);
        if (files == null)
            return;
        for (File file: files) {
            boolean deleted = file.delete();
            if (!deleted)
                throw new RuntimeException("Cannot delete file " + file);
        }
    }

    void createTables(DBSPCompiler compiler) {
        for (SqlStatement statement : this.tablePreparation.statements) {
            String stat = statement.statement;
            compiler.compileStatement(stat, stat);
        }
    }

    @Override
    public TestStatistics execute(SLTTestFile file)
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
                boolean status;
                try {
                    if (this.buggyOperations.contains(stat.statement)) {
                        Logger.INSTANCE.from(this, 1)
                                .append("Skipping buggy test ")
                                .append(stat.statement)
                                .newline();
                        status = stat.shouldPass;
                    } else {
                        status = this.statement(stat);
                    }
                } catch (SQLException ex) {
                    Logger.INSTANCE.from(this, 1)
                            .append("Statement failed ")
                            .append(stat.statement)
                            .newline();
                    status = false;
                }
                this.statementsExecuted++;
                if (this.validateStatus &&
                        status != stat.shouldPass)
                    throw new RuntimeException("Statement " + stat.statement + " status " + status + " expected " + stat.shouldPass);
            } else {
                SqlTestQuery query = operation.to(SqlTestQuery.class);
                if (toSkip > 0) {
                    toSkip--;
                    result.ignored++;
                    continue;
                }
                if (this.buggyOperations.contains(query.query)) {
                    Logger.INSTANCE.from(this, 1)
                            .append("Skipping buggy test ")
                            .append(query.query)
                            .newline();
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
        Logger.INSTANCE.from(this, 1)
                .append("Finished executing ")
                .append(file.toString())
                .newline();
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
                new DBSPApplyExpression(circuit.name, DBSPTypeAny.INSTANCE), true);
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
                    DBSPTypeAny.INSTANCE.var("_in"), index);
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
                        DBSPTypeUSize.INSTANCE,
                        output0.borrow());
            } else {
                count = new DBSPApplyMethodExpression("weighted_count",
                        DBSPTypeUSize.INSTANCE,
                        output0);
            }
            list.add(new DBSPExpressionStatement(
                    new DBSPApplyExpression("assert_eq!", null,
                            new DBSPAsExpression(count, DBSPTypeZSet.defaultWeightType), new DBSPAsExpression(
                                    new DBSPI32Literal(description.getExpectedOutputSize()),
                            DBSPTypeZSet.defaultWeightType))));
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
                        DBSPTypeAny.INSTANCE.var(functionProducingStrings),
                        elementType,
                        oType.weightType
                );
                list.add(new DBSPExpressionStatement(
                        new DBSPApplyExpression("assert_eq!", null,
                                new DBSPApplyExpression(functionProducingStrings, DBSPTypeAny.INSTANCE,
                                        output0.borrow(),
                                        columnTypes,
                                        sort),
                                new DBSPApplyExpression(zset_to_strings,
                                        output.borrow(),
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
                    new DBSPApplyExpression(hash, DBSPTypeString.INSTANCE,
                            output0.borrow(),
                            columnTypes,
                            sort)));
            list.add(
                    new DBSPExpressionStatement(
                            new DBSPApplyExpression("assert_eq!", null,
                                    DBSPTypeString.INSTANCE.var("_hash"),
                                    new DBSPStringLiteral(description.hash))));
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction(name, new ArrayList<>(), null, body)
                .addAnnotation("#[test]");
    }

    public boolean statement(SqlStatement statement) throws SQLException {
        Logger.INSTANCE.from(this, 1)
                .append("Executing ")
                .append(statement.toString())
                .newline();
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

    public String writeCodeToFile(
            List<DBSPFunction> inputFunctions,
            List<ProgramAndTester> functions
    ) throws FileNotFoundException, UnsupportedEncodingException {
        String genFileName = testFileName + ".rs";
        String testFilePath = rustDirectory + "/" + genFileName;
        PrintStream stream = new PrintStream(testFilePath, "UTF-8");
        RustFileWriter rust = new RustFileWriter(stream);

        for (DBSPFunction function: inputFunctions)
            rust.add(function);
        for (ProgramAndTester pt: functions) {
            rust.add(pt.program);
            rust.add(pt.tester);
        }
        rust.writeAndClose();
        return testFileName;
    }
}
