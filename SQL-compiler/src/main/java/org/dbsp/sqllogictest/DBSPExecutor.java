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
 *
 *
 */

package org.dbsp.sqllogictest;

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
import org.dbsp.sqlCompiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.frontend.CalciteProgram;
import org.dbsp.sqlCompiler.frontend.SimulatorResult;
import org.dbsp.sqlCompiler.frontend.TableModifyStatement;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Sql test executor that uses DBSP as a SQL runtime.
 * Does not support arbitrary tests: only tests that can be recast as a standing query will work.
 */
@SuppressWarnings("FieldCanBeLocal")
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

    private final boolean debug = false;
    static final String rustDirectory = "../temp/src/";
    static final String testFileName = "test";
    int queryNo;
    @Nullable
    private CalciteCompiler calcite;
    private final List<ProgramAndTester> queries;
    private final String inputFunctionName = "input";
    @Nullable
    private DBSPFunction inputFunction = null;
    // If this is 'false' we just parse and compile the tests.
    private final boolean execute;
    private final List<String> filesGenerated;

    public DBSPExecutor(boolean execute) {
        this.calcite = null;
        this.execute = execute;
        this.queryNo = 0;
        this.queries = new ArrayList<>();
        this.filesGenerated = new ArrayList<>();
    }

    @Override
    public void reset() {
        this.calcite = new CalciteCompiler();
        this.queries.clear();
        this.filesGenerated.clear();
        this.inputFunction = null;
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

    @Override
    public void createTables(SqlTestPrepareTables prepare) throws SqlParseException {
        if (this.calcite == null)
            throw new RuntimeException("Calcite compiler not initialized yet");
        this.calcite.startCompilation();
        for (String statement : prepare.statements)
            this.calcite.compile(statement);
    }

    @Override
    public void addQuery(
            String query, SqlTestPrepareInput inputDescription,
            SqlTestOutputDescription output) throws SqlParseException {
        //if (!query.equals("SELECT DISTINCT - 15 - + - 2 FROM ( tab0 AS cor0 CROSS JOIN tab1 AS cor1 )")) return;
        if (this.calcite == null)
            throw new RuntimeException("Calcite compiler not initialized yet");
        // heuristic: add a "CREATE VIEW V AS" in front
        query = "CREATE VIEW V AS (" + query + ")";
        if (this.debug) {
            System.out.println("Query:\n" + query);
            if (output.queryResults != null)
                System.out.println("Expected results " + output.queryResults);
        }
        this.calcite.compile(query);
        CalciteProgram program = calcite.getProgram();
        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler(calcite);
        DBSPCircuit dbsp = compiler.compile(program, "c" + this.queryNo);

        DBSPZSetLiteral expectedOutput = null;
        if (output.queryResults != null) {
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
            for (String s: output.queryResults) {
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
            if (output.hash == null)
                throw new RuntimeException("No hash or outputs specified");
        }

        if (this.inputFunction == null) {
            // Here we assume that the input statements are the same for all queries in a test,
            // and that all queries in a test depend on the same set of input tables.
            // So we generate a single input function to produce this input.
            // Makes for shorter code.
            DBSPTransaction transaction = new DBSPTransaction();
            for (String statement : inputDescription.statements) {
                SimulatorResult result = this.calcite.compile(statement);
                TableModifyStatement stat = (TableModifyStatement) result;
                compiler.extendTransaction(transaction, stat);
            }
            this.inputFunction = transaction.inputGeneratingFunction(inputFunctionName, dbsp);
        }

        String rust = ToRustVisitor.toRustString(dbsp);
        DBSPFunction func = RustTestGenerator.createTesterCode(
                "tester" + this.queryNo, inputFunctionName,
                dbsp, expectedOutput, output);
        this.queries.add(new ProgramAndTester(rust, func));
        this.queryNo++;
    }

    @Override
    public void generateCode(int index) throws FileNotFoundException, UnsupportedEncodingException {
        if (this.inputFunction == null)
            return;
        String genFileName = testFileName + index + ".rs";
        this.filesGenerated.add(testFileName + index);
        String testFilePath = rustDirectory + "/" + genFileName;
        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());

        writer.println(ToRustVisitor.toRustString(this.inputFunction));
        for (ProgramAndTester pt: this.queries) {
            writer.println(pt.program);
            writer.println("#[test]");
            writer.println(ToRustVisitor.toRustString(pt.tester));
        }
        writer.close();
        this.queries.clear();
    }

    public void run() throws IOException, InterruptedException {
        if (this.execute) {
            Utilities.writeRustMain(rustDirectory + "/main.rs", this.filesGenerated);
            Utilities.compileAndTestRust(rustDirectory);
        }
    }
}
