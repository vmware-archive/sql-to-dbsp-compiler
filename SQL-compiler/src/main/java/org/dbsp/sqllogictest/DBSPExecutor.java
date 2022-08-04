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
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.*;
import org.dbsp.sqlCompiler.dbsp.circuit.type.*;
import org.dbsp.sqlCompiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.frontend.CalciteProgram;
import org.dbsp.sqlCompiler.frontend.SimulatorResult;
import org.dbsp.sqlCompiler.frontend.TableModifyStatement;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    private final boolean debug = true;
    static final String rustDirectory = "../temp";
    static final String testFilePath = rustDirectory + "/src/test.rs";
    int queryNo;
    @Nullable
    private CalciteCompiler calcite;
    private final List<ProgramAndTester> queries;
    private final String inputFunctionName = "input";
    @Nullable
    private DBSPFunction inputFunction = null;

    public DBSPExecutor() {
        this.calcite = null;
        this.queryNo = 0;
        this.queries = new ArrayList<>();
    }

    @Override
    public void reset() {
        this.calcite = new CalciteCompiler();
        this.queries.clear();
    }

    @Override
    public void prepareTables(SqlTestPrepareTables prepare) throws SqlParseException {
        if (this.calcite == null)
            throw new RuntimeException("Calcite compiler not initialized yet");
        this.calcite.startCompilation();
        for (String statement : prepare.statements)
            this.calcite.compile(statement);
    }

    @Override
    public void addQuery(
            String query, SqlTestPrepareInput inputs,
            SqlTestOutputDescription output) throws SqlParseException {
        //if (!query.equals("SELECT ALL * FROM tab2 WHERE col1 NOT BETWEEN col0 AND NULL")) return;
        if (this.calcite == null)
            throw new RuntimeException("Calcite compiler not initialized yet");
        // heuristic: add a "CREATE VIEW V AS" in front
        query = "CREATE VIEW V AS " + query;
        if (this.debug) {
            System.out.println("Query:\n" + query);
            if (output.queryResults != null)
                System.out.println("Expected results " + output.queryResults);
        }
        this.calcite.compile(query);
        CalciteProgram program = calcite.getProgram();
        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler();
        DBSPCircuit dbsp = compiler.compile(program, "c" + this.queryNo);

        DBSPZSetLiteral expectedOutput = null;
        if (output.queryResults != null) {
            if (dbsp.getOutputCount() != 1)
                throw new RuntimeException(
                        "Didn't expect a query to have " + dbsp.getOutputCount() + " outputs");
            DBSPType outputType = dbsp.getOutputType(0);
            expectedOutput = new DBSPZSetLiteral(outputType);
            DBSPTypeTuple outputElementType = expectedOutput.getElementType().to(DBSPTypeTuple.class);

            List<DBSPExpression> fields = new ArrayList<>();
            int col = 0;
            DBSPExpression field;
            for (String s: output.queryResults) {
                DBSPType colType = outputElementType.tupArgs[col];
                if (s.equalsIgnoreCase("null"))
                    field = new DBSPLiteral(colType);
                else if (colType.is(DBSPTypeInteger.class))
                    field = new DBSPLiteral(Integer.parseInt(s));
                else if (colType.is(DBSPTypeDouble.class))
                    field = new DBSPLiteral(Double.parseDouble(s));
                else if (colType.is(DBSPTypeFloat.class))
                    field = new DBSPLiteral(Float.parseFloat(s));
                else if (colType.is(DBSPTypeString.class))
                    field = new DBSPLiteral(s);
                else
                    throw new RuntimeException("Unexpected type " + colType);
                if (!colType.same(field.getNonVoidType()))
                    field = new DBSPCastExpression(field.getNode(), colType, field);
                fields.add(field);
                col++;
                if (col == outputElementType.size()) {
                    expectedOutput.add(new DBSPTupleExpression(null, outputElementType, fields));
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
            for (String statement : inputs.statements) {
                SimulatorResult result = this.calcite.compile(statement);
                TableModifyStatement stat = (TableModifyStatement) result;
                compiler.extendTransaction(transaction, stat);
            }
            this.inputFunction = transaction.inputGeneratingFunction(inputFunctionName, dbsp);
        }

        String rust = dbsp.toRustString();
        DBSPFunction func = SqlRuntimeLibrary.createTesterCode(
                "tester" + this.queryNo, inputFunctionName,
                dbsp, expectedOutput, output);
        this.queries.add(new ProgramAndTester(rust, func));
        this.queryNo++;
    }

    public void run() throws IOException, InterruptedException {
        File file = new File(testFilePath);
        //noinspection ResultOfMethodCallIgnored
        file.delete();
        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(DBSPCircuit.generatePreamble());

        writer.println(Objects.requireNonNull(this.inputFunction).toRustString());
        for (ProgramAndTester pt: this.queries) {
            writer.println(pt.program);
            writer.println("#[test]");
            writer.println(pt.tester.toRustString());
        }
        writer.close();
        Utilities.compileAndTestRust(rustDirectory);
    }
}
