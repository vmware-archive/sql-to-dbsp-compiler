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

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.midend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.compiler.midend.TableContents;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.compiler.backend.ToRustVisitor;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for SQL-based tets.
 */
public class BaseSQLTests {
    static final String rustDirectory = "../temp/src";
    static final String testFilePath = rustDirectory + "/test0.rs";

    static class InputOutputPair {
        public final TableContents transaction;
        public final DBSPZSetLiteral result;

        InputOutputPair(TableContents transaction, DBSPZSetLiteral result) {
            this.transaction = transaction;
            this.result = result;
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    @BeforeClass
    public static void generateLib() throws IOException {
        SqlRuntimeLibrary.instance.writeSqlLibrary( "../lib/genlib/src/lib.rs");
        Utilities.writeRustMain(rustDirectory + "/main.rs",
                Linq.list("test0"));
    }

    DBSPCompiler compileQuery(String query) throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ", COL5 INT" +
                ", COL6 DOUBLE" +
                ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        return compiler;
    }

    final DBSPTupleExpression e0 = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            new DBSPDoubleLiteral(12.0),
            DBSPBoolLiteral.True,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)),
            DBSPLiteral.none(DBSPTypeDouble.instance.setMayBeNull(true))
    );
    final DBSPTupleExpression e1 = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            new DBSPDoubleLiteral(1.0),
            DBSPBoolLiteral.False,
            new DBSPStringLiteral("Hi"),
            new DBSPIntegerLiteral(1, true),
            new DBSPDoubleLiteral(0.0, true)
    );
    final DBSPZSetLiteral z0 = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e0);
    final DBSPZSetLiteral z1 = new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e1);
    final DBSPZSetLiteral empty = new DBSPZSetLiteral(this.z0.getNonVoidType());

    /**
     * Returns the table containing:
     * -------------------------------------------
     * | 10 | 12.0 | true  | Hi | NULL    | NULL |
     * | 10 |  1.0 | false | Hi | Some[1] |  0.0 |
     * -------------------------------------------
     */
    DBSPZSetLiteral createInput() {
        return new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType, e0, e1);
    }

    /**
     * Generates a Rust function which tests a DBSP circuit.
     * @param circuit       DBSP circuit that will be tested.
     * @param data          Input/output pairs of data for circuit.
     * @return              The code for a function that runs the circuit with the specified
     *                      input and tests the produced output.
     */
    static DBSPFunction createTesterCode(
            DBSPCircuit circuit,
            List<InputOutputPair> data) {
        List<DBSPStatement> list = new ArrayList<>();
        list.add(new DBSPLetStatement("circuit",
                new DBSPApplyExpression(circuit.name, DBSPTypeAny.instance), true));
        DBSPType circuitOutputType = circuit.getOutputType(0);
        // the following may not be the same, since SqlLogicTest sometimes lies about the output type
        DBSPExpression[] arguments = new DBSPExpression[circuit.getInputTables().size()];
        list.add(new DBSPLetStatement("_in",
                new DBSPApplyExpression("input", DBSPTypeAny.instance)));

        for (InputOutputPair pairs: data) {
            TableContents transaction = pairs.transaction;
            DBSPZSetLiteral output = pairs.result;

            for (int i = 0; i < arguments.length; i++) {
                String inputI = circuit.getInputTables().get(i);
                int index = transaction.getTableIndex(inputI);
                arguments[i] = new DBSPFieldExpression(null,
                        new DBSPVariableReference("_in", DBSPTypeAny.instance), index);
            }
            list.add(new DBSPLetStatement("output",
                    new DBSPApplyExpression("circuit", circuitOutputType, arguments)));
            DBSPExpression output0 = new DBSPFieldExpression(null,
                    new DBSPVariableReference("output", DBSPTypeAny.instance), 0);
            list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                    "assert_eq!", null, output0, output)));
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction("test", new ArrayList<>(), null, body)
                .addAnnotation("#[test]");
    }

    void createTester(PrintWriter writer, DBSPCircuit circuit,
                      TableContents contents, DBSPZSetLiteral expectedOutput) {
        DBSPZSetLiteral input = this.createInput();
        contents.addToTable("T", input);
        DBSPFunction inputGen = contents.functionWithTableContents("input");
        writer.println(ToRustVisitor.toRustString(inputGen));
        List<InputOutputPair> pairs = new ArrayList<>();
        pairs.add(new InputOutputPair(contents, expectedOutput));
        DBSPFunction tester = createTesterCode(circuit, pairs);
        writer.println(ToRustVisitor.toRustString(tester));
    }

    void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCompiler compiler = this.compileQuery(query);
            PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
            writer.println(ToRustVisitor.generatePreamble());
            DBSPCircuit circuit = compiler.getResult();
            writer.println(ToRustVisitor.toRustString(circuit));
            this.createTester(writer, circuit, compiler.getTableContents(), expectedOutput);
            writer.close();
            Utilities.compileAndTestRust(rustDirectory, false);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
