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
import org.dbsp.sqlCompiler.compiler.visitors.*;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Utilities;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for SQL-based tests.
 */
public class BaseSQLTests {
    public static final String rustDirectory = "../temp/src";
    public static final String testFilePath = rustDirectory + "/lib.rs";

    static class InputOutputPair {
        public final DBSPZSetLiteral[] inputs;
        public final DBSPZSetLiteral[] outputs;

        @SuppressWarnings("unused")
        InputOutputPair(DBSPZSetLiteral[] inputs, DBSPZSetLiteral[] outputs) {
            this.inputs = inputs;
            this.outputs = outputs;
        }

        InputOutputPair(DBSPZSetLiteral input, DBSPZSetLiteral output) {
            this.inputs = new DBSPZSetLiteral[1];
            this.inputs[0] = input;
            this.outputs = new DBSPZSetLiteral[1];
            this.outputs[0] = output;
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    @BeforeClass
    public static void generateLib() throws IOException {
        SqlRuntimeLibrary.instance.writeSqlLibrary( "../lib/genlib/src/lib.rs");
    }

    void createTester(PrintWriter writer, DBSPCircuit circuit,
                      InputOutputPair... streams) {
        DBSPFunction tester = createTesterCode(circuit, streams);
        writer.println(ToRustVisitor.irToRustString(tester));
    }

    CircuitVisitor getOptimizer() {
        DeadCodeVisitor dead = new DeadCodeVisitor();
        return new PassesVisitor(
                new OptimizeIncrementalVisitor(),
                dead,
                new RemoveOperatorsVisitor(dead.reachable),
                new NoIntegralVisitor()
        );
    }

    void testQueryBase(String query, boolean incremental, boolean optimize, InputOutputPair... streams) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCompiler compiler = this.compileQuery(query);
            PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
            writer.println(ToRustVisitor.generatePreamble());
            DBSPCircuit circuit = compiler.getResult();
            circuit = new OptimizeDistinctVisitor().apply(circuit);
            if (incremental)
                circuit = new IncrementalizeVisitor().apply(circuit);
            if (optimize) {
                CircuitVisitor optimizer = this.getOptimizer();
                circuit = optimizer.apply(circuit);
            }
            writer.println(ToRustVisitor.circuitToRustString(circuit));
            this.createTester(writer, circuit, streams);
            writer.close();
            Utilities.compileAndTestRust(rustDirectory, false);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    final static CompilerOptions options = new CompilerOptions();

    DBSPCompiler compileQuery(String query) throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler(options).newCircuit("circuit");
        compiler.setGenerateInputsFromTables(true);
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ", COL5 INT" +
                ", COL6 DOUBLE" +
                ")";
        compiler.compileStatement(ddl, null);
        compiler.compileStatement(query, null);
        return compiler;
    }

    public static final DBSPTupleExpression e0 = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            new DBSPDoubleLiteral(12.0),
            DBSPBoolLiteral.True,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)),
            DBSPLiteral.none(DBSPTypeDouble.instance.setMayBeNull(true))
    );
    public static final DBSPTupleExpression e1 = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            new DBSPDoubleLiteral(1.0),
            DBSPBoolLiteral.False,
            new DBSPStringLiteral("Hi"),
            new DBSPIntegerLiteral(1, true),
            new DBSPDoubleLiteral(0.0, true)
    );

    public static final DBSPTupleExpression e0NoDouble = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            DBSPBoolLiteral.True,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true))
    );
    public static final DBSPTupleExpression e1NoDouble = new DBSPTupleExpression(
            new DBSPIntegerLiteral(10),
            DBSPBoolLiteral.False,
            new DBSPStringLiteral("Hi"),
            new DBSPIntegerLiteral(1, true)
    );
    static final DBSPZSetLiteral z0 = new DBSPZSetLiteral(e0);
    static final DBSPZSetLiteral z1 = new DBSPZSetLiteral(e1);
    static final DBSPZSetLiteral empty = new DBSPZSetLiteral(z0.getNonVoidType());

    /**
     * Returns the table containing:
     * -------------------------------------------
     * | 10 | 12.0 | true  | Hi | NULL    | NULL |
     * | 10 |  1.0 | false | Hi | Some[1] |  0.0 |
     * -------------------------------------------
     */
    DBSPZSetLiteral createInput() {
        return new DBSPZSetLiteral(e0, e1);
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
            InputOutputPair... data) {
        List<DBSPStatement> list = new ArrayList<>();
        DBSPLetStatement circ = new DBSPLetStatement("circuit",
                new DBSPApplyExpression(circuit.name, DBSPTypeAny.instance), true);
        list.add(circ);
        for (InputOutputPair pairs: data) {
            DBSPLetStatement out = new DBSPLetStatement("output",
                    new DBSPApplyExpression(circ.getVarReference(), pairs.inputs));
            list.add(out);
            list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                    "assert_eq!", null, out.getVarReference(), new DBSPRawTupleExpression(pairs.outputs))));
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction("test", new ArrayList<>(), null, body)
                .addAnnotation("#[test]");
    }
}
