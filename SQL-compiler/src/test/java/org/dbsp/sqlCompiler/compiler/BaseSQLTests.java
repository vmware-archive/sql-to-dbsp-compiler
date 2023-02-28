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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.*;
import org.dbsp.sqlCompiler.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Utilities;
import org.junit.AfterClass;
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

    public static class InputOutputPair {
        public final DBSPZSetLiteral[] inputs;
        public final DBSPZSetLiteral[] outputs;

        public InputOutputPair(DBSPZSetLiteral[] inputs, DBSPZSetLiteral[] outputs) {
            this.inputs = inputs;
            this.outputs = outputs;
        }

        public InputOutputPair(DBSPZSetLiteral input, DBSPZSetLiteral output) {
            this.inputs = new DBSPZSetLiteral[1];
            this.inputs[0] = input;
            this.outputs = new DBSPZSetLiteral[1];
            this.outputs[0] = output;
        }
    }

    protected static DBSPCircuit getCircuit(DBSPCompiler compiler) {
        String name = "circuit" + testsToRun.size();
        return compiler.getFinalCircuit(name);
    }

    private static class TestCase {
        public final DBSPCircuit circuit;
        public final InputOutputPair[] data;
        public final int sequenceNo;

        TestCase(DBSPCircuit circuit, InputOutputPair... data) {
            this.circuit = circuit;
            this.data = data;
            this.sequenceNo = testsToRun.size();
        }

        /**
         * Generates a Rust function which tests a DBSP circuit.
         * @return The code for a function that runs the circuit with the specified
         *         input and tests the produced output.
         */
        DBSPFunction createTesterCode() {
            List<DBSPStatement> list = new ArrayList<>();
            DBSPLetStatement circ = new DBSPLetStatement("circuit",
                    new DBSPApplyExpression(this.circuit.name, DBSPTypeAny.INSTANCE), true);
            list.add(circ);
            for (InputOutputPair pairs: this.data) {
                DBSPLetStatement out = new DBSPLetStatement("output",
                        new DBSPApplyExpression(circ.getVarReference(), pairs.inputs));
                list.add(out);
                for (int i = 0; i < pairs.outputs.length; i++) {
                    list.add(
                            new DBSPExpressionStatement(
                                    new DBSPApplyExpression("assert!", null,
                                            new DBSPApplyExpression("must_equal", DBSPTypeBool.INSTANCE,
                                                    new DBSPFieldExpression(null, out.getVarReference(), i).borrow(),
                                                    pairs.outputs[i].borrow()))));
                }
            }
            DBSPExpression body = new DBSPBlockExpression(list, null);
            return new DBSPFunction("test" + this.sequenceNo, new ArrayList<>(), null, body)
                    .addAnnotation("#[test]");
        }
    }

    // Collect here all the tests to run and execute them using a single Rust compilation
    static final List<TestCase> testsToRun = new ArrayList<>();

    @BeforeClass
    public static void prepareTests() throws IOException {
        generateLib();
        testsToRun.clear();
    }

    @AfterClass
    public static void runAllTests() throws IOException, InterruptedException {
        if (testsToRun.isEmpty())
            return;
        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());
        for (TestCase test: testsToRun) {
            writer.println(ToRustVisitor.circuitToRustString(test.circuit));
            DBSPFunction tester = test.createTesterCode();
            writer.println(ToRustVisitor.irToRustString(tester));
        }
        writer.close();
        Utilities.compileAndTestRust(rustDirectory, false);
        testsToRun.clear();
    }

    public static void generateLib() throws IOException {
        SqlRuntimeLibrary.INSTANCE.writeSqlLibrary( "../lib/genlib/src/lib.rs");
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
            if (compiler.hasErrors()) {
                compiler.showErrors(System.err);
                throw new RuntimeException("Aborting test");
            }
            DBSPCircuit circuit = getCircuit(compiler);
            circuit = new OptimizeDistinctVisitor().apply(circuit);
            if (incremental)
                circuit = new IncrementalizeVisitor().apply(circuit);
            if (optimize) {
                CircuitVisitor optimizer = this.getOptimizer();
                circuit = optimizer.apply(circuit);
            }

            // Test json serialization
            //ToJSONVisitor.validateJson(circuit);
            //ToJitVisitor.validateJson(circuit);
            this.addRustTestCase(circuit, streams);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void addRustTestCase(DBSPCircuit circuit, InputOutputPair... streams) {
        TestCase test = new TestCase(circuit, streams);
        testsToRun.add(test);
    }

    static CompilerOptions testOptions() {
        CompilerOptions options = new CompilerOptions();
        options.optimizerOptions.throwOnError = true;
        return options;
    }

    static DBSPCompiler testCompiler() {
        return new DBSPCompiler(testOptions());
    }

    public DBSPCompiler compileQuery(String query) {
        DBSPCompiler compiler = testCompiler();
        // This is necessary if we want queries that do not depend on the input
        // to generate circuits that still have inputs.
        compiler.setGenerateInputsFromTables(true);
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

    public static final DBSPTupleExpression e0 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(12.0),
            DBSPBoolLiteral.True,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32),
            DBSPLiteral.none(DBSPTypeDouble.NULLABLE_INSTANCE)
    );
    public static final DBSPTupleExpression e1 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(1.0),
            DBSPBoolLiteral.False,
            new DBSPStringLiteral("Hi"),
            new DBSPI32Literal(1, true),
            new DBSPDoubleLiteral(0.0, true)
    );

    public static final DBSPTupleExpression e0NoDouble = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            DBSPBoolLiteral.True,
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true))
    );
    public static final DBSPTupleExpression e1NoDouble = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            DBSPBoolLiteral.False,
            new DBSPStringLiteral("Hi"),
            new DBSPI32Literal(1, true)
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
}
