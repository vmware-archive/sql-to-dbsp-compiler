/*
 * Copyright 2023 VMware, Inc.
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
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.junit.Test;

public class ArrayTests extends BaseSQLTests {
    @Test
    public void testArray() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "ID INTEGER,\n"
                + "VALS INTEGER ARRAY,\n"
                + "VALVALS VARCHAR(10) ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT *, CARDINALITY(VALS), ARRAY[ID, 5], VALS[1] FROM ARR_TABLE";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(circuit);
    }

    // @Test
    public void testUnnest() {
        // TODO: not yet implemented.
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "ID INTEGER,\n"
                + "VALS INTEGER ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT ID, VAL FROM ARR_TABLE, UNNEST(VALS) AS VAL";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(circuit);
    }

    //@Test
    public void test2DArray() {
        // TODO: not yet implemented
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT *, CARDINALITY(VALS), VALS[1] FROM ARR_TABLE";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(circuit);
    }
}
