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
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.frontend.CalciteProgram;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DBSPExecutor implements ISqlTestExecutor {
    @Nullable
    private CalciteCompiler calcite;

    public DBSPExecutor() {
        this.calcite = null;
    }

    @Override
    public void reset() {
        this.calcite = new CalciteCompiler();
    }

    @Override
    public void prepare(SqlTestPrepare prepare) throws SqlParseException {
        assert this.calcite != null;
        for (String statement : prepare.statements)
            this.calcite.compile(statement);
    }

    @Override
    public void executeAndValidate(String query) throws SqlParseException {
        assert this.calcite != null;
        // heuristic: add a "CREATE VIEW V AS" in front
        query = "CREATE VIEW V AS " + query;
        // Compile query to a Calcite representation
        this.calcite.compile(query);
        CalciteProgram program = calcite.getProgram();
        // Compile Calcite representation to DBSP
        //CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler();
        // DBSPCircuit dbsp = compiler.compile(program);
        // TODO: Generate Rust code for program and inputs
        // TODO: Generate Rust code for output validation
        // TODO: execute the query and validate the output
    }
}
