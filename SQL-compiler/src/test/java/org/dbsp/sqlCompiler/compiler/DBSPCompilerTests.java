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

package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.dbsp.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.dbsp.DBSPTransaction;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.frontend.CalciteProgram;
import org.dbsp.sqlCompiler.frontend.SimulatorResult;
import org.dbsp.sqlCompiler.frontend.TableModifyStatement;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that invoke the CalciteToDBSPCompiler.
 */
public class DBSPCompilerTests {
    @Test
    public void DDLTest() throws SqlParseException {
        CalciteCompiler calcite = new CalciteCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ")";

        calcite.startCompilation();
        SimulatorResult i = calcite.compile(ddl);
        Assert.assertNotNull(i);
        CalciteProgram program = calcite.getProgram();
        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler(calcite);
        DBSPCircuit dbsp = compiler.compile(program, "circuit");
        Assert.assertNotNull(dbsp);
    }

    @Test
    public void DDLAndInsertTest() throws SqlParseException {
        CalciteCompiler calcite = new CalciteCompiler();

        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ")";
        calcite.startCompilation();
        calcite.compile(ddl);
        CalciteProgram program = calcite.getProgram();
        CalciteToDBSPCompiler compiler = new CalciteToDBSPCompiler(calcite);
        DBSPCircuit dbsp = compiler.compile(program, "circuit");
        Assert.assertNotNull(dbsp);

        String insert = "INSERT INTO T VALUES(0, 0.0, true, 'Hi')";
        SimulatorResult i = calcite.compile(insert);
        Assert.assertNotNull(i);
        Assert.assertTrue(i instanceof TableModifyStatement);
        DBSPTransaction transaction = new DBSPTransaction();
        compiler.extendTransaction(transaction, (TableModifyStatement)i);
        DBSPZSetLiteral t = transaction.perInputChange.get("T");
        Assert.assertNotNull(t);
        Assert.assertEquals(1, t.size());
    }
}
