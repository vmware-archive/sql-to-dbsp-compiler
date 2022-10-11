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
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.midend.TableContents;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that invoke the CalciteToDBSPCompiler.
 */
public class DBSPCompilerTests {
    @Test
    public void DDLTest() throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ")";

        compiler.compileStatement(ddl, null);
        DBSPCircuit dbsp = compiler.getResult();
        Assert.assertNotNull(dbsp);
    }

    @Test
    public void DDLAndInsertTest() throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ")";
        String insert = "INSERT INTO T VALUES(0, 0.0, true, 'Hi')";
        compiler.compileStatement(ddl, null);
        compiler.compileStatement(insert, null);
        TableContents tableContents = compiler.getTableContents();
        DBSPZSetLiteral t = tableContents.getTableContents("T");
        Assert.assertNotNull(t);
        Assert.assertEquals(1, t.size());
    }
}
