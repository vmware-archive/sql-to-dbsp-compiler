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
import org.dbsp.sqlCompiler.compiler.midend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

public class OtherTests implements IModule {
    private DBSPCompiler compileDef() throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 BOOLEAN NOT NULL" +
                ", COL4 VARCHAR NOT NULL" +
                ", COL5 INT" +
                ", COL6 DOUBLE" +
                ")";

        compiler.compileStatement(ddl, null);
        return compiler;
    }

    private DBSPCircuit compileQuery(String query) throws SqlParseException {
        DBSPCompiler compiler = this.compileDef();
        compiler.compileStatement(query, null);
        return compiler.getResult();
    }

    private void testQuery(String query) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCircuit circuit = this.compileQuery(query);
            String rust = ToRustVisitor.toRustString(circuit);
            Assert.assertNotNull(rust);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void toRustTest() {
        String query = "SELECT T.COL3 FROM T";
        this.testQuery(query);
    }

    @Test
    public void loggerTest() {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.instance.setDebugStream(builder);
        Logger.instance.setDebugLevel(this.getModule(), 1);
        Assert.assertEquals("OtherTests", this.getModule());
        Assert.assertEquals(1, this.getDebugLevel());
        if (this.getDebugLevel() > 0)
            Logger.instance.append("Logging one statement")
                    .newline();
        Logger.instance.setDebugLevel(this.getModule(), 0);
        if (this.getDebugLevel() > 0)
            Logger.instance.append("This one is not logged")
                    .newline();
        Logger.instance.setDebugStream(save);
        Assert.assertEquals("Logging one statement\n", builder.toString());
    }

    @Test
    public void toCsvTest() {
        DBSPZSetLiteral s = new DBSPZSetLiteral(
                CalciteToDBSPCompiler.weightType, BaseSQLTests.e0, BaseSQLTests.e1);
        StringBuilder builder = new StringBuilder();
        ToCsvVisitor visitor = new ToCsvVisitor(builder, () -> "");
        visitor.traverse(s);
        Assert.assertEquals(
                "10,12.0,true,Hi,,\n" +
                "10,1.0,false,Hi,1,0.0\n",
                builder.toString());
    }
}
