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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.visitors.ToJSONVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that invoke the CalciteToDBSPCompiler.
 */
public class DBSPCompilerTests {
    static final CompilerOptions options = new CompilerOptions();
    final String ddl = "CREATE TABLE T (\n" +
            "COL1 INT NOT NULL" +
            ", COL2 DOUBLE NOT NULL" +
            ", COL3 BOOLEAN NOT NULL" +
            ", COL4 VARCHAR NOT NULL" +
            ")";

    @Test
    public void DDLTest() throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(ddl);
        DBSPCircuit dbsp = compiler.getFinalCircuit("circuit");
        Assert.assertNotNull(dbsp);
    }

    @Test
    public void circuitToJsonTest() throws SqlParseException, JsonProcessingException {
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(ddl);
        compiler.compileStatement("CREATE VIEW V AS SELECT * FROM T WHERE COL1 > 5");
        DBSPCircuit dbsp = compiler.getFinalCircuit("circuit");
        String json = ToJSONVisitor.circuitToJSON(dbsp);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        Assert.assertNotNull(root);
    }

    @Test
    public void DDLAndInsertTest() throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler(options);
        String insert = "INSERT INTO T VALUES(0, 0.0, true, 'Hi')";
        compiler.compileStatement(ddl);
        compiler.compileStatement(insert);
        TableContents tableContents = compiler.getTableContents();
        DBSPZSetLiteral t = tableContents.getTableContents("T");
        Assert.assertNotNull(t);
        Assert.assertEquals(1, t.size());
    }
}
