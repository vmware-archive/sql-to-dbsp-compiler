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
import org.dbsp.sqlCompiler.circuit.operator.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.*;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.junit.Test;

import java.math.BigDecimal;

public class CastTests extends BaseSQLTests {
    final DBSPTypeDecimal tenTwo = new DBSPTypeDecimal(null, 2, true);
    final DBSPTypeDecimal tenFour = new DBSPTypeDecimal(null, 4, false);

    @Override
    DBSPCompiler compileQuery(String query) throws SqlParseException {
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.setGenerateInputsFromTables(true);
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 VARCHAR NOT NULL" +
                ", COL4 DECIMAL(10,2)" +
                ", COL5 DECIMAL(10, 4) NOT NULL" +
                ")";
        compiler.compileStatement(ddl, null);
        compiler.compileStatement(query, null);
        return compiler;
    }

    @Override
    DBSPZSetLiteral createInput() {
        return new DBSPZSetLiteral(new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(12.0),
                new DBSPStringLiteral("100100"),
                DBSPLiteral.none(tenTwo),
                new DBSPDecimalLiteral(null, tenFour, new BigDecimal(100103123))));
    }

    void testQuery(String query, DBSPZSetLiteral expectedOutput) {
        try {
            query = "CREATE VIEW V AS " + query;
            DBSPCompiler compiler = this.compileQuery(query);
            DBSPCircuit circuit = getCircuit(compiler);
            InputOutputPair streams = new InputOutputPair(this.createInput(), expectedOutput);
            this.addRustTestCase(circuit, streams);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void intAndString() {
        String query = "SELECT '1' + 2";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(3))));
    }

    @Test
    public void intAndStringTable() {
        String query = "SELECT T.COL1 + T.COL3 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(
                new DBSPTupleExpression(new DBSPI32Literal(100110))));
    }

    @Test
    public void idTest() {
        String query = "SELECT * FROM T";
        this.testQuery(query, this.createInput());
    }

    @Test
    public void castFromFPTest() {
        String query = "SELECT T.COL1 + T.COL2 + T.COL3 + T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral(new DBSPTupleExpression(new DBSPDoubleLiteral(100203245.0))));
    }
}
