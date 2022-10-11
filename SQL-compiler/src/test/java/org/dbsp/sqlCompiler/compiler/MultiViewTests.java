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
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.midend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntegerLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Utilities;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Tests where multiple views are defined in the same circuit.
 */
public class MultiViewTests extends BaseSQLTests {
    /**
     * Two output views.
     */
    @Test
    public void twoViewTest() throws IOException, SqlParseException, InterruptedException {
        String query1 = "CREATE VIEW V1 AS SELECT T.COL3 FROM T";
        String query2 = "CREATE VIEW V2 as SELECT T.COL2 FROM T";

        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
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
        compiler.compileStatement(query1, null);
        compiler.compileStatement(query2, null);

        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());
        DBSPCircuit circuit = compiler.getResult();
        writer.println(ToRustVisitor.toRustString(circuit));
        InputOutputPair stream = new InputOutputPair(
                new DBSPZSetLiteral[] { this.createInput() },
                new DBSPZSetLiteral[] {
                        new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                                new DBSPTupleExpression(DBSPBoolLiteral.True),
                                new DBSPTupleExpression(DBSPBoolLiteral.False)),
                        new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                                new DBSPTupleExpression(new DBSPDoubleLiteral(12.0)),
                                new DBSPTupleExpression(new DBSPDoubleLiteral(1.0)))
                }
        );
        this.createTester(writer, circuit, stream);
        writer.close();
        Utilities.compileAndTestRust(rustDirectory, false);
    }

    /**
     * A view is an input for another view.
     */
    @Test
    public void nestedViewTest() throws IOException, SqlParseException, InterruptedException {
        String query1 = "CREATE VIEW V1 AS SELECT T.COL3 FROM T";
        String query2 = "CREATE VIEW V2 as SELECT * FROM V1";

        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
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
        compiler.compileStatement(query1, null);
        compiler.compileStatement(query2, null);

        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());
        DBSPCircuit circuit = compiler.getResult();
        writer.println(ToRustVisitor.toRustString(circuit));
        InputOutputPair stream = new InputOutputPair(
                new DBSPZSetLiteral[] { this.createInput() },
                new DBSPZSetLiteral[] {
                        new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                                new DBSPTupleExpression(DBSPBoolLiteral.True),
                                new DBSPTupleExpression(DBSPBoolLiteral.False)),
                        new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                                new DBSPTupleExpression(DBSPBoolLiteral.True),
                                new DBSPTupleExpression(DBSPBoolLiteral.False))
                }
        );
        this.createTester(writer, circuit, stream);
        writer.close();
        Utilities.compileAndTestRust(rustDirectory, false);
    }

    /**
     * A view is used twice.
     */
    @Test
    public void multiViewTest() throws IOException, SqlParseException, InterruptedException {
        String query1 = "CREATE VIEW V1 AS SELECT T.COL3 AS COL3 FROM T";
        String query2 = "CREATE VIEW V2 as SELECT DISTINCT COL1 FROM (SELECT * FROM V1 JOIN T ON V1.COL3 = T.COL3)";

        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
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
        compiler.compileStatement(query1, null);
        compiler.compileStatement(query2, null);

        PrintWriter writer = new PrintWriter(testFilePath, "UTF-8");
        writer.println(ToRustVisitor.generatePreamble());
        DBSPCircuit circuit = compiler.getResult();
        writer.println(ToRustVisitor.toRustString(circuit));
        InputOutputPair stream = new InputOutputPair(
                new DBSPZSetLiteral[] { this.createInput() },
                new DBSPZSetLiteral[] {
                        new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                                new DBSPTupleExpression(DBSPBoolLiteral.True),
                                new DBSPTupleExpression(DBSPBoolLiteral.False)),
                        new DBSPZSetLiteral(CalciteToDBSPCompiler.weightType,
                                new DBSPTupleExpression(new DBSPIntegerLiteral(10)))
                }
        );
        this.createTester(writer, circuit, stream);
        writer.close();
        Utilities.compileAndTestRust(rustDirectory, false);
    }
}
