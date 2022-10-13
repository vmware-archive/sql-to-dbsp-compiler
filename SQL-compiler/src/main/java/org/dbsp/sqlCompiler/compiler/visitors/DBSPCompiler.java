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

package org.dbsp.sqlCompiler.compiler.visitors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.midend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.midend.TableContents;
import org.dbsp.util.IModule;

import javax.annotation.Nullable;

/**
 * This class compiles SQL statements into DBSP circuits.
 * The protocol is:
 * - create compiler
 * - create a new circuit (newCircuit())
 * - compile a sequence of SQL statements
 *   (CREATE TABLE, CREATE VIEW)
 * - get the resulting circuit
 * The compiler can also compile INSERT statements by keeping track of table contents.
 * The contents after insertions can be obtained using getTableContents().
 */
public class DBSPCompiler implements IModule {
    final CalciteCompiler frontend;
    final CalciteToDBSPCompiler midend;

    public DBSPCompiler() {
        this.frontend = new CalciteCompiler();
        this.frontend.startCompilation();
        this.midend = new CalciteToDBSPCompiler(this.frontend, true);
    }

    /**
     * If argument is true, the inputs to the circuit are generated from the CREATE TABLE
     * statements.  Otherwise they are generated from the LogicalTableScan
     * operations in a view plan.  Default value is 'false'.
     */
    public void setGenerateInputsFromTables(boolean generateInputsFromTables) {
        this.midend.setGenerateInputsFromTables(generateInputsFromTables);
    }

    public DBSPCompiler newCircuit(String circuitName) {
        this.midend.newCircuit(circuitName);
        return this;
    }

    /**
     * @param generate
     * If 'false' the next "create view" statements will not generate
     * an output for the circuit
     */
    public void generateOutputForNextView(boolean generate) {
        this.midend.generateOutputForNextView(generate);
    }

    public void compileStatement(String statement, @Nullable String comment) throws SqlParseException {
        FrontEndStatement fe = this.frontend.compile(statement, comment);
        this.midend.compile(fe);
    }

    public DBSPCircuit getResult() {
        return this.midend.getCircuit();
    }

    /**
     * Get the contents of the tables as a result of all the INSERT statements compiled.
     */
    public TableContents getTableContents() {
        return this.midend.getTableContents();
    }
}
