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

package org.dbsp.sqlCompiler.compiler.midend;

import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.UnsupportedException;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class keeps track of the contents of the tables as
 * it exists because of the execution of simple INSERT or DELETE statements.
 * It gives an instantaneous view of the table contents, after the execution
 * of a sequence of statements.
 */
public class TableContents {
    final List<String> tablesCreated = new ArrayList<>();
    /**
     * Remember the last statement that created each table.
     */
    final Map<String, CreateTableStatement> tableCreation = new HashMap<>();
        /**
         * Keep track of the contents of each table.
         */
    @Nullable
        final Map<String, DBSPZSetLiteral> tableContents;

    public TableContents(boolean trackTableContents) {
        if (trackTableContents)
            this.tableContents = new HashMap<>();
        else
            this.tableContents = null;
    }

    public DBSPZSetLiteral getTableContents(String tableName) {
        if (this.tableContents == null)
            throw new UnsupportedException("Not keeping track of table contents");
        return Utilities.getExists(this.tableContents, tableName);
    }

    /**
     * Generates code for a function that returns a tuple
     * containing the contents of all tables.  The tables
     * are sorted in the order they were created.
     * @param functionName  Name of the generated function.
     */
    public DBSPFunction functionWithTableContents(String functionName) {
        DBSPZSetLiteral[] tuple = new DBSPZSetLiteral[this.tablesCreated.size()];
        for (int i = 0; i < this.tablesCreated.size(); i++) {
            String table = this.tablesCreated.get(i);
            tuple[i] = this.getTableContents(table);
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(tuple);
        return new DBSPFunction(functionName, new ArrayList<>(),
                result.getType(), result);
    }

    public void execute(FrontEndStatement statement) {
        if (statement.is(CreateTableStatement.class)) {
            CreateTableStatement create = statement.to(CreateTableStatement.class);
            Utilities.putNew(this.tableCreation, create.tableName, create);
            this.tablesCreated.add(create.tableName);
            if (this.tableContents != null)
                Utilities.putNew(this.tableContents, create.tableName, new DBSPZSetLiteral(create.getTableType()));
        } else if (statement.is(DropTableStatement.class)) {
            DropTableStatement drop = statement.to(DropTableStatement.class);
            this.tableCreation.remove(drop.tableName);
            this.tablesCreated.remove(drop.tableName);
            if (this.tableContents != null)
                this.tableContents.remove(drop.tableName);
        }
    }

    public CreateTableStatement getTableDefinition(String tableName) {
        return Utilities.getExists(this.tableCreation, tableName);
    }

    public void addToTable(String tableName, DBSPZSetLiteral value) {
        if (this.tableContents == null)
            throw new UnsupportedException("Not keeping track of table contents");
        this.tableContents.get(tableName).add(value);
    }

    public int getTableIndex(String tableName) {
        for (int i = 0; i < this.tablesCreated.size(); i++)
            if (this.tablesCreated.get(i).equals(tableName))
                return i;
        throw new RuntimeException("No table named " + tableName);
    }

    public void setTable(String tableName, DBSPZSetLiteral value) {
        Utilities.putNew(this.tableContents, tableName, value);
    }
}
