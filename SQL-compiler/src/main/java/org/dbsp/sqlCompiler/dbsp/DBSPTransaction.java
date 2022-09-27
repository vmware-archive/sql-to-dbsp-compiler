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

package org.dbsp.sqlCompiler.dbsp;

import org.dbsp.sqlCompiler.dbsp.rust.DBSPFunction;
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.frontend.TableDDL;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A DBSPTransaction holds a set of changes for the inputs of a DBSP program.
 * Transactions can be created from some forms of SQL INSERT or DELETE statements.
 */
public class DBSPTransaction {
    public final List<String> tables;
    /**
     * A Map from input (table) name to a ZSet value that will be used to update the table.
     */
    public final Map<String, DBSPZSetLiteral> perInputChange;
    /**
     * For each table the statement that defined it.
     */
    public final Map<String, TableDDL> perInputDefinition;

    public DBSPTransaction() {
        this.perInputChange = new HashMap<>();
        this.tables = new ArrayList<>();
        this.perInputDefinition = new HashMap<>();
    }

    public void addSet(String table, DBSPZSetLiteral set) {
        if (!this.tables.contains(table))
            throw new RuntimeException("Unknown input table " + table);
        if (this.perInputChange.containsKey(table)) {
            this.perInputChange.get(table).add(set);
        } else {
            this.perInputChange.put(table, set);
        }
    }

    public DBSPFunction inputGeneratingFunction(String name) {
        DBSPZSetLiteral[] tuple = new DBSPZSetLiteral[this.tables.size()];
        for (int i = 0; i < this.tables.size(); i++) {
            String table = this.tables.get(i);
            DBSPZSetLiteral lit = this.perInputChange.get(table);
            if (lit == null)
                throw new RuntimeException("No input found for table " + table);
            tuple[i] = lit;
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(tuple);
        return new DBSPFunction(name, new ArrayList<>(),
                result.getType(), result);
    }

    public DBSPZSetLiteral getSet(String tableName) {
        return Utilities.getExists(this.perInputChange, tableName);
    }

    public void addTable(TableDDL def) {
        this.tables.add(def.name);
        Utilities.putNew(this.perInputDefinition, def.name, def);
    }

    public int getTableIndex(String tableName) {
        for (int i = 0; i < this.tables.size(); i++)
            if (this.tables.get(i).equals(tableName))
                return i;
        throw new RuntimeException("No table named " + tableName);
    }
}
