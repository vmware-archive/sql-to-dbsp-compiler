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

package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Maintains the catalog: a mapping from table names to table objects.
 */
public class Catalog extends AbstractSchema {
    public final String schemaName;
    private final Map<String, Table> tableMap;
    private final Map<String, Supplier<Table>> toBeGenerated;

    public Catalog(String schemaName) {
        this.schemaName = schemaName;
        this.tableMap = new HashMap<>();
        this.toBeGenerated = new HashMap<>();
    }

    public static String identifierToString(SqlIdentifier identifier) {
        if (!identifier.isSimple())
            throw new RuntimeException("Not a simple identifier " + identifier);
        return identifier.getSimple();
    }

    public void addTable(String name, Table table) {
        this.tableMap.put(name, table);
    }

    /**
     * Sometimes we don't know everything about the table when it's inserted,
     * in that case we reach for the table only when needed.
     */
    public void addGeneratedTable(String name, Supplier<Table> tableGen) {
        this.toBeGenerated.put(name, tableGen);
    }

    @Override
    public Map<String, Table> getTableMap() {
        this.toBeGenerated.forEach((n, t) -> this.tableMap.put(n, t.get()));
        this.toBeGenerated.clear();
        return this.tableMap;
    }

    public static String toString(SqlNode node) {
        SqlWriter writer = new SqlPrettyWriter();
        node.unparse(writer, 0, 0);
        return writer.toString();
    }

    public void dropTable(String tableName) {
        this.tableMap.remove(tableName);
        this.toBeGenerated.remove(tableName);
    }
}
