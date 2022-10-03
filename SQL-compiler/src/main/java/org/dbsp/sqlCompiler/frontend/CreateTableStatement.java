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

package org.dbsp.sqlCompiler.frontend;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeTuple;
import org.dbsp.util.TranslationException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Describes a table as produced by a CREATE TABLE DDL statement.
 */
public class CreateTableStatement extends AbstractTable implements ScannableTable, SimulatorResult {
    @Nullable
    private final SqlNode node;
    public final String name;
    public final List<ColumnInfo> columns;

    public CreateTableStatement(@Nullable SqlNode node, String name) {
        this.node = node;
        this.name = name;
        this.columns = new ArrayList<>();
    }

    public void addColumn(ColumnInfo info) {
        this.columns.add(info);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (ColumnInfo ci: this.columns) {
            RelDataType type = ci.type;
            builder.add(ci.name, type);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // We don't plan to use this method, but the optimizer requires this API
        throw new NotImplementedException();
    }

    @Override
    @Nullable
    public SqlNode getNode() {
        return this.node;
    }

    /**
     * Return the index of the specified column.
     */
    public int getColumnIndex(SqlIdentifier id) {
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).name.equals(id.toString()))
                return i;
        }
        throw new TranslationException("Column not found", id);
    }

    public DBSPTypeTuple getType() {
        TypeCompiler compiler = new TypeCompiler();
        List<DBSPType> fields = new ArrayList<>();
        for (ColumnInfo col: this.columns) {
            DBSPType fType = compiler.convertType(col.type);
            fields.add(fType);
        }
        return new DBSPTypeTuple(fields);
    }
}
