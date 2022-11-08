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

package org.dbsp.sqlCompiler.compiler.sqlparser;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.dbsp.sqlCompiler.compiler.sqlparser.statements.*;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Simulate the execution of a SQL DDL or DML statement.
 */
public class FrontEndState {
    final Catalog schema;
    final RelDataTypeFactory typeFactory;
    final SqlValidator validator;

    public FrontEndState(Catalog schema, RelDataTypeFactory typeFactory, SqlValidator validator) {
        this.schema = schema;
        this.typeFactory = typeFactory;
        this.validator = validator;
    }

    RelDataType convertType(SqlDataTypeSpec spec) {
        SqlTypeNameSpec type = spec.getTypeNameSpec();
        RelDataType result = type.deriveType(this.validator);
        if (Objects.requireNonNull(spec.getNullable()))
            result = this.typeFactory.createTypeWithNullability(result, true);
        return result;
    }

    List<RelDataTypeField> getColumnTypes(SqlNodeList list) {
        List<RelDataTypeField> result = new ArrayList<>();
        int index = 0;
        for (SqlNode col: Objects.requireNonNull(list)) {
            if (col.getKind().equals(SqlKind.COLUMN_DECL)) {
                SqlColumnDeclaration cd = (SqlColumnDeclaration)col;
                RelDataType type = this.convertType(cd.dataType);
                String name = Catalog.identifierToString(cd.name);
                RelDataTypeField field = new RelDataTypeFieldImpl(name, index++, type);
                result.add(field);
                continue;
            }
            throw new Unimplemented(col);
        }
        return result;
    }

    FrontEndStatement emulate(SqlNode node, String statement, @Nullable String comment) {
        SqlKind kind = node.getKind();
        if (kind == SqlKind.CREATE_TABLE) {
            SqlCreateTable ct = (SqlCreateTable)node;
            String tableName = Catalog.identifierToString(ct.name);
            List<RelDataTypeField> cols = this.getColumnTypes(Objects.requireNonNull(ct.columnList));
            CreateTableStatement table = new CreateTableStatement(node, statement, tableName, comment, cols);
            this.schema.addTable(tableName, table.getEmulatedTable());
            return table;
        } else if (kind == SqlKind.INSERT) {
            SqlInsert insert = (SqlInsert)node;
            SqlNode table = insert.getTargetTable();
            if (!(table instanceof SqlIdentifier))
                throw new Unimplemented(table);
            SqlIdentifier id = (SqlIdentifier)table;
            return new TableModifyStatement(node, statement, id.toString(), insert.getSource(), comment);
        } else if (kind == SqlKind.DROP_TABLE) {
            SqlDropTable dt = (SqlDropTable) node;
            String tableName = Catalog.identifierToString(dt.name);
            this.schema.dropTable(tableName);
            return new DropTableStatement(node, statement, tableName, comment);
        }
        throw new Unimplemented(node);
    }
}
