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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.util.Unimplemented;

import java.util.ArrayList;
import java.util.List;

public class TypeCompiler {
    public TypeCompiler() {}

    public static DBSPType makeZSet(DBSPType elementType) {
        return new DBSPTypeZSet(elementType.getNode(), elementType);
    }

    public DBSPType convertType(RelDataType dt) {
        boolean nullable = dt.isNullable();
        if (dt.isStruct()) {
            List<DBSPType> fields = new ArrayList<>();
            for (RelDataTypeField field: dt.getFieldList()) {
                DBSPType type = this.convertType(field.getType());
                fields.add(type);
            }
            return new DBSPTypeTuple(dt, fields);
        } else {
            SqlTypeName tn = dt.getSqlTypeName();
            switch (tn) {
                case BOOLEAN:
                    return new DBSPTypeBool(tn, nullable);
                case TINYINT:
                    return new DBSPTypeInteger(tn, 8, nullable);
                case SMALLINT:
                    return new DBSPTypeInteger(tn, 16, nullable);
                case INTEGER:
                    return new DBSPTypeInteger(tn, 32, nullable);
                case BIGINT:
                    return new DBSPTypeInteger(tn, 64, nullable);
                case DECIMAL:
                    // TODO: do we need a more precise approximation?
                    return DBSPTypeDouble.instance.setMayBeNull(nullable);
                case FLOAT:
                case REAL:
                    return DBSPTypeFloat.instance.setMayBeNull(nullable);
                case DOUBLE:
                    return DBSPTypeDouble.instance.setMayBeNull(nullable);
                case CHAR:
                case VARCHAR:
                    return DBSPTypeString.instance.setMayBeNull(nullable);
                case NULL:
                    return DBSPTypeNull.instance;
                case BINARY:
                case VARBINARY:
                case UNKNOWN:
                case ANY:
                case SYMBOL:
                case MULTISET:
                case ARRAY:
                case MAP:
                case DISTINCT:
                case STRUCTURED:
                case ROW:
                case OTHER:
                case CURSOR:
                case COLUMN_LIST:
                case DYNAMIC_STAR:
                case SARG:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    throw new Unimplemented(tn);
                case GEOMETRY:
                    return new DBSPTypeRawTuple(DBSPTypeDouble.instance, DBSPTypeDouble.instance).setMayBeNull(nullable);
                case TIMESTAMP:
                    return DBSPTypeTimestamp.instance.setMayBeNull(nullable);
                case DATE:
                    return DBSPTypeDate.instance.setMayBeNull(nullable);
            }
        }
        throw new Unimplemented(dt);
    }
}
