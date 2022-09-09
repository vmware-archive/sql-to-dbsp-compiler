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

package org.dbsp.sqlCompiler.dbsp.rust.type;

import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPPath;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPSimplePathSegment;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class DBSPTypeTuple extends DBSPType {
    /**
     * Keep track of the size of the maximum tuple size allocated.
     */
    public static int maxTupleSize = 0;

    public final DBSPType[] tupFields;

    @SuppressWarnings("unused")
    public static final DBSPTypeTuple emptyTupleType = new DBSPTypeTuple();

    protected DBSPTypeTuple(@Nullable Object node, boolean mayBeNull, DBSPType... tupFields) {
        super(node, mayBeNull);
        this.tupFields = tupFields;
        if (this.tupFields.length > maxTupleSize)
            maxTupleSize = this.tupFields.length;
    }

    public DBSPTypeTuple(@Nullable Object node, DBSPType... tupFields) {
        this(node, false, tupFields);
    }

    public DBSPTypeTuple(DBSPType... tupFields) {
        this(null, tupFields);
    }

    public DBSPTypeTuple(@Nullable Object node, List<DBSPType> tupFields) {
        this(node, tupFields.toArray(new DBSPType[0]));
    }

    public DBSPTypeTuple(List<DBSPType> tupFields) {
        this(null, tupFields);
    }

    public DBSPType getFieldType(int index) {
        return this.tupFields[index];
    }

    public int size() {
        return this.tupFields.length;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        if (this.tupFields.length == 0)
            return builder.append("()");
        if (this.mayBeNull)
            builder.append("Option<");
        builder.append("Tuple")
                .append(this.tupFields.length)
                .append("<")
                .join(", ", this.tupFields)
                .append(">");
        if (this.mayBeNull)
            builder.append(">");
        return builder;
    }

    public DBSPPath toPath() {
        return new DBSPPath(new DBSPSimplePathSegment("Tuple" + this.tupFields.length, this.tupFields));
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeTuple(this.getNode(), mayBeNull, this.tupFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPTypeTuple that = (DBSPTypeTuple) o;
        return Arrays.equals(tupFields, that.tupFields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tupFields);
    }

    @Override
    public boolean same(@Nullable DBSPType type) {
        if (!super.same(type))
            return false;
        assert type != null;
        if (!type.is(DBSPTypeTuple.class))
            return false;
        DBSPTypeTuple other = type.to(DBSPTypeTuple.class);
        if (this.tupFields.length != other.tupFields.length)
            return false;
        for (int i = 0; i < this.tupFields.length; i++)
            if (!this.tupFields[i].same(other.tupFields[i]))
                return false;
        return true;
    }
}
