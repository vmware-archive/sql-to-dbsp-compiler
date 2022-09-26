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

import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPPath;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPSimplePathSegment;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DBSPTypeTuple extends DBSPType {
    /**
     * Keep track of the tuple sizes that appear in the program to properly generate
     * Rust macros to instantiate them.
     */
    public static final Set<Integer> tupleSizesUsed = new HashSet<>();

    public static void clearSizesUsed() {
        tupleSizesUsed.clear();
    }

    public final DBSPType[] tupFields;

    @SuppressWarnings("unused")
    public static final DBSPTypeTuple emptyTupleType = new DBSPTypeTuple();

    protected DBSPTypeTuple(@Nullable Object node, boolean mayBeNull, DBSPType... tupFields) {
        super(node, mayBeNull);
        this.tupFields = tupFields;
        tupleSizesUsed.add(this.tupFields.length);
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

    public DBSPTypeTuple slice(int start, int endExclusive) {
        if (endExclusive <= start)
            throw new RuntimeException("Incorrect slice parameters " + start + ":" + endExclusive);
        return new DBSPTypeTuple(Utilities.arraySlice(this.tupFields, start, endExclusive));
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        for (DBSPType type: this.tupFields)
            type.accept(visitor);
        visitor.postorder(this);
    }
}
