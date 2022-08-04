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

package org.dbsp.sqlCompiler.dbsp.circuit.type;

import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPExpression;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class DBSPTypeTuple extends DBSPType {
    /**
     * Keep track of the size of the maximum tuple size allocated.
     */
    public static int maxTupleSize = 0;

    public final DBSPType[] tupArgs;

    public static final DBSPTypeTuple emptyTupleType = new DBSPTypeTuple(null);

    protected DBSPTypeTuple(@Nullable Object node, boolean mayBeNull, DBSPType... tupArgs) {
        super(node, mayBeNull);
        this.tupArgs = tupArgs;
        if (this.tupArgs.length > maxTupleSize)
            maxTupleSize = this.tupArgs.length;
    }

    public DBSPTypeTuple(@Nullable Object node, DBSPType... tupArgs) {
        this(node, false, tupArgs);
    }

    public DBSPTypeTuple(@Nullable Object node, List<DBSPType> tupArgs) {
        this(node, tupArgs.toArray(new DBSPType[0]));
    }

    public int size() {
        return this.tupArgs.length;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        if (this.tupArgs.length == 0)
            return builder.append("()");
        builder.append("Tuple")
                .append(this.tupArgs.length)
                .append("<");
        builder.join(", ", this.tupArgs);
        return builder.append(">");
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeTuple(this.getNode(), mayBeNull, this.tupArgs);
    }

    @Override
    public IndentStringBuilder castFrom(IndentStringBuilder builder, DBSPExpression source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPTypeTuple that = (DBSPTypeTuple) o;
        return Arrays.equals(tupArgs, that.tupArgs);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tupArgs);
    }

    @Override
    public boolean same(DBSPType type) {
        if (!super.same(type))
            return false;
        if (!type.is(DBSPTypeTuple.class))
            return false;
        DBSPTypeTuple other = type.to(DBSPTypeTuple.class);
        if (this.tupArgs.length != other.tupArgs.length)
            return false;
        for (int i = 0; i < this.tupArgs.length; i++)
            if (!this.tupArgs[i].same(other.tupArgs[i]))
                return false;
        return true;
    }
}
