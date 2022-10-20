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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.IndentStream;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;

public abstract class DBSPType extends DBSPNode implements IDBSPInnerNode {
    /**
     * True if this type may include null values.
     */
    public final boolean mayBeNull;

    protected DBSPType(@Nullable Object node, boolean mayBeNull) {
        super(node);
        this.mayBeNull = mayBeNull;
    }

    @SuppressWarnings("SameParameterValue")
    protected DBSPType(boolean mayBeNull) {
        super(null);
        this.mayBeNull = mayBeNull;
    }

    public void wrapOption(IndentStream builder, String type) {
        if (this.mayBeNull) {
            builder.append("Option<").append(type).append(">");
            return;
        }
        builder.append(type);
    }

    public static boolean sameType(@Nullable DBSPType left, @Nullable DBSPType right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        return left.sameType(right);
    }

    public boolean sameType(@Nullable DBSPType other) {
        if (other == null)
            return false;
        return this.mayBeNull == other.mayBeNull;
    }

    /**
     * Return a copy of this type with the mayBeNull bit set to the specified value.
     * @param mayBeNull  Value for the mayBeNull bit.
     */
    public abstract DBSPType setMayBeNull(boolean mayBeNull);

    /**
     * Default implementation of cast of a source expression to the 'this' type.
     * Only defined for base types, should be overridden for other types.
     * For example, to cast source which is an Option[i16] to a bool
     * the function called will be cast_to_b_i16N.
     */
    public DBSPExpression castFrom(DBSPExpression source) {
        DBSPTypeBaseType base = this.as(DBSPTypeBaseType.class);
        if (base == null)
            throw new UnsupportedException(this);
        DBSPType sourceType = source.getNonVoidType();
        DBSPTypeBaseType baseSource = sourceType.as(DBSPTypeBaseType.class);
        if (baseSource == null)
            throw new UnsupportedException(sourceType);
        String destName = base.shortName();
        String srcName = baseSource.shortName();
        String functionName = "cast_to_" + destName + (base.mayBeNull ? "N" : "") +
                "_" + srcName + (baseSource.mayBeNull ? "N" : "");
        return new DBSPApplyExpression(functionName, this, source);
    }

    /**
     * Similar to 'to', but handles Ref types specially.
     */
    public <T> T toRef(Class<T> clazz) {
        DBSPTypeRef ref = this.as(DBSPTypeRef.class);
        if (ref != null)
            return ref.type.to(clazz);
        return this.to(clazz);
    }
}
