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

import org.dbsp.sqlCompiler.dbsp.circuit.DBSPNode;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPVariableReference;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public abstract class DBSPType extends DBSPNode {
    /**
     * True if this type may include null values.
     */
    public final boolean mayBeNull;

    protected DBSPType(@Nullable Object node, boolean mayBeNull) {
        super(node);
        this.mayBeNull = mayBeNull;
    }

    protected DBSPType(boolean mayBeNull) {
        super(null);
        this.mayBeNull = mayBeNull;
    }

    IndentStringBuilder wrapOption(IndentStringBuilder builder, String type) {
        if (this.mayBeNull)
            return builder.append("Option<" + type + ">");
        return builder.append(type);
    }

    public boolean same(DBSPType other) {
        return this.mayBeNull == other.mayBeNull;
    }

    /**
     * Return a copy of this type with the mayBeNull bit set to the specified value.
     * @param mayBeNull  Value for the mayBeNull bit.
     */
    public abstract DBSPType setMayBeNull(boolean mayBeNull);

    /**
     * Typical implementation of castFrom, which handles nullable types.
     */
    public IndentStringBuilder standardCastFrom(IndentStringBuilder builder, DBSPExpression source) {
        DBSPType type = source.getType();
        if (type.mayBeNull) {
            if (!this.mayBeNull)
                throw new RuntimeException("Unexpected nullable source " + source +
                        " and non-nullable result " + this);
            builder.append("(match ")
                    .append(source)
                    .append(" {\n").increase()
                    .append("Some(x) => Some(");
            DBSPExpression expr = new DBSPVariableReference("x", type.setMayBeNull(false));
            return this.setMayBeNull(false).castFrom(builder, expr)
                    .append("),\n")
                    .append("_ => None,\n").decrease()
                    .append("})");
        } else {
            if (this.mayBeNull) {
                builder.append("(Some(");
                this.setMayBeNull(false).castFrom(builder, source);
                return builder.append("))");
            } else {
                return this.castFrom(builder, source);
            }
        }
    }

    /**
     * Generate code for a cast from the specified expression to this type.
     * This function does not need to handle nullable types, that's done in
     * 'standardCastFrom'
     */
    public abstract IndentStringBuilder castFrom(IndentStringBuilder builder, DBSPExpression source);
}
