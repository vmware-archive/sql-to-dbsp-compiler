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
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPAsExpression;
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPIntegerLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPLongLiteral;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.util.Objects;

public class DBSPTypeInteger extends DBSPType
        implements IsNumericType, IDBSPBaseType {
    private final int width;
    public static final DBSPTypeInteger signed16 = new DBSPTypeInteger(null, 16, false);
    public static final DBSPTypeInteger signed32 = new DBSPTypeInteger(null, 32, false);
    public static final DBSPTypeInteger signed64 = new DBSPTypeInteger(null, 64, false);

    public DBSPTypeInteger(@Nullable Object node, int width, boolean mayBeNull) {
        super(node, mayBeNull);
        this.width = width;
    }

    @Override
    public int hashCode() {
        return Objects.hash(width);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return this.wrapOption(builder, this.getRustString()); }

    @Override
    public String getRustString() {
        return "i" + this.width;
    }

    @Override
    public DBSPLiteral getZero() {
        if (this.width <= 32) {
            return new DBSPIntegerLiteral(0, this.mayBeNull);
        } else {
            return new DBSPLongLiteral(0L, this.mayBeNull);
        }
    }

    @Override
    public DBSPLiteral getOne() {
        if (this.width <= 32) {
            return new DBSPIntegerLiteral(1, this.mayBeNull);
        } else {
            return new DBSPLongLiteral(1L, this.mayBeNull);
        }
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeInteger(this.getNode(), this.width, mayBeNull);
    }

    @Override
    public DBSPExpression castFrom(DBSPExpression source) {
        // Recall: we ignore nullability of this
        DBSPType argtype = source.getNonVoidType();
        if (argtype.is(DBSPTypeFP.class)) {
            return new DBSPAsExpression(
                    new DBSPApplyMethodExpression("into_inner", source.getNonVoidType(), source),
                    this);
        } else if (argtype.is(DBSPTypeInteger.class)){
            return new DBSPAsExpression(source, this);
        } else {
            throw new Unimplemented();
        }
    }

    @Override
    public String shortName() {
        return "i" + this.width;
    }

    public int getWidth() {
        return this.width;
    }

    @Override
    public boolean same(@Nullable DBSPType type) {
        if (!super.same(type))
            return false;
        assert type != null;
        if (!type.is(DBSPTypeInteger.class))
            return false;
        DBSPTypeInteger other = type.to(DBSPTypeInteger.class);
        return this.width == other.width;
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
