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

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.math.BigDecimal;

public class DBSPTypeDecimal extends DBSPType
        implements IsNumericType, IDBSPBaseType {
    public final int scale;

    public DBSPTypeDecimal(@Nullable Object node, int scale, boolean mayBeNull) {
        super(node, mayBeNull);
        this.scale = scale;
    }

    @Override
    public String getRustString() {
        return "rust_decimal";
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPDecimalLiteral(null, this, new BigDecimal(0));
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPDecimalLiteral(null, this, new BigDecimal(1));
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeDecimal(this.getNode(), this.scale, mayBeNull);
    }

    @Override
    public DBSPExpression castFrom(DBSPExpression source) {
        // Recall: we ignore nullability of this
        DBSPType argtype = source.getNonVoidType();
        if (argtype.is(DBSPTypeFP.class)) {
            DBSPTypeFP fp = argtype.to(DBSPTypeFP.class);
            return new DBSPApplyMethodExpression("unwrap",
                    this,
                    new DBSPApplyMethodExpression(
                            "from_f" + fp.getWidth(),
                            this.setMayBeNull(true),
                                new DBSPApplyMethodExpression(
                                        "into_inner", source.getNonVoidType(), source)));
        } else if (argtype.is(DBSPTypeInteger.class)){
            DBSPTypeFP fp = argtype.to(DBSPTypeFP.class);
            return new DBSPApplyMethodExpression("unwrap",
                    this,
                    new DBSPApplyMethodExpression(
                            "from_f" + fp.getWidth(),
                            this.setMayBeNull(true),
                            source));
        } else {
            throw new Unimplemented();
        }
    }

    @Override
    public String shortName() {
        return "rust_decimal";
    }

    @Override
    public boolean sameType(@Nullable DBSPType type) {
        if (!super.sameType(type))
            return false;
        assert type != null;
        if (!type.is(DBSPTypeDecimal.class))
            return false;
        DBSPTypeDecimal other = type.to(DBSPTypeDecimal.class);
        return this.scale == other.scale;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
