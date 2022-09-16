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

import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPUSizeLiteral;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

/**
 * Represents the usize Rust type.
 */
public class DBSPTypeUSize extends DBSPType
        implements IsNumericType, IDBSPBaseType {
    public static final DBSPTypeUSize instance = new DBSPTypeUSize(null, false);

    @SuppressWarnings("SameParameterValue")
    protected DBSPTypeUSize(@Nullable Object node, boolean mayBeNull) {
        super(node, mayBeNull);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return this.wrapOption(builder, "usize"); }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new UnsupportedOperationException();
        return this;
    }

    @Override
    public String shortName() {
        return "u";
    }

    @Override
    public boolean same(@Nullable DBSPType type) {
        if (!super.same(type))
            return false;
        assert type != null;
        return type.is(DBSPTypeUSize.class);
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPUSizeLiteral(0);
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPUSizeLiteral(1);
    }
}
