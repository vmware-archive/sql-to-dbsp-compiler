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
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents the usize Rust type.
 */
public class DBSPTypeUSize extends DBSPType
        implements IsNumericType, IDBSPBaseType {
    public static final DBSPTypeUSize instance = new DBSPTypeUSize(null, false);

    protected DBSPTypeUSize(@Nullable Object node, boolean mayBeNull) {
        super(node, mayBeNull);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return this.wrapOption(builder, "usize"); }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndentStringBuilder castFrom(IndentStringBuilder builder, DBSPExpression source) {
        throw new Unimplemented();
    }

    @Override
    public String shortName() {
        return "u";
    }

    @Override
    public boolean same(DBSPType type) {
        if (!super.same(type))
            return false;
        return type.is(DBSPTypeUSize.class);
    }
}