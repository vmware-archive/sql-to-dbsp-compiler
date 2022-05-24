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

import org.dbsp.util.IndentStringBuilder;

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
        return this.wrapOption(builder, "i" + this.width); }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        return new DBSPTypeInteger(this.getNode(), this.width, mayBeNull);
    }

    public int getWidth() {
        return this.width;
    }

    @Override
    public boolean same(DBSPType type) {
        if (!super.same(type))
            return false;
        if (!type.is(DBSPTypeInteger.class))
            return false;
        DBSPTypeInteger other = type.to(DBSPTypeInteger.class);
        return this.width == other.width;
    }
}
