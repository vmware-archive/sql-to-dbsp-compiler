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

package org.dbsp.sqlCompiler.dbsp.rust.expression.literal;

import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeFloat;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class DBSPFloatLiteral extends DBSPLiteral {
    @Nullable
    public final Float value;

    @SuppressWarnings("unused")
    public DBSPFloatLiteral() {
        this(null, true);
    }

    public DBSPFloatLiteral(float value) {
        this(value, false);
    }

    public DBSPFloatLiteral(@Nullable Float f, boolean nullable) {
        super(null, DBSPTypeFloat.instance.setMayBeNull(nullable), f);
        if (f == null && !nullable)
            throw new RuntimeException("Null value with non-nullable type");
        this.value = f;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        if (this.value == null)
            return builder.append(this.noneString());
        String val = Float.toString(this.value);
        if (Float.isNaN(value))
            val = "std::f32::NAN";
        return builder.append(this.wrapSome("F32::new(" + val + ")"));
    }
}
