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

import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeString;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

public class DBSPStringLiteral extends DBSPLiteral {
    @Nullable
    public final String value;

    public DBSPStringLiteral() {
        this(null, true);
    }

    public DBSPStringLiteral(String value) {
        this(value, false);
    }

    static String toString(@Nullable String value) {
        if (value == null)
            return "None::<" + DBSPTypeString.instance.toString() + ">";
        return value;
    }

    public DBSPStringLiteral(@Nullable String value, boolean nullable) {
        super(null, DBSPTypeString.instance.setMayBeNull(nullable), value);
        if (value == null && !nullable)
            throw new RuntimeException("Null value with non-nullable type");
        this.value = value;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        if (this.value == null)
            return builder.append(this.noneString());
        return builder.append(this.wrapSome(
                "String::from(" + Utilities.escapeString(this.value) + ")"));
    }
}
