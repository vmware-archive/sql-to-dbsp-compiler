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

package org.dbsp.sqlCompiler.dbsp.rust.expression;

import org.dbsp.sqlCompiler.dbsp.rust.type.*;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

public class DBSPLiteral extends DBSPExpression {
    @Nullable
    private final String value;

    public DBSPLiteral(@Nullable Object node, DBSPType type, String value) {
        super(node, type);
        this.value = type.mayBeNull ? "Some(" + value + ")" : value;
    }

    public DBSPLiteral(int value, boolean nullable) {
        this(null, DBSPTypeInteger.signed32.setMayBeNull(nullable), String.valueOf(value));
    }

    public DBSPLiteral(int value) {
        this(value, false);
    }

    public DBSPLiteral(String value, boolean nullable) {
        this(null, DBSPTypeString.instance.setMayBeNull(nullable), "String::from(" + Utilities.escapeString(value) + ")");
    }

    public DBSPLiteral(String value) {
        this(value, false);
    }

    public DBSPLiteral(float value) {
        this(value, false);
    }

    public DBSPLiteral(double value) {
        this(value, false);
    }

    public DBSPLiteral(boolean b) {
        this(b, false);
    }

    public DBSPLiteral(float value, boolean nullable) {
        this(null, DBSPTypeFloat.instance.setMayBeNull(nullable), "OrderedFloat::<f32>(" + value + ")");
    }

    public DBSPLiteral(double value, boolean nullable) {
        this(null, DBSPTypeDouble.instance.setMayBeNull(nullable), "OrderedFloat::<f64>(" + value + ")");
    }

    public DBSPLiteral(boolean b, boolean nullable) {
        this(null, DBSPTypeBool.instance.setMayBeNull(nullable), String.valueOf(b));
    }

    /**
     * Create a literal holding the value NULL with the specified type.
     * @param type  Type of the literal.
     */
    public DBSPLiteral(DBSPType type) {
        super(null, type);
        if (!type.mayBeNull)
            throw new RuntimeException("Type " + type + " cannot represent the NULL value");
        this.value = "None::<" + this.getNonVoidType().setMayBeNull(false) + ">";
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append(this.value != null ? this.value : "None");
    }
}
