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

package org.dbsp.sqlCompiler.dbsp.rust.pattern;

import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.rust.path.DBSPPath;
import org.dbsp.util.IndentStringBuilder;

public class DBSPTupleStructPattern extends DBSPPattern {
    public final DBSPPath path;
    public final DBSPPattern[] arguments;

    public DBSPTupleStructPattern(DBSPPath path, DBSPPattern... arguments) {
        super(null);
        this.path = path;
        this.arguments = arguments;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append(this.path)
                .append("(")
                .join(", ", this.arguments)
                .append(")");
    }

    /**
     * Shortcut to generate a Some(x) pattern.
     */
    public static DBSPPattern somePattern(DBSPPattern argument) {
        return new DBSPTupleStructPattern(new DBSPPath("Some"), argument);
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        this.path.accept(visitor);
        for (DBSPPattern pattern: this.arguments)
            pattern.accept(visitor);
        visitor.postorder(this);
    }
}
