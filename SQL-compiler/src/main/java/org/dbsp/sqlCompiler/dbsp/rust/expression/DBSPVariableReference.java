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

import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.DBSPPattern;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeRef;

/**
 * Reference to a variable by name.
 */
public class DBSPVariableReference extends DBSPExpression {
    public final String variable;

    public DBSPVariableReference(String variable, DBSPType type) {
        super(null, type);
        this.variable = variable;
    }

    public DBSPPattern asPattern(boolean mutable) {
        return new DBSPIdentifierPattern(this.variable, mutable);
    }

    public DBSPPattern asPattern() {
        return this.asPattern(false);
    }

    public DBSPClosureExpression.Parameter asParameter(boolean mutable) {
        return new DBSPClosureExpression.Parameter(this.asPattern(mutable), this.getNonVoidType());
    }

    public DBSPClosureExpression.Parameter asParameter() {
        return this.asParameter(false);
    }

    public DBSPClosureExpression.Parameter asRefParameter(boolean mutable) {
        return new DBSPClosureExpression.Parameter(
                this.asPattern(),
                new DBSPTypeRef(this.getNonVoidType(), mutable));
    }

    public DBSPClosureExpression.Parameter asRefParameter() {
        return this.asRefParameter(false);
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        visitor.postorder(this);
    }
}
