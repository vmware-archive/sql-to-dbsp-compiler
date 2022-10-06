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

package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.ir.Visitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.TranslationException;

import javax.annotation.Nullable;

public class DBSPUnaryExpression extends DBSPExpression {
    public final DBSPExpression left;
    public final String operation;

    @SuppressWarnings("ConstantConditions")
    public DBSPUnaryExpression(@Nullable Object node, DBSPType type, String operation, DBSPExpression operand) {
        super(node, type);
        this.operation = operation;
        this.left = operand;
        if (operand.getNonVoidType().mayBeNull && !type.mayBeNull)
            throw new RuntimeException("Unary operation produces non-nullable from nullable " + this);
        if (this.left == null)
            throw new TranslationException("Null operand", node);
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        this.left.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public boolean shallowSameExpression(DBSPExpression other) {
        if (this == other)
            return true;
        DBSPUnaryExpression fe = other.as(DBSPUnaryExpression.class);
        if (fe == null)
            return false;
        return this.operation.equals(fe.operation) && this.left == fe.left;
    }
}