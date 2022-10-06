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
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;

/**
 * Function application expression.
 */
public class DBSPApplyMethodExpression extends DBSPExpression {
    public final DBSPExpression function;
    public final DBSPExpression self;
    public final DBSPExpression[] arguments;

    public DBSPApplyMethodExpression(
            String function, @Nullable DBSPType returnType,
            DBSPExpression self, DBSPExpression... arguments) {
        super(null, returnType);
        this.function = new DBSPPathExpression(DBSPTypeAny.instance, new DBSPPath(function));
        this.self = self;
        this.arguments = arguments;
    }

    @SuppressWarnings("unused")
    public DBSPApplyMethodExpression(
            DBSPExpression function,
            DBSPExpression self, DBSPExpression... arguments) {
        super(null, DBSPApplyExpression.getReturnType(function.getNonVoidType()));
        this.function = function;
        this.self = self;
        this.arguments = arguments;
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        this.self.accept(visitor);
        this.function.accept(visitor);
        for (DBSPExpression arg: this.arguments)
            arg.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public boolean shallowSameExpression(DBSPExpression other) {
        if (this == other)
            return true;
        DBSPApplyMethodExpression oe = other.as(DBSPApplyMethodExpression.class);
        if (oe == null)
            return false;
        return this.function == oe.function &&
                this.self == oe.self &&
                Linq.same(this.arguments, oe.arguments);
    }
}