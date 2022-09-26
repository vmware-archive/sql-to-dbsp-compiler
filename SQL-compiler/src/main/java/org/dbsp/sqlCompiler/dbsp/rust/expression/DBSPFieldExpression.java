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

import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeTuple;

import javax.annotation.Nullable;

/**
 * Tuple field reference expression.
 */
public class DBSPFieldExpression extends DBSPExpression {
    public final DBSPExpression expression;
    public final int fieldNo;

    protected DBSPFieldExpression(@Nullable RexNode node, DBSPExpression expression, int fieldNo, DBSPType type) {
        super(node, type);
        this.expression = expression;
        this.fieldNo = fieldNo;
    }

    protected DBSPFieldExpression(DBSPExpression expression, int fieldNo, DBSPType type) {
        this(null, expression, fieldNo, type);
    }

    static DBSPType getFieldType(DBSPType type, int fieldNo) {
        if (type.is(DBSPTypeAny.class))
            return type;
        DBSPTypeTuple tuple = type.toRef(DBSPTypeTuple.class);
        return tuple.getFieldType(fieldNo);
    }

    public DBSPFieldExpression(@Nullable RexNode node, DBSPExpression expression, int fieldNo) {
        this(node, expression, fieldNo, getFieldType(expression.getNonVoidType(), fieldNo));
    }

    public DBSPFieldExpression(DBSPExpression expression, int fieldNo) {
        this(null, expression, fieldNo);
    }

    public DBSPExpression simplify() {
        if (this.expression.is(DBSPTupleExpression.class)) {
            return this.expression.to(DBSPTupleExpression.class).get(this.fieldNo);
        }
        return this;
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        this.expression.accept(visitor);
        visitor.postorder(this);
    }
}
