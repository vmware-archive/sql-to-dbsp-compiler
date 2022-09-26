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
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeTuple;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A Raw tuple expression generates a raw Rust tuple.
 */
public class DBSPRawTupleExpression extends DBSPTupleExpression {
    public DBSPRawTupleExpression(DBSPExpression... fields) {
        super(fields);
    }

    @Nullable
    @Override
    public DBSPType getType() {
        assert this.type != null;
        return new DBSPTypeRawTuple(this.type.to(DBSPTypeTuple.class).tupFields);
    }

    public <T extends DBSPExpression> DBSPRawTupleExpression(List<T> fields) {
        this(fields.toArray(new DBSPExpression[0]));
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        for (DBSPExpression expression: this.fields)
            expression.accept(visitor);
        visitor.postorder(this);
    }
}
