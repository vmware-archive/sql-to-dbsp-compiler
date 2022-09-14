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

import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeVec;
import org.dbsp.util.IndentStringBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a (constant) vector described by its elements.
 */
public class DBSPVecLiteral extends DBSPExpression implements IDBSPContainter {
    private final List<DBSPExpression> data;
    public final DBSPTypeVec vecType;

    public DBSPVecLiteral(DBSPType elementType) {
        super(null, new DBSPTypeVec(elementType));
        this.data = new ArrayList<>();
        this.vecType = new DBSPTypeVec(elementType);
    }

    public DBSPVecLiteral(DBSPExpression... data) {
        super(null, new DBSPTypeVec(data[0].getNonVoidType()));
        this.vecType = this.getNonVoidType().to(DBSPTypeVec.class);
        this.data = new ArrayList<>();
        for (DBSPExpression e: data) {
            if (!e.getNonVoidType().same(data[0].getNonVoidType()))
                throw new RuntimeException("Not all values of set have the same type:" +
                    e.getType() + " vs " + data[0].getType());
            this.add(e);
        }
    }

    public DBSPType getElementType() {
        return this.vecType.getTypeArg(0);
    }

    public void add(DBSPExpression expression) {
        // We expect the expression to be a constant value (a literal)
        if (!expression.getNonVoidType().same(this.getElementType()))
            throw new RuntimeException("Added element does not match vector type " +
                    expression.getType() + " vs " + this.getElementType());
        this.data.add(expression);
    }

    public void add(DBSPVecLiteral other) {
        if (!this.getNonVoidType().same(other.getNonVoidType()))
            throw new RuntimeException("Added vectors do not have the same type " +
                    this.getElementType() + " vs " + other.getElementType());
        other.data.forEach(this::add);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append("vec!(")
                .increase()
                .intercalate(", ", this.data)
                .decrease()
                .append(")");
    }

    public int size() {
        return this.data.size();
    }
}
