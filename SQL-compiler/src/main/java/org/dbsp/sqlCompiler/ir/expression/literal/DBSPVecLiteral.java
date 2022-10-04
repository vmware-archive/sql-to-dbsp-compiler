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

package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.ir.Visitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.IDBSPContainer;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a (constant) vector described by its elements.
 */
public class DBSPVecLiteral extends DBSPLiteral implements IDBSPContainer {
    public final List<DBSPExpression> data;
    public final DBSPTypeVec vecType;

    public DBSPVecLiteral(DBSPType elementType) {
        super(null, new DBSPTypeVec(elementType), 0); // The 0 is not used
        this.data = new ArrayList<>();
        this.vecType = new DBSPTypeVec(elementType);
    }

    public DBSPVecLiteral(DBSPExpression... data) {
        super(null, new DBSPTypeVec(data[0].getNonVoidType()), 0); // The 0 is not used
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

    public int size() {
        return this.data.size();
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        for (DBSPExpression expr: this.data)
            expr.accept(visitor);
        visitor.postorder(this);
    }
}
