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
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeZSet;
import org.dbsp.util.IndentStringBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a (constant) ZSet described by its elements.
 * A ZSet is a map from tuples to integer weights.
 * In general weights should not be zero.
 */
public class DBSPZSetLiteral extends DBSPExpression {
    private final Map<DBSPExpression, Integer> data;
    private final DBSPTypeZSet zsetType;

    /**
     * Create a ZSet literal from a set of data values.
     * @param weightType  Type of weight used.
     * @param data Data to insert in zset - cannot be empty, since
     *             it is used to extract the zset type.
     *             To create empty zsets use the constructor
     *             with just a type argument.
     */
    public DBSPZSetLiteral(DBSPType weightType, DBSPExpression... data) {
        super(null, new DBSPTypeZSet(null, data[0].getNonVoidType(), weightType));
        this.zsetType = this.getNonVoidType().to(DBSPTypeZSet.class);
        this.data = new HashMap<>();
        for (DBSPExpression e: data) {
            if (!e.getNonVoidType().same(data[0].getNonVoidType()))
                throw new RuntimeException("Not all values of set have the same type:" +
                    e.getType() + " vs " + data[0].getType());
            this.data.put(e, 1);
        }
    }

    public DBSPZSetLiteral(DBSPType type) {
        super(null, type);
        this.zsetType = this.getNonVoidType().to(DBSPTypeZSet.class);
        this.data = new HashMap<>();
    }

    public DBSPType getElementType() {
        return this.zsetType.elementType;
    }

    public void add(DBSPExpression expression) {
        // We expect the expression to be a constant value (a literal)
        if (!expression.getNonVoidType().same(this.getElementType()))
            throw new RuntimeException("Added element does not match zset type " +
                    expression.getType() + " vs " + this.getElementType());
        this.data.put(expression, 1);
    }

    public void add(DBSPExpression expression, int weight) {
        // We expect the expression to be a constant value (a literal)
        if (!expression.getNonVoidType().same(this.getElementType()))
            throw new RuntimeException("Added element does not match zset type " +
                    expression.getType() + " vs " + this.getElementType());
        this.data.put(expression, weight);
    }

    public void add(DBSPZSetLiteral other) {
        if (!this.getNonVoidType().same(other.getNonVoidType()))
            throw new RuntimeException("Added zsets do not have the same type " +
                    this.getElementType() + " vs " + other.getElementType());
        other.data.forEach(this::add);
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        builder.append("zset!(").increase();
        for (Map.Entry<DBSPExpression, Integer> e: data.entrySet()) {
            builder.append(e.getKey())
                    .append(" => ")
                    .append(e.getValue())
                    .append(",\n");
        }
        return builder.decrease().append(")");
    }

    public int size() {
        return this.data.size();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{ ");
        boolean first = true;
        for (Map.Entry<DBSPExpression, Integer> e: data.entrySet()) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(e.getKey())
                    .append(" => ")
                    .append(e.getValue());
        }
        builder.append("}");
        return builder.toString();
    }
}
