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
 *
 *
 */

package org.dbsp.sqlCompiler.dbsp.circuit.expression;

import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPZSetType;
import org.dbsp.util.IndentStringBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a (constant) ZSet described by its elements.
 */
public class DBSPZSetLiteral extends DBSPExpression {
    private final Map<DBSPExpression, Integer> data;

    public DBSPZSetLiteral(DBSPType type, Map<DBSPExpression, Integer> data) {
        super(null, type);
        this.data = data;
        assert type.is(DBSPZSetType.class);
        DBSPZSetType zt = type.to(DBSPZSetType.class);
        for (DBSPExpression e: data.keySet()) {
            assert e.getType().same(zt.elementType);
        }
    }

    public DBSPZSetLiteral(DBSPExpression... data) {
        super(null, new DBSPZSetType(null, data[0].getType(), DBSPTypeInteger.signed32));
        this.data = new HashMap<>();
        for (DBSPExpression e: data) {
            assert e.getType().same(data[0].getType());
            this.data.put(e, 1);
        }
    }

    public DBSPZSetLiteral(DBSPType type) {
        super(null, type);
        this.data = new HashMap<>();
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        builder.append("zset!(");
        for (Map.Entry<DBSPExpression, Integer> e: data.entrySet()) {
            builder.append(e.getKey())
                    .append(" => ")
                    .append(e.getValue())
                    .append(",\n");
        }
        return builder.append(")");
    }
}
