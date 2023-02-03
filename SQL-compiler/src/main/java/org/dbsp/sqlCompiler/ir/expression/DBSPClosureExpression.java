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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;

/**
 * An expression of the form |param0, param1, ...| body.
 */
public class DBSPClosureExpression extends DBSPExpression {
    public final DBSPExpression body;
    public final DBSPParameter[] parameters;

    @JsonIgnore
    public DBSPTypeFunction getFunctionType() {
        return this.getNonVoidType().to(DBSPTypeFunction.class);
    }

    @Nullable @JsonIgnore
    public DBSPType getResultType() {
        return this.getFunctionType().resultType;
    }

    public DBSPClosureExpression(@Nullable Object node, DBSPExpression body, DBSPParameter... variables) {
        // In Rust in general we can't write the type of a closure.
        super(node, new DBSPTypeFunction(body.getType(), Linq.map(variables, DBSPParameter::getType, DBSPType.class)));
        this.body = body;
        this.parameters = variables;
    }

    public DBSPClosureExpression(DBSPExpression body, DBSPParameter... variables) {
        this(null, body, variables);
    }

    /**
     * Convert a closure into a function.
     * @param name  Name of the function.
     */
    DBSPFunction asFunction(String name) {
        return new DBSPFunction(name, Linq.list(parameters), this.getResultType(), this.body);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.type != null)
            this.type.accept(visitor);
        for (DBSPParameter param: this.parameters)
            param.accept(visitor);
        this.body.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public boolean shallowSameExpression(DBSPExpression other) {
        if (this == other)
            return true;
        DBSPClosureExpression cl = other.as(DBSPClosureExpression.class);
        if (cl == null)
            return false;
        return this.body == cl.body &&
                Linq.same(this.parameters, cl.parameters);
    }
}