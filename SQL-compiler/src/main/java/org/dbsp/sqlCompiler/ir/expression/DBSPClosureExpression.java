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

import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * An expression of the form |param0, param1, ...| body.
 */
public class DBSPClosureExpression extends DBSPExpression {
    public final DBSPExpression body;
    public final DBSPParameter[] parameters;

    public DBSPTypeFunction getFunctionType() {
        return this.getNonVoidType().to(DBSPTypeFunction.class);
    }

    @Nullable
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

    /**
     * Given a list of closure expressions with the same number of arguments,
     * create a closure that calls all of them and assembles the results in a tuple.
     */
    public static DBSPClosureExpression parallelClosure(DBSPClosureExpression... closures) {
        DBSPParameter[][] allParams = Linq.map(closures, c -> c.parameters, DBSPParameter[].class);
        int paramCount = -1;
        for (DBSPParameter[] params: allParams) {
            if (paramCount == -1)
                paramCount = params.length;
            else if (paramCount != params.length)
                throw new RuntimeException("Closures cannot be combined");
        }

        DBSPVariablePath[] resultParams = new DBSPVariablePath[paramCount];
        for (int i = 0; i < paramCount; i++) {
            int finalI = i;
            DBSPParameter[] first = Linq.map(allParams, p -> p[finalI], DBSPParameter.class);
            String name = "p" + i;
            DBSPVariablePath pi = new DBSPVariablePath(name, new DBSPTypeTuple(Linq.map(first, p -> p.type, DBSPType.class)));
            resultParams[i] = pi;
        }

        List<DBSPStatement> body = new ArrayList<>();
        List<DBSPExpression> tmps = new ArrayList<>();
        for (int i = 0; i < closures.length; i++) {
            DBSPClosureExpression closure = closures[i];
            String tmp = "tmp" + i;
            int finalI = i;
            DBSPExpression[] args = Linq.map(resultParams, p -> p.field(finalI), DBSPExpression.class);
            DBSPExpression init = closure.call(args);
            DBSPLetStatement stat = new DBSPLetStatement(tmp, init);
            tmps.add(new DBSPVariablePath(tmp, init.getNonVoidType()));
            body.add(stat);
        }

        DBSPExpression last = new DBSPTupleExpression(tmps, false);
        DBSPBlockExpression block = new DBSPBlockExpression(body, last);
        DBSPParameter[] params = Linq.map(resultParams, DBSPVariablePath::asParameter, DBSPParameter.class);
        return new DBSPClosureExpression(block, params);
    }

    public DBSPExpression call(DBSPExpression... arguments) {
        if (arguments.length != this.parameters.length)
            throw new RuntimeException("Received " + arguments.length + " but need " + this.parameters.length);
        return new DBSPApplyExpression(this, arguments);
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
}