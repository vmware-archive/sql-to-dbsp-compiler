/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.visitors;

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

/**
 * A visitor which rewrites expressions into "three-operand" code introducing temporaries.
 * For example a = b + c * d;
 * is rewritten to
 * a = {
 *   let tmp = c * d;
 *   let tmp1 = b + tmp;
 *   tmp1
 * };
 */
public class ThreeOperandVisitor extends InnerVisitor implements Function<IDBSPInnerNode, IDBSPInnerNode>, IModule {
    @Nullable
    protected IDBSPInnerNode lastResult;
    private final NameGen gen;
    protected final List<DBSPStatement> toInsert;

    public ThreeOperandVisitor() {
        super(true);
        this.gen = new NameGen("tmp");
        this.toInsert = new ArrayList<>();
        this.lastResult = null;
    }

    @Override
    @Nullable
    public IDBSPInnerNode apply(@Nullable IDBSPInnerNode dbspNode) {
        if (dbspNode == null)
            return null;
        dbspNode.accept(this);
        return this.getResult();
    }

    void map(IDBSPInnerNode old, IDBSPInnerNode newOp) {
        Logger.instance.from(this, 1)
                .append(this.toString())
                .append(":")
                .append(old.toString())
                .append(" -> ")
                .append(newOp.toString())
                .newline();
        this.lastResult = newOp;
    }

    IDBSPInnerNode getResult() {
        return Objects.requireNonNull(this.lastResult);
    }

    DBSPExpression getResultExpression() {
        return this.getResult().to(DBSPExpression.class);
    }

    @Override
    public boolean preorder(DBSPExpression expression) {
        // Catches all unimplemented expressions.
        // For example, DBSPLiteral, DBSPVariablePath, DBSPRangeExpression,
        // DBSPPathExpression, DBSPEnumValue
        this.map(expression, expression);
        return false;
    }

    /**
     * Given an expression replace it by a temporary variable.
     * As a side-effect insert a statement defining the variable in the toInsert list.
     */
    DBSPVariablePath remap(DBSPExpression expression) {
        expression.accept(this);
        DBSPExpression replacement = this.getResultExpression();
        String tmp = this.gen.toString();
        DBSPLetStatement stat = new DBSPLetStatement(tmp, replacement, false);
        toInsert.add(stat);
        return stat.getVarReference();
    }

    DBSPExpression makeBlock(@Nullable DBSPExpression last) {
        if (this.toInsert.isEmpty())
            return Objects.requireNonNull(last);
        DBSPExpression result = new DBSPBlockExpression(new ArrayList<>(this.toInsert), last);
        this.toInsert.clear();
        return result;
    }

    @Override
    public boolean preorder(DBSPType node) {
        return false;
    }

    //////////////////////////// Expressions

    @Override
    public boolean preorder(DBSPQualifyTypeExpression expression) {
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPForExpression expression) {
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPMatchExpression expression) {
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPApplyExpression expression) {
        expression.function.accept(this);
        DBSPExpression functionReplacement = this.getResultExpression();
        if (functionReplacement != expression.function) {
            String tmp = this.gen.toString();
            DBSPLetStatement stat = new DBSPLetStatement(tmp, functionReplacement, false);
            toInsert.add(stat);
            functionReplacement = stat.getVarReference();
        }
        DBSPExpression[] args = Linq.map(expression.arguments, this::remap, DBSPExpression.class);
        DBSPExpression result = new DBSPApplyExpression(functionReplacement, expression.getType(), args);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyMethodExpression expression) {
        expression.function.accept(this);
        DBSPExpression functionReplacement = this.getResultExpression();
        if (functionReplacement != expression.function) {
            String tmp = this.gen.toString();
            DBSPLetStatement stat = new DBSPLetStatement(tmp, functionReplacement, false);
            toInsert.add(stat);
            functionReplacement = stat.getVarReference();
        }
        DBSPExpression self = this.remap(expression.self);
        DBSPExpression[] args = Linq.map(expression.arguments, this::remap, DBSPExpression.class);
        DBSPExpression result = new DBSPApplyMethodExpression(
                functionReplacement, expression.getType(), self, args);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPAsExpression expression) {
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPAssignmentExpression expression) {
        DBSPVariablePath left = this.remap(expression.left);
        DBSPVariablePath right = this.remap(expression.right);
        DBSPExpression result = new DBSPAssignmentExpression(left, right);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPDerefExpression expression) {
        DBSPVariablePath var = this.remap(expression.expression);
        DBSPExpression result = new DBSPDerefExpression(var);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPBorrowExpression expression) {
        DBSPVariablePath var = this.remap(expression.expression);
        DBSPExpression result = var.borrow();
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPIfExpression expression) {
        DBSPVariablePath cond = this.remap(expression.condition);
        DBSPExpression condBlock = this.makeBlock(cond);
        DBSPVariablePath positive = this.remap(expression.positive);
        DBSPExpression positiveBlock = this.makeBlock(positive);
        DBSPVariablePath negative = this.remap(expression.negative);
        DBSPExpression negativeBlock = this.makeBlock(negative);
        DBSPExpression result = new DBSPIfExpression(
                expression.getNode(), condBlock, positiveBlock, negativeBlock);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldExpression expression) {
        if (expression.expression.is(DBSPVariablePath.class)) {
            this.map(expression, expression);
            return false;
        }
        DBSPVariablePath var = this.remap(expression.expression);
        DBSPExpression result = new DBSPFieldExpression(expression.getNode(), var, expression.fieldNo);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression expression) {
        DBSPVariablePath var = this.remap(expression.left);
        DBSPType type = expression.getNonVoidType();
        DBSPExpression result = new DBSPUnaryExpression(expression.getNode(), type,
                expression.operation, var);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        DBSPExpression[] sources = Linq.map(expression.fields, this::remap, DBSPExpression.class);
        DBSPExpression result = new DBSPTupleExpression(sources);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPRawTupleExpression expression) {
        DBSPExpression[] sources = Linq.map(expression.fields, this::remap, DBSPExpression.class);
        DBSPExpression result = new DBSPRawTupleExpression(sources);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPStructExpression expression) {
        DBSPExpression[] fields = Linq.map(expression.arguments, this::remap, DBSPExpression.class);
        DBSPExpression result = new DBSPStructExpression(expression.function, expression.getNonVoidType(), fields);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPCastExpression expression) {
        DBSPVariablePath var = this.remap(expression.source);
        DBSPType type = expression.getNonVoidType();
        DBSPExpression result = new DBSPCastExpression(expression.getNode(), var, type);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        DBSPVariablePath left = this.remap(expression.left);
        DBSPVariablePath right = this.remap(expression.right);
        DBSPType type = expression.getNonVoidType();
        DBSPExpression result = new DBSPBinaryExpression(expression.getNode(), type, expression.operation, left, right);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        expression.body.accept(this);
        DBSPExpression newBody = this.getResultExpression();
        DBSPExpression block = this.makeBlock(newBody);
        DBSPExpression result = new DBSPClosureExpression(expression.getNode(),
                block, expression.parameters);
        this.map(expression, result);
        return false;
    }

    ////////////////// Statements

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        if (statement.initializer == null)
            return false;
        statement.initializer.accept(this);
        DBSPExpression result = this.getResultExpression();
        DBSPExpression init = this.makeBlock(result);
        this.map(statement, new DBSPLetStatement(statement.variable, init));
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        for (DBSPStatement statement: expression.contents) {
            statement.accept(this);
        }
        DBSPExpression last = null;
        if (expression.lastExpression != null) {
            expression.lastExpression.accept(this);
            last = this.getResultExpression();
        }
        DBSPExpression result = this.makeBlock(last);
        this.map(expression, result);
        return false;
    }
}
