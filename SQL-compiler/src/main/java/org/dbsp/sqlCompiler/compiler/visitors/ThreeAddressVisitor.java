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
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;

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
public class ThreeAddressVisitor extends InnerVisitor implements Function<IDBSPInnerNode, IDBSPInnerNode>, IModule {
    final Map<IDBSPInnerNode, IDBSPInnerNode> remap = new HashMap<>();
    private final NameGen gen;
    private final List<DBSPStatement> toInsert;

    public ThreeAddressVisitor() {
        super(true);
        this.gen = new NameGen("tmp");
        this.toInsert = new ArrayList<>();
    }

    @Override
    @Nullable
    public IDBSPInnerNode apply(@Nullable IDBSPInnerNode dbspNode) {
        if (dbspNode == null)
            return null;
        dbspNode.accept(this);
        return this.getReplacement(dbspNode);
    }

    void map(IDBSPInnerNode old, IDBSPInnerNode newOp) {
        Logger.instance.from(this, 1)
                .append(this.toString())
                .append(":")
                .append(old.toString())
                .append(" -> ")
                .append(newOp.toString())
                .newline();
        Utilities.putNew(this.remap, old, newOp);
    }

    IDBSPInnerNode getReplacement(IDBSPInnerNode node) {
        if (this.remap.containsKey(node))
            return this.remap.get(node);
        return node;
    }

    DBSPExpression getReplacementExpression(IDBSPInnerNode node) {
        return this.getReplacement(node).to(DBSPExpression.class);
    }

    @Override
    public boolean preorder(DBSPType node) {
        return false;
    }

    @Override
    public boolean preorder(DBSPLiteral node) {
        return false;
    }

    @Override
    public boolean preorder(DBSPVariablePath node) {
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression node) {
        String tmp = this.gen.toString();
        DBSPExpression source = this.getReplacementExpression(node.left);
        source.accept(this);
        DBSPType type = node.getNonVoidType();
        DBSPExpression result = new DBSPUnaryExpression(node.getNode(), type,
                node.operation, source);
        DBSPLetStatement stat = new DBSPLetStatement(tmp, result, false);
        toInsert.add(stat);
        this.map(node, new DBSPVariablePath(tmp, type));
        return false;
    }

    @Override
    public boolean preorder(DBSPBinaryExpression node) {
        String tmp = this.gen.toString();
        node.left.accept(this);
        node.right.accept(this);
        DBSPExpression left = this.getReplacementExpression(node.left);
        DBSPExpression right = this.getReplacementExpression(node.right);
        DBSPType type = node.getNonVoidType();
        DBSPExpression result = new DBSPBinaryExpression(node.getNode(), type, node.operation, left, right);
        DBSPLetStatement stat = new DBSPLetStatement(tmp, result, false);
        toInsert.add(stat);
        this.map(node, new DBSPVariablePath(tmp, type));
        return false;
    }

    @Nullable
    DBSPExpression makeBlock(@Nullable DBSPExpression last) {
        if (this.toInsert.isEmpty())
            return last;
        DBSPExpression result = new DBSPBlockExpression(this.toInsert, last);
        this.toInsert.clear();
        return result;
    }

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        if (statement.initializer == null)
            return false;
        statement.initializer.accept(this);
        DBSPExpression result = this.remap.get(statement.initializer).to(DBSPExpression.class);
        DBSPExpression init = this.makeBlock(result);
        this.map(statement, new DBSPLetStatement(statement.variable, Objects.requireNonNull(init)));
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression node) {
        for (DBSPStatement statement: node.contents) {
            statement.accept(this);
        }
        DBSPExpression last = null;
        if (node.lastExpression != null) {
            node.lastExpression.accept(this);
            last = this.getReplacementExpression(node.lastExpression);
        }
        DBSPExpression result = this.makeBlock(last);
        this.map(node, Objects.requireNonNull(result));
        return false;
    }
}
