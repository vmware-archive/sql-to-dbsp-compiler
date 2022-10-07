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

package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerDeclaration;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariableReference;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.IHasType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A (Rust) function.
 */
public class DBSPFunction extends DBSPNode implements IHasType, IDBSPInnerDeclaration {
    public static class Argument extends DBSPNode implements IHasType, IDBSPInnerNode {
        public final String name;
        public final DBSPType type;

        public Argument(String name, DBSPType type) {
            super(null);
            this.name = name;
            this.type = type;
        }

        @Override
        public DBSPType getType() {
            return this.type;
        }

        @Override
        public void accept(InnerVisitor visitor) {
            if (!visitor.preorder(this)) return;
            visitor.postorder(this);
        }
    }

    public final String name;
    public final List<Argument> arguments;
    // Null if function returns void.
    @Nullable
    public final DBSPType returnType;
    public final DBSPExpression body;
    public final List<String> annotations;
    public final DBSPTypeFunction type;

    public DBSPFunction(String name, List<Argument> arguments, @Nullable DBSPType returnType, DBSPExpression body) {
        super(null);
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
        this.body = body;
        this.annotations = new ArrayList<>();
        DBSPType[] argTypes = new DBSPType[arguments.size()];
        for (int i = 0; i < argTypes.length; i++)
            argTypes[i] = arguments.get(i).getNonVoidType();
        this.type = new DBSPTypeFunction(returnType, argTypes);
    }

    public DBSPFunction addAnnotation(String annotation) {
        this.annotations.add(annotation);
        return this;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Nullable
    @Override
    public DBSPType getType() {
        return this.type;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.returnType != null)
            this.returnType.accept(visitor);
        for (Argument argument: this.arguments)
            argument.accept(visitor);
        this.body.accept(visitor);
        visitor.postorder(this);
    }

    public DBSPExpression getReference() {
        return new DBSPVariableReference(this.name, this.type);
    }
}
