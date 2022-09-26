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

package org.dbsp.sqlCompiler.dbsp.rust;

import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPNode;
import org.dbsp.sqlCompiler.dbsp.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.IHasType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A (Rust) function.
 */
public class DBSPFunction extends DBSPNode implements IDBSPDeclaration {
    public static class Argument extends DBSPNode implements IHasType {
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
        public void accept(Visitor visitor) {
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

    public DBSPFunction(String name, List<Argument> arguments, @Nullable DBSPType returnType, DBSPExpression body) {
        super(null);
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
        this.body = body;
        this.annotations = new ArrayList<>();
    }

    public void addAnnotation(String annotation) {
        this.annotations.add(annotation);
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.returnType != null)
            this.returnType.accept(visitor);
        for (Argument argument: this.arguments)
            argument.accept(visitor);
        this.body.accept(visitor);
        visitor.postorder(this);
    }
}
