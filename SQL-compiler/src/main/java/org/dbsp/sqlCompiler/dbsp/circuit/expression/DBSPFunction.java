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

import org.dbsp.sqlCompiler.dbsp.circuit.DBSPNode;
import org.dbsp.sqlCompiler.dbsp.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.circuit.type.IHasType;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A (Rust) function.
 */
public class DBSPFunction extends DBSPNode implements IDBSPDeclaration {
    public static class DBSPArgument extends DBSPNode implements IHasType {
        public final String name;
        public final DBSPType type;

        public DBSPArgument(String name, DBSPType type) {
            super(null);
            this.name = name;
            this.type = type;
        }

        @Override
        public IndentStringBuilder toRustString(IndentStringBuilder builder) {
            return builder.append(this.name)
                    .append(": ")
                    .append(this.type);
        }

        @Override
        public DBSPType getType() {
            return this.type;
        }
    }

    public final String name;
    public final List<DBSPArgument> arguments;
    // Null if function returns void.
    @Nullable
    public final DBSPType returnType;
    public final DBSPExpression body;
    public final List<String> annotations;

    public DBSPFunction(String name, List<DBSPArgument> arguments, @Nullable DBSPType returnType, DBSPExpression body) {
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
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        builder.intercalateS("\n", this.annotations)
                .append("pub fn ")
                .append(this.name)
                .append("(")
                .join(", ", this.arguments)
                .append(") ");
        if (this.returnType != null)
            builder.append("-> ")
                .append(this.returnType);
        if (this.body.is(DBSPSeqExpression.class))
            return builder.append(this.body);
        return builder.append("\n{").increase()
                .append(this.body).decrease()
                .append("\n}");
    }

    @Override
    public String getName() {
        return this.name;
    }
}
