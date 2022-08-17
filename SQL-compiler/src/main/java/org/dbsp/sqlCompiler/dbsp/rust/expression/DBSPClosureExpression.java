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

import org.dbsp.sqlCompiler.dbsp.circuit.DBSPNode;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.DBSPPattern;
import org.dbsp.sqlCompiler.dbsp.rust.pattern.DBSPTuplePattern;
import org.dbsp.sqlCompiler.dbsp.rust.type.*;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;

/**
 * An expression of the form |var1, var2, ...| body.
 * Note: the type of the expression is in fact the type of the body,
 * and not a closure type.
 */
public class DBSPClosureExpression extends DBSPExpression {
    private final DBSPExpression body;
    private final Parameter[] varNames;

    public static class Parameter extends DBSPNode implements IHasType {
        public final DBSPPattern pattern;
        @Nullable
        public final DBSPType type;

        public Parameter(DBSPPattern pattern, @Nullable DBSPType type) {
            super(null);
            this.pattern = pattern;
            this.type = type;
        }

        public Parameter(DBSPVariableReference... variables) {
            super(null);
            this.pattern = new DBSPTuplePattern(Linq.map(variables, DBSPVariableReference::asPattern, DBSPPattern.class));
            this.type = new DBSPTypeRawTuple(Linq.map(variables, DBSPExpression::getType, DBSPType.class));
        }

        public Parameter(String pattern, @Nullable DBSPType type) {
            super(null);
            this.pattern = new DBSPIdentifierPattern(pattern);
            this.type = type;
        }

        @Override
        public IndentStringBuilder toRustString(IndentStringBuilder builder) {
            builder.append(this.pattern);
            if (this.type != null)
                builder.append(":")
                        .append(this.type);
            return builder;
        }

        @Nullable
        @Override
        public DBSPType getType() {
            return this.type;
        }
    }

    public DBSPClosureExpression(@Nullable Object node, DBSPExpression body, String... varNames) {
        super(node, body.getType());
        this.body = body;
        this.varNames = Linq.map(varNames, v -> new Parameter(v, null), Parameter.class);
    }

    public DBSPClosureExpression(@Nullable Object node, DBSPExpression body, Parameter... variables) {
        super(node, body.getType());
        this.body = body;
        this.varNames = variables;
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        builder.append("move |")
                .intercalate(", ", this.varNames)
                .append("| ");
        if (this.getType() != null)
            builder.append("-> ")
                    .append(this.getType())
                    .append(" ");
        return builder.append("{")
                .increase()
                .append(this.body)
                .decrease()
                .append("}");
    }

    public DBSPType[] getParameterTypes() {
        return Linq.map(this.varNames, Parameter::getType, DBSPType.class);
    }
}