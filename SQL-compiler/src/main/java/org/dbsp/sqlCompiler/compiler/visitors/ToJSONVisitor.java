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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import org.dbsp.sqlCompiler.circuit.*;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

public class ToJSONVisitor extends CircuitVisitor {
    protected final IndentStream builder;

    public ToJSONVisitor(IndentStream builder) {
        super(true);
        this.builder = builder;
    }

    @Override
    public boolean preorder(DBSPCircuit circuit) {
        this.builder.append("{").increase();
        circuit.circuit.accept(this);
        this.builder.decrease().append("}").newline();
        return false;
    }

    void json(Object object) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.activateDefaultTypingAsProperty(
                            LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL, "class")
                    .enable(SerializationFeature.INDENT_OUTPUT);
            String representation = mapper.writeValueAsString(object);
            this.builder.append(representation);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean preorder(DBSPPartialCircuit circuit) {
        this.builder.append("\"functions\": [").increase();
        boolean first = true;
        for (IDBSPNode node : circuit.code) {
            IDBSPDeclaration decl = node.as(IDBSPDeclaration.class);
            if (decl != null) {
                if (!first)
                    this.builder.append(",");
                this.json(decl);
                first = false;
            }
        }
        this.builder.decrease().append("],").newline();
        this.builder.append("\"operators\": [").increase();
        first = true;
        for (IDBSPNode node : circuit.code) {
            DBSPOperator op = node.as(DBSPOperator.class);
            if (op != null) {
                if (!first)
                    this.builder.append(",").newline();
                op.accept(this);
                first = false;
            }
        }
        this.builder.decrease().append("]").newline();
        return false;
    }

    @Override
    public boolean preorder(DBSPOperator operator) {
        this.builder.append("{").increase()
                .append("\"operation\": ")
                .append(Utilities.doubleQuote(operator.operation))
                .append(",").newline()
                .append("\"output\" :")
                .append(Utilities.doubleQuote(operator.outputName))
                .append(",").newline();
        this.builder.append("\"inputs\": [");
        for (int i = 0; i < operator.inputs.size(); i++) {
            this.builder.append(Utilities.doubleQuote(operator.inputs.get(i).outputName));
        }
        this.builder.append("]");
        if (operator.comment != null) {
            this.builder.append(",").newline();
            this.builder.append("\"comment\": ")
                    .append(Utilities.doubleQuote(operator.comment));
        }
        if (operator.function != null) {
            this.builder.append(",").newline();
            this.builder.append("\"function\" : ");
            this.json(operator.function);
        }
        this.builder.decrease().append("}");
        return false;
    }

    public static String circuitToJSON(IDBSPOuterNode node) {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        ToJSONVisitor visitor = new ToJSONVisitor(stream);
        node.accept(visitor);
        return builder.toString();
    }
}
