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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.*;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;

public class ToJSONVisitor extends CircuitVisitor implements IModule {
    protected final ObjectNode root;
    protected final ObjectMapper topMapper;
    @Nullable
    protected ObjectNode currentOperator = null;

    public ToJSONVisitor() {
        super(true);
        this.topMapper = new ObjectMapper();
        this.root = this.topMapper.createObjectNode();
    }

    JsonNode json(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.activateDefaultTypingAsProperty(
                        LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL, "class")
                .enable(SerializationFeature.INDENT_OUTPUT);
        return mapper.valueToTree(object);
    }

    @Override
    public boolean preorder(DBSPPartialCircuit circuit) {
        ArrayNode functions = this.topMapper.createArrayNode();
        for (IDBSPNode node : circuit.code) {
            IDBSPDeclaration decl = node.as(IDBSPDeclaration.class);
            if (decl != null) {
                JsonNode tree = this.json(decl);
                functions.add(tree);
            }
        }
        this.root.set("functions", functions);

        ArrayNode operators = this.topMapper.createArrayNode();
        for (IDBSPNode node : circuit.code) {
            DBSPOperator op = node.as(DBSPOperator.class);
            if (op != null) {
                op.accept(this);
                if (this.currentOperator == null)
                    throw new RuntimeException("Could not produce JSON for operator " + op);
                operators.add(this.currentOperator);
            }
        }
        this.root.set("operators", operators);
        return false;
    }

    @Override
    public boolean preorder(DBSPOperator operator) {
        this.currentOperator = this.topMapper.createObjectNode();
        this.currentOperator.put("operation", operator.operation);
        this.currentOperator.put("output", operator.outputName);
        this.currentOperator.set("type", this.json(operator.outputType));
        ArrayNode inputs = this.topMapper.createArrayNode();
        this.currentOperator.set("inputs", inputs);
        for (int i = 0; i < operator.inputs.size(); i++) {
            inputs.add(operator.inputs.get(i).outputName);
        }
        if (operator.comment != null)
            this.currentOperator.put("comment", operator.comment);
        if (operator.function != null) {
            this.currentOperator.set("function", this.json(operator.function));
        }
        return false;
    }

    public static String circuitToJSON(DBSPCircuit node) {
        ToJSONVisitor visitor = new ToJSONVisitor();
        ThreeAddressVisitor tav = new ThreeAddressVisitor();
        CircuitFunctionRewriter rewriter = new CircuitFunctionRewriter(tav);
        node = rewriter.apply(node);
        Logger.instance.from(visitor, 1)
                        .append("Rewritten circuit is ")
                        .append(node.toString())
                        .newline();
        node.accept(visitor);
        return visitor.root.toPrettyString();
    }
}
