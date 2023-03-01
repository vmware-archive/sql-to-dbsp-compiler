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

package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.IDBSPNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import java.util.function.Function;

/**
 * Applies a function (this.transform) to every function within an operator and
 * to every declaration in a circuit.
 */
public class CircuitFunctionRewriter extends CircuitCloneVisitor {
    private final Function<IDBSPInnerNode, IDBSPInnerNode> transform;

    public CircuitFunctionRewriter(Function<IDBSPInnerNode, IDBSPInnerNode> transform) {
        super(true);
        this.transform = transform;
    }

    @Override
    public void postorder(DBSPOperator node) {
        IDBSPInnerNode function = this.transform.apply(node.function);
        DBSPOperator result;
        if (function != null) {
            DBSPExpression funcExpr = function.to(DBSPExpression.class);
            result = node.withFunction(funcExpr);
            this.map(node, result);
        } else {
            this.replace(node);
        }
    }

    @Override
    public boolean preorder(DBSPPartialCircuit circuit) {
        for (IDBSPNode node : circuit.getCode()) {
            DBSPOperator op = node.as(DBSPOperator.class);
            if (op != null)
                op.accept(this);
            else {
                IDBSPInnerNode result = this.transform.apply(node.to(IDBSPInnerNode.class));
                this.getResult().declare(result.to(IDBSPDeclaration.class));
            }
        }
        return false;
    }
}
