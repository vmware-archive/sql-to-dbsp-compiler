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

package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.ir.Visitor;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This visitor clones a circuit into a new one.
 * Each operator is cloned, but the declarations are left unchanged.
 */
public class CircuitCloneVisitor extends Visitor {
    final DBSPCircuit result;
    final Map<DBSPOperator, DBSPOperator> remap;

    public CircuitCloneVisitor(String outputName) {
        super(true);
        this.result = new DBSPCircuit(outputName);
        this.remap = new HashMap<>();
    }

    public DBSPOperator mapped(DBSPOperator original) {
        return Utilities.getExists(this.remap, original);
    }

    void map(DBSPOperator old, DBSPOperator newOp) {
        Utilities.putNew(this.remap, old, newOp);
        this.result.addOperator(newOp);
    }

    /// postorder methods

    @Override
    public void postorder(DBSPCircuit circuit) {
        for (IDBSPDeclaration decl: circuit.declarations)
            this.result.declare(decl);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPAggregateOperator result = new DBSPAggregateOperator
                (operator.getNode(), operator.getFunction(),
                        operator.keyType, operator.outputElementType, source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        DBSPConstantOperator result = new DBSPConstantOperator(
                operator.getNode(), operator.getFunction(), operator.isMultiset);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPDifferentialOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPDifferentialOperator result = new DBSPDifferentialOperator(operator.getNode(), source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPDistinctOperator result = new DBSPDistinctOperator(operator.getNode(), source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPFilterOperator result = new DBSPFilterOperator(
                operator.getNode(), operator.getFunction(), source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPFlatMapOperator result = new DBSPFlatMapOperator(
                operator.getNode(), operator.getFunction(), operator.outputType, source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIndexOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPIndexOperator result = new DBSPIndexOperator(
                operator.getNode(), operator.getFunction(),
                operator.keyType, operator.elementType, operator.isMultiset, source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPIntegralOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPIntegralOperator result = new DBSPIntegralOperator(operator.getNode(), source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPJoinOperator result = new DBSPJoinOperator(operator.getNode(), operator.elementResultType,
                operator.getFunction(), operator.isMultiset, sources.get(0), sources.get(1));
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPMapOperator result = new DBSPMapOperator(operator.getNode(),
                operator.getFunction(), operator.outputElementType, source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPNegateOperator result = new DBSPNegateOperator(operator.getNode(), source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        DBSPSinkOperator result = new DBSPSinkOperator(operator.getNode(), operator.outputName, operator.query, source);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPSourceOperator operator) {
        DBSPOperator result = new DBSPSourceOperator(operator.getNode(), operator.outputType, operator.outputName);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSubtractOperator result = new DBSPSubtractOperator(
                operator.getNode(), sources.get(0), sources.get(1));
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPSumOperator result = new DBSPSumOperator(operator.getNode(), sources);
        this.map(operator, result);
    }

    public DBSPCircuit getResult() {
        return this.result;
    }
}
