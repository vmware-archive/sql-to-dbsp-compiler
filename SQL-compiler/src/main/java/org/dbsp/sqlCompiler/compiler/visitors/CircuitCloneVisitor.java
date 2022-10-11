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

package org.dbsp.sqlCompiler.compiler.visitors;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This visitor clones a circuit into an equivalent one.
 * Each operator is cloned in one of two cases:
 * - any of its inputs has changed
 * - the 'force' flag is 'true'.
 * The declarations in the circuit are left unchanged.
 */
public class CircuitCloneVisitor extends CircuitVisitor implements Function<DBSPCircuit, DBSPCircuit> {
    final DBSPCircuit result;
    final Map<DBSPOperator, DBSPOperator> remap;
    final boolean force;

    public CircuitCloneVisitor(String outputName, boolean force) {
        super(true, new EmptyInnerVisitor());
        this.result = new DBSPCircuit(outputName);
        this.remap = new HashMap<>();
        this.force = force;
    }

    public CircuitCloneVisitor(String outputName) {
        this(outputName, false);
    }

    public DBSPOperator mapped(DBSPOperator original) {
        return Utilities.getExists(this.remap, original);
    }

    void map(DBSPOperator old, DBSPOperator newOp) {
        Utilities.putNew(this.remap, old, newOp);
        this.result.addOperator(newOp);
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        for (IDBSPInnerDeclaration decl: circuit.declarations)
            this.result.declare(decl);
    }

    public void replace(DBSPOperator operator) {
        List<DBSPOperator> sources = Linq.map(operator.inputs, this::mapped);
        DBSPOperator result = operator.replaceInputs(sources, this.force);
        this.map(operator, result);
    }

    @Override
    public void postorder(DBSPAggregateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDifferentialOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPFlatMapOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPIndexOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPIntegralOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSourceOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSubtractOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPSumOperator operator) {
        this.replace(operator);
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        this.replace(operator);
    }

    public DBSPCircuit getResult() {
        return this.result;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        circuit.accept(this);
        DBSPCircuit result = this.getResult();
        if (circuit.sameCircuit(result))
            return circuit;
        //System.out.println(this.getClass() + " changed circuit");
        return result;
    }
}
