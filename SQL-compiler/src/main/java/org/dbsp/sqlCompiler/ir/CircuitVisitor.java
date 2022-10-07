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

import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;

/**
 * Depth-first traversal of an DBSPNode hierarchy.
 */
@SuppressWarnings("SameReturnValue")
public abstract class CircuitVisitor {
    /// If true each visit call will visit by default the superclass.
    final boolean visitSuper;
    public final InnerVisitor innerVisitor;

    public CircuitVisitor(boolean visitSuper, InnerVisitor visitor) {
        this.visitSuper = visitSuper;
        this.innerVisitor = visitor;
    }

    /************************* PREORDER *****************************/

    // preorder methods return 'true' when normal traversal is desired,
    // and 'false' when the traversal should stop right away at the current node.
    // base classes
    public boolean preorder(DBSPOperator node) { return true; }

    public boolean preorder(DBSPCircuit circuit) {
        return true;
    }

    public boolean preorder(DBSPUnaryOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPIndexOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }
    
    public boolean preorder(DBSPSubtractOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSumOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPJoinOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPConstantOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPDifferentialOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPIntegralOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPNegateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPFlatMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPFilterOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPDistinctOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSinkOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSourceOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    ////////////////////////////////////

    public void postorder(DBSPOperator ignored) {}

    public void postorder(DBSPCircuit circuit) {}

    public void postorder(DBSPUnaryOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPIndexOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPSubtractOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPSumOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPJoinOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPAggregateOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPConstantOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPMapOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPDifferentialOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPIntegralOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPNegateOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPFlatMapOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPFilterOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPDistinctOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPSinkOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPSourceOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }
}
