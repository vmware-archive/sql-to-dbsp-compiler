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

import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.util.Linq;

import java.util.List;

public class OptimizeDistinctVisitor extends CircuitCloneVisitor {
    public OptimizeDistinctVisitor(String outputName) {
        super(outputName);
    }

    @Override
    public void postorder(DBSPDistinctOperator distinct) {
        // distinct (distinct) = distinct
        DBSPOperator input = this.mapped(distinct.input());
        if (input.is(DBSPDistinctOperator.class)) {
            this.map(distinct, input);
            return;
        }
        if (input.is(DBSPFilterOperator.class) ||
            input.is(DBSPJoinOperator.class) ||
            input.is(DBSPMapOperator.class) ||
            input.is(DBSPSumOperator.class)) {
            boolean allDistinct = Linq.all(input.inputs, i -> i.is(DBSPDistinctOperator.class));
            if (allDistinct) {
                // distinct(filter(distinct)) = distinct(filter)
                List<DBSPOperator> newInputs = Linq.map(input.inputs, i -> i.inputs.get(0));
                DBSPOperator newInput = input.replaceInputs(newInputs, false);
                this.result.addOperator(newInput);
                distinct.replaceInputs(Linq.list(newInput), false);
                this.map(distinct, distinct);
                return;
            }
        }
        super.postorder(distinct);
    }

    public void postorder(DBSPFilterOperator filter) {
        DBSPOperator input = this.mapped(filter.input());
        if (input.is(DBSPDistinctOperator.class)) {
            DBSPDistinctOperator distinct = input.to(DBSPDistinctOperator.class);
            // swap distinct after filter
            DBSPOperator newFilter = filter.replaceInputs(input.inputs, false);
            this.result.addOperator(newFilter);
            DBSPOperator result = distinct.replaceInputs(Linq.list(filter), false);
            this.map(filter, result);
            return;
        }
        super.postorder(filter);
    }

    public void postorder(DBSPJoinOperator join) {
        DBSPOperator left = this.mapped(join.inputs.get(0));
        DBSPOperator right = this.mapped(join.inputs.get(1));
        // join(distinct) = distinct(join)
        if (left.is(DBSPDistinctOperator.class) &&
            right.is(DBSPDistinctOperator.class)) {
            // swap distinct after filter
            DBSPOperator newLeft = left.inputs.get(0);
            DBSPOperator newRight = right.inputs.get(0);
            DBSPOperator result = join.replaceInputs(Linq.list(newLeft, newRight), false);
            this.result.addOperator(result);
            DBSPOperator distinct = new DBSPDistinctOperator(join.getNode(), result);
            this.map(join, distinct);
            return;
        }
        super.postorder(join);
    }
}
