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

package org.dbsp.sqlCompiler.dbsp.circuit.operator;

import org.dbsp.sqlCompiler.dbsp.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;

public class DBSPAggregateOperator extends DBSPOperator {
    public DBSPAggregateOperator(@Nullable Object node, DBSPExpression function, DBSPType keyType, DBSPType outputElementType, DBSPOperator input) {
        super(node, "stream_aggregate", function,
                new DBSPTypeIndexedZSet(node, keyType, outputElementType, CalciteToDBSPCompiler.weightType), false);
        this.addInput(input);
        this.checkResultType(function, outputElementType);
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        if (this.function != null)
            this.function.accept(visitor);
        for (DBSPOperator input: this.inputs)
            input.accept(visitor);
        visitor.postorder(this);
    }
}
