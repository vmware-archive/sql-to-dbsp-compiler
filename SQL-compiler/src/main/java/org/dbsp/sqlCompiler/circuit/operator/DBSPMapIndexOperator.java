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

package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class DBSPMapIndexOperator extends DBSPUnaryOperator {
    public final DBSPType keType;
    public final DBSPType valueType;

    // Expression must return a tuple that is composed of a key and a value
    public DBSPMapIndexOperator(@Nullable Object node, DBSPExpression expression,
                                DBSPType keyType, DBSPType valueType, DBSPOperator input) {
        super(node, "map_index", expression,
                new DBSPTypeIndexedZSet(node, keyType, valueType), true, input);
        DBSPType outputElementType = new DBSPTypeRawTuple(keyType, valueType);
        this.keType = keyType;
        this.valueType = valueType;
        this.checkResultType(expression, outputElementType);
        this.checkArgumentFunctionType(expression, 0, input);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression) {
        return new DBSPMapIndexOperator(
                this.getNode(), Objects.requireNonNull(expression), this.keType, this.valueType, this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPMapIndexOperator(
                    this.getNode(), this.getFunction(), this.keType, this.valueType, newInputs.get(0));
        return this;
    }
}
