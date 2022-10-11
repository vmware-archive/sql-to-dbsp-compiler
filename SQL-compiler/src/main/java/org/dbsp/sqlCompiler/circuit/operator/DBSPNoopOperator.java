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

import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariableReference;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;

import javax.annotation.Nullable;
import java.util.List;

public class DBSPNoopOperator extends DBSPUnaryOperator {
    static DBSPClosureExpression getClosure() {
        DBSPVariableReference var = new DBSPVariableReference("i", DBSPTypeAny.instance);
        return new DBSPClosureExpression(new DBSPDerefExpression(var), var.asRefParameter());
    }

    public DBSPNoopOperator(@Nullable Object node, DBSPOperator source, String outputName) {
        super(node, "map", getClosure(),
                source.getNonVoidType(), source.isMultiset, source, outputName);
    }

    @Override
    public DBSPOperator replaceInputs(List<DBSPOperator> newInputs, boolean force) {
        return new DBSPNoopOperator(this.getNode(), newInputs.get(0), this.outputName);
    }
}
