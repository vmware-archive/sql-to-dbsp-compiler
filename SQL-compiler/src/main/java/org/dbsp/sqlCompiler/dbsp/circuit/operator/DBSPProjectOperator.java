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
 *
 *
 */

package org.dbsp.sqlCompiler.dbsp.circuit.operator;

import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeTuple;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Relational projection operator.  Projects a tuple on a set of columns.
 */
public class DBSPProjectOperator extends DBSPOperator {
    static String projectFunction(List<Integer> projectIndexes, DBSPType tupleType) {
        List<DBSPExpression> fields = new ArrayList<>();
        DBSPTypeTuple tuple = tupleType.to(DBSPTypeTuple.class);
        assert tuple.size() == projectIndexes.size();
        for (int i = 0; i < projectIndexes.size(); i++) {
            int index = projectIndexes.get(i);
            DBSPType etype = tuple.component(i);
            DBSPFieldExpression exp = new DBSPFieldExpression(null, index, etype);
            fields.add(exp);
        }
        DBSPTupleExpression exp = new DBSPTupleExpression(null, fields, tupleType);
        DBSPClosureExpression clo = new DBSPClosureExpression(null, tupleType, exp);
        return clo.toRustString();
    }

    public DBSPProjectOperator(@Nullable Object node, List<Integer> projectIndexes,
                               DBSPType resultType) {
        super(node, "map", projectFunction(projectIndexes, resultType),
                TypeCompiler.makeZSet(resultType));
    }
}
