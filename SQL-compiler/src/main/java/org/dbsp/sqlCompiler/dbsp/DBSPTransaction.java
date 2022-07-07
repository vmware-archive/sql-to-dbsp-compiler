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

package org.dbsp.sqlCompiler.dbsp;

import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPZSetLiteral;

import java.util.HashMap;
import java.util.Map;

/**
 * A DBSPTransaction holds a set of changes for the inputs of a DBSP program.
 * Transactions can be created from some forms of SQL INSERT or DELETE statements.
 */
public class DBSPTransaction {
    /**
     * A transaction is always associated with a circuit.
     */
    public final DBSPCircuit circuit;
    /**
     * A Map from input name to value.
     */
    public final Map<String, DBSPZSetLiteral> perInputChange;

    public DBSPTransaction(DBSPCircuit circuit) {
        this.circuit = circuit;
        this.perInputChange = new HashMap<>();
    }

    public void addTuple(String table, DBSPExpression expression) {
        if (this.perInputChange.containsKey(table))
            this.perInputChange.get(table).add(expression);
        else
            this.perInputChange.put(table, new DBSPZSetLiteral(expression));
    }

    public void addSet(String table, DBSPZSetLiteral set) {
        if (this.perInputChange.containsKey(table))
            this.perInputChange.get(table).add(set);
        else
            this.perInputChange.put(table, set);
    }
}
