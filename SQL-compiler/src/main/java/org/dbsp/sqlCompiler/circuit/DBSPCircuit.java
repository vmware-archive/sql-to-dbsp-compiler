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

package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.Linq;
import org.dbsp.util.NameGen;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBSPCircuit extends DBSPNode implements IDBSPOuterNode {
    public final List<DBSPSourceOperator> inputOperators = new ArrayList<>();
    public final List<DBSPSinkOperator> outputOperators = new ArrayList<>();
    public final List<DBSPOperator> operators = new ArrayList<>();
    public final List<IDBSPInnerDeclaration> declarations = new ArrayList<>();
    public final Map<String, DBSPOperator> operatorDeclarations = new HashMap<>();
    public final String name;

    public DBSPCircuit(String name) {
        super(null);
        this.name = name;
    }

    /**
     * @return the names of the input tables.
     * The order of the tables corresponds to the inputs of the generated circuit.
     */
    public List<String> getInputTables() {
        return Linq.map(this.inputOperators, DBSPOperator::getName);
    }

    public int getOutputCount() {
        return this.outputOperators.size();
    }

    public DBSPType getOutputType(int outputNo) {
        return this.outputOperators.get(outputNo).getNonVoidType();
    }

    public DBSPTypeRawTuple getOutputType() {
        return new DBSPTypeRawTuple(null, Linq.map(this.outputOperators, IHasType::getNonVoidType));
    }

    public void addOperator(DBSPOperator operator) {
        // System.out.println("Adding " + operator);
        Utilities.putNew(this.operatorDeclarations, operator.outputName, operator);
        if (operator.is(DBSPSourceOperator.class))
            this.inputOperators.add(operator.to(DBSPSourceOperator.class));
        else if (operator.is(DBSPSinkOperator.class))
            this.outputOperators.add(operator.to(DBSPSinkOperator.class));
        else
            this.operators.add(operator);
    }

    public void declare(IDBSPInnerDeclaration declaration) {
        this.declarations.add(declaration);
    }

    public DBSPLetStatement declareLocal(String prefix, DBSPExpression init) {
        String name = new NameGen(prefix).toString();
        DBSPLetStatement let = new DBSPLetStatement(name, init);
        this.declare(let);
        return let;
    }

    @Nullable
    public DBSPOperator getOperator(String tableName) {
        return this.operatorDeclarations.get(tableName);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (IDBSPInnerDeclaration decl: this.declarations)
            decl.accept(visitor.innerVisitor);
        for (DBSPSourceOperator source: this.inputOperators)
            source.accept(visitor);
        for (DBSPOperator op: this.operators)
            op.accept(visitor);
        for (DBSPSinkOperator sink: this.outputOperators)
            sink.accept(visitor);
        visitor.postorder(this);
    }

    public boolean sameCircuit(DBSPCircuit other) {
        if (this == other)
            return true;
        return Linq.same(this.inputOperators, other.inputOperators) &&
                Linq.same(this.operators, other.operators) &&
                Linq.same(this.outputOperators, other.outputOperators) &&
                Linq.same(this.declarations, other.declarations);
    }
}
