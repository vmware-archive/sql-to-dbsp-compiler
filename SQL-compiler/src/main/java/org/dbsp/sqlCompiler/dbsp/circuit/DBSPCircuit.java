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

package org.dbsp.sqlCompiler.dbsp.circuit;

import org.dbsp.sqlCompiler.dbsp.Visitor;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.dbsp.rust.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.dbsp.rust.type.IHasType;
import org.dbsp.util.IndentStringBuilder;
import org.dbsp.util.Linq;
import org.dbsp.util.NameGen;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DBSPCircuit extends DBSPNode {
    public final List<DBSPSourceOperator> inputOperators = new ArrayList<>();
    public final List<DBSPSinkOperator> outputOperators = new ArrayList<>();
    public final List<DBSPOperator> operators = new ArrayList<>();
    public final List<IDBSPDeclaration> declarations = new ArrayList<>();
    public final String name;
    public final String query;

    public DBSPCircuit(@Nullable Object node, String name, String query) {
        super(node);
        this.name = name;
        this.query = query;
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

    public DBSPTypeRawTuple getOutputtype() {
        return new DBSPTypeRawTuple(null, Linq.map(this.outputOperators, IHasType::getNonVoidType));
    }

    public void addOperator(DBSPOperator operator) {
        if (operator instanceof DBSPSourceOperator)
            this.inputOperators.add((DBSPSourceOperator)operator);
        else if (operator instanceof DBSPSinkOperator)
            this.outputOperators.add((DBSPSinkOperator)operator);
        else
            this.operators.add(operator);
    }

    public DBSPLetStatement declareLocal(String prefix, DBSPExpression init) {
        String name = new NameGen(prefix).toString();
        DBSPLetStatement let = new DBSPLetStatement(name, init);
        this.declarations.add(let);
        return let;
    }

    private void genRcCell(IndentStringBuilder builder, DBSPOperator op) {
        builder.append("let ")
                .append(op.getName())
                .append(" = Rc::new(RefCell::<")
                .append(op.getNonVoidType())
                .append(">::new(Default::default()));")
                .newline();
        builder.append("let ")
                .append(op.getName())
                .append("_external = ")
                .append(op.getName())
                .append(".clone();")
                .newline();
        if (op instanceof DBSPSourceOperator) {
            builder.append("let ")
                    .append(op.getName())
                    .append(" = Generator::new(move || ")
                    .append(op.getName())
                    .append(".borrow().clone());")
                    .newline();
        }
    }

    /**
     * Generates a Rust function that returns a closure which evaluates the circuit.
     * TODO: generate an IR node.
     */
    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        // function prototype:
        // fn name() -> impl FnMut(T0, T1) -> (O0, O1) {
        builder.append("fn ")
                .append(this.name)
                .append("() -> impl FnMut(");

        boolean first = true;
        for (DBSPOperator i: this.inputOperators) {
            if (!first)
                builder.append(",");
            first = false;
            builder.append(i.getNonVoidType());
        }
        builder.append(") -> ");
        DBSPTypeTuple tuple = new DBSPTypeRawTuple(null, Linq.map(this.outputOperators, DBSPOperator::getNonVoidType));
        builder.append(tuple)
                .append(" {")
                .increase();

        builder.append("// ")
                .append(this.query)
                .append("\n");
        // For each input and output operator a corresponding Rc cell
        for (DBSPOperator i: this.inputOperators)
            this.genRcCell(builder, i);

        for (DBSPOperator o: this.outputOperators)
            this.genRcCell(builder, o);

        // Circuit body
        builder.append("let root = Circuit::build(|circuit| {")
                .increase();
        for (IDBSPDeclaration decl: this.declarations)
            builder.append(decl)
                    .newline();
        for (DBSPOperator i: this.inputOperators)
            builder.append(i)
                    .newline();
        for (DBSPOperator op: this.operators)
            op.toRustString(builder)
                    .newline();
        for (DBSPOperator i: this.outputOperators)
            builder.append(i)
                    .newline();

        builder.decrease()
                .append("})")
                .append(".unwrap();")
                .newline();

        // Create the closure and return it.
        builder.append("return move |")
                .joinS(", ", Linq.map(this.inputOperators, DBSPOperator::getName))
                .append("| {")
                .increase();

        for (DBSPOperator i: this.inputOperators)
            builder.append("*")
                    .append(i.getName())
                    .append("_external.borrow_mut() = ")
                    .append(i.getName())
                    .append(";")
                    .newline();
        builder.append("root.0.step().unwrap();")
                .newline()
                .append("return ")
                .append("(")
                .intercalateS(", ",
                        Linq.map(this.outputOperators, o -> o.getName() + "_external.borrow().clone()"))
                .append(")")
                .append(";")
                .newline()
                .decrease()
                .append("};")
                .newline()
                .decrease()
                .append("}")
                .newline();
        return builder;
    }

    @Override
    public void accept(Visitor visitor) {
        if (!visitor.preorder(this)) return;
        for (IDBSPDeclaration decl: this.declarations)
            decl.accept(visitor);
        for (DBSPSourceOperator source: this.inputOperators)
            source.accept(visitor);
        for (DBSPOperator op: this.operators)
            op.accept(visitor);
        for (DBSPSinkOperator sink: this.outputOperators)
            sink.accept(visitor);
        visitor.postorder(this);
    }
}
