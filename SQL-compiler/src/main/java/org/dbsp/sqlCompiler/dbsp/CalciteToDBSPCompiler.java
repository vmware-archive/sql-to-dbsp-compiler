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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.*;
import org.dbsp.sqlCompiler.dbsp.circuit.type.*;
import org.dbsp.sqlCompiler.frontend.*;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

public class CalciteToDBSPCompiler extends RelVisitor {
    static class Context {
        @Nullable
        RelNode parent;
        int inputNo;

        public Context(@Nullable RelNode parent, int inputNo) {
            this.parent = parent;
            this.inputNo = inputNo;
        }
    }

    private final DBSPCircuit circuit;
    boolean debug = false;
    // The path in the IR tree used to reach the current node.
    final List<Context> stack;
    // Map an input or output name to the corresponding operator
    final Map<String, DBSPOperator> ioOperator;
    // Map a RelNode operator to its DBSP implementation.
    final Map<RelNode, DBSPOperator> nodeOperator;

    TypeCompiler typeCompiler = new TypeCompiler();
    ExpressionCompiler expressionCompiler = new ExpressionCompiler(true);

    public CalciteToDBSPCompiler() {
        this.circuit = new DBSPCircuit(null, "circuit");
        this.stack = new ArrayList<>();
        this.ioOperator = new HashMap<>();
        this.nodeOperator = new HashMap<>();
    }

    private DBSPType convertType(RelDataType dt) {
        return this.typeCompiler.convertType(dt);
    }

    private DBSPType makeZSet(DBSPType type) {
        return TypeCompiler.makeZSet(type);
    }

    DBSPCircuit getProgram() {
        return Objects.requireNonNull(this.circuit);
    }

    <T> boolean visitIfMatches(RelNode node, Class<T> clazz, Consumer<T> method) {
        T value = ICastable.as(node, clazz);
        if (value != null) {
            if (debug)
                System.out.println("Processing " + node);
            method.accept(value);
            return true;
        }
        return false;
    }

    public void visitScan(LogicalTableScan scan) {
        List<String> name = scan.getTable().getQualifiedName();
        String tname = name.get(name.size() - 1);
        DBSPOperator op = Utilities.getExists(this.ioOperator, tname);
        this.assignOperator(scan, op);
    }

    void assignOperator(RelNode rel, DBSPOperator op) {
        Utilities.putNew(this.nodeOperator, rel, op);
        if (!(op instanceof DBSPSourceOperator))
            // These are already added
            this.circuit.addOperator(op);
    }

    DBSPOperator getOperator(RelNode node) {
        return Utilities.getExists(this.nodeOperator, node);
    }

    public void visitProject(LogicalProject project) {
        RelNode input = project.getInput();
        DBSPOperator opinput = this.getOperator(input);
        DBSPType type = this.convertType(project.getRowType());
        List<Integer> projectColumns = new ArrayList<>();
        for (RexNode column : project.getProjects()) {
            RexInputRef in = ICastable.as(column, RexInputRef.class);
            assert in != null : "Unhandled columnn reference in project: " + column;
            projectColumns.add(in.getIndex());
        }
        DBSPProjectOperator op = new DBSPProjectOperator(project, projectColumns, type);
        op.addInput(opinput);
        this.circuit.addOperator(op);

        DBSPDistinctOperator d = new DBSPDistinctOperator(project, type);
        d.addInput(op);
        this.assignOperator(project, d);
    }

    private void visitUnion(LogicalUnion union) {
        DBSPType type = this.convertType(union.getRowType());
        DBSPSumOperator sum = new DBSPSumOperator(union, type);
        for (RelNode input : union.getInputs()) {
            DBSPOperator opin = this.getOperator(input);
            sum.addInput(opin);
        }

        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(union, type);
            d.addInput(sum);
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        DBSPType type = this.convertType(minus.getRowType());
        DBSPSumOperator sum = new DBSPSumOperator(minus, type);
        boolean first = true;
        for (RelNode input : minus.getInputs()) {
            DBSPOperator opin = this.getOperator(input);
            if (!first) {
                DBSPNegateOperator neg = new DBSPNegateOperator(minus, type);
                this.circuit.addOperator(neg);
                neg.addInput(opin);
                sum.addInput(neg);
            } else {
                sum.addInput(opin);
            }
            first = false;
        }

        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(minus, type);
            d.addInput(sum);
            this.assignOperator(minus, d);
        }
    }

    public void visitFilter(LogicalFilter filter) {
        DBSPType type = this.convertType(filter.getRowType());
        DBSPExpression condition = this.expressionCompiler.compile(filter.getCondition());
        condition = new DBSPClosureExpression(filter.getCondition(), condition.getType(), condition);
        DBSPFilterOperator fop = new DBSPFilterOperator(filter, condition, type);
        DBSPOperator input = this.getOperator(filter.getInput());
        fop.addInput(input);
        this.assignOperator(filter, fop);
    }

    @Nullable
    private DBSPZSetLiteral logicalValueTranslation = null;

    public void visitLogicalValues(LogicalValues values) {
        DBSPType type = this.convertType(values.getRowType());
        DBSPZSetLiteral result = new DBSPZSetLiteral(new DBSPZSetType(null, type, DBSPTypeInteger.signed32));
        for (ImmutableList<RexLiteral> t: values.getTuples()) {
            List<DBSPExpression> exprs = new ArrayList<>();
            for (RexLiteral rl : t) {
                DBSPExpression expr = this.expressionCompiler.compile(rl);
                exprs.add(expr);
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(t, exprs, type);
            result.add(expression);
        }
        assert this.logicalValueTranslation == null;
        this.logicalValueTranslation = result;
    }

    @Override public void visit(
            RelNode node, int ordinal,
            @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
        stack.add(new Context(parent, ordinal));
        if (debug)
            System.out.println("Visiting " + node);
        // First process children
        super.visit(node, ordinal, parent);
        // Synthesize current node
        boolean success =
                this.visitIfMatches(node, LogicalTableScan.class, this::visitScan) ||
                this.visitIfMatches(node, LogicalProject.class, this::visitProject) ||
                this.visitIfMatches(node, LogicalUnion.class, this::visitUnion) ||
                this.visitIfMatches(node, LogicalMinus.class, this::visitMinus) ||
                this.visitIfMatches(node, LogicalFilter.class, this::visitFilter) ||
                this.visitIfMatches(node, LogicalValues.class, this::visitLogicalValues);
        if (!success)
            throw new Unimplemented(node);
        assert stack.size() > 0 : "Empty stack";
        stack.remove(stack.size() - 1);
    }

    public DBSPCircuit compile(CalciteProgram program) {
        for (TableDDL i: program.inputTables) {
            DBSPSourceOperator si = this.createInput(i);
            this.circuit.addOperator(si);
        }
        for (ViewDDL view: program.views) {
            DBSPSinkOperator o = this.createOutput(view);
            this.circuit.addOperator(o);
            RelNode rel = Objects.requireNonNull(view.compiled).rel;
            this.go(rel);
            // TODO: connect the result of the query compilation with
            // the fields of rel; for now we assume that these are 1/1
            DBSPOperator op = this.getOperator(rel);
            o.addInput(op);
        }
        return this.getProgram();
    }

    public void extendTransaction(DBSPTransaction transaction, UpdateStatment statement) {
        this.go(statement.rel);
        assert this.logicalValueTranslation != null;
        transaction.addSet(statement.table, this.logicalValueTranslation);
        this.logicalValueTranslation = null;
    }

    static DBSPType weightType = new DBSPTypeUser(null, "Weight", false);

    private DBSPSourceOperator createInput(TableDDL i) {
        List<DBSPType> fields = new ArrayList<>();
        for (ColumnInfo col: i.columns) {
            DBSPType ftype = this.convertType(col.type);
            fields.add(ftype);
        }
        DBSPTypeTuple type = new DBSPTypeTuple(i, fields);
        DBSPSourceOperator result = new DBSPSourceOperator(i, this.makeZSet(type), i.name);
        return Utilities.putNew(this.ioOperator, i.name, result);
    }

    private DBSPSinkOperator createOutput(ViewDDL v) {
        List<DBSPType> fields = new ArrayList<>();
        assert v.compiled != null;
        for (RelDataTypeField field: v.compiled.validatedRowType.getFieldList()) {
            DBSPType ftype = this.convertType(field.getType());
            fields.add(ftype);
        }
        DBSPTypeTuple type = new DBSPTypeTuple(v, fields);
        DBSPSinkOperator result = new DBSPSinkOperator(v, this.makeZSet(type), v.name);
        return Utilities.putNew(this.ioOperator, v.name, result);
    }
}
