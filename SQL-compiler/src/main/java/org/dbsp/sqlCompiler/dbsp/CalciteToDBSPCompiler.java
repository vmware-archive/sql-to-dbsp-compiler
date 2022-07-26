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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.*;
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
    boolean debug = true;
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

    public void visitAggregate(LogicalAggregate aggregate) {
        DBSPType type = this.convertType(aggregate.getRowType());
        RelNode input = aggregate.getInput();
        DBSPOperator opinput = this.getOperator(input);
        if (!aggregate.getAggCallList().isEmpty())
            throw new Unimplemented(aggregate);
        if (aggregate.containsDistinctCall() || aggregate.getAggCallList().isEmpty()) {
            DBSPOperator dist = new DBSPDistinctOperator(aggregate, type, opinput);
            this.assignOperator(aggregate, dist);
        } else {
            throw new Unimplemented();
        }
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
        // LogicalProject is not really SQL project, it is rather map.
        RelNode input = project.getInput();
        DBSPOperator opinput = this.getOperator(input);
        DBSPType type = this.convertType(project.getRowType());
        List<DBSPExpression> resultColumns = new ArrayList<>();
        for (RexNode column : project.getProjects()) {
            DBSPExpression exp = this.expressionCompiler.compile(column);
            resultColumns.add(exp);
        }
        DBSPExpression exp = new DBSPTupleExpression(project, type, resultColumns);
        DBSPExpression closure = new DBSPClosureExpression(project, "t", type, exp);
        DBSPMapOperator op = new DBSPMapOperator(project, closure, type, opinput);
        // No distinct needed - in SQL project may produce a multiset.
        this.assignOperator(project, op);
    }

    private void visitUnion(LogicalUnion union) {
        DBSPType type = this.convertType(union.getRowType());
        List<DBSPOperator> inputs = Linq.map(union.getInputs(), this::getOperator);
        DBSPSumOperator sum = new DBSPSumOperator(union, type, inputs);
        if (union.all) {
            this.assignOperator(union, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(union, type, sum);
            this.assignOperator(union, d);
        }
    }

    private void visitMinus(LogicalMinus minus) {
        DBSPType type = this.convertType(minus.getRowType());
        boolean first = true;
        List<DBSPOperator> inputs = new ArrayList<>();
        for (RelNode input : minus.getInputs()) {
            DBSPOperator opin = this.getOperator(input);
            if (!first) {
                DBSPNegateOperator neg = new DBSPNegateOperator(minus, type, opin);
                this.circuit.addOperator(neg);
                inputs.add(neg);
            } else {
                inputs.add(opin);
            }
            first = false;
        }

        DBSPSumOperator sum = new DBSPSumOperator(minus, type, inputs);
        if (minus.all) {
            this.assignOperator(minus, sum);
        } else {
            this.circuit.addOperator(sum);
            DBSPDistinctOperator d = new DBSPDistinctOperator(minus, type, sum);
            this.assignOperator(minus, d);
        }
    }

    public void visitFilter(LogicalFilter filter) {
        DBSPType type = this.convertType(filter.getRowType());
        DBSPExpression condition = this.expressionCompiler.compile(filter.getCondition());
        if (condition.getType().mayBeNull) {
            condition = new DBSPApplyExpression("wrap_bool", condition.getType(), condition);
        }
        condition = new DBSPClosureExpression(filter.getCondition(), "t", condition.getType(), condition);
        DBSPOperator input = this.getOperator(filter.getInput());
        DBSPFilterOperator fop = new DBSPFilterOperator(filter, condition, type, input);
        this.assignOperator(filter, fop);
    }

    private void visitJoin(LogicalJoin join) {
        DBSPType type = this.convertType(join.getRowType());
        if (join.getInputs().size() != 2)
            throw new TranslationException("Unexpected join with " + join.getInputs().size() + " inputs", join);
        DBSPOperator left = this.getOperator(join.getInput(0));
        DBSPOperator right = this.getOperator(join.getInput(1));
        DBSPType leftElementType = left.getType().to(DBSPTypeZSet.class).elementType;
        DBSPType rightElementType = right.getType().to(DBSPTypeZSet.class).elementType;

        DBSPExpression toEmptyLeft = new DBSPClosureExpression(
                null, "t", leftElementType,
                new DBSPTupleExpression(DBSPTupleExpression.emptyTuple,
                        new DBSPVariableReference("t", leftElementType)));
        DBSPIndexOperator lindex = new DBSPIndexOperator(
                join, toEmptyLeft, DBSPTypeTuple.emptyTupleType, leftElementType, left);
        this.circuit.addOperator(lindex);

        DBSPExpression toEmptyRight = new DBSPClosureExpression(
                null, "t", leftElementType,
                new DBSPTupleExpression(DBSPTupleExpression.emptyTuple,
                        new DBSPVariableReference("t", leftElementType)));
        DBSPIndexOperator rindex = new DBSPIndexOperator(
                join, toEmptyRight, DBSPTypeTuple.emptyTupleType, rightElementType, right);
        this.circuit.addOperator(rindex);

        DBSPCartesianOperator cart = new DBSPCartesianOperator(join, type, lindex, rindex);
        this.circuit.addOperator(cart);
        DBSPExpression condition = this.expressionCompiler.compile(join.getCondition());
        if (condition.getType().mayBeNull) {
            condition = new DBSPApplyExpression("wrap_bool", condition.getType(), condition);
        }
        condition = new DBSPClosureExpression(join.getCondition(), "t", condition.getType(), condition);
        DBSPFilterOperator fop = new DBSPFilterOperator(join, condition, type, cart);
        this.assignOperator(join, fop);
    }

    /// Stores result for the next visitor
    @Nullable
    private DBSPZSetLiteral logicalValueTranslation = null;

    /**
     * Visit a logicalvalue: a SQL literal, as produced by a VALUES expression
     */
    public void visitLogicalValues(LogicalValues values) {
        DBSPType type = this.convertType(values.getRowType());
        DBSPZSetLiteral result = new DBSPZSetLiteral(new DBSPTypeZSet(null, type, DBSPTypeInteger.signed32));
        for (List<RexLiteral> t: values.getTuples()) {
            List<DBSPExpression> exprs = new ArrayList<>();
            for (RexLiteral rl : t) {
                DBSPExpression expr = this.expressionCompiler.compile(rl);
                exprs.add(expr);
            }
            DBSPTupleExpression expression = new DBSPTupleExpression(t, type, exprs);
            result.add(expression);
        }
        if (this.logicalValueTranslation != null)
            throw new RuntimeException("Overwriting logical value translation");
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
                this.visitIfMatches(node, LogicalValues.class, this::visitLogicalValues) ||
                this.visitIfMatches(node, LogicalAggregate.class, this::visitAggregate) ||
                this.visitIfMatches(node, LogicalJoin.class, this::visitJoin);
        if (!success)
            throw new Unimplemented(node);
        if (stack.size() == 0)
            throw new TranslationException("Empty stack", node);
        stack.remove(stack.size() - 1);
    }

    public DBSPCircuit compile(CalciteProgram program) {
        for (TableDDL i: program.inputTables) {
            DBSPSourceOperator si = this.createInput(i);
            this.circuit.addOperator(si);
        }
        for (ViewDDL view: program.views) {
            RelNode rel = Objects.requireNonNull(view.compiled).rel;
            this.go(rel);
            // TODO: connect the result of the query compilation with
            // the fields of rel; for now we assume that these are 1/1
            DBSPOperator op = this.getOperator(rel);
            DBSPSinkOperator o = this.createOutput(view, op);
            this.circuit.addOperator(o);
        }
        return this.getProgram();
    }

    /**
     * Translate the specified statement and add to the given transaction.
     * @param transaction  A transaction that will contain the translation of 'statement'
     * @param statement A statement that updates a table.
     */
    public void extendTransaction(DBSPTransaction transaction, TableModifyStatement statement) {
        this.go(statement.rel);
        if (this.logicalValueTranslation == null)
            throw new TranslationException("Could not compile ", statement.rel);
        transaction.addSet(statement.table, this.logicalValueTranslation);
        this.logicalValueTranslation = null;
    }

    public static final DBSPType weightType = new DBSPTypeUser(null, "Weight", false);

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

    private DBSPSinkOperator createOutput(ViewDDL v, DBSPOperator source) {
        List<DBSPType> fields = new ArrayList<>();
        if (v.compiled == null)
            throw new TranslationException("Could not compile ", v.getNode());
        for (RelDataTypeField field: v.compiled.validatedRowType.getFieldList()) {
            DBSPType ftype = this.convertType(field.getType());
            fields.add(ftype);
        }
        DBSPTypeTuple type = new DBSPTypeTuple(v, fields);
        DBSPSinkOperator result = new DBSPSinkOperator(v, this.makeZSet(type), v.name, source);
        return Utilities.putNew(this.ioOperator, v.name, result);
    }
}
