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

package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPPathSegment;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.pattern.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Depth-first traversal of an DBSPNode hierarchy.
 */
@SuppressWarnings("SameReturnValue")
public abstract class Visitor {
    public final List<IDBSPNode> context;

    /// If true each visit call will visit by default the superclass.
    final boolean visitSuper;

    public Visitor(boolean visitSuper) {
        this.visitSuper = visitSuper;
        this.context = new ArrayList<>();
    }

    /************************* PREORDER *****************************/

    // preorder methods return 'true' when normal traversal is desired,
    // and 'false' when the traversal should stop right away at the current node.
    // base classes
    public boolean preorder(IDBSPNode ignored) {
        return true;
    }

    public boolean preorder(DBSPNode ignored) {
        return true;
    }

    public boolean preorder(DBSPExpression node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPStatement node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPCircuit node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPTypeStruct.Field node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPType node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPFile node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPOperator node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPFunction node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPPath node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPPattern node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPFunction.Argument node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }
    
    public boolean preorder(DBSPClosureExpression.Parameter node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }
    
    // Statements
    
    public boolean preorder(DBSPExpressionStatement node) {
        if (this.visitSuper) return this.preorder((DBSPStatement) node);
        else return true;
    }

    public boolean preorder(DBSPLetStatement node) {
        if (this.visitSuper) return this.preorder((DBSPStatement) node);
        else return true;
    }

    // Various
    
    public boolean preorder(DBSPMatchExpression.Case node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPPathSegment node) {
        if (this.visitSuper) return this.preorder((DBSPNode) node);
        else return true;
    }

    public boolean preorder(DBSPSimplePathSegment node) {
        if (this.visitSuper) return this.preorder((DBSPPathSegment) node);
        else return true;
    }

    // Types
    
    public boolean preorder(DBSPTypeFP node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeFloat node) {
        if (this.visitSuper) return this.preorder((DBSPTypeFP) node);
        else return true;
    }

    public boolean preorder(DBSPTypeDouble node) {
        if (this.visitSuper) return this.preorder((DBSPTypeFP) node);
        else return true;
    }

    public boolean preorder(DBSPTypeISize node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeStruct node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeString node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }
    
    public boolean preorder(DBSPTypeUSize node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeTuple node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeRawTuple node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeInteger node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeNull node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeFunction node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeBool node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeStream node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }
    
    public boolean preorder(DBSPTypeUser node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPTypeIndexedZSet node) {
        if (this.visitSuper) return this.preorder((DBSPTypeUser) node);
        else return true;
    }

    public boolean preorder(DBSPTypeZSet node) {
        if (this.visitSuper) return this.preorder((DBSPTypeUser) node);
        else return true;
    }

    public boolean preorder(DBSPTypeVec node) {
        if (this.visitSuper) return this.preorder((DBSPTypeUser) node);
        else return true;
    }
    
    public boolean preorder(DBSPTypeAny node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }
    
    public boolean preorder(DBSPTypeRef node) {
        if (this.visitSuper) return this.preorder((DBSPType) node);
        else return true;
    }

    public boolean preorder(DBSPUnaryOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    /// Operators
    public boolean preorder(DBSPIndexOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }
    
    public boolean preorder(DBSPSubtractOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSumOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPJoinOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPAggregateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPConstantOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPDifferentialOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPIntegralOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPNegateOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPFlatMapOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPFilterOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPDistinctOperator node) {
        if (this.visitSuper) return this.preorder((DBSPUnaryOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSinkOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }

    public boolean preorder(DBSPSourceOperator node) {
        if (this.visitSuper) return this.preorder((DBSPOperator) node);
        else return true;
    }
    
    // Patterns
    public boolean preorder(DBSPTupleStructPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPTuplePattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPRefPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPWildcardPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPLiteralPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    public boolean preorder(DBSPIdentifierPattern node) {
        if (this.visitSuper) return this.preorder((DBSPPattern) node);
        else return true;
    }

    // Expressions

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean preorder(DBSPStructExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPBorrowExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPClosureExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPQualifyTypeExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPMatchExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPBinaryExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPEnumValue node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPDerefExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPApplyMethodExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPPathExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPUnaryExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPTupleExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPRawTupleExpression node) {
        // We treat expression as the superclass
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPFieldExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPIfExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPBlockExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPApplyExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPAssignmentExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPAsExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPVariableReference node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }

    public boolean preorder(DBSPRangeExpression node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }
    
    // Literals
    public boolean preorder(DBSPLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPExpression) node);
        else return true;
    }
    
    public boolean preorder(DBSPVecLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }
    
    public boolean preorder(DBSPFloatLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPUSizeLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPZSetLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPStringLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPIntegerLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPLongLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPBoolLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPDoubleLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    public boolean preorder(DBSPISizeLiteral node) {
        if (this.visitSuper) return this.preorder((DBSPLiteral) node);
        else return true;
    }

    /*************************** POSTORDER *****************************/

    @SuppressWarnings("EmptyMethod")
    public void postorder(IDBSPNode ignored) {}

    @SuppressWarnings("EmptyMethod")
    public void postorder(DBSPNode ignored) {}

    public void postorder(DBSPExpression node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPStatement node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPCircuit node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPType node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPFile node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPOperator node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPFunction node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPPath node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPPattern node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPFunction.Argument node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPClosureExpression.Parameter node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    // Statements

    public void postorder(DBSPExpressionStatement node) {
        if (this.visitSuper) this.postorder((DBSPStatement) node);
    }

    public void postorder(DBSPLetStatement node) {
        if (this.visitSuper) this.postorder((DBSPStatement) node);
    }

    // Various

    public void postorder(DBSPMatchExpression.Case node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPPathSegment node) {
        if (this.visitSuper) this.postorder((DBSPNode) node);
    }

    public void postorder(DBSPSimplePathSegment node) {
        if (this.visitSuper) this.postorder((DBSPPathSegment) node);
    }

    // Types

    public void postorder(DBSPTypeFP node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeFloat node) {
        if (this.visitSuper) this.postorder((DBSPTypeFP) node);
    }

    public void postorder(DBSPTypeDouble node) {
        if (this.visitSuper) this.postorder((DBSPTypeFP) node);
    }

    public void postorder(DBSPTypeISize node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeStruct node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeString node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeUSize node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeTuple node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeRawTuple node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeInteger node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeNull node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeFunction node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeBool node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeStream node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeUser node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeIndexedZSet node) {
        if (this.visitSuper) this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeZSet node) {
        if (this.visitSuper) this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeVec node) {
        if (this.visitSuper) this.postorder((DBSPTypeUser) node);
    }

    public void postorder(DBSPTypeAny node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    public void postorder(DBSPTypeRef node) {
        if (this.visitSuper) this.postorder((DBSPType) node);
    }

    /// Operators
    public void postorder(DBSPIndexOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPSubtractOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPSumOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPJoinOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPAggregateOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPConstantOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPMapOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPDifferentialOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPIntegralOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPNegateOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPFlatMapOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPFilterOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPDistinctOperator node) {
        if (this.visitSuper) this.postorder((DBSPUnaryOperator) node);
    }

    public void postorder(DBSPSinkOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    public void postorder(DBSPSourceOperator node) {
        if (this.visitSuper) this.postorder((DBSPOperator) node);
    }

    // Patterns
    public void postorder(DBSPTupleStructPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPTuplePattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPRefPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPWildcardPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPLiteralPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    public void postorder(DBSPIdentifierPattern node) {
        if (this.visitSuper) this.postorder((DBSPPattern) node);
    }

    // Expressions

    public void postorder(DBSPStructExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBorrowExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPClosureExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPQualifyTypeExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPMatchExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBinaryExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPEnumValue node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPDerefExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPApplyMethodExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPPathExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPUnaryExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPTupleExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPRawTupleExpression node) {
        // We treat expression as the superclass
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPFieldExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPIfExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPBlockExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPApplyExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPAssignmentExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPAsExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPVariableReference node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPRangeExpression node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    // Literals
    public void postorder(DBSPLiteral node) {
        if (this.visitSuper) this.postorder((DBSPExpression) node);
    }

    public void postorder(DBSPVecLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPFloatLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPUSizeLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPZSetLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPStringLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPIntegerLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPLongLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPBoolLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPDoubleLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }

    public void postorder(DBSPISizeLiteral node) {
        if (this.visitSuper) this.postorder((DBSPLiteral) node);
    }
}
