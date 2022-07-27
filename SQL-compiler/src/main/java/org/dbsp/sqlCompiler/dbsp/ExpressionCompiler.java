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

import org.apache.calcite.rex.*;
import org.dbsp.sqlCompiler.dbsp.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.*;
import org.dbsp.sqlCompiler.dbsp.circuit.type.*;
import org.dbsp.util.Linq;
import org.dbsp.util.TranslationException;
import org.dbsp.util.Unimplemented;

import java.util.List;

public class ExpressionCompiler extends RexVisitorImpl<DBSPExpression> {
    private final TypeCompiler typeCompiler = new TypeCompiler();
    public ExpressionCompiler(boolean deep) {
        super(deep);
    }

    @Override
    public DBSPExpression visitInputRef(RexInputRef inputRef) {
        DBSPType type = this.typeCompiler.convertType(inputRef.getType());
        return new DBSPFieldExpression(
                inputRef, new DBSPVariableReference("t", DBSPTypeAny.instance),
                inputRef.getIndex(), type);
    }

    @Override
    public DBSPExpression visitLiteral(RexLiteral literal) {
        DBSPType type = this.typeCompiler.convertType(literal.getType());
        return new DBSPLiteral(literal, type, literal.toString());
    }

    /**
     * Given operands for "operation" with left and right types,
     * compute the type that both operands must be cast to.
     * Note: this ignores nullability of types.
     * @param left       Left operand type.
     * @param right      Right operand type.
     * @return           Common type operands must be cast to.
     */
    public static DBSPType reduceType(DBSPType left, DBSPType right) {
        left = left.setMayBeNull(false);
        right = right.setMayBeNull(false);
        if (left.same(right))
            return left;

        DBSPTypeInteger li = left.as(DBSPTypeInteger.class);
        DBSPTypeInteger ri = right.as(DBSPTypeInteger.class);
        DBSPTypeFP lf = left.as(DBSPTypeFP.class);
        DBSPTypeFP rf = right.as(DBSPTypeFP.class);
        if (li != null) {
            if (ri != null) {
                return new DBSPTypeInteger(null, Math.max(li.getWidth(), ri.getWidth()), false);
            }
            if (rf != null) {
                return right.setMayBeNull(false);
            }
        }
        if (lf != null) {
            if (ri != null)
                return left.setMayBeNull(false);
            if (rf != null) {
                if (lf.getWidth() < rf.getWidth())
                    return right.setMayBeNull(false);
                else
                    return left.setMayBeNull(false);
            }
        }
        throw new Unimplemented("Cast from " + right + " to " + left);
    }

    private static DBSPExpression makeBinaryExpression(
            RexNode node, String op, List<DBSPExpression> operands) {
        // Why doesn't Calcite do this?
        if (operands.size() != 2)
            throw new TranslationException("Expected 2 operands", node);
        DBSPExpression left = operands.get(0);
        DBSPExpression right = operands.get(1);
        if (left == null || right == null)
            throw new Unimplemented("Found unimplemented expression in " + node);
        DBSPType leftType = left.getType();
        DBSPType rightType = right.getType();
        DBSPType commonBase = reduceType(leftType, rightType);
        if (!leftType.setMayBeNull(false).same(commonBase))
            left = new DBSPCastExpression(node, commonBase.setMayBeNull(leftType.mayBeNull), left);
        if (!rightType.setMayBeNull(false).same(commonBase))
            right = new DBSPCastExpression(node, commonBase.setMayBeNull(rightType.mayBeNull), right);
        SqlRuntimeLibrary.FunctionDescription function = SqlRuntimeLibrary.instance.getFunction(
                op, commonBase.setMayBeNull(leftType.mayBeNull), commonBase.setMayBeNull(rightType.mayBeNull));
        return new DBSPApplyExpression(function.function, function.returnType, left, right);
    }

    @Override
    public DBSPExpression visitCall(RexCall call) {
        List<DBSPExpression> ops = Linq.map(call.operands, e -> e.accept(this));
        DBSPType type = this.typeCompiler.convertType(call.getType());
        switch (call.op.kind) {
            case TIMES:
                return makeBinaryExpression(call, "*", ops);
            case DIVIDE:
                return makeBinaryExpression(call, "/", ops);
            case MOD:
                return makeBinaryExpression(call, "%", ops);
            case PLUS:
                return makeBinaryExpression(call, "+", ops);
            case MINUS:
                return makeBinaryExpression(call, "-", ops);
            case LESS_THAN:
                return makeBinaryExpression(call, "<", ops);
            case GREATER_THAN:
                return makeBinaryExpression(call, ">", ops);
            case LESS_THAN_OR_EQUAL:
                return makeBinaryExpression(call, "<=", ops);
            case GREATER_THAN_OR_EQUAL:
                return makeBinaryExpression(call, ">=", ops);
            case EQUALS:
                return makeBinaryExpression(call, "==", ops);
            case NOT_EQUALS:
                return makeBinaryExpression(call, "!=", ops);
            case OR:
                return makeBinaryExpression(call, "||", ops);
            case AND:
                return makeBinaryExpression(call, "&&", ops);
            case DOT:
                return makeBinaryExpression(call, ".", ops);
            case NOT:
            case IS_FALSE:
            case IS_NOT_TRUE:
                return new DBSPUnaryExpression(call, type, "!", ops.get(0));
            case PLUS_PREFIX:
                return new DBSPUnaryExpression(call, type, "+", ops.get(0));
            case MINUS_PREFIX:
                return new DBSPUnaryExpression(call, type, "-", ops.get(0));
            case IS_TRUE:
            case IS_NOT_FALSE:
                if (ops.size() != 1)
                    throw new TranslationException("Expected 1 operand", call);
                return ops.get(0);
            case BIT_AND:
                return makeBinaryExpression(call, "&", ops);
            case BIT_OR:
                return makeBinaryExpression(call, "|", ops);
            case BIT_XOR:
                return makeBinaryExpression(call, "^", ops);
            case CAST:
                return new DBSPCastExpression(call, type, ops.get(0));
            case IS_NULL:
            case IS_NOT_NULL:
            case FLOOR:
            case CEIL:default:
                throw new Unimplemented(call);
        }
    }

    DBSPExpression compile(RexNode expression) {
        return expression.accept(this);
    }
}
