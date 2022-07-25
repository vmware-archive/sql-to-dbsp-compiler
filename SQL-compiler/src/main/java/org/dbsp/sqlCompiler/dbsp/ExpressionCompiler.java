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
        return new DBSPFieldExpression(inputRef, inputRef.getIndex(), type);
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
     * @param operation  Sql operation string.
     * @param left       Left operand type.
     * @param right      Right operand type.
     * @return           Common type operands must be cast to.
     */
    public static DBSPType reduceType(String operation, DBSPType left, DBSPType right) {
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
            RexNode node, DBSPType resultType, String op, List<DBSPExpression> operands) {
        // Why doesn't Calcite do this?
        assert operands.size() == 2;
        DBSPExpression left = operands.get(0);
        DBSPType leftType = left.getType();
        DBSPExpression right = operands.get(1);
        DBSPType rightType = right.getType();
        DBSPType commonBase = reduceType(op, leftType, rightType);
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
                return makeBinaryExpression(call, type, "*", ops);
            case DIVIDE:
                return makeBinaryExpression(call, type, "/", ops);
            case MOD:
                return makeBinaryExpression(call, type, "%", ops);
            case PLUS:
                return makeBinaryExpression(call, type, "+", ops);
            case MINUS:
                return makeBinaryExpression(call, type, "-", ops);
            case LESS_THAN:
                return makeBinaryExpression(call, type, "<", ops);
            case GREATER_THAN:
                return makeBinaryExpression(call, type, ">", ops);
            case LESS_THAN_OR_EQUAL:
                return makeBinaryExpression(call, type, "<=", ops);
            case GREATER_THAN_OR_EQUAL:
                return makeBinaryExpression(call, type, ">=", ops);
            case EQUALS:
                return makeBinaryExpression(call, type, "==", ops);
            case NOT_EQUALS:
                return makeBinaryExpression(call, type, "!=", ops);
            case OR:
                return makeBinaryExpression(call, type, "||", ops);
            case AND:
                return makeBinaryExpression(call, type, "&&", ops);
            case DOT:
                return makeBinaryExpression(call, type, ".", ops);
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
                assert ops.size() == 1 : "Expected 1 operand " + ops;
                return ops.get(0);
            case BIT_AND:
                return makeBinaryExpression(call, type, "&", ops);
            case BIT_OR:
                return makeBinaryExpression(call, type, "|", ops);
            case BIT_XOR:
                return makeBinaryExpression(call, type, "^", ops);
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
