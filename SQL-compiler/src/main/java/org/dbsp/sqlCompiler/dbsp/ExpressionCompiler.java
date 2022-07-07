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
import org.dbsp.sqlCompiler.dbsp.circuit.expression.*;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.dbsp.circuit.type.IIsFloat;
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

    private static DBSPType castType(String operation, DBSPType left, DBSPType right) {
        if (left.same(right))
            return left;
        DBSPTypeInteger li = left.as(DBSPTypeInteger.class);
        DBSPTypeInteger ri = right.as(DBSPTypeInteger.class);
        IIsFloat lf = left.as(IIsFloat.class);
        IIsFloat rf = right.as(IIsFloat.class);
        if (li != null) {
            if (ri != null) {
                return new DBSPTypeInteger(null, Math.max(li.getWidth(), ri.getWidth()), left.mayBeNull || right.mayBeNull);
            }
            if (rf != null) {
                return right;
            }
        }
        if (lf != null) {
            if (ri != null)
                return left;
            if (rf != null) {
                if (lf.getWidth() < rf.getWidth())
                    return right;
                else
                    return left;
            }
        }
        throw new Unimplemented();
    }

    private static DBSPExpression makeBinaryExpression(
            RexNode node, DBSPType resultType, String op, List<DBSPExpression> operands) {
        // Why doesn't Calcite do this?
        assert operands.size() == 2;
        DBSPExpression left = operands.get(0);
        DBSPType leftType = left.getType();
        DBSPExpression right = operands.get(1);
        DBSPType rightType = right.getType();
        DBSPType common = castType(op, leftType, rightType);
        if (!leftType.same(common))
            left = new DBSPCastExpression(node, common, left);
        if (!rightType.same(common))
            right = new DBSPCastExpression(node, common, right);
        return new DBSPBinaryExpression(node, resultType, op, left, right);
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
