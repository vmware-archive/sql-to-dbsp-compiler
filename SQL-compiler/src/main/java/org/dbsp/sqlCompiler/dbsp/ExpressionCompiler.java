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
        return new DBSPLiteralExpression(literal, type, literal.getValue().toString());
    }

    @Override
    public DBSPExpression visitCall(RexCall call) {
        List<DBSPExpression> ops = Linq.map(call.operands, e -> e.accept(this));
        DBSPType type = this.typeCompiler.convertType(call.getType());
        switch (call.op.kind) {
            case TIMES:
                return new DBSPBinaryExpression(call, type, "*", ops.toArray(new DBSPExpression[2]));
            case DIVIDE:
                return new DBSPBinaryExpression(call, type, "/", ops.toArray(new DBSPExpression[2]));
            case MOD:
                return new DBSPBinaryExpression(call, type, "%", ops.toArray(new DBSPExpression[2]));
            case PLUS:
                return new DBSPBinaryExpression(call, type, "+", ops.toArray(new DBSPExpression[2]));
            case MINUS:
                return new DBSPBinaryExpression(call, type, "-", ops.toArray(new DBSPExpression[2]));
            case LESS_THAN:
                return new DBSPBinaryExpression(call, type, "<", ops.toArray(new DBSPExpression[2]));
            case GREATER_THAN:
                return new DBSPBinaryExpression(call, type, ">", ops.toArray(new DBSPExpression[2]));
            case LESS_THAN_OR_EQUAL:
                return new DBSPBinaryExpression(call, type, "<=", ops.toArray(new DBSPExpression[2]));
            case GREATER_THAN_OR_EQUAL:
                return new DBSPBinaryExpression(call, type, ">=", ops.toArray(new DBSPExpression[2]));
            case EQUALS:
                return new DBSPBinaryExpression(call, type, "==", ops.toArray(new DBSPExpression[2]));
            case NOT_EQUALS:
                return new DBSPBinaryExpression(call, type, "!=", ops.toArray(new DBSPExpression[2]));
            case OR:
                return new DBSPBinaryExpression(call, type, "||", ops.toArray(new DBSPExpression[2]));
            case AND:
                return new DBSPBinaryExpression(call, type, "&&", ops.toArray(new DBSPExpression[2]));
            case DOT:
                return new DBSPBinaryExpression(call, type, ".", ops.toArray(new DBSPExpression[2]));
            case NOT:
            case IS_FALSE:
            case IS_NOT_TRUE:
                return new DBSPUnaryExpression(call, type, "!", ops.toArray(new DBSPExpression[1]));
            case PLUS_PREFIX:
                return new DBSPUnaryExpression(call, type, "+", ops.toArray(new DBSPExpression[1]));
            case MINUS_PREFIX:
                return new DBSPUnaryExpression(call, type, "-", ops.toArray(new DBSPExpression[1]));
            case IS_TRUE:
            case IS_NOT_FALSE:
                assert ops.size() == 1 : "Expected 1 operand " + ops;
                return ops.get(0);
            case BIT_AND:
                return new DBSPBinaryExpression(call, type, "&", ops.toArray(new DBSPExpression[2]));
            case BIT_OR:
                return new DBSPBinaryExpression(call, type, "|", ops.toArray(new DBSPExpression[2]));
            case BIT_XOR:
                return new DBSPBinaryExpression(call, type, "^", ops.toArray(new DBSPExpression[2]));
            case IS_NULL:
            case IS_NOT_NULL:
            case CAST:
            case FLOOR:
            case CEIL:default:
                throw new Unimplemented(call);
        }
    }

    DBSPExpression compile(RexNode expression) {
        DBSPExpression compile = expression.accept(this);
        return new DBSPClosureExpression(expression, compile.getType(), compile);
    }
}
