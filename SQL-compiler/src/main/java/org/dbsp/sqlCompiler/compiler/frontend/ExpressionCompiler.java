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

package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.circuit.SqlRuntimeLibrary;
import org.dbsp.sqlCompiler.compiler.sqlparser.CalciteCompiler;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public class ExpressionCompiler extends RexVisitorImpl<DBSPExpression> implements IModule {
    /**
     * Identity function.
     */
    private final TypeCompiler typeCompiler = new TypeCompiler();
    @Nullable
    public final DBSPVariablePath inputRow;
    private final RexBuilder rexBuilder;
    private final List<RexLiteral> constants;

    public ExpressionCompiler(@Nullable DBSPVariablePath inputRow, CalciteCompiler calciteCompiler) {
        this(inputRow, Linq.list(), calciteCompiler);
    }

    /**
     * Create a compiler that will translate expressions pertaining to a row.
     * @param inputRow         Variable representing the row being compiled.
     * @param constants        Additional constants.  Expressions compiled
     *                         may use RexInputRef, which are field references
     *                         within the row.  Calcite seems to number constants
     *                         as additional fields within the row, after the end of
     *                         the input row.
     * @param calciteCompiler  Handle to the Calcite compiler, in case we need
     *                         to do some more preprocessing of the expressions.
     */
    public ExpressionCompiler(@Nullable DBSPVariablePath inputRow,
                              List<RexLiteral> constants,
                              CalciteCompiler calciteCompiler) {
        super(true);
        this.inputRow = inputRow;
        this.constants = constants;
        this.rexBuilder = calciteCompiler.getRexBuilder();
        if (inputRow != null &&
                !inputRow.getNonVoidType().is(DBSPTypeRef.class))
            throw new TranslationException("Expected a reference type for row", inputRow.getNode());
    }

    /**
     * Convert an expression that refers to a field in the input row.
     * @param inputRef   index in the input row.
     * @return           the corresponding DBSP expression.
     */
    @Override
    public DBSPExpression visitInputRef(RexInputRef inputRef) {
        // DBSPType type = this.typeCompiler.convertType(inputRef.getType());
        // Unfortunately it looks like we can't trust the type coming from Calcite.
        if (this.inputRow == null)
            throw new RuntimeException("Row referenced without a row context");
        DBSPTypeTuple type = this.inputRow.getNonVoidType().deref().to(DBSPTypeTuple.class);
        int index = inputRef.getIndex();
        if (index < type.size())
            return new DBSPFieldExpression(
                    inputRef, this.inputRow,
                    inputRef.getIndex());
        if (index - type.size() < this.constants.size())
            return this.visitLiteral(this.constants.get(index - type.size()));
        throw new TranslationException("Index in row out of bounds ", inputRef);
    }

    @Override
    public DBSPExpression visitLiteral(RexLiteral literal) {
        DBSPType type = this.typeCompiler.convertType(literal.getType());
        if (literal.isNull())
            return DBSPLiteral.none(type);
        if (type.is(DBSPTypeInteger.class)) {
            DBSPTypeInteger itype = type.to(DBSPTypeInteger.class);
            switch (itype.getWidth()) {
                case 16:
                    return new DBSPI32Literal(Objects.requireNonNull(literal.getValueAs(Short.class)));
                case 32:
                    return new DBSPI32Literal(Objects.requireNonNull(literal.getValueAs(Integer.class)));
                case 64:
                    return new DBSPI64Literal(Objects.requireNonNull(literal.getValueAs(Long.class)));
                default:
                    throw new UnsupportedOperationException("Unsupported integer width type " + itype.getWidth());
            }
        }
        else if (type.is(DBSPTypeDouble.class))
            return new DBSPDoubleLiteral(Objects.requireNonNull(literal.getValueAs(Double.class)));
        else if (type.is(DBSPTypeFloat.class))
            return new DBSPFloatLiteral(Objects.requireNonNull(literal.getValueAs(Float.class)));
        else if (type.is(DBSPTypeString.class))
            return new DBSPStringLiteral(Objects.requireNonNull(literal.getValueAs(String.class)));
        else if (type.is(DBSPTypeBool.class))
            return new DBSPBoolLiteral(Objects.requireNonNull(literal.getValueAs(Boolean.class)));
        else if (type.is(DBSPTypeDecimal.class))
            return new DBSPDecimalLiteral(
                    literal, type, Objects.requireNonNull(literal.getValueAs(BigDecimal.class)));
        else if (type.is(DBSPTypeKeyword.class))
            return new DBSPKeywordLiteral(literal, Objects.requireNonNull(literal.getValue()).toString());
        else if (type.is(DBSPTypeMillisInterval.class))
            return new DBSPIntervalMillisLiteral(literal, type, Objects.requireNonNull(
                    literal.getValueAs(BigDecimal.class)).longValue());
        else if (type.is(DBSPTypeTimestamp.class)) {
            return new DBSPTimestampLiteral(literal, type,
                    Objects.requireNonNull(literal.getValueAs(TimestampString.class)));
        } else if (type.is(DBSPTypeDate.class)) {
            return new DBSPDateLiteral(literal, type, Objects.requireNonNull(literal.getValueAs(DateString.class)));
        } else if (type.is(DBSPTypeGeoPoint.class)) {
            Point point = literal.getValueAs(Point.class);
            Coordinate c = Objects.requireNonNull(point).getCoordinate();
            return new DBSPGeoPointLiteral(literal,
                    new DBSPDoubleLiteral(c.getOrdinate(0)),
                    new DBSPDoubleLiteral(c.getOrdinate(1)));
        }
        throw new Unimplemented(literal);
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
        if (left.is(DBSPTypeNull.class))
            return right.setMayBeNull(true);
        if (right.is(DBSPTypeNull.class))
            return left.setMayBeNull(true);
        left = left.setMayBeNull(false);
        right = right.setMayBeNull(false);
        if (left.sameType(right))
            return left;

        DBSPTypeInteger li = left.as(DBSPTypeInteger.class);
        DBSPTypeInteger ri = right.as(DBSPTypeInteger.class);
        DBSPTypeDecimal ld = left.as(DBSPTypeDecimal.class);
        DBSPTypeDecimal rd = right.as(DBSPTypeDecimal.class);
        DBSPTypeFP lf = left.as(DBSPTypeFP.class);
        DBSPTypeFP rf = right.as(DBSPTypeFP.class);
        if (li != null) {
            if (ri != null)
                return new DBSPTypeInteger(null, Math.max(li.getWidth(), ri.getWidth()), true,false);
            if (rf != null || rd != null)
                return right.setMayBeNull(false);
        }
        if (lf != null) {
            if (ri != null || rd != null)
                return left.setMayBeNull(false);
            if (rf != null) {
                if (lf.getWidth() < rf.getWidth())
                    return right.setMayBeNull(false);
                else
                    return left.setMayBeNull(false);
            }
        }
        if (ld != null) {
            if (ri != null)
                return left.setMayBeNull(false);
            if (rf != null)
                return right.setMayBeNull(false);
        }
        throw new Unimplemented("Cast from " + right + " to " + left);
    }

    public static DBSPExpression aggregateOperation(
            SqlOperator node, String op, DBSPType type, DBSPExpression left, DBSPExpression right) {
        DBSPType leftType = left.getNonVoidType();
        DBSPType rightType = right.getNonVoidType();
        DBSPType commonBase = reduceType(leftType, rightType);
        if (commonBase.is(DBSPTypeNull.class)) {
            return DBSPLiteral.none(type);
        }
        SqlRuntimeLibrary.FunctionDescription function = SqlRuntimeLibrary.instance.getFunction(
                op, type, commonBase.setMayBeNull(leftType.mayBeNull), commonBase.setMayBeNull(rightType.mayBeNull), true);
        DBSPExpression apply = new DBSPApplyExpression("agg_" + function.function, function.returnType, left, right);
        return ExpressionCompiler.makeCast(node, apply, type);
    }

    // Like makeBinaryExpression, but accepts multiple operands.
    private static DBSPExpression makeBinaryExpressions(
            Object node, DBSPType type, String op, List<DBSPExpression> operands) {
        if (operands.size() < 2)
            throw new Unimplemented(node);
        DBSPExpression accumulator = operands.get(0);
        for (int i = 1; i < operands.size(); i++)
            accumulator = makeBinaryExpression(node, type, op, Linq.list(accumulator, operands.get(i)));
        return makeCast(node, accumulator, type);
    }

    @SuppressWarnings("unused")
    public static boolean needCommonType(String op, DBSPType result, DBSPType left, DBSPType right) {
        return !left.is(IsDateType.class) && !right.is(IsDateType.class);
    }

    public static DBSPExpression makeBinaryExpression(
            Object node, DBSPType type, String op, List<DBSPExpression> operands) {
        // Why doesn't Calcite do this?
        if (operands.size() != 2)
            throw new TranslationException("Expected 2 operands, got " + operands.size(), node);
        DBSPExpression left = operands.get(0);
        DBSPExpression right = operands.get(1);
        if (left == null || right == null)
            throw new Unimplemented(node);
        DBSPType leftType = left.getNonVoidType();
        DBSPType rightType = right.getNonVoidType();

        if (needCommonType(op, type, leftType, rightType)) {
            DBSPType commonBase = reduceType(leftType, rightType);
            if (commonBase.is(DBSPTypeNull.class)) {
                // Result is always NULL.  Perhaps we should give a warning?
                return DBSPLiteral.none(type);
            }
            if (!leftType.setMayBeNull(false).sameType(commonBase))
                left = makeCast(node, left, commonBase.setMayBeNull(leftType.mayBeNull));
            if (!rightType.setMayBeNull(false).sameType(commonBase))
                right = makeCast(node, right, commonBase.setMayBeNull(rightType.mayBeNull));
        }
        // TODO: we don't need the whole function here, just the result type.
        SqlRuntimeLibrary.FunctionDescription function = SqlRuntimeLibrary.instance.getFunction(
                op, type, left.getNonVoidType(), right.getNonVoidType(), false);
        DBSPExpression call = new DBSPBinaryExpression(node, function.returnType, op, left, right);
        return makeCast(node, call, type);
    }

    public static DBSPExpression makeCast(@Nullable Object node, DBSPExpression from, DBSPType to) {
        DBSPType fromType = from.getNonVoidType();
        if (fromType.sameType(to)) {
            return from;
        }
        return new DBSPCastExpression(node, from, to);
    }

    public static DBSPExpression makeUnaryExpression(
            Object node, DBSPType type, String op, List<DBSPExpression> operands) {
        if (operands.size() != 1)
            throw new TranslationException("Expected 1 operands, got " + operands.size(), node);
        DBSPExpression operand = operands.get(0);
        if (operand == null)
            throw new Unimplemented("Found unimplemented expression in " + node);
        DBSPExpression expr = new DBSPUnaryExpression(node, operand.getNonVoidType(), op, operand);
        return makeCast(node, expr, type);
    }

    public static DBSPExpression wrapBoolIfNeeded(DBSPExpression expression) {
        DBSPType type = expression.getNonVoidType();
        if (type.mayBeNull) {
            return new DBSPApplyExpression(
                    "wrap_bool", type.setMayBeNull(false), expression);
        }
        return expression;
    }

    @Override
    public DBSPExpression visitCall(RexCall call) {
        Logger.instance.from(this, 2)
                .append(call.toString())
                .append(" ")
                .append(call.getType().toString());
        if (call.op.kind == SqlKind.SEARCH) {
            // TODO: ideally the optimizer should do this before handing the expression to us.
            // Then we can get rid of the rexBuilder field too.
            call = (RexCall)RexUtil.expandSearch(this.rexBuilder, null, call);
        }
        List<DBSPExpression> ops = Linq.map(call.operands, e -> e.accept(this));
        DBSPType type = this.typeCompiler.convertType(call.getType());
        switch (call.op.kind) {
            case TIMES:
                return makeBinaryExpression(call, type, "*", ops);
            case DIVIDE:
                // We enforce that the type of the result of division is always nullable
                type = type.setMayBeNull(true);
                return makeBinaryExpression(call, type, "/", ops);
            case MOD:
                return makeBinaryExpression(call, type, "%", ops);
            case PLUS:
                return makeBinaryExpressions(call, type, "+", ops);
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
            case IS_DISTINCT_FROM:
                return makeBinaryExpression(call, type, "is_distinct", ops);
            case IS_NOT_DISTINCT_FROM: {
                DBSPExpression op = makeBinaryExpression(call, type, "is_distinct", ops);
                return makeUnaryExpression(call, DBSPTypeBool.instance, "!", Linq.list(op));
            }
            case NOT_EQUALS:
                return makeBinaryExpression(call, type, "!=", ops);
            case OR:
                return makeBinaryExpressions(call, type, "||", ops);
            case AND:
                return makeBinaryExpressions(call, type, "&&", ops);
            case DOT:
                return makeBinaryExpression(call, type, ".", ops);
            case NOT:
                return makeUnaryExpression(call, type, "!", ops);
            case IS_FALSE:
            case IS_NOT_TRUE:
            case IS_TRUE:
            case IS_NOT_FALSE: {
                if (ops.size() != 1)
                    throw new TranslationException("Expected 1 operand", call);
                DBSPExpression arg = ops.get(0);
                String functionName;
                switch (call.op.kind) {
                    case IS_FALSE:
                        functionName = "is_false";
                        break;
                    case IS_TRUE:
                        functionName = "is_true";
                        break;
                    case IS_NOT_TRUE:
                        functionName = "is_not_true";
                        break;
                    case IS_NOT_FALSE:
                        functionName = "is_not_false";
                        break;
                    default:
                        throw new RuntimeException("Should not be reachable");
                }
                SqlRuntimeLibrary.FunctionDescription function =
                        SqlRuntimeLibrary.instance.getFunction(
                                functionName, type, arg.getNonVoidType(), null, false);
                return function.getCall(arg);
            }
            case PLUS_PREFIX:
                return makeUnaryExpression(call, type, "+", ops);
            case MINUS_PREFIX:
                return makeUnaryExpression(call, type, "-", ops);
            case BIT_AND:
                return makeBinaryExpressions(call, type, "&", ops);
            case BIT_OR:
                return makeBinaryExpressions(call, type, "|", ops);
            case BIT_XOR:
                return makeBinaryExpressions(call, type, "^", ops);
            case CAST:
            case REINTERPRET:
                return makeCast(call, ops.get(0), type);
            case IS_NULL:
            case IS_NOT_NULL: {
                if (!type.sameType(DBSPTypeBool.instance))
                    throw new TranslationException("Expected expression to produce a boolean result", call);
                DBSPExpression arg = ops.get(0);
                DBSPType argType = arg.getNonVoidType();
                if (argType.mayBeNull) {
                    if (call.op.kind == SqlKind.IS_NULL)
                        return new DBSPApplyExpression("is_null", type, ops.get(0));
                    else
                        return new DBSPUnaryExpression(call,
                                type,
                                "!",
                                new DBSPApplyExpression("is_null", type, ops.get(0)));
                } else {
                    if (call.op.kind == SqlKind.IS_NULL)
                        return new DBSPBoolLiteral(false);
                    else
                        return new DBSPBoolLiteral(true);
                }
            }
            case CASE: {
                /*
                A switched case (CASE x WHEN x1 THEN v1 ... ELSE e END)
                has an even number of arguments and odd-numbered arguments are predicates.
                A condition case (CASE WHEN p1 THEN v1 ... ELSE e END) has an odd number of
                arguments and even-numbered arguments are predicates, except for the last argument.
                */
                DBSPExpression result = ops.get(ops.size() - 1);
                if (ops.size() % 2 == 0) {
                    DBSPExpression value = ops.get(0);
                    // Compute casts if needed.
                    DBSPType finalType = result.getNonVoidType();
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        if (ops.get(i + 1).getNonVoidType().mayBeNull)
                            finalType = finalType.setMayBeNull(true);
                    }
                    if (!result.getNonVoidType().sameType(finalType))
                        result = makeCast(call, result, finalType);
                    for (int i = 1; i < ops.size() - 1; i += 2) {
                        DBSPExpression alt = ops.get(i + 1);
                        if (!alt.getNonVoidType().sameType(finalType))
                            alt = makeCast(call, alt, finalType);
                        DBSPExpression comp = makeBinaryExpression(
                                call, DBSPTypeBool.instance, "==", Linq.list(value, ops.get(i)));
                        comp = wrapBoolIfNeeded(comp);
                        result = new DBSPIfExpression(call, comp, alt, result);
                    }
                } else {
                    // Compute casts if needed.
                    // Build this backwards
                    DBSPType finalType = result.getNonVoidType();
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        if (ops.get(index).getNonVoidType().mayBeNull)
                            finalType = finalType.setMayBeNull(true);
                    }

                    if (!result.getNonVoidType().sameType(finalType))
                        result = makeCast(call, result, finalType);
                    for (int i = 0; i < ops.size() - 1; i += 2) {
                        int index = ops.size() - i - 2;
                        DBSPExpression alt = ops.get(index);
                        if (!alt.getNonVoidType().sameType(finalType))
                            alt = makeCast(call, alt, finalType);
                        DBSPExpression condition = wrapBoolIfNeeded(ops.get(index - 1));
                        result = new DBSPIfExpression(call, condition, alt, result);
                    }
                }
                return result;
            }
            case ST_POINT: {
                DBSPExpression tuple = new DBSPGeoPointLiteral(
                        call,
                        makeCast(call, ops.get(0), DBSPTypeDouble.instance),
                        makeCast(call, ops.get(1), DBSPTypeDouble.instance));
                return makeCast(call, tuple, type);
            }
            case OTHER_FUNCTION: {
                String opName = call.op.getName().toLowerCase();
                switch (opName) {
                    case "abs":
                        if (call.operands.size() != 1)
                            throw new Unimplemented(call);
                        DBSPExpression arg = ops.get(0);
                        DBSPType argType = arg.getNonVoidType();
                        SqlRuntimeLibrary.FunctionDescription abs =
                                SqlRuntimeLibrary.instance.getFunction("abs", type, argType, null, false);
                        return abs.getCall(arg);
                    case "st_distance":
                        if (call.operands.size() != 2)
                            throw new Unimplemented(call);
                        DBSPExpression left = ops.get(0);
                        DBSPExpression right = ops.get(1);
                        SqlRuntimeLibrary.FunctionDescription dist =
                                SqlRuntimeLibrary.instance.getFunction("st_distance", type, left.getNonVoidType(), right.getNonVoidType(), false);
                        return dist.getCall(left, right);
                    case "division":
                        return makeBinaryExpression(call, type, "/", ops);

                }
                throw new Unimplemented(call);
            }
            case OTHER:
                String opName = call.op.getName().toLowerCase();
                switch (opName) {
                    case "||":
                        return makeBinaryExpression(call, type, "||", ops);
                    default:
                        break;
                }
                throw new Unimplemented(call);
            case EXTRACT: {
                if (call.operands.size() != 2)
                    throw new Unimplemented(call);
                DBSPKeywordLiteral keyword = ops.get(0).to(DBSPKeywordLiteral.class);
                DBSPType type1 = ops.get(1).getNonVoidType();
                String functionName = "extract_" + type1.to(IsNumericType.class).getRustString() + "_" + keyword;
                if (type1.mayBeNull)
                    functionName += "N";
                return new DBSPApplyExpression(functionName, type, ops.get(1));
            }
            case FLOOR:
            case CEIL: {
                if (call.operands.size() != 2)
                    throw new Unimplemented(call);
                DBSPKeywordLiteral keyword = ops.get(1).to(DBSPKeywordLiteral.class);
                String functionName = call.getKind().toString().toLowerCase() + "_" + type.to(IsNumericType.class).getRustString() + "_" + keyword;
                if (type.mayBeNull)
                    functionName += "N";
                return new DBSPApplyExpression(functionName, type, ops.get(0));
            }
            default:
                throw new Unimplemented(call);
        }
    }

    DBSPExpression compile(RexNode expression) {
        Logger.instance.from(this, 3)
                .append("Compiling ")
                .append(expression.toString())
                .newline();
        return expression.accept(this);
    }
}
