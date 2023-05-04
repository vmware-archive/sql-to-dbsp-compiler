/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITParameter;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITParameterMapping;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBlock;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBlockReference;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBranchTerminator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITJumpTerminator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITReturnTerminator;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITBinaryInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITCastInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITConstantInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITCopyInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITFunctionCall;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstructionPair;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstructionReference;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITIsNullInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITLoadInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITMuxInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITSetNullInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITStoreInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITUnaryInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITUninitRowInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITBoolType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITI64Type;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITScalarType;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITType;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.Unimplemented;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Generate code for the JIT compiler.
 * Handles InnerNodes - i.e., expressions, closures, statements.
 */
public class ToJitInnerVisitor extends InnerVisitor {
    /**
     * Contexts keep track of variables defined.
     * Variable names can be redefined, as in Rust, and a context
     * will always return the one in the current scope.
     */
    class Context {
        /**
         * Maps variable names to expression ids.
         */
        final Map<String, JITInstructionPair> variables;

        public Context() {
            this.variables = new HashMap<>();
        }

        @Nullable
        JITInstructionPair lookup(String varName) {
            if (this.variables.containsKey(varName))
                return this.variables.get(varName);
            return null;
        }

        /**
         * Add a new variable to the current context.
         * @param varName   Variable name.
         * @param needsNull True if the variable is a scalar nullable variable.
         *                  Then we allocate an extra value to hold its
         *                  nullability.
         */
        JITInstructionPair addVariable(String varName, boolean needsNull) {
            if (this.variables.containsKey(varName)) {
                throw new RuntimeException("Duplicate declaration " + varName);
            }
            JITInstructionReference value = ToJitInnerVisitor.this.nextId();
            JITInstructionReference isNull = new JITInstructionReference();
            if (needsNull) {
                isNull = ToJitInnerVisitor.this.nextId();
            }
            JITInstructionPair result = new JITInstructionPair(value, isNull);
            this.variables.put(varName, result);
            return result;
        }
    }

    /**
     * Used to allocate ids for expressions and blocks.
     */
    private long nextInstrId = 1;
    /**
     * Write here the blocks generated.
     */
    final List<JITBlock> blocks;
    /**
     * Maps each expression to the values that it produces.
     */
    final Map<DBSPExpression, JITInstructionPair> expressionToValues;
    /**
     * A context for each block expression.
     */
    public final List<Context> declarations;

    @Nullable
    JITBlock currentBlock;
    /**
     * The type catalog shared with the ToJitVisitor.
     */
    public final TypeCatalog typeCatalog;
    /**
     * The names of the variables currently being assigned.
     * This is a stack, because we can have nested blocks:
     * let var1 = { let v2 = 1 + 2; v2 + 1 }
     */
    final List<String> variableAssigned;
    /**
     * A description how closure parameters are mapped to JIT parameters.
     */
    final JITParameterMapping mapping;

    public ToJitInnerVisitor(List<JITBlock> blocks, TypeCatalog typeCatalog, JITParameterMapping mapping) {
        super(true);
        this.blocks = blocks;
        this.typeCatalog = typeCatalog;
        this.expressionToValues = new HashMap<>();
        this.declarations = new ArrayList<>();
        this.currentBlock = null;
        this.mapping = mapping;
        this.variableAssigned = new ArrayList<>();
    }

    long nextInstructionId() {
        long result = this.nextInstrId;
        this.nextInstrId++;
        return result;
    }

    JITInstructionReference nextId() {
        long id = this.nextInstructionId();
        return new JITInstructionReference(id);
    }

    JITInstructionPair resolve(String varName) {
        // Look in the contexts in backwards order
        for (int i = 0; i < this.declarations.size(); i++) {
            int index = this.declarations.size() - 1 - i;
            Context current = this.declarations.get(index);
            JITInstructionPair ids = current.lookup(varName);
            if (ids != null)
                return ids;
        }
        throw new RuntimeException("Could not resolve " + varName);
    }

    void map(DBSPExpression expression, JITInstructionPair pair) {
        Utilities.putNew(this.expressionToValues, expression, pair);
    }

    JITInstructionPair getExpressionValues(DBSPExpression expression) {
        return Utilities.getExists(this.expressionToValues, expression);
    }
    
    JITInstructionPair accept(DBSPExpression expression) {
        expression.accept(this);
        return this.getExpressionValues(expression);
    }

    public JITInstructionPair constantBool(boolean value) {
        return this.accept(new DBSPBoolLiteral(value));
    }

    static JITScalarType convertScalarType(DBSPExpression expression) {
        return JITScalarType.scalarType(expression.getNonVoidType());
    }

    JITInstruction add(JITInstruction instruction) {
        this.getCurrentBlock().add(instruction);
        return instruction;
    }

    void newContext() {
        this.declarations.add(new Context());
    }

    Context getCurrentContext() {
        return this.declarations.get(this.declarations.size() - 1);
    }

    void popContext() {
        Utilities.removeLast(this.declarations);
    }

    JITInstructionPair declare(String var, boolean needsNull) {
        return this.getCurrentContext().addVariable(var, needsNull);
    }

    /**
     * True if this type needs to store an "is_null" value in a separate place.
     * Tuples don't.  Only scalar nullable types may.
     */
    static boolean needsNull(DBSPType type) {
        if (type.is(DBSPTypeTuple.class))
            return false;
        return type.mayBeNull;
    }

    static boolean needsNull(DBSPExpression expression) {
        return needsNull(expression.getNonVoidType());
    }

    public JITType convertType(DBSPType type) {
        if (ToJitVisitor.isScalarType(type)) {
            return JITScalarType.scalarType(type);
        } else {
            return this.typeCatalog.convertTupleType(type);
        }
    }

    public JITBlock getCurrentBlock() {
        return Objects.requireNonNull(this.currentBlock);
    }

    void createFunctionCall(String name,
                            DBSPExpression expression,  // usually an ApplyExpression, but not always
                            DBSPExpression... arguments) {
        // This assumes that the function is called only if no argument is nullable,
        // and that if any argument IS nullable the result is NULL.
        List<JITInstructionReference> nullableArgs = new ArrayList<>();
        List<JITType> argumentTypes = new ArrayList<>();
        List<JITInstructionReference> args = new ArrayList<>();
        for (DBSPExpression arg: arguments) {
            JITInstructionPair argValues = this.accept(arg);
            DBSPType argType = arg.getNonVoidType();
            args.add(argValues.value);
            argumentTypes.add(this.convertType(argType));
            if (argValues.hasNull())
                nullableArgs.add(argValues.isNull);
        }

        // Is any arg nullable?
        JITInstructionReference isNull = new JITInstructionReference();
        for (JITInstructionReference arg: nullableArgs) {
            if (!isNull.isValid())
                isNull = arg;
            else  {
                long id = this.nextInstructionId();
                JITInstruction or = new JITBinaryInstruction(id, JITBinaryInstruction.Operation.OR,
                        isNull, arg, JITBoolType.INSTANCE);
                this.add(or);
                isNull = or.getInstructionReference();
            }
        }

        // If any operand is null we create a branch and only call the function
        // conditionally.
        // if (!anyNull) funcCall(args);
        JITBlock onFalseBlock;
        JITBlock nextBlock = null;
        if (isNull.isValid()) {
            onFalseBlock = this.newBlock();
            nextBlock = this.newBlock();
            JITBranchTerminator branch = new JITBranchTerminator(
                    isNull, nextBlock.getBlockReference(),
                    onFalseBlock.getBlockReference());
            this.getCurrentBlock().terminate(branch);
            this.currentBlock = onFalseBlock;
            // The function call will be inserted in the onFalseBlock
        }

        JITScalarType resultType = convertScalarType(expression);
        long id = this.nextInstructionId();
        JITFunctionCall call = new JITFunctionCall(id, name, args, argumentTypes, resultType);
        this.add(call);
        JITInstructionPair result;

        if (isNull.isValid()) {
            JITBlockReference next = Objects.requireNonNull(nextBlock).getBlockReference();
            JITJumpTerminator terminator = new JITJumpTerminator(next);
            this.getCurrentBlock().terminate(terminator);

            this.currentBlock = nextBlock;
            JITCopyInstruction copy = new JITCopyInstruction(this.nextInstructionId(), isNull, JITBoolType.INSTANCE);
            this.currentBlock.add(copy);
            result = new JITInstructionPair(call, copy);
        } else {
            result = new JITInstructionPair(call);
        }
        this.map(expression, result);
    }

    /////////////////////////// Code generation

    @Override
    public boolean preorder(DBSPExpression expression) {
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPApplyExpression expression) {
        DBSPPathExpression path = expression.function.as(DBSPPathExpression.class);
        if (path != null) {
            String jitFunction = null;
            String function = path.path.toString();
            switch (function) {
                case "extract_Timestamp_second":
                    jitFunction = "dbsp.timestamp.second";
                    break;
                case "extract_Timestamp_minute":
                    jitFunction = "dbsp.timestamp.minute";
                    break;
                case "extract_Timestamp_hour":
                    jitFunction = "dbsp.timestamp.hour";
                    break;
                case "extract_Timestamp_day":
                    jitFunction = "dbsp.timestamp.day";
                    break;
                case "extract_Timestamp_dow":
                    jitFunction = "dbsp.timestamp.day_of_week";
                    break;
                case "extract_Timestamp_doy":
                    jitFunction = "dbsp.timestamp.day_of_year";
                    break;
                case "extract_Timestamp_isodow":
                    jitFunction = "dbsp.timestamp.iso_day_of_week";
                    break;
                case "extract_Timestamp_week":
                    jitFunction = "dbsp.timestamp.week";
                    break;
                case "extract_Timestamp_month":
                    jitFunction = "dbsp.timestamp.month";
                    break;
                case "extract_Timestamp_year":
                    jitFunction = "dbsp.timestamp.year";
                    break;
                case "extract_Timestamp_isoyear":
                    jitFunction = "dbsp.timestamp.isoyear";
                    break;
                case "extract_Timestamp_quarter":
                    jitFunction = "dbsp.timestamp.quarter";
                    break;
                case "extract_Timestamp_decade":
                    jitFunction = "dbsp.timestamp.decade";
                    break;
                case "extract_Timestamp_century":
                    jitFunction = "dbsp.timestamp.century";
                    break;
                case "extract_Timestamp_millennium":
                    jitFunction = "dbsp.timestamp.millennium";
                    break;
                case "extract_Timestamp_epoch":
                    jitFunction = "dbsp.timestamp.epoch";
                    break;
                default:
                    break;
            }
            if (jitFunction != null) {
                this.createFunctionCall(jitFunction, expression, expression.arguments);
                return false;
            }
        }
        throw new Unimplemented(expression);
    }

    @Override
    public boolean preorder(DBSPLiteral expression) {
        JITLiteral literal = new JITLiteral(expression);
        boolean mayBeNull = expression.getNonVoidType().mayBeNull;
        JITScalarType type = convertScalarType(expression);
        JITConstantInstruction value = new JITConstantInstruction(
                this.nextInstructionId(), type, literal, true);
        this.add(value);

        JITInstructionReference isNull = new JITInstructionReference();
        if (mayBeNull) {
            JITInstructionPair nullValue = this.constantBool(expression.isNull);
            isNull = nullValue.value;
        }
        JITInstructionPair pair = new JITInstructionPair(value.getInstructionReference(), isNull);
        this.map(expression, pair);
        return false;
    }

    public boolean preorder(DBSPBorrowExpression expression) {
        JITInstructionPair sourceId = this.accept(expression.expression);
        Utilities.putNew(this.expressionToValues, expression, sourceId);
        return false;
    }

    @Override
    public boolean preorder(DBSPCastExpression expression) {
        JITInstructionPair sourceId = this.accept(expression.source);
        long id = this.nextInstructionId();
        JITScalarType sourceType = convertScalarType(expression.source);
        JITScalarType destinationType = convertScalarType(expression);
        JITInstruction cast = new JITCastInstruction(id, sourceId.value, sourceType, destinationType);
        this.add(cast);

        JITInstructionReference isNull = new JITInstructionReference();
        if (needsNull(expression)) {
            if (needsNull(expression.source)) {
                isNull = sourceId.isNull;
            } else {
                // TODO: if source is nullable and is null must panic at runtime
                // this.createFunctionCall("dbsp.error.abort", expression);
            }
        } else {
            if (needsNull(expression.source)) {
                JITInstructionPair f = this.constantBool(false);
                isNull = f.value;
            }
            // else nothing to do
        }
        this.map(expression, new JITInstructionPair(cast.getInstructionReference(), isNull));
        return false;
    }

    static final Map<String, JITBinaryInstruction.Operation> opNames = new HashMap<>();

    static {
        // https://github.com/vmware/database-stream-processor/blob/dataflow-jit/crates/dataflow-jit/src/ir/expr.rs, the BinaryOpKind enum
        opNames.put("+", JITBinaryInstruction.Operation.ADD);
        opNames.put("-", JITBinaryInstruction.Operation.SUB);
        opNames.put("*", JITBinaryInstruction.Operation.MUL);
        opNames.put("/", JITBinaryInstruction.Operation.DIV);
        opNames.put("==",JITBinaryInstruction.Operation.EQ);
        opNames.put("!=",JITBinaryInstruction.Operation.NEQ);
        opNames.put("<", JITBinaryInstruction.Operation.LT);
        opNames.put(">", JITBinaryInstruction.Operation.GT);
        opNames.put("<=",JITBinaryInstruction.Operation.LTE);
        opNames.put(">=",JITBinaryInstruction.Operation.GTE);
        opNames.put("&", JITBinaryInstruction.Operation.AND);
        opNames.put("&&",JITBinaryInstruction.Operation.AND);
        opNames.put("|", JITBinaryInstruction.Operation.OR);
        opNames.put("||",JITBinaryInstruction.Operation.OR);
        opNames.put("^", JITBinaryInstruction.Operation.OR);
        opNames.put("agg_max", JITBinaryInstruction.Operation.MAX);
        opNames.put("agg_min", JITBinaryInstruction.Operation.MIN);
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        // a || b for strings is concatenation.
        if (expression.left.getNonVoidType().is(DBSPTypeString.class) &&
                expression.operation.equals("||")) {
            this.createFunctionCall("dbsp.str.concat_clone", expression,
                    expression.left, expression.right);
            return false;
        }
        if (expression.operation.equals("/")) {
            // TODO: division by 0 returns null.
            throw new Unimplemented(expression);
        }

        JITInstructionPair leftId = this.accept(expression.left);
        JITInstructionPair rightId = this.accept(expression.right);
        JITInstructionPair cf = new JITInstructionPair(new JITInstructionReference());
        if (needsNull(expression))
            cf = this.constantBool(false);
        JITInstructionReference leftNullId;
        if (leftId.hasNull())
            leftNullId = leftId.isNull;
        else
            // Not nullable: use false.
            leftNullId = cf.value;
        JITInstructionReference rightNullId;
        if (rightId.hasNull())
            rightNullId = rightId.isNull;
        else
            rightNullId = cf.value;
        boolean special = expression.operation.equals("&&") ||
                (expression.operation.equals("||") && expression.left.getNonVoidType().is(DBSPTypeBool.class));
        if (needsNull(expression.getNonVoidType()) && special) {
            if (expression.operation.equals("&&")) {
                // Nullable bit computation
                // (a && b).is_null = a.is_null ? (b.is_null ? true     : !b.value)
                //                              : (b.is_null ? !a.value : false)

                // !b.value
                JITInstruction notB = this.add(new JITUnaryInstruction(this.nextInstructionId(),
                        JITUnaryInstruction.Operation.NOT, rightId.value, JITBoolType.INSTANCE));
                // true
                JITInstructionPair trueLit = this.constantBool(true);
                // cond1 = (b.is_null ? true : !b.value)
                JITInstruction cond1 = this.add(new JITMuxInstruction(this.nextInstructionId(),
                        rightNullId, trueLit.value, notB.getInstructionReference()));
                // false
                JITInstructionPair falseLit = this.constantBool(false);
                // !a
                JITInstruction notA = this.add(new JITUnaryInstruction(this.nextInstructionId(),
                        JITUnaryInstruction.Operation.NOT, leftId.value, JITBoolType.INSTANCE));
                // cond2 = (b.is_null ? !a.value   : false)
                JITInstruction cond2 = this.add(new JITMuxInstruction(this.nextInstructionId(),
                        rightNullId, notA.getInstructionReference(), falseLit.value));

                // (a && b).value = a.is_null ? b.value
                //                            : (b.is_null ? a.value : a.value && b.value)
                // (The value for a.is_null & b.is_null does not matter, so we can choose it to be b.value)
                // a.value && b.value
                JITInstruction and = this.add(
                        new JITBinaryInstruction(this.nextInstructionId(),
                        JITBinaryInstruction.Operation.AND,
                        leftId.value, rightId.value, convertScalarType(expression.left)));
                // (b.is_null ? a.value : a.value && b.value)
                JITInstruction secondBranch = this.add(
                        new JITMuxInstruction(this.nextInstructionId(),
                        rightNullId, leftId.value, and.getInstructionReference()));
                // Final Mux
                JITInstruction value = this.add(
                        new JITMuxInstruction(this.nextInstructionId(),
                        leftNullId, rightId.value, secondBranch.getInstructionReference()));
                JITInstruction isNull = this.add(
                        new JITMuxInstruction(this.nextInstructionId(), leftNullId,
                                cond1.getInstructionReference(), cond2.getInstructionReference()));
                this.map(expression, new JITInstructionPair(value, isNull));
            } else { // Boolean ||
                // Nullable bit computation
                // (a || b).is_null = a.is_null ? (b.is_null ? true : b.value)
                //                              : (b.is_null ? a.value : false)
                // true
                JITInstructionPair trueLit = this.constantBool(true);
                // cond1 = (b.is_null ? true : b.value)
                JITInstruction cond1 = this.add(
                        new JITMuxInstruction(this.nextInstructionId(),
                        rightNullId, trueLit.value, rightId.value));
                // false
                JITInstructionPair falseLit = this.constantBool(false);
                // cond2 = (b.is_null ? a.value : false)
                JITInstruction cond2 = this.add(
                        new JITMuxInstruction(this.nextInstructionId(),
                                rightNullId, leftId.value, falseLit.value));

                // (a || b).value = a.is_null ? b.value
                //                            : a.value || b.value
                // a.value || b.value
                JITInstruction or = this.add(
                        new JITBinaryInstruction(this.nextInstructionId(),
                                JITBinaryInstruction.Operation.OR,
                                leftId.value, rightId.value, convertScalarType(expression.left)));
                // Result
                JITInstruction value = this.add(
                        new JITMuxInstruction(this.nextInstructionId(), leftNullId,
                                cond1.getInstructionReference(), cond2.getInstructionReference()));
                JITInstruction isNull = this.add(
                        new JITMuxInstruction(this.nextInstructionId(), leftNullId,
                                rightId.value, or.getInstructionReference()));
                this.map(expression, new JITInstructionPair(value, isNull));
            }
            return false;
        } else if (expression.operation.equals("agg_plus")) {
            JITInstruction value = this.add(new JITBinaryInstruction(
                    this.nextInstructionId(), JITBinaryInstruction.Operation.ADD,
                    leftId.value, rightId.value,
                    convertScalarType(expression.left)));
            JITInstruction isNull = null;
            if (needsNull(expression)) {
                // The result is null if both operands are null.
                isNull = this.add(new JITBinaryInstruction(
                        this.nextInstructionId(), JITBinaryInstruction.Operation.AND,
                        leftNullId, rightNullId, JITBoolType.INSTANCE));
            }
            this.map(expression, new JITInstructionPair(value, isNull));
            return false;
        } else if (expression.operation.equals("mul_weight")) {
            // (a * w).value = (a.value * (type_of_a)w)
            // (a * w).is_null = a.is_null
            JITInstructionReference left;
            DBSPType rightType = ToJitVisitor.resolveWeightType(expression.right.getNonVoidType());
            if (expression.left.getNonVoidType().sameType(rightType)) {
                JITInstruction cast = this.add(new JITCastInstruction(this.nextInstructionId(),
                    rightId.value, convertScalarType(expression.right), convertScalarType(expression.left)));
                left = cast.getInstructionReference();
            } else {
                left = leftId.value;
            }
            JITInstruction value = this.add(new JITBinaryInstruction(this.nextInstructionId(),
                JITBinaryInstruction.Operation.MUL, left, rightId.value, convertScalarType(expression.left)));
            JITInstruction isNull = null;
            if (needsNull(expression)) {
                isNull = this.add(new JITCopyInstruction(this.nextInstructionId(),
                        leftId.isNull, JITBoolType.INSTANCE));
            }
            this.map(expression, new JITInstructionPair(value, isNull));
            return false;
        }

        JITInstruction value = this.add(new JITBinaryInstruction(this.nextInstructionId(),
                Utilities.getExists(opNames, expression.operation), leftId.value, rightId.value,
                convertScalarType(expression.left)));
        JITInstruction isNull = null;
        if (needsNull(expression)) {
            // The result is null if either operand is null.
            isNull = this.add(new JITBinaryInstruction(this.nextInstructionId(),
                JITBinaryInstruction.Operation.OR, leftNullId, rightNullId, JITBoolType.INSTANCE));
        }
        this.map(expression, new JITInstructionPair(value, isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression expression) {
        JITInstructionPair source = this.accept(expression.source);
        boolean isWrapBool = expression.operation.equals("wrap_bool");
        JITInstructionPair cf = null;
        if (isWrapBool)
            cf = this.constantBool(false);
        JITUnaryInstruction.Operation kind;
        switch (expression.operation) {
            case "-":
                kind = JITUnaryInstruction.Operation.NEG;
                break;
            case "!":
                kind = JITUnaryInstruction.Operation.NOT;
                break;
            case "wrap_bool": {
                JITInstruction value = this.add(new JITMuxInstruction(this.nextInstructionId(),
                        source.isNull, cf.value, source.value));
                this.map(expression, new JITInstructionPair(value));
                return false;
            }
            case "is_false": {
                if (source.hasNull()) {
                    // result = left.is_null ? false : !left.value
                    // ! left.value
                    JITInstruction ni = this.add(new JITUnaryInstruction(this.nextInstructionId(),
                        JITUnaryInstruction.Operation.NOT, source.value, convertScalarType(expression.source)));
                    JITInstructionPair False = this.constantBool(false);
                    // result
                    JITInstruction value = this.add(new JITMuxInstruction(this.nextInstructionId(),
                            source.isNull, False.value, ni.getInstructionReference()));
                    this.map(expression, new JITInstructionPair(value));
                    return false;
                } else {
                    kind = JITUnaryInstruction.Operation.NOT;
                }
                break;
            }
            case "is_true": {
                if (source.hasNull()) {
                    // result = left.is_null ? false : left.value
                    JITInstructionPair False = this.constantBool(false);
                    // result
                    JITInstruction value = this.add(new JITMuxInstruction(this.nextInstructionId(),
                        source.isNull, False.value, source.value));
                    this.map(expression, new JITInstructionPair(value));
                } else {
                    this.map(expression, new JITInstructionPair(source.value));
                }
                return false;
            }
            case "is_not_true": {
                if (source.hasNull()) {
                    // result = left.is_null ? true : !left.value
                    // ! left.value
                    JITInstruction ni = this.add(new JITUnaryInstruction(this.nextInstructionId(), 
                        JITUnaryInstruction.Operation.NOT, source.value, convertScalarType(expression.source)));
                    JITInstructionPair True = this.constantBool(true);
                    // result
                    JITInstruction value = this.add(new JITMuxInstruction(this.nextInstructionId(), 
                        source.isNull, True.value, ni.getInstructionReference()));
                    this.map(expression, new JITInstructionPair(value));
                    return false;
                } else {
                    kind = JITUnaryInstruction.Operation.NOT;
                }
                break;
            }
            case "is_not_false": {
                if (source.hasNull()) {
                    // result = left.is_null ? true : left.value
                    JITInstructionPair True = this.constantBool(true);
                    // result
                    JITInstruction value = this.add(new JITMuxInstruction(this.nextInstructionId(),
                        source.isNull, True.value, source.value));
                    this.map(expression, new JITInstructionPair(value));
                } else {
                    this.map(expression, new JITInstructionPair(source.value));
                }
                return false;
            }
            case "indicator": {
                if (!source.hasNull())
                    throw new RuntimeException("indicator called on non-nullable expression" + expression);
                JITInstruction value = this.add(new JITCastInstruction(this.nextInstructionId(),
                    source.isNull, JITBoolType.INSTANCE, JITI64Type.INSTANCE));
                this.map(expression, new JITInstructionPair(value));
                return false;
            }
            default:
                throw new Unimplemented(expression);
        }
        JITInstruction value = this.add(new JITUnaryInstruction(this.nextInstructionId(),
            kind, source.value, convertScalarType(expression.source)));
        JITInstructionReference isNull = new JITInstructionReference();
        if (source.hasNull())
            isNull = source.isNull;
        this.map(expression, new JITInstructionPair(value.getInstructionReference(), isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPVariablePath expression) {
        JITInstructionPair pair = this.resolve(expression.variable);
        this.expressionToValues.put(expression, pair);
        // may already be there, but this may be a new variable with the same name,
        // and then we overwrite with the new definition.
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression closure) {
        for (DBSPParameter param: closure.parameters) {
            DBSPIdentifierPattern identifier = param.pattern.to(DBSPIdentifierPattern.class);
            this.declare(identifier.identifier, needsNull(param.type));
        }

        for (DBSPParameter param: this.mapping.outputParameters) {
            DBSPIdentifierPattern identifier = param.pattern.to(DBSPIdentifierPattern.class);
            String varName = identifier.identifier;
            this.declare(varName, needsNull(param.type));
            this.variableAssigned.add(varName);
        }

        closure.body.accept(this);

        for (int i = 0; i < this.mapping.outputParameters.size(); i++)
            Utilities.removeLast(this.variableAssigned);
        return false;
    }

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        boolean isTuple = statement.type.is(DBSPTypeTuple.class);
        JITInstructionPair ids = this.declare(statement.variable, needsNull(statement.type));
        this.variableAssigned.add(statement.variable);
        JITType type = this.convertType(statement.type);
        if (isTuple)
            this.add(new JITUninitRowInstruction(ids.value.getId(), type.to(JITRowType.class)));
        if (statement.initializer != null)
            statement.initializer.accept(this);
        Utilities.removeLast(this.variableAssigned);
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldExpression expression) {
        if (expression.expression.is(DBSPParameter.class)) {
            // check for parameter decompositions.
            JITParameter param  = this.mapping.getParameterReference(expression.expression.to(DBSPParameter.class), expression.fieldNo);
            if (param != null) {
                JITInstructionReference value = new JITInstructionReference(param.getId());
                JITInstructionReference isNullRef = new JITInstructionReference();
                if (needsNull(expression)) {
                    JITInstruction isNull = this.add(new JITIsNullInstruction(this.nextInstructionId(),
                            param.getInstructionReference(), param.type, 0));
                    isNullRef = isNull.getInstructionReference();
                }
                this.map(expression, new JITInstructionPair(value, isNullRef));
                return false;
            }
        }

        JITInstruction isNull = null;
        JITInstructionPair sourceId = this.accept(expression.expression);
        JITRowType sourceType = this.typeCatalog.convertTupleType(expression.expression.getNonVoidType());
        JITInstruction load = this.add(new JITLoadInstruction(
                this.nextInstructionId(), sourceId.value, sourceType,
                expression.fieldNo, convertScalarType(expression)));
        if (needsNull(expression)) {
            isNull = this.add(new JITIsNullInstruction(this.nextInstructionId(), sourceId.value,
                sourceType, expression.fieldNo));
        }
        this.map(expression, new JITInstructionPair(load, isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPIsNullExpression expression) {
        JITInstructionPair sourceId = this.accept(expression.expression);
        this.map(expression, new JITInstructionPair(sourceId.isNull));
        return false;
    }

    @Override
    public boolean preorder(DBSPCloneExpression expression) {
        // Treat clone as a noop.
        JITInstructionPair source = this.accept(expression.expression);
        this.map(expression, source);
        return false;
    }

    @Override
    public boolean preorder(DBSPIfExpression expression) {
        // TODO
        return false;
    }

    @Override
    public boolean preorder(DBSPRawTupleExpression expression) {
        // Each field is assigned to a different variable.
        // Remove the last n variables
        List<String> tail = new ArrayList<>();
        for (DBSPExpression ignored: expression.fields)
            tail.add(Utilities.removeLast(this.variableAssigned));
        for (DBSPExpression field: expression.fields) {
            // Add each variable and process the corresponding field.
            this.variableAssigned.add(Utilities.removeLast(tail));
            // Convert RawTuples inside RawTuples to regular Tuples
            if (field.is(DBSPRawTupleExpression.class))
                field = new DBSPTupleExpression(field.to(DBSPRawTupleExpression.class).fields);
            field.accept(this);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        // Compile this as an assignment to the currently assigned variable
        String variableAssigned = this.variableAssigned.get(this.variableAssigned.size() - 1);
        JITInstructionPair retValId = this.resolve(variableAssigned);
        JITRowType tupleTypeId = this.typeCatalog.convertTupleType(expression.getNonVoidType());
        int index = 0;
        for (DBSPExpression field: expression.fields) {
            // Generates 1 or 2 instructions for each field (depending on nullability)
            JITInstructionPair fieldId = this.accept(field);
            this.add(new JITStoreInstruction(this.nextInstructionId(),
                    retValId.value, tupleTypeId, index, fieldId.value,
                    JITScalarType.scalarType(field.getNonVoidType())));
            if (fieldId.hasNull()) {
                this.add(new JITSetNullInstruction(this.nextInstructionId(),
                        retValId.value, tupleTypeId, index, fieldId.isNull));
            }
            index++;
        }
        this.map(expression, retValId);
        return false;
    }

    JITBlock newBlock() {
        int blockId = this.blocks.size() + 1;
        JITBlock result = new JITBlock(blockId);
        this.blocks.add(result);
        return result;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        this.newContext();
        JITBlock saveBlock = this.currentBlock;
        JITBlock newBlock = this.newBlock();
        this.currentBlock = newBlock;
        for (DBSPStatement stat: expression.contents)
            stat.accept(this);

        // TODO: handle nullability
        JITInstructionPair resultId = new JITInstructionPair(new JITInstructionReference());
        if (expression.lastExpression != null) {
            if (ToJitVisitor.isScalarType(expression.lastExpression.getType())) {
                resultId = this.accept(expression.lastExpression);
            } else {
                // If the result is a tuple, this will store the result in the return value
                // and this block will return Unit.
                expression.lastExpression.accept(this);
                if (ToJitVisitor.isScalarType(expression.getNonVoidType())) {
                    // Otherwise the result of the block
                    // is the result computed by the last expression
                    JITInstructionPair id = this.getExpressionValues(expression.lastExpression);
                    this.map(expression, id);
                }
            }
        }

        JITReturnTerminator ret = new JITReturnTerminator(resultId.value);
        this.currentBlock.terminate(ret);
        this.popContext();
        this.currentBlock = saveBlock;
        if (this.currentBlock != null) {
            JITJumpTerminator terminator = new JITJumpTerminator(newBlock.getBlockReference());
            this.currentBlock.terminate(terminator);
        }
        return false;
    }

    /**
     * Convert the body of a closure expression to JIT representation.
     * @param expression  Expression to generate code for.
     * @param parameterMapping  Mapping that describes the function parameters.
     * @param catalog     The catalog of Tuple types.
     */
    static List<JITBlock> convertClosure(
            JITParameterMapping parameterMapping,
            DBSPClosureExpression expression, TypeCatalog catalog) {
        List<JITBlock> blocks = new ArrayList<>();
        ToJitInnerVisitor visitor = new ToJitInnerVisitor(blocks, catalog, parameterMapping);
        visitor.newContext();
        expression.accept(visitor);
        visitor.popContext();
        return blocks;
    }
}
