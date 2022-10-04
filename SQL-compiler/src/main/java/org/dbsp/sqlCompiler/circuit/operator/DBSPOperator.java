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

package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.util.IHasName;
import org.dbsp.util.NameGen;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A DBSP operator that applies a function to the inputs and produces an output.
 */
public abstract class DBSPOperator extends DBSPNode implements IHasName, IHasType {
    public final List<DBSPOperator> inputs;
    /**
     * Operation that is invoked on inputs; corresponds to a DBSP operator name, e.g., join.
     */
    public final String operation;
    /**
     * Computation invoked by the operator, usually a closure.
     */
    @Nullable
    public final DBSPExpression function;
    /**
     * Output assigned to this variable.
     */
    public final String outputName;
    /**
     * Type of output produced.
     */
    public final DBSPType outputType;
    /**
     * True if the output of the operator is a multiset.
     */
    public final boolean isMultiset;

    protected DBSPOperator(@Nullable Object node, String operation,
                           @Nullable DBSPExpression function, DBSPType outputType,
                           boolean isMultiset, String outputName) {
        super(node);
        this.inputs = new ArrayList<>();
        this.operation = operation;
        this.function = function;
        this.outputName = outputName;
        this.outputType = outputType;
        this.isMultiset = isMultiset;
    }

    public DBSPOperator(@Nullable Object node, String operation,
                        @Nullable DBSPExpression function,
                        DBSPType outputType, boolean isMultiset) {
        this(node, operation, function, outputType, isMultiset,
                new NameGen("stream").toString());
    }

    /**
     * Check that the result type of function is the same as expected.
     * @param function  An expression with a function type.
     * @param expected  Type expected to be returned by the function.
     */
    public void checkResultType(DBSPExpression function, DBSPType expected) {
        if (function.getNonVoidType().is(DBSPTypeAny.class))
            return;
        DBSPType type = function.getNonVoidType().to(DBSPTypeFunction.class).resultType;
        if (!expected.same(type))
            throw new RuntimeException(this + ": Expected function to return " + expected +
                    " but it returns " + type);
    }

    /**
     * Check that the specified source operator produces a ZSet/IndexedZSet with element types that can be fed
     * to the specified function.
     * @param function Function with multiple arguments
     * @param source   Source operator producing the arg input to function.
     * @param arg      Argument number of the function supplied from source operator.
     */
    protected void checkArgumentFunctionType(DBSPExpression function, int arg, DBSPOperator source) {
        if (function.getNonVoidType().is(DBSPTypeAny.class))
            return;
        DBSPType sourceElementType;
        DBSPTypeZSet zSet = source.outputType.as(DBSPTypeZSet.class);
        DBSPTypeIndexedZSet iZSet = source.outputType.as(DBSPTypeIndexedZSet.class);
        if (zSet != null) {
            sourceElementType = new DBSPTypeRef(zSet.elementType);
        } else if (iZSet != null) {
            sourceElementType = new DBSPTypeRawTuple(
                    new DBSPTypeRef(iZSet.keyType),
                    new DBSPTypeRef(iZSet.elementType));
        } else {
            throw new RuntimeException("Source " + source + " does not produce an (Indexed)ZSet, but "
                    + source.outputType);
        }
        DBSPTypeFunction funcType = function.getNonVoidType().to(DBSPTypeFunction.class);
        DBSPType argType = funcType.argumentTypes[arg];
        if (argType.is(DBSPTypeAny.class))
            return;
        if (!sourceElementType.same(argType))
            throw new RuntimeException(this + ": Expected function to accept " + sourceElementType +
                    " as argument " + arg + " but it expects " + funcType.argumentTypes[arg]);
    }

    protected void addInput(DBSPOperator node) {
        //noinspection ConstantConditions
        if (node == null)
            throw new RuntimeException("Null input to operator");
        this.inputs.add(node);
    }

    @Override
    public String getName() {
        return this.outputName;
    }

    @Override
    public DBSPType getType() {
        return this.outputType;
    }
}
