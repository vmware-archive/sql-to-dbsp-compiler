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

package org.dbsp.zetasqltest;

import org.dbsp.Zetatest;
import org.dbsp.ZetatestBaseVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This visitor converts a parse tree produced by ANTLR into
 * a ZetaSQLTestFile object.
 */
public class ZetatestVisitor extends ZetatestBaseVisitor<Void> {
    /**
     * Result generated.
     */
    public final ZetaSQLTestFile tests;
    /**
     * The test that is currently being converted.
     */
    @Nullable
    private ZetaSQLTest currentTest;
    /**
     * The type of the result of the test that is currently being converted.
     */
    @Nullable
    private DBSPType resultType;
    /**
     * The type of the current expression that is being converted.
     */
    @Nullable
    private DBSPType currentExpressionType;
    /**
     * The current value that is being converted.
     */
    @Nullable
    private DBSPExpression currentValue;

    public ZetatestVisitor() {
        this.tests = new ZetaSQLTestFile();
        this.currentTest = null;
        this.currentValue = null;
        this.currentExpressionType = null;
        this.resultType = null;
    }

    @Override
    public Void visitTest(Zetatest.TestContext ctx) {
        this.currentTest = new ZetaSQLTest();
        this.visitQuery(ctx.query());
        this.visitResult(ctx.result());
        this.tests.add(this.currentTest);
        this.currentTest = null;
        return null;
    }

    @Override
    public Void visitQuery(Zetatest.QueryContext ctx) {
        Objects.requireNonNull(this.currentTest).statement = ctx.getText();
        return super.visitQuery(ctx);
    }

    @Override
    public Void visitTypedvalue(Zetatest.TypedvalueContext ctx) {
        this.visitSqltype(ctx.sqltype());
        Objects.requireNonNull(this.currentTest).type = Objects.requireNonNull(this.resultType);
        this.currentExpressionType = this.resultType;
        this.visitSqlvalue(ctx.sqlvalue());
        this.currentTest.result = this.currentValue;
        this.currentValue = null;
        return null;
    }

    @Override
    public Void visitInttype(Zetatest.InttypeContext ctx) {
        this.resultType = DBSPTypeInteger.signed64;
        return super.visitInttype(ctx);
    }

    @Override
    public Void visitBooltype(Zetatest.BooltypeContext ctx) {
        this.resultType = DBSPTypeBool.instance;
        return super.visitBooltype(ctx);
    }

    @Override
    public Void visitDatetype(Zetatest.DatetypeContext ctx) {
        this.resultType = DBSPTypeDate.instance;
        return super.visitDatetype(ctx);
    }

    @Override
    public Void visitArraytype(Zetatest.ArraytypeContext ctx) {
        this.visitSqltype(ctx.sqltype());
        Objects.requireNonNull(this.resultType);
        this.resultType = new DBSPTypeZSet(this.resultType);
        return null;
    }

    @Override
    public Void visitStructtype(Zetatest.StructtypeContext ctx) {
        List<DBSPType> fields = new ArrayList<>();
        for (Zetatest.SqltypeContext context: ctx.typeArguments().sqltype()) {
            this.visitSqltype(context);
            fields.add(this.resultType);
        }
        this.resultType = new DBSPTypeTuple(fields);
        return null;
    }

    /////////////////// values

    @Override
    public Void visitBoolvalue(Zetatest.BoolvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeBool.class))
            throw new RuntimeException("Boolean value found, expected type " + this.currentExpressionType);
        switch (ctx.getText()) {
            case "false":
                this.currentValue = new DBSPBoolLiteral(false);
                break;
            case "true":
                this.currentValue = new DBSPBoolLiteral(true);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return super.visitBoolvalue(ctx);
    }

    @Override
    public Void visitArrayvalue(Zetatest.ArrayvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeZSet.class))
            throw new RuntimeException("Array value found, expected type " + this.currentExpressionType);
        DBSPTypeZSet ztype = this.currentExpressionType.to(DBSPTypeZSet.class);
        this.currentExpressionType = ztype.elementType;
        DBSPZSetLiteral result = new DBSPZSetLiteral(ztype);
        for (Zetatest.SqlvalueContext value: ctx.sqlvalue()) {
            this.visitSqlvalue(value);
            Objects.requireNonNull(this.currentValue);
            result.add(this.currentValue);
        }
        this.currentValue = result;
        return null;
    }

    @Override
    public Void visitStructvalue(Zetatest.StructvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeTuple.class))
            throw new RuntimeException("Struct value found, expected type " + this.currentExpressionType);
        DBSPTypeTuple ttype = this.currentExpressionType.to(DBSPTypeTuple.class);
        List<DBSPExpression> fields = new ArrayList<>();
        if (ttype.tupFields.length != ctx.sqlvalue().size())
            throw new RuntimeException("Type expects " + ttype.tupFields.length + " struct value has " + ctx.sqlvalue().size());
        for (int i = 0; i < ttype.tupFields.length; i++) {
            this.currentExpressionType = ttype.tupFields[i];
            this.visitSqlvalue(ctx.sqlvalue(i));
            Objects.requireNonNull(this.currentValue);
            fields.add(this.currentValue);
        }
        this.currentValue = new DBSPTupleExpression(fields);
        return null;
    }

    @Override
    public Void visitDatevalue(Zetatest.DatevalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeDate.class))
            throw new RuntimeException("Date value found, expected type " + this.currentExpressionType);
        this.currentValue = new DBSPDateLiteral(ctx.getText());
        return null;
    }

    @Override
    public Void visitIntvalue(Zetatest.IntvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        BigInteger value = new BigInteger(ctx.getText());
        if (this.currentExpressionType.is(DBSPTypeInteger.class)) {
            DBSPTypeInteger intType = this.currentExpressionType.to(DBSPTypeInteger.class);
            switch (intType.getWidth()) {
                case 32:
                    this.currentValue = new DBSPIntegerLiteral(value.intValue());
                    break;
                case 64:
                    this.currentValue = new DBSPLongLiteral(value.longValue());
                    break;
                default:
                    throw new RuntimeException("Unexpected type " + intType);
            }
        } else {
            throw new RuntimeException("Integer value found, expected type " + this.currentExpressionType);
        }
        return super.visitIntvalue(ctx);
    }
}
