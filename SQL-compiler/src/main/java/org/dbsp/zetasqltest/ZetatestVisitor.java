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

import org.apache.commons.text.StringEscapeUtils;
import org.dbsp.Zetatest;
import org.dbsp.ZetatestBaseVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;

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
        this.resultType = DBSPTypeInteger.signed64.setMayBeNull(true);
        return super.visitInttype(ctx);
    }

    @Override
    public Void visitBooltype(Zetatest.BooltypeContext ctx) {
        this.resultType = DBSPTypeBool.instance.setMayBeNull(true);
        return super.visitBooltype(ctx);
    }

    @Override
    public Void visitDatetype(Zetatest.DatetypeContext ctx) {
        this.resultType = DBSPTypeDate.instance.setMayBeNull(true);
        return super.visitDatetype(ctx);
    }

    @Override
    public Void visitDoubletype(Zetatest.DoubletypeContext ctx) {
        this.resultType = DBSPTypeDouble.instance.setMayBeNull(true);
        return super.visitDoubletype(ctx);
    }

    @Override
    public Void visitStringtype(Zetatest.StringtypeContext ctx) {
        this.resultType = DBSPTypeString.instance.setMayBeNull(true);
        return super.visitStringtype(ctx);
    }

    @Override
    public Void visitArraytype(Zetatest.ArraytypeContext ctx) {
        if (ctx.sqltype() != null) {
            this.visitSqltype(ctx.sqltype());
            Objects.requireNonNull(this.resultType);
            this.resultType = new DBSPTypeZSet(this.resultType);
        } else {
            // ARRAY<> will be translated to a vector type
            // unfortunately the element type is not specified at this point.
            this.resultType = new DBSPTypeVec(DBSPTypeAny.instance);
        }
        return null;
    }

    @Override
    public Void visitStructtype(Zetatest.StructtypeContext ctx) {
        List<DBSPType> fields = new ArrayList<>();
        for (Zetatest.OptNamedSqlTypeContext context: ctx.fields().optNamedSqlType()) {
            this.visitSqltype(context.sqltype());
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
        if (ctx.TRUE() != null) {
            this.currentValue = new DBSPBoolLiteral(true, this.currentExpressionType.mayBeNull);
        } else if (ctx.FALSE() != null) {
            this.currentValue = new DBSPBoolLiteral(false, this.currentExpressionType.mayBeNull);
        } else {
            throw new UnsupportedOperationException();
        }
        return super.visitBoolvalue(ctx);
    }

    @Override
    public Void visitStringvalue(Zetatest.StringvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeString.class))
            throw new RuntimeException("String value found, expected type " + this.currentExpressionType);
        String text = StringEscapeUtils.unescapeJava(ctx.getText());
        this.currentValue = new DBSPStringLiteral(text, this.currentExpressionType.mayBeNull);
        return super.visitStringvalue(ctx);
    }

    @Override
    public Void visitFloatvalue(Zetatest.FloatvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeDouble.class))
            throw new RuntimeException("Double value found, expected type " + this.currentExpressionType);
        this.currentValue = new DBSPDoubleLiteral(Double.parseDouble(ctx.getText()), this.currentExpressionType.mayBeNull);
        return super.visitFloatvalue(ctx);
    }

    @Override
    public Void visitNullvalue(Zetatest.NullvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        this.currentValue = DBSPLiteral.none(this.currentExpressionType);
        return super.visitNullvalue(ctx);
    }

    @Override
    public Void visitArrayvalue(Zetatest.ArrayvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (this.currentExpressionType.is(DBSPTypeZSet.class)) {
            List<DBSPExpression> values = new ArrayList<>();
            DBSPTypeZSet ztype = this.currentExpressionType.to(DBSPTypeZSet.class);
            for (Zetatest.SqlvalueContext value : ctx.sqlvalue()) {
                this.currentExpressionType = ztype.elementType;
                this.visitSqlvalue(value);
                Objects.requireNonNull(this.currentValue);
                values.add(this.currentValue);
            }
            this.currentValue = new DBSPZSetLiteral(values.toArray(new DBSPExpression[0]));
            return null;
        } else if (this.currentExpressionType.is(DBSPTypeVec.class)) {
            if (ctx.arraytype() == null)
                throw new RuntimeException("Array type not specified for array literal " + ctx.getText());
            visitSqltype(ctx.arraytype().sqltype());
            Objects.requireNonNull(this.resultType);
            DBSPTypeVec vecType = new DBSPTypeVec(this.resultType);
            DBSPVecLiteral result = new DBSPVecLiteral(vecType.getElementType());
            for (Zetatest.SqlvalueContext value : ctx.sqlvalue()) {
                this.currentExpressionType = vecType.getElementType();
                this.visitSqlvalue(value);
                Objects.requireNonNull(this.currentValue);
                result.add(this.currentValue);
            }
            this.currentValue = result;
            return null;
        }
        throw new RuntimeException("Array value found, expected type " + this.currentExpressionType);
    }

    @Override
    public Void visitStructvalue(Zetatest.StructvalueContext ctx) {
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeTuple.class))
            throw new RuntimeException("Struct value found " + ctx.getText() +
                    " expected type " + this.currentExpressionType);
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
                    this.currentValue = new DBSPIntegerLiteral(value.intValue(), this.currentExpressionType.mayBeNull);
                    break;
                case 64:
                    this.currentValue = new DBSPLongLiteral(value.longValue(), this.currentExpressionType.mayBeNull);
                    break;
                default:
                    throw new RuntimeException("Unexpected type " + intType);
            }
        } else if (this.currentExpressionType.is(DBSPTypeDouble.class)) {
            this.currentValue = new DBSPDoubleLiteral((double)value.longValue(), this.currentExpressionType.mayBeNull);
        } else {
            throw new RuntimeException("Integer value found, expected type " + this.currentExpressionType);
        }
        return super.visitIntvalue(ctx);
    }
}
