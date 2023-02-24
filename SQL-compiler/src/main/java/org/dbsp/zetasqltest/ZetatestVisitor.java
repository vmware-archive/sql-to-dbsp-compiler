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
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This visitor converts a parse tree produced by ANTLR into
 * a ZetaSQLTestFile object.
 */
public class ZetatestVisitor extends ZetatestBaseVisitor<Void> implements IModule {
    /**
     * Result generated.
     */
    public final ZetaSQLTestFile tests;
    /**
     * The test result that is currently being converted.
     */
    @Nullable
    private ZetaSQLTest.TestResult currentResult;
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
    int structNestingLevel;

    public ZetatestVisitor() {
        this.tests = new ZetaSQLTestFile();
        this.currentValue = null;
        this.currentExpressionType = null;
        this.resultType = null;
        this.structNestingLevel = 0;
    }

    static class ContainsTypeAny extends InnerVisitor {
        boolean hasAny = false;

        public ContainsTypeAny() {
            super(true);
        }

        @Override
        public boolean preorder(DBSPTypeAny any) {
            this.hasAny = true;
            return false;
        }

        public static boolean hasAnyType(DBSPType type) {
            ContainsTypeAny cta = new ContainsTypeAny();
            type.accept(cta);
            return cta.hasAny;
        }
    }

    @Override
    public Void visitTest(Zetatest.TestContext ctx) {
        try {
            Logger.INSTANCE.from(this, 2)
                    .append("Visiting test ")
                    .append(ctx.getText()).newline();
            ZetaSQLTest test = new ZetaSQLTest();
            test.statement = ctx.query().getText();
            for (Zetatest.ResultContext result : ctx.result()) {
                this.currentResult = new ZetaSQLTest.TestResult();
                for (Zetatest.FeatureContext feature : result.feature()) {
                    this.currentResult.features.add(feature.FeatureDescription().getText());
                }
                this.visitResult(result);
                test.results.add(this.currentResult);
            }
            this.currentResult = null;
            this.tests.add(test);
            return null;
        } catch (Exception ex) {
            System.err.println("Error while processing " + ctx.getText());
            throw ex;
        }
    }

    @Override
    public Void visitTypedvalue(Zetatest.TypedvalueContext ctx) {
        Logger.INSTANCE.from(this, 2)
               .append("Visiting typedvalue ")
               .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentResult);
        if (ctx.error() != null) {
            this.currentResult.error = ctx.error().getText();
        } else if (ctx.valueNotSpecified() != null) {
            this.currentResult.error = ctx.valueNotSpecified().getText();
        } else {
            try {
                this.visitSqltype(ctx.sqltype());
                this.currentResult.type = Objects.requireNonNull(this.resultType);
                this.currentExpressionType = this.resultType;
                this.visitSqlvalue(ctx.sqlvalue());
                this.currentResult.result = this.currentValue;
            } catch (Unimplemented ex) {
                this.currentResult.error = ex.getMessage();
            }
        }
        this.currentValue = null;
        return null;
    }

    //////////////////// Types


    @Override
    public Void visitBytestype(Zetatest.BytestypeContext ctx) {
        // TODO: add support for byte arrays
        Logger.INSTANCE.from(this, 1)
                .append("Visiting bytes ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeString.NULLABLE_INSTANCE;
        return super.visitBytestype(ctx);
    }

    @Override
    public Void visitInt64type(Zetatest.Int64typeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting int64 ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeInteger.SIGNED_64.setMayBeNull(true);
        return super.visitInt64type(ctx);
    }

    @Override
    public Void visitInt32type(Zetatest.Int32typeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting int32 ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeInteger.SIGNED_32.setMayBeNull(true);
        return super.visitInt32type(ctx);
    }

    @Override
    public Void visitUint32type(Zetatest.Uint32typeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting uint32 ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeInteger.UNSIGNED_32.setMayBeNull(true);
        return super.visitUint32type(ctx);
    }

    @Override
    public Void visitUint64type(Zetatest.Uint64typeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting uint64 ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeInteger.UNSIGNED_64.setMayBeNull(true);
        return super.visitUint64type(ctx);
    }

    @Override
    public Void visitBooltype(Zetatest.BooltypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting bool ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeBool.NULLABLE_INSTANCE;
        return super.visitBooltype(ctx);
    }

    @Override
    public Void visitDatetype(Zetatest.DatetypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting date ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeDate.NULLABLE_INSTANCE;
        return super.visitDatetype(ctx);
    }

    @Override
    public Void visitTimestamptype(Zetatest.TimestamptypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting timestamp ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeTimestamp.NULLABLE_INSTANCE;
        return super.visitTimestamptype(ctx);
    }

    @Override
    public Void visitTimetype(Zetatest.TimetypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting time ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeTime.NULLABLE_INSTANCE;
        return super.visitTimetype(ctx);
    }

    @Override
    public Void visitDatetimetype(Zetatest.DatetimetypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting datetime ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeTimestamp.NULLABLE_INSTANCE;
        return super.visitDatetimetype(ctx);
    }

    @Override
    public Void visitGeographytype(Zetatest.GeographytypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting geography ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeGeoPoint.NULLABLE_INSTANCE;
        return super.visitGeographytype(ctx);
    }

    @Override
    public Void visitDoubletype(Zetatest.DoubletypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting double ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeDouble.NULLABLE_INSTANCE;
        return super.visitDoubletype(ctx);
    }

    @Override
    public Void visitFloattype(Zetatest.FloattypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting float ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeFloat.NULLABLE_INSTANCE;
        return super.visitFloattype(ctx);
    }

    @Override
    public Void visitStringtype(Zetatest.StringtypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting string ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeString.NULLABLE_INSTANCE;
        return super.visitStringtype(ctx);
    }

    @Override
    public Void visitNumerictype(Zetatest.NumerictypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting numeric ")
                .append(ctx.getText()).newline();
        this.resultType = new DBSPTypeDecimal(null,128, 30,true);
        return super.visitNumerictype(ctx);
    }

    @Override
    public Void visitBignumerictype(Zetatest.BignumerictypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting bignumeric ")
                .append(ctx.getText()).newline();
        this.resultType = new DBSPTypeDecimal(null,256, 30,true);
        return super.visitBignumerictype(ctx);
    }

    @Override
    public Void visitArraytype(Zetatest.ArraytypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting array ")
                .append(ctx.getText()).newline();
        if (ctx.sqltype() != null) {
            this.visitSqltype(ctx.sqltype());
            Objects.requireNonNull(this.resultType);
            this.resultType = new DBSPTypeZSet(this.resultType);
        } else {
            // ARRAY<> will be translated to a vector type
            // unfortunately the element type is not specified at this point.
            this.resultType = new DBSPTypeVec(DBSPTypeAny.INSTANCE);
        }
        return null;
    }

    @Override
    public Void visitIntervaltype(Zetatest.IntervaltypeContext ctx) {
        throw new Unimplemented("INTERVAL type not yet supported");
    }

    @Override
    public Void visitPrototype(Zetatest.PrototypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting proto ")
                .append(ctx.getText()).newline();
        this.resultType = DBSPTypeAny.INSTANCE;
        throw new Unimplemented("PROTO type not yet supported");
    }

    @Override
    public Void visitStructtype(Zetatest.StructtypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting struct ")
                .append(ctx.getText()).newline();
        this.structNestingLevel++;
        List<DBSPType> fields = new ArrayList<>();
        for (Zetatest.OptNamedSqlTypeContext context: ctx.fields().optNamedSqlType()) {
            this.visitSqltype(context.sqltype(context.sqltype().size() - 1));
            fields.add(this.resultType);
        }
        // we allow only nested structs to be nullable
        boolean mayBeNull = this.structNestingLevel > 1;
        this.resultType = new DBSPTypeTuple(fields).setMayBeNull(mayBeNull);
        this.structNestingLevel--;
        return null;
    }

    @Override
    public Void visitEnumtype(Zetatest.EnumtypeContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting enum ")
                .append(ctx.getText()).newline();
        // TODO: What should this be?
        this.resultType = DBSPTypeString.NULLABLE_INSTANCE;
        return super.visitEnumtype(ctx);
    }

    /////////////////// values

    @Override
    public Void visitBytesvalue(Zetatest.BytesvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting bytesvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeString.class))
            throw new RuntimeException("Bytes value found, expected type " + this.currentExpressionType);
        this.currentValue = new DBSPStringLiteral(ctx.getText(), this.currentExpressionType.mayBeNull);
        return super.visitBytesvalue(ctx);
    }

    @Override
    public Void visitBoolvalue(Zetatest.BoolvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting boolvalue ")
                .append(ctx.getText()).newline();
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
        Logger.INSTANCE.from(this, 1)
                .append("Visiting stringvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeString.class))
            throw new RuntimeException("String value found, expected type " + this.currentExpressionType);
        String text = StringEscapeUtils.unescapeJava(ctx.getText());
        this.currentValue = new DBSPStringLiteral(text, this.currentExpressionType.mayBeNull);
        return super.visitStringvalue(ctx);
    }

    @Override
    public Void visitFloatvalue(Zetatest.FloatvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting floatvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        String text = ctx.getText();
        boolean mayBeNull = this.currentExpressionType.mayBeNull;
        if (this.currentExpressionType.is(DBSPTypeDouble.class)) {
            switch (text) {
                case "inf":
                    this.currentValue = new DBSPDoubleLiteral(Double.POSITIVE_INFINITY, mayBeNull);
                    break;
                case "-inf":
                    this.currentValue = new DBSPDoubleLiteral(Double.NEGATIVE_INFINITY, mayBeNull);
                    break;
                case "nan":
                    this.currentValue = new DBSPDoubleLiteral(Double.NaN, mayBeNull);
                    break;
                default:
                    this.currentValue = new DBSPDoubleLiteral(Double.parseDouble(ctx.getText()), mayBeNull);
                    break;
            }
        } else if (this.currentExpressionType.is(DBSPTypeFloat.class)) {
            switch (text) {
                case "inf":
                    this.currentValue = new DBSPFloatLiteral(Float.POSITIVE_INFINITY, mayBeNull);
                    break;
                case "-inf":
                    this.currentValue = new DBSPFloatLiteral(Float.NEGATIVE_INFINITY, mayBeNull);
                    break;
                case "nan":
                    this.currentValue = new DBSPFloatLiteral(Float.NaN, mayBeNull);
                    break;
                default:
                    this.currentValue = new DBSPFloatLiteral(Float.parseFloat(ctx.getText()), mayBeNull);
                    break;
            }
        } else if (this.currentExpressionType.is(DBSPTypeDecimal.class)) {
            BigDecimal decimal = new BigDecimal(text);
            this.currentValue = new DBSPDecimalLiteral(null, this.currentExpressionType, decimal);
        } else {
            throw new RuntimeException("Double value '" + text
                    + "' found, expected type " + this.currentExpressionType);
        }
        return super.visitFloatvalue(ctx);
    }

    @Override
    public Void visitNullvalue(Zetatest.NullvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting nullvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        this.currentValue = DBSPLiteral.none(this.currentExpressionType);
        return super.visitNullvalue(ctx);
    }

    @Override
    public Void visitArrayvalue(Zetatest.ArrayvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting arrayvalue ")
                .append(ctx.getText()).newline();
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
            if (values.isEmpty()) {
                this.currentValue = new DBSPZSetLiteral(this.currentExpressionType);
            } else {
                this.currentValue = new DBSPZSetLiteral(values.toArray(new DBSPExpression[0]));
            }
            return null;
        } else if (this.currentExpressionType.is(DBSPTypeVec.class)) {
            if (ctx.arraytype() == null)
                throw new RuntimeException("Array type not specified for array literal " + ctx.getText());
            visitSqltype(ctx.arraytype().sqltype());
            Objects.requireNonNull(this.resultType);
            DBSPTypeVec vecType = new DBSPTypeVec(this.resultType);
            DBSPVecLiteral result = null;
            List<DBSPExpression> components = new ArrayList<>();
            if (!ContainsTypeAny.hasAnyType(vecType))
                result = new DBSPVecLiteral(vecType.getElementType());
            for (Zetatest.SqlvalueContext value : ctx.sqlvalue()) {
                this.currentExpressionType = vecType.getElementType();
                this.visitSqlvalue(value);
                Objects.requireNonNull(this.currentValue);
                if (result != null)
                    result.add(this.currentValue);
                else
                    components.add(this.currentValue);
            }
            if (result != null)
                this.currentValue = result;
            else
                this.currentValue = new DBSPVecLiteral(components.toArray(new DBSPExpression[0]));
            return null;
        }
        throw new RuntimeException("Array value found, expected type " + this.currentExpressionType);
    }

    @Override
    public Void visitProtovalue(Zetatest.ProtovalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting protovalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeAny.class))
            throw new RuntimeException("Proto value found " + ctx.getText() +
                    " expected type " + this.currentExpressionType);
        List<DBSPExpression> fields = new ArrayList<>();
        for (Zetatest.ProtofieldvalueContext sqlvalue: ctx.protofieldvalue()) {
            this.currentExpressionType = DBSPTypeAny.INSTANCE;
            this.visitSqlvalue(sqlvalue.sqlvalue());
            Objects.requireNonNull(this.currentValue);
            fields.add(this.currentValue);
        }
        this.currentValue = new DBSPTupleExpression(fields, true);
        return null;
    }

    @Override
    public Void visitStructvalue(Zetatest.StructvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting structvalue ")
                .append(ctx.getText()).newline();
        DBSPType saveExpressionType = this.currentExpressionType;
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
        this.currentValue = new DBSPTupleExpression(fields, saveExpressionType.mayBeNull);
        return null;
    }

    @Override
    public Void visitDatetimevalue(Zetatest.DatetimevalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting datetime ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeTimestamp.class))
            throw new RuntimeException("Datetime value found " + ctx.getText() +
                    " expected type " + this.currentExpressionType);
        this.currentValue = new DBSPTimestampLiteral(
                ctx.DATEVALUE().getText() + " " + ctx.TIMEVALUE().getText(),
                this.currentExpressionType.mayBeNull);
        return super.visitDatetimevalue(ctx);
    }

    @Override
    public Void visitTimevalue(Zetatest.TimevalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting timevalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeTime.class))
            throw new RuntimeException("Time value found " + ctx.getText() +
                    " expected type " + this.currentExpressionType);
        this.currentValue = new DBSPTimeLiteral(ctx.getText(), this.currentExpressionType.mayBeNull);
        return super.visitTimevalue(ctx);
    }

    @Override
    public Void visitEnumvalue(Zetatest.EnumvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting enumvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeString.class))
            throw new RuntimeException("Enum value found " + ctx.getText() +
                    " expected type " + this.currentExpressionType);
        this.currentValue = new DBSPStringLiteral(ctx.getText(), this.currentExpressionType.mayBeNull);
        return super.visitEnumvalue(ctx);
    }

    @Override
    public Void visitDatevalue(Zetatest.DatevalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting datevalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeDate.class))
            throw new RuntimeException("Date value found, expected type " + this.currentExpressionType);
        this.currentValue = new DBSPDateLiteral(ctx.getText(), this.currentExpressionType.mayBeNull);
        return null;
    }

    @Override
    public Void visitTimestampvalue(Zetatest.TimestampvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting timestampvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeTimestamp.class))
            throw new RuntimeException("Timestamp value found, expected type " + this.currentExpressionType);
        // Drop timezone for now
        // TODO: add support for timezones
        String text = ctx.getText();
        text = text.substring(0, text.length() - 3);
        this.currentValue = new DBSPTimestampLiteral(text, this.currentExpressionType.mayBeNull);
        return super.visitTimestampvalue(ctx);
    }

    @Override
    public Void visitIntvalue(Zetatest.IntvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting intvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        BigInteger value = new BigInteger(ctx.getText());
        if (this.currentExpressionType.is(DBSPTypeInteger.class)) {
            DBSPTypeInteger intType = this.currentExpressionType.to(DBSPTypeInteger.class);
            switch (intType.getRustString()) {
                case "i32":
                    this.currentValue = new DBSPI32Literal(value.intValue(), this.currentExpressionType.mayBeNull);
                    break;
                case "u32":
                    this.currentValue = new DBSPU32Literal(value.intValue(), this.currentExpressionType.mayBeNull);
                    break;
                case "i64":
                    this.currentValue = new DBSPI64Literal(value.longValue(), this.currentExpressionType.mayBeNull);
                    break;
                case "u64":
                    this.currentValue = new DBSPU64Literal(value.longValue(), this.currentExpressionType.mayBeNull);
                    break;
                default:
                    throw new RuntimeException("Unexpected type " + intType);
            }
        } else if (this.currentExpressionType.is(DBSPTypeDouble.class)) {
            this.currentValue = new DBSPDoubleLiteral((double)value.longValue(), this.currentExpressionType.mayBeNull);
        } else if (this.currentExpressionType.is(DBSPTypeFloat.class)) {
            this.currentValue = new DBSPFloatLiteral((float)value.longValue(), this.currentExpressionType.mayBeNull);
        } else if (this.currentExpressionType.is(DBSPTypeDecimal.class)) {
            BigDecimal decimal = new BigDecimal(value);
            this.currentValue = new DBSPDecimalLiteral(null, this.currentExpressionType, decimal);
        } else {
            throw new RuntimeException("Integer value '" + ctx.getText() +
                    "' found, expected type " + this.currentExpressionType);
        }
        return super.visitIntvalue(ctx);
    }

    @Override
    public Void visitSt_pointvalue(Zetatest.St_pointvalueContext ctx) {
        Logger.INSTANCE.from(this, 1)
                .append("Visiting pointvalue ")
                .append(ctx.getText()).newline();
        Objects.requireNonNull(this.currentExpressionType);
        if (!this.currentExpressionType.is(DBSPTypeGeoPoint.class))
            throw new RuntimeException("st_point value " + ctx.getText() +
                    " found, expected type is " + this.currentExpressionType);
        DBSPExpression left = new DBSPDoubleLiteral(Double.parseDouble(ctx.number(0).getText()));
        DBSPExpression right = new DBSPDoubleLiteral(Double.parseDouble(ctx.number(1).getText()));
        this.currentValue = new DBSPGeoPointLiteral(null, left, right);
        return null;
    }
}
