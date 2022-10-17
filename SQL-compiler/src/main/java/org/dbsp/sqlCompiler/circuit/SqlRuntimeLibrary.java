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

package org.dbsp.sqlCompiler.circuit;

import org.dbsp.sqlCompiler.ir.DBSPFile;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.pattern.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

/**
 * This class generates Rust sources for the SQL
 * runtime library: support functions that implement the
 * SQL semantics.
 */
@SuppressWarnings({"FieldCanBeLocal", "MismatchedQueryAndUpdateOfCollection", "SpellCheckingInspection"})
public class SqlRuntimeLibrary {
    @Nullable
    private DBSPFile program;

    private final HashSet<String> aggregateFunctions = new HashSet<>();
    private final HashMap<String, String> arithmeticFunctions = new HashMap<>();
    private final HashMap<String, String> doubleFunctions = new HashMap<>();
    private final HashMap<String, String> stringFunctions = new HashMap<>();
    private final HashMap<String, String> booleanFunctions = new HashMap<>();
    private final Set<String> comparisons = new HashSet<>();
    private final Set<String> handWritten = new HashSet<>();

    public static final SqlRuntimeLibrary instance = new SqlRuntimeLibrary();

    protected SqlRuntimeLibrary() {
        this.aggregateFunctions.add("count");
        this.aggregateFunctions.add("sum");
        this.aggregateFunctions.add("avg");
        this.aggregateFunctions.add("min");
        this.aggregateFunctions.add("max");
        this.aggregateFunctions.add("some");
        this.aggregateFunctions.add("any");
        this.aggregateFunctions.add("every");
        this.aggregateFunctions.add("array_agg");
        this.aggregateFunctions.add("set_agg");

        this.arithmeticFunctions.put("eq", "==");
        this.arithmeticFunctions.put("neq", "!=");
        this.arithmeticFunctions.put("lt", "<");
        this.arithmeticFunctions.put("gt", ">");
        this.arithmeticFunctions.put("lte", "<=");
        this.arithmeticFunctions.put("gte", ">=");
        this.arithmeticFunctions.put("plus", "+");
        this.arithmeticFunctions.put("minus", "-");
        this.arithmeticFunctions.put("mod", "%");
        this.arithmeticFunctions.put("times", "*");
        this.arithmeticFunctions.put("div", "/");
        this.arithmeticFunctions.put("shiftr", ">>");
        this.arithmeticFunctions.put("shiftl", "<<");
        this.arithmeticFunctions.put("band", "&");
        this.arithmeticFunctions.put("bor", "|");
        this.arithmeticFunctions.put("bxor", "^");
        this.arithmeticFunctions.put("min", "min");
        this.arithmeticFunctions.put("max", "max");
        this.arithmeticFunctions.put("abs", "abs");
        this.arithmeticFunctions.put("st_distance", "st_distance");
        this.arithmeticFunctions.put("is_distinct", "is_distinct");

        this.handWritten.add("is_false");
        this.handWritten.add("is_not_true");
        this.handWritten.add("is_not_false");
        this.handWritten.add("is_true");
        this.handWritten.add("&&");
        this.handWritten.add("||");
        this.handWritten.add("min");
        this.handWritten.add("max");
        this.handWritten.add("/");
        this.handWritten.add("abs");
        this.handWritten.add("st_distance");
        this.handWritten.add("is_distinct");
        this.handWritten.add("is_not_distinct");

        this.doubleFunctions.put("eq", "==");
        this.doubleFunctions.put("neq", "!=");
        this.doubleFunctions.put("lt", "<");
        this.doubleFunctions.put("gt", ">");
        this.doubleFunctions.put("lte", "<=");
        this.doubleFunctions.put("gte", ">=");
        this.doubleFunctions.put("plus", "+");
        this.doubleFunctions.put("minus", "-");
        this.doubleFunctions.put("mod", "%");
        this.doubleFunctions.put("times", "*");
        this.doubleFunctions.put("div", "/");
        this.doubleFunctions.put("abs", "abs");
        this.arithmeticFunctions.put("is_distinct", "is_distinct");

        //this.stringFunctions.put("s_concat", "+");
        this.stringFunctions.put("eq", "==");
        this.stringFunctions.put("neq", "!=");
        this.arithmeticFunctions.put("is_distinct", "is_distinct");

        this.booleanFunctions.put("eq", "==");
        this.booleanFunctions.put("neq", "!=");
        this.booleanFunctions.put("and", "&&");
        this.booleanFunctions.put("or", "||");
        this.booleanFunctions.put("min", "min");
        this.booleanFunctions.put("max", "max");
        this.booleanFunctions.put("is_false", "is_false");
        this.booleanFunctions.put("is_not_true", "is_not_true");
        this.booleanFunctions.put("is_true", "is_true");
        this.booleanFunctions.put("is_not_false", "is_not_false");
        this.arithmeticFunctions.put("is_distinct", "is_distinct");

        this.comparisons.add("==");
        this.comparisons.add("!=");
        this.comparisons.add(">=");
        this.comparisons.add("<=");
        this.comparisons.add(">");
        this.comparisons.add("<");
        this.comparisons.add("is_distinct");
    }

    boolean isComparison(String op) {
        return this.comparisons.contains(op);
    }

    public static class FunctionDescription {
        public final String function;
        public final DBSPType returnType;

        public FunctionDescription(String function, DBSPType returnType) {
            this.function = function;
            this.returnType = returnType;
        }

        public DBSPApplyExpression getCall(DBSPExpression... arguments) {
            return new DBSPApplyExpression(this.function, this.returnType, arguments);
        }
    }
    
    public FunctionDescription getFunction(
            String op, DBSPType ltype, @Nullable DBSPType rtype, boolean aggregate) {
        HashMap<String, String> map = null;
        DBSPType returnType;
        boolean anyNull = ltype.mayBeNull || (rtype != null && rtype.mayBeNull);

        returnType = ltype.setMayBeNull(anyNull);
        if (ltype.as(DBSPTypeBool.class) != null) {
            map = this.booleanFunctions;
        } else if (ltype.is(IsNumericType.class)) {
            map = this.arithmeticFunctions;
        } else if (ltype.is(DBSPTypeString.class)){
            map = this.stringFunctions;
        }
        if (isComparison(op))
            returnType = DBSPTypeBool.instance.setMayBeNull(anyNull);
        if (op.equals("/"))
            // Always, for division by 0
            returnType = returnType.setMayBeNull(true);
        if (op.equals("is_true") || op.equals("is_not_true") ||
                op.equals("is_false") || op.equals("is_not_false") ||
                op.equals("is_distinct"))
            returnType = DBSPTypeBool.instance;

        String suffixl = ltype.mayBeNull ? "N" : "";
        String suffixr = rtype == null ? "" : (rtype.mayBeNull ? "N" : "");
        String tsuffixl;
        String tsuffixr;
        if (aggregate || op.equals("is_distinct")) {
            tsuffixl = "";
            tsuffixr = "";
        } else if (op.equals("st_distance")) {
            return new FunctionDescription("st_distance_" + suffixl + "_" + suffixr, DBSPTypeDouble.instance.setMayBeNull(anyNull));
        } else {
            tsuffixl = ltype.to(IDBSPBaseType.class).shortName();
            tsuffixr = (rtype == null) ? "" : rtype.to(IDBSPBaseType.class).shortName();
        }
        if (map == null)
            throw new Unimplemented(op);
        for (String k: map.keySet()) {
            if (map.get(k).equals(op)) {
                return new FunctionDescription(k + "_" + tsuffixl + suffixl + "_" + tsuffixr + suffixr, returnType);
            }
        }
        throw new Unimplemented("Could not find `" + op + "` for type " + ltype);
    }

    void generateProgram() {
        List<IDBSPInnerDeclaration> declarations = new ArrayList<>();
        DBSPFunction.Argument arg = new DBSPFunction.Argument(
                "b", DBSPTypeBool.instance.setMayBeNull(true));
        declarations.add(
                new DBSPFunction("wrap_bool",
                        Collections.singletonList(arg),
                        DBSPTypeBool.instance,
                        new DBSPMatchExpression(
                                new DBSPVariableReference("b", arg.getNonVoidType()),
                                Arrays.asList(
                                    new DBSPMatchExpression.Case(
                                            DBSPTupleStructPattern.somePattern(
                                                    new DBSPIdentifierPattern("x")),
                                            new DBSPVariableReference("x", DBSPTypeBool.instance)
                                    ),
                                    new DBSPMatchExpression.Case(
                                            DBSPWildcardPattern.instance,
                                            new DBSPBoolLiteral(false)
                                    )
                                ),
                                DBSPTypeBool.instance)));

        DBSPType[] numericTypes = new DBSPType[] {
                DBSPTypeInteger.signed16,
                DBSPTypeInteger.signed32,
                DBSPTypeInteger.signed64,
        };
        DBSPType[] boolTypes = new DBSPType[] {
                DBSPTypeBool.instance
        };
        DBSPType[] stringTypes = new DBSPType[] {
                DBSPTypeString.instance
        };
        DBSPType[] fpTypes = new DBSPType[] {
                DBSPTypeDouble.instance,
                DBSPTypeFloat.instance
        };

        for (HashMap<String, String> h: Arrays.asList(
                this.arithmeticFunctions, this.booleanFunctions, this.stringFunctions, this.doubleFunctions)) {
            for (String f : h.keySet()) {
                String op = h.get(f);
                if (this.handWritten.contains(op))
                    // Hand-written rules in a separate library
                    continue;
                for (int i = 0; i < 4; i++) {
                    DBSPType leftType;
                    DBSPType rightType;
                    DBSPType[] raw;
                    DBSPType withNull;
                    if (h.equals(this.stringFunctions)) {
                        raw = stringTypes;
                    } else if (h == this.booleanFunctions) {
                        raw = boolTypes;
                    } else if (h == this.doubleFunctions) {
                        raw = fpTypes;
                    } else {
                        raw = numericTypes;
                    }
                    for (DBSPType rawType: raw) {
                        if (op.equals("%") && rawType.is(DBSPTypeFP.class))
                            continue;
                        withNull = rawType.setMayBeNull(true);
                        DBSPPattern leftMatch = new DBSPIdentifierPattern("l");
                        DBSPPattern rightMatch = new DBSPIdentifierPattern("r");
                        if ((i & 1) == 1) {
                            leftType = withNull;
                            leftMatch = DBSPTupleStructPattern.somePattern(leftMatch);
                        } else {
                            leftType = rawType;
                        }
                        if ((i & 2) == 2) {
                            rightType = withNull;
                            rightMatch = DBSPTupleStructPattern.somePattern(rightMatch);
                        } else {
                            rightType = rawType;
                        }
                        /*
                        fn add_i32N_i32N(left: Option<i32>, right: Option<i32>): Option<i32> =
                        match ((left, right)) {
                            (Some{a}, Some{b}) -> Some{a + b},
                            (_, _)             -> None
                        }
                        */

                        // The general rule is: if any operand is NULL, the result is NULL.
                        FunctionDescription function = this.getFunction(op, leftType, rightType, false);
                        DBSPFunction.Argument left = new DBSPFunction.Argument("left", leftType);
                        DBSPFunction.Argument right = new DBSPFunction.Argument("right", rightType);
                        DBSPType type = function.returnType;
                        DBSPExpression def;
                        if (i == 0) {
                            DBSPExpression leftVar = new DBSPVariableReference("left", rawType);
                            DBSPExpression rightVar = new DBSPVariableReference("right", rawType);
                            def = new DBSPBinaryExpression(type, op, leftVar, rightVar);
                        } else {
                            def = new DBSPBinaryExpression(type, op,
                                    new DBSPVariableReference("l", rawType),
                                    new DBSPVariableReference("r", rawType));
                            def = new DBSPMatchExpression(
                                    new DBSPRawTupleExpression(
                                            new DBSPVariableReference("left", leftType),
                                            new DBSPVariableReference("right", rightType)),
                                    Arrays.asList(
                                            new DBSPMatchExpression.Case(
                                                    new DBSPTuplePattern(leftMatch, rightMatch),
                                                    new DBSPSomeExpression(def)),
                                            new DBSPMatchExpression.Case(
                                                    new DBSPTuplePattern(
                                                            DBSPWildcardPattern.instance,
                                                            DBSPWildcardPattern.instance),
                                                    DBSPLiteral.none(type))),
                                    type);
                        }
                        DBSPFunction func = new DBSPFunction(function.function, Arrays.asList(left, right), type, def);
                        func.addAnnotation("#[inline(always)]");
                        declarations.add(func);
                    }
                }
            }
        }
        this.program = new DBSPFile(declarations);
    }

    /**
     * Writes in the specified file the Rust code for the SQL runtime.
     * @param filename   File to write the code to.
     */
    public void writeSqlLibrary(String filename) throws IOException {
        this.generateProgram();
        File file = new File(filename);
        FileWriter writer = new FileWriter(file);
        if (this.program == null)
            throw new RuntimeException("No source program for writing the sql library");
        writer.append("// Automatically-generated file\n");
        writer.append("#![allow(unused_parens)]\n");
        writer.append("#![allow(non_snake_case)]\n");
        writer.append("use dbsp::algebra::{F32, F64};\n");
        writer.append("\n");
        writer.append(ToRustVisitor.toRustString(this.program));
        writer.close();
    }
}
