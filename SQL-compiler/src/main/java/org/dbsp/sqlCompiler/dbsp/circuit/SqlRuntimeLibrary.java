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

package org.dbsp.sqlCompiler.dbsp.circuit;

import org.dbsp.sqlCompiler.dbsp.circuit.expression.*;
import org.dbsp.sqlCompiler.dbsp.circuit.type.*;
import org.dbsp.util.IndentStringBuilder;
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

        //this.stringFunctions.put("s_concat", "+");
        this.stringFunctions.put("eq", "==");
        this.stringFunctions.put("neq", "!=");

        this.booleanFunctions.put("eq", "==");
        this.booleanFunctions.put("neq", "!=");
        this.booleanFunctions.put("and", "&&");
        this.booleanFunctions.put("or", "||");

        this.comparisons.add("==");
        this.comparisons.add("!=");
        this.comparisons.add(">=");
        this.comparisons.add("<=");
        this.comparisons.add(">");
        this.comparisons.add("<");
        this.generateProgram();
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
    }
    
    public FunctionDescription getFunction(String op, DBSPType ltype, @Nullable DBSPType rtype) {
        HashMap<String, String> map;
        DBSPType returnType;
        boolean anyNull = ltype.mayBeNull || (rtype != null && rtype.mayBeNull);

        returnType = ltype.setMayBeNull(anyNull);
        if (ltype.as(DBSPTypeBool.class) != null) {
            map = this.booleanFunctions;
        } else if (ltype.is(IsNumericType.class)) {
            map = this.arithmeticFunctions;
        } else {
            map = this.stringFunctions;
        }

        if (isComparison(op))
            returnType = DBSPTypeBool.instance.setMayBeNull(anyNull);
        String suffixl = ltype.mayBeNull ? "N" : "";
        String suffixr = rtype == null ? "" : (rtype.mayBeNull ? "N" : "");
        String tsuffixl = ltype.to(IDBSPBaseType.class).shortName();
        String tsuffixr = rtype == null ? "" : rtype.to(IDBSPBaseType.class).shortName();
        for (String k: map.keySet()) {
            if (map.get(k).equals(op)) {
                return new FunctionDescription(k + "_" + tsuffixl + suffixl + "_" + tsuffixr + suffixr, returnType);
            }
        }
        throw new Unimplemented("Could not find `" + op + "` for type " + ltype);
    }

    public static DBSPExpression wrapSome(DBSPExpression expr, DBSPType type) {
        return new DBSPConstructorExpression("Some", type, expr);
    }

    void generateProgram() {
        List<IDBSPDeclaration> declarations = new ArrayList<>();
        DBSPFunction.DBSPArgument arg = new DBSPFunction.DBSPArgument(
                "b", DBSPTypeBool.instance.setMayBeNull(true));
        declarations.add(
                new DBSPFunction("wrap_bool",
                        Collections.singletonList(arg),
                        DBSPTypeBool.instance,
                        new DBSPMatchExpression(
                                new DBSPVariableReference("b", arg.getType()),
                                Arrays.asList(
                                    new DBSPMatchExpression.Case(
                                            new DBSPConstructorExpression("Some",
                                                    arg.getType(),
                                                    new DBSPVariableReference("x", DBSPTypeBool.instance)),
                                            new DBSPVariableReference("x", DBSPTypeBool.instance)
                                    ),
                                    new DBSPMatchExpression.Case(
                                            new DBSPConstructorExpression("_", arg.getType()),
                                            new DBSPLiteral(false)
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
                        withNull = rawType.setMayBeNull(true);
                        DBSPExpression leftMatch = new DBSPVariableReference("l", rawType);
                        DBSPExpression rightMatch = new DBSPVariableReference("r", rawType);
                        if ((i & 1) == 1) {
                            leftType = withNull;
                            leftMatch = wrapSome(leftMatch, leftType);
                        } else {
                            leftType = rawType;
                        }
                        if ((i & 2) == 2) {
                            rightType = withNull;
                            rightMatch = wrapSome(rightMatch, rightType);
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

                        FunctionDescription function = this.getFunction(op, leftType, rightType);
                        DBSPFunction.DBSPArgument left = new DBSPFunction.DBSPArgument("left", leftType);
                        DBSPFunction.DBSPArgument right = new DBSPFunction.DBSPArgument("right", rightType);
                        DBSPType type = function.returnType;
                        DBSPExpression def;
                        if (i == 0) {
                            def = new DBSPBinaryExpression(null, type, op,
                                    new DBSPVariableReference("left", rawType),
                                    new DBSPVariableReference("right", rawType));
                        } else {
                            def = new DBSPBinaryExpression(null, type, op,
                                    new DBSPVariableReference("l", rawType),
                                    new DBSPVariableReference("r", rawType));
                            def = new DBSPMatchExpression(
                                    new DBSPRawTupleExpression(
                                            new DBSPVariableReference("left", leftType),
                                            new DBSPVariableReference("right", rightType)),
                                    Arrays.asList(
                                            new DBSPMatchExpression.Case(
                                                    new DBSPRawTupleExpression(leftMatch, rightMatch),
                                                    wrapSome(def, type)),
                                            new DBSPMatchExpression.Case(
                                                    new DBSPRawTupleExpression(
                                                            new DBSPDontCare(leftType),
                                                            new DBSPDontCare(rightType)),
                                                    new DBSPLiteral(type))),
                                    type);
                        }
                        DBSPFunction func = new DBSPFunction(function.function, Arrays.asList(left, right), type, def);
                        declarations.add(func);
                    }
                }
            }
        }
        this.program = new DBSPFile(null, declarations);
    }

    public void writeSqlLibrary(String filename) throws IOException {
        File file = new File(filename);
        FileWriter writer = new FileWriter(file);
        IndentStringBuilder builder = new IndentStringBuilder();
        if (this.program == null)
            throw new RuntimeException("No source program for writing the sql library");
        builder.append("#![allow(unused_parens)]\n");
        builder.append("use ordered_float::OrderedFloat;\n");
        builder.append("\n");
        this.program.toRustString(builder);
        writer.append(builder.toString());
        writer.close();
    }
}