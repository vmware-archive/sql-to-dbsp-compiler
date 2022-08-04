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
import org.dbsp.sqllogictest.SqlTestOutputDescription;
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
                                new DBSPVariableReference("b", arg.getNonVoidType()),
                                Arrays.asList(
                                    new DBSPMatchExpression.Case(
                                            new DBSPConstructorExpression("Some",
                                                    arg.getNonVoidType(),
                                                    new DBSPVariableReference("x", DBSPTypeBool.instance)),
                                            new DBSPVariableReference("x", DBSPTypeBool.instance)
                                    ),
                                    new DBSPMatchExpression.Case(
                                            new DBSPConstructorExpression("_", arg.getNonVoidType()),
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
                if (op.equals("&&") || op.equals("||"))
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

                        // The general rule is: if any operand is NULL, the result is NULL.
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
                        func.addAnnotation("#[inline(always)]");
                        declarations.add(func);
                    }
                }
            }
        }
        this.program = new DBSPFile(null, declarations);
    }

    /**
     * Writes in the specified file the Rust code for the SQL runtime.
     * @param filename   File to write the code to.
     */
    public void writeSqlLibrary(String filename) throws IOException {
        File file = new File(filename);
        FileWriter writer = new FileWriter(file);
        IndentStringBuilder builder = new IndentStringBuilder();
        if (this.program == null)
            throw new RuntimeException("No source program for writing the sql library");
        builder.append("// Automatically-generated file\n");
        builder.append("#![allow(unused_parens)]\n");
        builder.append("use ordered_float::OrderedFloat;\n");
        builder.append("\n");
        this.program.toRustString(builder);
        writer.append(builder.toString());
        writer.close();
    }

    /**
     * Generates a Rust function which tests a DBSP circuit.
     * @param name          Name of the generated function.
     * @param inputFunction Name of function which generates the input data.
     * @param circuit       DBSP circuit that will be tested.
     * @param output        Expected data from the circuit.
     * @param description   Description of the expected outputs.
     * @return              The code for a function that runs the circuit with the specified
     *                      input and tests the produced output.
     */
    public static DBSPFunction createTesterCode(
            String name,
            String inputFunction,
            DBSPCircuit circuit,
            @Nullable DBSPZSetLiteral output,
            SqlTestOutputDescription description) {
        List<DBSPExpression> list = new ArrayList<>();
        list.add(new DBSPLetExpression("circuit",
                new DBSPApplyExpression(circuit.name, DBSPTypeAny.instance), true));
        DBSPType outputType = output != null ? output.getNonVoidType() : DBSPTypeAny.instance;
        DBSPExpression[] arguments = new DBSPExpression[circuit.getInputTables().size()];

        list.add(new DBSPLetExpression("_in",
                new DBSPApplyExpression(inputFunction, DBSPTypeAny.instance)));
        if (arguments.length > 1) {
            for (int i = 0; i < arguments.length; i++) {
                arguments[i] = new DBSPFieldExpression(null,
                        new DBSPVariableReference("_in", DBSPTypeAny.instance), i, DBSPTypeAny.instance);
            }
        } else {
            arguments[0] = new DBSPVariableReference("_in", DBSPTypeAny.instance);
        }
        list.add(new DBSPLetExpression("output",
                new DBSPApplyExpression("circuit", outputType, arguments)));

        DBSPExpression sort = new DBSPEnumValue("SortOrder", description.order.toString());
        if (output != null) {
            if (description.columnTypes != null) {
                DBSPExpression columnTypes = new DBSPLiteral(description.columnTypes);
                DBSPTypeZSet otype = outputType.to(DBSPTypeZSet.class);
                DBSPExpression zset_to_strings = new DBSPQualifyTypeExpression(
                        new DBSPVariableReference("zset_to_strings", DBSPTypeAny.instance),
                        otype.elementType,
                        otype.weightType
                );
                list.add(new DBSPApplyExpression("assert_eq!", null,
                        new DBSPApplyExpression("zset_to_strings", DBSPTypeAny.instance,
                                new DBSPRefExpression(
                                        new DBSPVariableReference("output", outputType)),
                                columnTypes,
                                sort),
                        new DBSPApplyExpression(zset_to_strings, DBSPTypeAny.instance,
                                new DBSPRefExpression(output),
                                columnTypes,
                                sort)));
            } else {
                list.add(new DBSPApplyExpression("assert_eq!", null,
                        new DBSPVariableReference("output", output.getNonVoidType()),
                        output));
            }
        } else {
            if (description.columnTypes == null)
                throw new RuntimeException("Expected column types to be supplied");
            DBSPExpression columnTypes = new DBSPLiteral(description.columnTypes);
            if (description.hash == null)
                throw new RuntimeException("Expected hash to be supplied");
            list.add(new DBSPLetExpression("_hash",
                    new DBSPApplyExpression("hash", DBSPTypeString.instance,
                            new DBSPRefExpression(
                                    new DBSPVariableReference("output", DBSPTypeAny.instance)),
                            columnTypes,
                            sort)));
            list.add(new DBSPApplyExpression("assert_eq!", null,
                    new DBSPVariableReference("_hash", DBSPTypeString.instance),
                    new DBSPLiteral(description.hash)));
            list.add(new DBSPTupleExpression());
        }
        if (description.getExpectedOutputSize() >= 0) {
            list.add(new DBSPApplyExpression("assert_eq!", null,
                    new DBSPApplyMethodExpression("weighted_count",
                            DBSPTypeUSize.instance,
                            new DBSPVariableReference("output", DBSPTypeAny.instance)),
                    new DBSPLiteral(description.getExpectedOutputSize())));
        }
        DBSPExpression body = new DBSPSeqExpression(list);
        return new DBSPFunction(name, new ArrayList<>(), null, body);
    }
}
