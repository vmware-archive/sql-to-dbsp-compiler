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

package org.dbsp.sqllogictest;

import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.rust.DBSPFunction;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.dbsp.rust.statement.DBSPStatement;
import org.dbsp.sqlCompiler.dbsp.rust.type.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates Rust functions which produce inputs and compare outputs for a query.
 */
public class RustTestGenerator {
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
        List<DBSPStatement> list = new ArrayList<>();
        list.add(new DBSPLetStatement("circuit",
                new DBSPApplyExpression(circuit.name, DBSPTypeAny.instance), true));
        DBSPType circuitOutputType = circuit.getOutputType(0);
        // the following may not be the same, since SqlLogicTest sometimes lies about the output type
        DBSPType outputType = output != null ? new DBSPTypeRawTuple(output.getNonVoidType()) : circuitOutputType;
        DBSPExpression[] arguments = new DBSPExpression[circuit.getInputTables().size()];
        // True if the output is a zset of vectors (generated for orderby queries)
        boolean isVector = circuitOutputType.to(DBSPTypeZSet.class).elementType.is(DBSPTypeVec.class);

        list.add(new DBSPLetStatement("_in",
                new DBSPApplyExpression(inputFunction, DBSPTypeAny.instance)));
        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = new DBSPFieldExpression(null,
                    new DBSPVariableReference("_in", DBSPTypeAny.instance), i);
        }
        list.add(new DBSPLetStatement("output",
                new DBSPApplyExpression("circuit", outputType, arguments)));

        DBSPExpression sort = new DBSPEnumValue("SortOrder", description.order.toString());
        DBSPExpression output0 = new DBSPFieldExpression(null,
                new DBSPVariableReference("output", DBSPTypeAny.instance), 0);

        if (description.getExpectedOutputSize() >= 0) {
            DBSPExpression count;
            if (isVector) {
                count = new DBSPApplyExpression("weighted_vector_count",
                        DBSPTypeUSize.instance,
                        new DBSPBorrowExpression(output0));
            } else {
                count = new DBSPApplyMethodExpression("weighted_count",
                        DBSPTypeUSize.instance,
                        output0);
            }
            list.add(new DBSPExpressionStatement(
                    new DBSPApplyExpression("assert_eq!", null,
                            count, new DBSPISizeLiteral(description.getExpectedOutputSize()))));
        }if (output != null) {
            if (description.columnTypes != null) {
                DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
                DBSPTypeZSet oType = output.getNonVoidType().to(DBSPTypeZSet.class);
                String functionProducingStrings;
                DBSPType elementType;
                if (isVector) {
                    functionProducingStrings = "zset_of_vectors_to_strings";
                    elementType = oType.elementType.to(DBSPTypeVec.class).getElementType();
                } else {
                    functionProducingStrings = "zset_to_strings";
                    elementType = oType.elementType;
                }
                DBSPExpression zset_to_strings = new DBSPQualifyTypeExpression(
                        new DBSPVariableReference(functionProducingStrings, DBSPTypeAny.instance),
                        elementType,
                        oType.weightType
                );
                list.add(new DBSPExpressionStatement(
                        new DBSPApplyExpression("assert_eq!", null,
                                new DBSPApplyExpression(functionProducingStrings, DBSPTypeAny.instance,
                                        new DBSPBorrowExpression(output0),
                                        columnTypes,
                                        sort),
                                new DBSPApplyExpression(zset_to_strings,
                                        new DBSPBorrowExpression(output),
                                        columnTypes,
                                        sort))));
            } else {
                list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                        "assert_eq!", null, output0, output)));
            }
        } else {
            if (description.columnTypes == null)
                throw new RuntimeException("Expected column types to be supplied");
            DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
            if (description.hash == null)
                throw new RuntimeException("Expected hash to be supplied");
            String hash = isVector ? "hash_vectors" : "hash";
            list.add(new DBSPLetStatement("_hash",
                    new DBSPApplyExpression(hash, DBSPTypeString.instance,
                            new DBSPBorrowExpression(output0),
                            columnTypes,
                            sort)));
            list.add(
                    new DBSPExpressionStatement(
                            new DBSPApplyExpression("assert_eq!", null,
                                    new DBSPVariableReference("_hash", DBSPTypeString.instance),
                                    new DBSPStringLiteral(description.hash))));
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        return new DBSPFunction(name, new ArrayList<>(), null, body);
    }
}
