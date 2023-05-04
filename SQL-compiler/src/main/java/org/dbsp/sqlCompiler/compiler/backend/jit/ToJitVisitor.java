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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITFunction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITParameterMapping;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITProgram;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg.JITBlock;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITTupleLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITZSetLiteral;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.*;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.*;
import org.dbsp.sqlCompiler.compiler.backend.optimize.BetaReduction;
import org.dbsp.sqlCompiler.compiler.backend.optimize.Simplify;
import org.dbsp.sqlCompiler.compiler.backend.visitors.PassesVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Generates an encoding of the circuit as a JITProgram representation.
 */
public class ToJitVisitor extends CircuitVisitor implements IModule {
    JITProgram program;

    public ToJitVisitor() {
        super(true);
        this.program = new JITProgram();
    }
    
    public TypeCatalog getTypeCatalog() {
        return this.program.typeCatalog;
    }

    /**
     * Eliminate the Weight type and replace it with its definition.
     */
    @Nullable
    public static DBSPType resolveWeightType(@Nullable DBSPType type) {
        if (type == null)
            return null;
        if (type.is(DBSPTypeUser.class)) {
            DBSPTypeUser user = type.to(DBSPTypeUser.class);
            if (user.name.equals(TypeCompiler.WEIGHT_TYPE_NAME))
                type = TypeCompiler.WEIGHT_TYPE_IMPLEMENTATION;
        }
        return type;
    }

    class OperatorConversion {
        final @Nullable JITFunction function;
        final List<JITOperatorReference> inputs;
        final JITRowType type;

        public OperatorConversion(DBSPOperator operator) {
            this.type = ToJitVisitor.this.getTypeCatalog().convertTupleType(operator.getOutputZSetElementType());
            if (operator.function != null) {
                DBSPExpression func = ToJitVisitor.this.resolve(operator.function);
                this.function = ToJitVisitor.this.convertFunction(func.to(DBSPClosureExpression.class));
            } else {
                this.function = null;
            }
            this.inputs = Linq.map(operator.inputs, i -> new JITOperatorReference(i.id));
        }

        public JITFunction getFunction() { return Objects.requireNonNull(this.function); }
    }

    JITFunction convertFunction(DBSPClosureExpression function) {
        Logger.INSTANCE.from(this, 4)
                .append("Converting to JIT")
                .newline()
                .append(function.toString())
                .newline();
        DBSPType resultType = function.getResultType();
        JITParameterMapping mapping = new JITParameterMapping(this.getTypeCatalog());

        for (DBSPParameter param: function.parameters)
            mapping.addInputParameter(param);

        // If the result type is a scalar, it is marked as a result type.
        // Otherwise, we have to create a new parameter that is returned by reference.
        JITScalarType returnType = mapping.addReturn(resultType);

        List<JITBlock> blocks = ToJitInnerVisitor.convertClosure(mapping, function, this.getTypeCatalog());
        JITFunction result = new JITFunction(mapping.allParameters, blocks, returnType);
        Logger.INSTANCE.from(this, 4)
                .append(result.toAssembly())
                .newline();
        return result;
    }

    @Override
    public boolean preorder(DBSPSourceOperator operator) {
        JITRowType type = this.getTypeCatalog().convertTupleType(operator.getOutputZSetElementType());
        JITSourceOperator source = new JITSourceOperator(operator.id, type, operator.outputName);
        this.program.add(source);
        return false;
    }

    public static boolean isScalarType(@Nullable DBSPType type) {
        type = resolveWeightType(type);
        if (type == null)
            return true;
        if (type.is(DBSPTypeBaseType.class))
            return true;
        return type.is(DBSPTypeTupleBase.class) && type.to(DBSPTypeTupleBase.class).size() == 0;
    }

    @Override
    public boolean preorder(DBSPFilterOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITFilterOperator result = new JITFilterOperator(operator.id, conversion.type,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPMapIndexOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITRowType keyType = this.getTypeCatalog().convertTupleType(operator.keType);
        JITRowType valueType = this.getTypeCatalog().convertTupleType(operator.valueType);
        JITOperator result = new JITMapIndexOperator(operator.id,
                keyType, valueType, conversion.type,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPMapOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITRowType inputType = this.getTypeCatalog().convertTupleType(
                operator.input().getOutputZSetElementType());
        JITOperator result = new JITMapOperator(operator.id, conversion.type, inputType,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPFlatMapOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITFlatMapOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPSumOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITSumOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPDistinctOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITDistinctOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPSubtractOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITSubtractOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPIntegralOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITIntegrateOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPDifferentialOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITDifferentiateOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPIndexOperator operator) {
        DBSPExpression func = ToJitVisitor.this.resolve(operator.getFunction());
        JITFunction function = ToJitVisitor.this.convertFunction(func.to(DBSPClosureExpression.class));
        List<JITOperatorReference> inputs = Linq.map(operator.inputs, i -> new JITOperatorReference(i.id));

        JITRowType keyType = this.getTypeCatalog().convertTupleType(operator.keyType);
        JITRowType valueType = this.getTypeCatalog().convertTupleType(operator.elementType);
        JITOperator result = new JITIndexWithOperator(operator.id,
                keyType, valueType, inputs, function);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPJoinOperator operator) {
        JITRowType keyType = this.getTypeCatalog().convertTupleType(operator.elementResultType);
        JITRowType valueType = this.getTypeCatalog().convertTupleType(new DBSPTypeTuple(new DBSPTypeTuple()));
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITJoinOperator(operator.id, keyType, valueType, conversion.type,
                conversion.inputs, conversion.getFunction());
        this.program.add(result);
        return false;
    }

    /**
     * Given a closure expression, convert each parameter with a scalar type
     * into a parameter with a 1-dimensional tuple type.  E.g.
     * (t: Tuple1<i32>, u: i32) { body }
     * is converted to
     * (t: Tuple1<i32>, u: tuple1<i32>) { let u: i32 = u.0; body }
     * Pre-condition: the closure body is a BlockExpression.
     * The JIT representation only supports tuples for closure parameters.
     */
    DBSPClosureExpression tupleEachParameter(DBSPClosureExpression closure) {
        List<DBSPStatement> statements = new ArrayList<>();
        DBSPParameter[] newParams = new DBSPParameter[closure.parameters.length];
        int index = 0;
        for (DBSPParameter param: closure.parameters) {
            if (isScalarType(param.type)) {
                DBSPParameter tuple = new DBSPParameter(param.pattern, new DBSPTypeRawTuple(param.type));
                statements.add(new DBSPLetStatement(
                        tuple.asVariableReference().variable,
                        tuple.asVariableReference().field(0)));
                newParams[index] = tuple;
            } else {
                newParams[index] = param;
            }
            index++;
        }
        if (statements.isEmpty())
            return closure;
        DBSPBlockExpression block = closure.body.to(DBSPBlockExpression.class);
        statements.addAll(block.contents);
        DBSPBlockExpression newBlock = new DBSPBlockExpression(
                statements,
                block.lastExpression);
        return newBlock.closure(newParams);
    }

    @Override
    public boolean preorder(DBSPAggregateOperator operator) {
        if (operator.function != null)
            throw new RuntimeException("Didn't expect the Aggregate to have a function");

        List<JITOperatorReference> inputs = Linq.map(
                operator.inputs, i -> new JITOperatorReference(i.id));
        JITRowType outputType = this.getTypeCatalog().convertTupleType(operator.outputElementType);

        DBSPAggregate aggregate = operator.getAggregate();
        DBSPExpression initial = this.resolve(aggregate.getZero());
        DBSPTupleExpression elementValue = initial.to(DBSPTupleExpression.class);
        JITTupleLiteral init = new JITTupleLiteral(elementValue);

        DBSPClosureExpression closure = aggregate.getIncrement();
        BetaReduction reducer = new BetaReduction();
        IDBSPInnerNode reduced = reducer.apply(closure);
        closure = Objects.requireNonNull(reduced).to(DBSPClosureExpression.class);
        closure = this.tupleEachParameter(closure);
        JITFunction stepFn = this.convertFunction(closure);

        closure = aggregate.getPostprocessing();
        reduced = reducer.apply(closure);
        closure = Objects.requireNonNull(reduced).to(DBSPClosureExpression.class);
        closure = this.tupleEachParameter(closure);
        JITFunction finishFn = this.convertFunction(closure);

        JITRowType accLayout = this.getTypeCatalog().convertTupleType(aggregate.defaultZeroType());
        JITRowType stepLayout = this.getTypeCatalog().convertTupleType(
                Objects.requireNonNull(aggregate.getIncrement().getResultType()));
        JITOperator result = new JITAggregateOperator(
                operator.id, accLayout, stepLayout, outputType,
                inputs, init, stepFn, finishFn);
        this.program.add(result);

        return false;
    }

    @Override
    public boolean preorder(DBSPNegateOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITNegOperator(operator.id, conversion.type, conversion.inputs);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPSinkOperator operator) {
        OperatorConversion conversion = new OperatorConversion(operator);
        JITOperator result = new JITSinkOperator(operator.id, conversion.type, conversion.inputs, operator.query);
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPConstantOperator operator) {
        JITRowType type = this.getTypeCatalog().convertTupleType(operator.getOutputZSetElementType());
        DBSPZSetLiteral setValue = Objects.requireNonNull(operator.function)
                .to(DBSPZSetLiteral.class);
        JITZSetLiteral setLiteral = new JITZSetLiteral(setValue, type);
        JITOperator result = new JITConstantOperator(
                operator.id, type, setLiteral, operator.getFunction().toString());
        this.program.add(result);
        return false;
    }

    @Override
    public boolean preorder(DBSPOperator operator) {
        throw new Unimplemented(operator);
    }

    public static JITProgram circuitToJIT(DBSPCircuit circuit) {
        PassesVisitor rewriter = new PassesVisitor();
        rewriter.add(new BlockClosures());
        rewriter.add(new Simplify().circuitRewriter());
        circuit = rewriter.apply(circuit);
        Logger.INSTANCE.from("ToJitVisitor", 2)
                .append("Converting to JIT")
                .newline()
                .append(circuit.toString());
        ToJitVisitor visitor = new ToJitVisitor();
        visitor.apply(circuit);
        return visitor.program;
    }

    public static void validateJson(DBSPCircuit circuit) {
        try {
            JITProgram program = ToJitVisitor.circuitToJIT(circuit);
            Logger.INSTANCE.from("ToJitVisitor", 2)
                    .append(program.toAssembly())
                    .newline();
            String json = program.asJson().toPrettyString();
            Logger.INSTANCE.from("ToJitVisitor", 2)
                    .append(json);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);
            if (root == null)
                throw new RuntimeException("No JSON produced from circuit");
            File jsonFile = File.createTempFile("out", ".json", new File("."));
            jsonFile.deleteOnExit();
            PrintWriter writer = new PrintWriter(jsonFile);
            writer.println(json);
            writer.close();
            Utilities.compileAndTestJit("../../database-stream-processor", jsonFile);
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
