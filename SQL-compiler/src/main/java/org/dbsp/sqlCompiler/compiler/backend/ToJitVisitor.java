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

package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.IDBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.IModule;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Generates an encoding of the circuit as a JSON representation that
 * can be read by the JIT compiler from DBSP.
 */
public class ToJitVisitor extends CircuitVisitor implements IModule {
    static class TypeCatalog {
        public final Map<DBSPType, Integer> typeId;

        TypeCatalog() {
            this.typeId = new HashMap<>();
        }

        public int getTypeId(DBSPType type) {
            DBSPTypeRef ref = type.as(DBSPTypeRef.class);
            if (ref != null)
                type = ref.type;
            if (this.typeId.containsKey(type))
                return this.typeId.get(type);
            int result = this.typeId.size() + 1; // 0 is not a valid index
            this.typeId.put(type, result);
            return result;
        }

        /**
         * We are given a type that is a Tuple or RawTuple.
         * Expand it into a list of Tuple types as follows:
         * - if the type is a Tuple, just return a singleton list.
         * - if the type is a RawTuple, add all its components to the list.
         *   Each component is expected to be a Tuple.
         * This is used for functions like stream.index_with, which
         * take a closure that returns a tuple of values.  The JIT does
         * not support nested tuples.
         */
        public static List<DBSPTypeTuple> expandToTuples(DBSPType type) {
            List<DBSPTypeTuple> types = new ArrayList<>();
            if (type.is(DBSPTypeRawTuple.class)) {
                for (DBSPType field: type.to(DBSPTypeRawTuple.class).tupFields) {
                    DBSPTypeTuple tuple = makeTupleType(field);
                    types.add(tuple);
                }
            } else {
                types.add(type.to(DBSPTypeTuple.class));
            }
            return types;
        }
    }

    protected final TypeCatalog typeCatalog;
    protected final ObjectNode root;
    protected final ObjectMapper topMapper;
    protected final ObjectNode nodes;
    protected final ObjectNode structs;

    public ToJitVisitor() {
        super(true);
        this.topMapper = new ObjectMapper();
        this.root = this.topMapper.createObjectNode();
        this.nodes = this.topMapper.createObjectNode();
        this.root.set("nodes", this.nodes);
        this.structs = this.topMapper.createObjectNode();
        this.root.set("layouts", this.structs);
        this.typeCatalog = new TypeCatalog();
    }

    static String baseTypeName(DBSPType type) {
        if (type.sameType(new DBSPTypeTuple()))
            return "Unit";
        DBSPTypeBaseType base = type.as(DBSPTypeBaseType.class);
        if (base == null)
            throw new RuntimeException("Expected a base type, got " + type);
        switch (base.shortName()) {
            case "b":
                return "Bool";
            case "i16":
                return "I16";
            case "i32":
                return "I32";
            case "i64":
                return "I64";
            case "d":
                return "F64";
            case "f":
                return "F32";
            case "s":
                return "String";
            case "date":
                return "Date";
            case "timestamp":
                return "Timestamp";
            default:
                break;
        }
        throw new Unimplemented(type);
    }

    void addTuple(DBSPTypeTupleBase type, int id) {
        ObjectNode result = this.topMapper.createObjectNode();
        if (type.tupFields.length == 0) {
            result.put("ty", "Unit");
            return;
        }
        ArrayNode columns = this.topMapper.createArrayNode();
        result.set("columns", columns);
        this.structs.set(Integer.toString(id), result);
        for (DBSPType colType: type.tupFields) {
            ObjectNode col = this.topMapper.createObjectNode();
            col.put("nullable", colType.mayBeNull);
            String typeName = baseTypeName(colType);
            col.put("ty", typeName);
            columns.add(col);
        }
    }

    public int addElementType(DBSPType type) {
        DBSPTypeZSet set = type.as(DBSPTypeZSet.class);
        if (set == null)
            throw new RuntimeException("Expected a ZSet type, got " + type);
        DBSPType elementType = set.elementType;
        DBSPTypeTuple tuple = elementType.as(DBSPTypeTuple.class);
        if (tuple == null)
            throw new RuntimeException("Expected ZSet element type to be a tuple, got " + tuple);
        return this.typeCatalog.getTypeId(tuple);
    }

    @Override
    public boolean preorder(DBSPSourceOperator operator) {
        ObjectNode node = this.topMapper.createObjectNode();
        DBSPType type = operator.getNonVoidType();
        int typeId = this.addElementType(type);
        ObjectNode data = this.topMapper.createObjectNode();
        data.put("layout", typeId);
        node.set("Source", data);
        this.nodes.set(Long.toString(operator.id), node);
        return false;
    }

    // Indexed with the number of inputs of the operator.
    static final String[][] OPERATOR_INPUT_NAMES = new String[][] {
            {},
            { "input" },
            { "lhs", "rhs" },
    };

    void addInputs(ObjectNode node, DBSPOperator operator) {
        String[] names = OPERATOR_INPUT_NAMES[operator.inputs.size()];
        int index = 0;
        for (DBSPOperator sources: operator.inputs) {
            String name = names[index++];
            node.put(name, sources.id);
        }
    }

    /**
     * Convert the type into a tuple type.
     * Type may be either a reference type, or a RawTuple, or a normal Tuple.
     */
    static DBSPTypeTuple makeTupleType(DBSPType type) {
        DBSPTypeRef ref = type.as(DBSPTypeRef.class);
        if (ref != null)
            type = ref.type;
        if (type.is(DBSPTypeTuple.class))
            return type.to(DBSPTypeTuple.class);
        else if (type.is(DBSPTypeRawTuple.class))
            return new DBSPTypeTuple(type.to(DBSPTypeRawTuple.class).tupFields);
        throw new Unimplemented("Convertion to Tuple", type);
    }

    ObjectNode createFunction(DBSPClosureExpression function) {
        ObjectNode result = this.topMapper.createObjectNode();
        DBSPType resultType = function.getResultType();
        ArrayNode params = this.topMapper.createArrayNode();
        result.set("args", params);
        int index = 1;
        for (DBSPParameter param: function.parameters) {
            ObjectNode paramNode = this.topMapper.createObjectNode();
            params.add(paramNode);
            paramNode.put("id", index);
            DBSPTypeTuple type = makeTupleType(param.getNonVoidType());
            int typeId = this.typeCatalog.getTypeId(type);
            paramNode.put("layout", typeId);
            paramNode.put("flags", "input");
            index++;
        }
        // If the result type is a scalar, it is marked as a result type.
        // Otherwise, we have to create a new parameter that is returned by reference.
        if (isScalarType(resultType)) {
            if (resultType == null) {
                result.put("ret", "Unit");
            } else {
                DBSPTypeBaseType base = resultType.to(DBSPTypeBaseType.class);
                result.put("ret", baseTypeName(base));
            }
        } else {
            result.put("ret", "Unit");
            List<DBSPTypeTuple> types = TypeCatalog.expandToTuples(resultType);
            for (DBSPTypeTuple type: types) {
                ObjectNode paramNode = this.topMapper.createObjectNode();
                params.add(paramNode);
                paramNode.put("id", index);
                int typeId = this.typeCatalog.getTypeId(type);
                paramNode.put("layout", typeId);
                paramNode.put("flags", "output");
                index++;
            }
        }
        result.put("entry_block", 1);
        ObjectNode blocks = this.topMapper.createObjectNode();
        ToJitInnerVisitor.convertClosure(function, blocks, this.typeCatalog);
        result.set("blocks", blocks);
        return result;
    }

    public static boolean isScalarType(@Nullable DBSPType type) {
        return type == null || type.is(DBSPTypeBaseType.class);
    }

    void addTypes() {
        for (Map.Entry<DBSPType, Integer> entry: this.typeCatalog.typeId.entrySet()) {
            DBSPType type = entry.getKey();
            if (type.is(DBSPTypeRef.class))
                type = type.to(DBSPTypeRef.class).deref();
            DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
            this.addTuple(tuple, entry.getValue());
        }
    }

    ObjectNode createOperator(DBSPOperator operator, String kind, String function) {
        ObjectNode node = this.topMapper.createObjectNode();
        ObjectNode data = node.putObject(kind);
        this.addInputs(data, operator);
        if (operator.function != null) {
            DBSPExpression func = operator.function;
            if (operator.function.is(DBSPVariablePath.class)) {
                DBSPVariablePath path = operator.function.to(DBSPVariablePath.class);
                IDBSPDeclaration definition = this.getCircuit().circuit.getDefinition(path.variable);
                DBSPLetStatement stat = definition.to(DBSPLetStatement.class);
                func = Objects.requireNonNull(stat.initializer);
            }
            DBSPClosureExpression closure = func.to(DBSPClosureExpression.class);
            ObjectNode funcNode = this.createFunction(closure);
            data.set(function, funcNode);
        }
        this.nodes.set(Long.toString(operator.id), node);
        return data;
    }

    @Override
    public boolean preorder(DBSPFilterOperator operator) {
        this.createOperator(operator, "Filter", "filter_fn");
        return false;
    }

    @Override
    public boolean preorder(DBSPMapOperator operator) {
        ObjectNode map = this.createOperator(operator, "Map", "map_fn");
        DBSPType type = operator.outputElementType;
        map.put("layout", this.typeCatalog.getTypeId(type));
        return false;
    }

    @Override
    public boolean preorder(DBSPSumOperator operator) {
        this.createOperator(operator, "Sum", "");
        return false;
    }

    @Override
    public boolean preorder(DBSPDistinctOperator operator) {
        this.createOperator(operator, "Distinct", "");
        return false;
    }

    @Override
    public boolean preorder(DBSPSubtractOperator operator) {
        this.createOperator(operator, "Minus", "");
        return false;
    }

    @Override
    public boolean preorder(DBSPIntegralOperator operator) {
        this.createOperator(operator, "Integrate", "");
        return false;
    }

    @Override
    public boolean preorder(DBSPDifferentialOperator operator) {
        this.createOperator(operator, "Differentiate", "");
        return false;
    }

    @Override
    public boolean preorder(DBSPIndexOperator operator) {
        ObjectNode node = this.createOperator(operator, "IndexWith", "index_fn");
        node.put("key_layout", this.typeCatalog.getTypeId(operator.keyType));
        node.put("value_layout", this.typeCatalog.getTypeId(operator.elementType));
        return false;
    }

    public boolean preorder(DBSPJoinOperator operator) {
        ObjectNode node = this.createOperator(operator, "JoinCore", "join_fn");
        node.put("value_layout", this.typeCatalog.getTypeId(
                new DBSPTypeTuple(new DBSPTypeTuple())));
        node.put("key_layout", this.typeCatalog.getTypeId(operator.elementResultType));
        node.put("output_kind", "Set");
        return false;
    }

    @Override
    public boolean preorder(DBSPNegateOperator operator) {
        this.createOperator(operator, "Neg", "");
        return false;
    }

    @Override
    public boolean preorder(DBSPSinkOperator operator) {
        this.createOperator(operator, "Sink", "");
        return false;
    }

    @Override
    public boolean preorder(DBSPOperator operator) {
        throw new Unimplemented(operator);
    }

    public static String circuitToJSON(DBSPCircuit circuit) {
        JitNormalizeInnerVisitor norm = new JitNormalizeInnerVisitor();
        CircuitFunctionRewriter rewriter = new CircuitFunctionRewriter(norm);
        circuit = rewriter.apply(circuit);
        ToJitVisitor visitor = new ToJitVisitor();
        circuit.accept(visitor);
        visitor.addTypes();
        return visitor.root.toPrettyString();
    }

    public static void validateJson(DBSPCircuit circuit) {
        try {
            //System.out.println(ToRustVisitor.circuitToRustString(circuit));
            String json = ToJitVisitor.circuitToJSON(circuit);
            System.out.println(json);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);
            if (root == null)
                throw new RuntimeException("No JSON produced from circuit");
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
