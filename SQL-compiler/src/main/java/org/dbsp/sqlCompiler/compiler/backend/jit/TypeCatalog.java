package org.dbsp.sqlCompiler.compiler.backend.jit;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps each tuple type to an integer id.
 */
public class TypeCatalog {
    public final Map<DBSPType, JITRowType> typeId;

    public TypeCatalog() {
        this.typeId = new HashMap<>();
    }

    public JITRowType convertType(DBSPType type) {
        if (type.is(DBSPTypeRef.class))
            type = type.to(DBSPTypeRef.class).type;
        DBSPTypeTupleBase tuple = type.to(DBSPTypeTupleBase.class);
        if (this.typeId.containsKey(tuple))
            return this.typeId.get(tuple);
        long id = this.typeId.size() + 1;  // 0 is not a valid id
        JITRowType result = new JITRowType(id, tuple);
        this.typeId.put(tuple, result);
        return result;
    }

    /**
     * We are given a type that is a Tuple or RawTuple.
     * Expand it into a list of Tuple types as follows:
     * - if the type is a Tuple, just return a singleton list.
     * - if the type is a RawTuple, add all its components to the list.
     * Each component is expected to be a Tuple.
     * This is used for functions like stream.index_with, which
     * take a closure that returns a tuple of values.  The JIT does
     * not support nested tuples.
     */
    public static List<DBSPTypeTuple> expandToTuples(DBSPType type) {
        List<DBSPTypeTuple> types = new ArrayList<>();
        if (type.is(DBSPTypeRawTuple.class)) {
            for (DBSPType field : type.to(DBSPTypeRawTuple.class).tupFields) {
                DBSPTypeTuple tuple = ToJitVisitor.makeTupleType(field);
                types.add(tuple);
            }
        } else {
            types.add(type.to(DBSPTypeTuple.class));
        }
        return types;
    }

    public BaseJsonNode asJson() {
        ObjectNode result = JITNode.jsonFactory().createObjectNode();
        for (JITRowType row: this.typeId.values()) {
            result.set(Long.toString(row.id), row.asJson());
        }
        return result;
    }
}
