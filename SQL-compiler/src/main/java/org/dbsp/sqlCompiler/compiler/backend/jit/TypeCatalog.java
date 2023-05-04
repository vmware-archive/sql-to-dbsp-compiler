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

    public JITRowType convertTupleType(DBSPType type) {
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

    public BaseJsonNode asJson() {
        ObjectNode result = JITNode.jsonFactory().createObjectNode();
        for (JITRowType row: this.typeId.values()) {
            result.set(Long.toString(row.id), row.asJson());
        }
        return result;
    }
}
