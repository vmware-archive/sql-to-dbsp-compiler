package org.dbsp.sqlCompiler.compiler.backend.jit.ir;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.TypeCatalog;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.JITOperator;

import java.util.ArrayList;
import java.util.List;

public class JITProgram extends JITNode {
    final List<JITOperator> operators;
    public final TypeCatalog typeCatalog;

    public JITProgram() {
        this.operators = new ArrayList<>();
        this.typeCatalog = new TypeCatalog();
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode nodes = result.putObject("nodes");
        for (JITOperator operator: this.operators) {
            BaseJsonNode opNode = operator.asJson();
            nodes.set(Long.toString(operator.getId()), opNode);
        }
        result.set("layouts", this.typeCatalog.asJson());
        return result;
    }

    public void add(JITOperator source) {
        this.operators.add(source);
    }
}
