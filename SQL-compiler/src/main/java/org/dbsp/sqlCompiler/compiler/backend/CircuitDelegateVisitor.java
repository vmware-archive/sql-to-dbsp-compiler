package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.InnerVisitor;

/**
 * Invokes an InnerVisitor on each InnerNode reachable from this circuit.
 */
public class CircuitDelegateVisitor extends CircuitVisitor {
    private final InnerVisitor innerVisitor;

    public CircuitDelegateVisitor(InnerVisitor visitor) {
        super(true);
        this.innerVisitor = visitor;
    }

    @Override
    public void postorder(DBSPOperator node) {
        if (node.function != null)
            node.function.accept(this.innerVisitor);
    }

    @Override
    public void postorder(DBSPAggregateOperator node) {
        node.aggregate.accept(this.innerVisitor);
    }
}
