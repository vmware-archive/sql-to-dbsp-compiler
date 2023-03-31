package org.dbsp.sqlCompiler.compiler.backend.visitors;

import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPAggregate;
import org.dbsp.sqlCompiler.ir.InnerVisitor;

import javax.annotation.Nullable;

/**
 * Invokes an InnerVisitor on each InnerNode reachable from this circuit.
 */
public class CircuitDelegateVisitor extends CircuitVisitor {
    private final InnerVisitor innerVisitor;

    public CircuitDelegateVisitor(InnerVisitor visitor) {
        super(true);
        this.innerVisitor = visitor;
    }

    void doFunction(DBSPOperator node) {
        if (node.function != null)
            node.function.accept(this.innerVisitor);
    }

    void doOutputType(DBSPOperator node) {
        node.outputType.accept(this.innerVisitor);
    }

    void doAggregate(@Nullable DBSPAggregate aggregate) {
        if (aggregate != null)
            aggregate.accept(this.innerVisitor);
    }

    @Override
    public void postorder(DBSPOperator node) {
        this.doFunction(node);
        this.doOutputType(node);
    }

    @Override
    public void postorder(DBSPAggregateOperatorBase node) {
        this.doAggregate(node.aggregate);
        this.doFunction(node);
        this.doOutputType(node);
    }
}
