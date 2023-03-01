package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;

/**
 * Perform some IR rewriting to prepare for JIT code generation.
 * - make sure every closure body is a BlockExpression.
 */
public class JitNormalizeInnerVisitor
        extends InnerExpressionRewriteVisitor
        implements IModule {

    public JitNormalizeInnerVisitor() {
        super(true);
    }

    @Override
    public boolean preorder(DBSPType node) {
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        if (expression.body.is(DBSPBlockExpression.class))
            return false;
        expression.body.accept(this);
        DBSPExpression newBody = this.getResultExpression();
        DBSPBlockExpression block = new DBSPBlockExpression(Linq.list(), newBody);
        DBSPExpression result = new DBSPClosureExpression(expression.getNode(),
                block, expression.parameters);
        this.map(expression, result);
        return false;
    }
}
