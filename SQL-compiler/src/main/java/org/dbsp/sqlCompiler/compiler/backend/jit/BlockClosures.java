package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.dbsp.sqlCompiler.compiler.backend.visitors.InnerExpressionRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;

/**
 * Make sure every closure body is a BlockExpression.
 */
public class BlockClosures
        extends InnerExpressionRewriteVisitor
        implements IModule {

    public BlockClosures() {
        super();
    }

    @Override
    public boolean preorder(DBSPType node) {
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        expression.body.accept(this);
        DBSPExpression newBody = this.getResultExpression();
        DBSPBlockExpression block ;
        if (!newBody.is(DBSPBlockExpression.class))
            block = new DBSPBlockExpression(Linq.list(), newBody);
        else
            block = newBody.to(DBSPBlockExpression.class);
        DBSPExpression result = expression;
        if (block != expression.body)
            result = new DBSPClosureExpression(expression.getNode(),
                block, expression.parameters);
        this.map(expression, result);
        return false;
    }
}
