package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.compiler.backend.visitors.InnerExpressionRewriteVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;

/**
 * Performs beta reduction on an Expression.
 * I.e., replace an application of a closure with the closure
 * body with arguments substituted.
 * This code makes some simplifying assumptions:
 * - all parameter are simple path patterns
 * - arguments do not have side effects.
 */
public class BetaReduction extends InnerExpressionRewriteVisitor {
    final ExpressionSubstitutionContext context;

    public BetaReduction() {
        this.context = new ExpressionSubstitutionContext();
    }

    @Override
    public boolean preorder(DBSPApplyExpression expression) {
        if (expression.function.is(DBSPClosureExpression.class)) {
            DBSPClosureExpression closure = expression.function.to(DBSPClosureExpression.class);
            this.context.newContext();
            if (closure.parameters.length != expression.arguments.length)
                throw new RuntimeException("Closure with " + closure.parameters.length +
                        " parameters called with " + expression.arguments.length + " arguments");
            for (int i = 0; i < closure.parameters.length; i++) {
                DBSPParameter param = closure.parameters[i];
                DBSPIdentifierPattern paramPattern = param.pattern.to(DBSPIdentifierPattern.class);
                DBSPExpression arg = this.transform(expression.arguments[i]);
                this.context.substitute(paramPattern.identifier, arg);
            }

            DBSPExpression newBody = this.transform(closure.body);
            this.map(expression, newBody);
            this.context.popContext();
            return false;
        }
        super.preorder(expression);
        return false;
    }

    @Override
    public boolean preorder(DBSPVariablePath variable) {
        DBSPExpression replacement = this.context.lookup(variable);
        this.map(variable, replacement);
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression block) {
        this.context.newContext();
        super.preorder(block);
        this.context.popContext();
        return false;
    }

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        this.context.substitute(statement.variable, null);
        super.preorder(statement);
        return false;
    }

    @Override
    public void startVisit() {
        this.context.newContext();
        super.startVisit();
    }

    @Override
    public void endVisit() {
        this.context.popContext();
        this.context.mustBeEmpty();
        super.endVisit();
    }
}
