package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

/**
 * A substitution that replaces names with expressions.
 */
public class ExpressionSubstitutionContext extends SubstitutionContext<DBSPExpression> {
    public DBSPExpression lookup(DBSPVariablePath var) {
        for (int i = 0; i < this.stack.size(); i++) {
            int index = this.stack.size() - i - 1;
            Substitution<DBSPExpression> subst = this.stack.get(index);
            if (subst.contains(var.variable)) {
                DBSPExpression expression = subst.getReplacement(var.variable);
                if (expression == null)
                    return var;
                return expression;
            }
        }
        return var;
    }
}
