package org.dbsp.sqlCompiler.compiler.backend;

import org.apache.commons.lang3.tuple.Pair;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.util.Unimplemented;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Performs beta reduction on an Expression.
 * I.e., replace an application of a closure with the closure
 * body with arguments substituted.
 * This code makes some simplifying assumptions:
 * - all parameter are simple path patterns
 * - arguments do not have side-effects.
 */
public class BetaReduction extends InnerExpressionRewriteVisitor {
    static class Substitution {
        /**
         * Maps parameter names to expressions.
         */
        final Map<String, DBSPExpression> replacement;
        /**
         * When a variable is defined it shadows parameters with the same \
         * name.  These are represented in the tombstones.
         */
        final Set<String> tombstone;

        Substitution() {
            this.replacement = new HashMap<>();
            this.tombstone = new HashSet<>();
        }

        void substitute(String name, @Nullable DBSPExpression expression) {
            if (expression == null)
                this.tombstone.add(name);
            else
                this.replacement.put(name, expression);
        }

        /**
         * Returns null if there is tombstone with this name.
         * Returns an expression otherwise.
         * Throws if there is no such substitution.
         */
        @Nullable
        DBSPExpression getReplacement(String name) {
            if (this.tombstone.contains(name))
                return null;
            return Utilities.getExists(this.replacement, name);
        }

        boolean contains(String name) {
            return this.tombstone.contains(name) ||
                    this.replacement.containsKey(name);
        }
    }

    static class Context {
        final List<Substitution> stack;

        Context() {
            this.stack = new ArrayList<>();
        }

        void newContext() {
            this.stack.add(new Substitution());
        }

        void popContext() {
            Utilities.removeLast(this.stack);
        }

        void substitute(String name, @Nullable DBSPExpression expression) {
            this.stack.get(this.stack.size() - 1).substitute(name, expression);
        }

        DBSPExpression lookup(DBSPVariablePath var) {
            for (int i = 0; i < this.stack.size(); i++) {
                int index = this.stack.size() - i - 1;
                Substitution subst = this.stack.get(i);
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

    final Context context;

    public BetaReduction() {
        super(true);
        this.context = new Context();
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

    public boolean preorder(DBSPVariablePath variable) {
        DBSPExpression replacement = this.context.lookup(variable);
        this.map(variable, replacement);
        return false;
    }

    public boolean preorder(DBSPBlockExpression block) {
        this.context.newContext();
        super.preorder(block);
        this.context.popContext();
        return false;
    }

    public boolean preorder(DBSPLetStatement statement) {
        this.context.substitute(statement.variable, null);
        super.preorder(statement);
        return false;
    }
}
