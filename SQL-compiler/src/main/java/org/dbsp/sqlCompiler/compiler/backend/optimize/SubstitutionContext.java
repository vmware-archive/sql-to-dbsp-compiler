package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of nested contexts where substitution is performed.
 * Each context is a namespace which can define new substitutions, which may
 * shadow substitutions in the outer contexts.
 */
public class SubstitutionContext<T> {
    protected final List<Substitution<T>> stack;

    public SubstitutionContext() {
        this.stack = new ArrayList<>();
    }

    public void newContext() {
        this.stack.add(new Substitution());
    }

    public void popContext() {
        Utilities.removeLast(this.stack);
    }

    public void substitute(String name, @Nullable T value) {
        if (this.stack.isEmpty())
            throw new RuntimeException("Empty context");
        this.stack.get(this.stack.size() - 1).substitute(name, value);
    }

    public void mustBeEmpty() {
        if (!this.stack.isEmpty())
            throw new RuntimeException("Non-empty context");
    }

    /**
     * The substitution for this name.
     * null if there is a tombstone or no substitution.
     */
    @Nullable
    public T get(String name) {
        for (int i = 0; i < this.stack.size(); i++) {
            int index = this.stack.size() - i - 1;
            Substitution<T> subst = this.stack.get(index);
            if (subst.contains(name))
                return subst.getReplacement(name);
        }
        return null;
    }

    /**
     * True if there is a substitution for this variable.
     */
    public boolean containsSubstitution(String name) {
        for (int i = 0; i < this.stack.size(); i++) {
            int index = this.stack.size() - i - 1;
            Substitution<T> subst = this.stack.get(index);
            if (subst.contains(name)) {
                T expression = subst.getReplacement(name);
                return expression != null;
            }
        }
        return false;
    }
}
