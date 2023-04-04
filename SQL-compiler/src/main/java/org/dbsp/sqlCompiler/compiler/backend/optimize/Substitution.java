package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Describes a substitution from names to values.
 */
public class Substitution<T> {
    /**
     * Maps parameter names to values.
     */
    final Map<String, T> replacement;
    /**
     * When a variable is defined it shadows parameters with the same
     * name.  These are represented in the tombstones.
     */
    final Set<String> tombstone;

    public Substitution() {
        this.replacement = new HashMap<>();
        this.tombstone = new HashSet<>();
    }

    public void substitute(String name, @Nullable T expression) {
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
    public T getReplacement(String name) {
        if (this.tombstone.contains(name))
            return null;
        return Utilities.getExists(this.replacement, name);
    }

    public boolean contains(String name) {
        return this.tombstone.contains(name) ||
                this.replacement.containsKey(name);
    }
}
