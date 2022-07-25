/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 *
 */

package org.dbsp.sqlCompiler.dbsp.circuit.type;

import org.dbsp.sqlCompiler.dbsp.circuit.expression.DBSPExpression;
import org.dbsp.util.IndentStringBuilder;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;

public class DBSPTypeStruct extends DBSPType {
    private final String name;
    private final List<DBSPStructField> args;
    private final HashSet<String> fields = new HashSet<>();

    public DBSPTypeStruct(@Nullable Object node, String name, List<DBSPStructField> args) {
        super(node,false);
        this.name = name;
        this.args = args;
        for (DBSPStructField f: args) {
            if (this.hasField(f.getName()))
                this.error("Field name " + f + " is duplicated");
            fields.add(f.getName());
        }
    }

    @Override
    public IndentStringBuilder toRustString(IndentStringBuilder builder) {
        return builder.append(this.name)
                .append("{")
                .join(", ", this.args)
                .append("}");
    }

    @Override
    public IndentStringBuilder castFrom(IndentStringBuilder builder, DBSPExpression source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        if (mayBeNull)
            this.error("Nullable structs not supported");
        return this;
    }

    public boolean hasField(String fieldName) {
        return this.fields.contains(fieldName);
    }

    public String getName() { return this.name; }

    public List<DBSPStructField> getFields() { return this.args; }

    @Override
    public boolean same(DBSPType type) {
        if (!super.same(type))
            return false;
        if (!type.is(DBSPTypeStruct.class))
            return false;
        DBSPTypeStruct other = type.to(DBSPTypeStruct.class);
        if (!this.name.equals(other.name))
            return false;
        if (this.args.size() != other.args.size())
            return false;
        for (int i = 0; i < this.args.size(); i++)
            if (!this.args.get(i).equals(other.args.get(i)))
                return false;
        return true;
    }

    public DBSPType getFieldType(String col) {
        for (DBSPStructField f : this.getFields()) {
            if (f.getName().equals(col))
                return f.getType();
        }
        this.error("Field " + col + " not present in struct " + this.name);
        throw new RuntimeException("unreachable");
    }
}
