/*
 * Copyright 2023 VMware, Inc.
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
 */

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.types;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.IJITId;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITReference;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

public class JITRowType extends JITType implements IJITId {
    static class NullableScalarType extends JITNode {
        public final boolean nullable;
        public final JITScalarType type;

        NullableScalarType(boolean nullable, JITScalarType type) {
            this.nullable = nullable;
            this.type = type;
        }

        @Override
        public BaseJsonNode asJson() {
            ObjectNode result = jsonFactory().createObjectNode();
            result.put("nullable", this.nullable);
            result.put("ty", this.type.toString());
            return result;
        }

        @Override
        public String toString() {
            return (this.nullable ? "?" : "") + this.type.toString();
        }
    }

    public final long id;
    final List<NullableScalarType> fields;

    public JITRowType(long id, DBSPTypeTupleBase type) {
        this.id = id;
        this.fields = new ArrayList<>();
        for (DBSPType colType: type.tupFields) {
            JITScalarType scalarType = JITScalarType.scalarType(colType);
            this.fields.add(new NullableScalarType(colType.mayBeNull, scalarType));
        }
    }

    public long getId() {
        return this.id;
    }

    @Override
    public JITReference getReference() {
        return new JITReference(this.id);
    }

    @Override
    public boolean isScalarType() {
        return false;
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ArrayNode columns = result.putArray("columns");
        if (this.fields.isEmpty()) {
            ObjectNode col = columns.addObject();
            col.put("nullable", false);
            col.put("ty", "Unit");
        } else {
            for (int i = 0; i < this.fields.size(); i++)
                columns.add(this.fields.get(i).asJson());
        }
        return result;
    }

    @Override
    public String toString() {
        List<String> fields = Linq.map(this.fields, NullableScalarType::toString);
        return "[" + String.join(", ", fields) + "]";
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.toString());
    }
}
