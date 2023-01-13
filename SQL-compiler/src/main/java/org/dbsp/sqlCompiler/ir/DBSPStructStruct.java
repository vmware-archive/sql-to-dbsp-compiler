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

package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeParameter;

import java.util.List;

/**
 * A simplified representation of Rust StructStruct.
 */
public class DBSPStructStruct extends DBSPNode implements IDBSPInnerNode {
    public static class Field extends DBSPNode implements IDBSPInnerNode {
        public final String name;
        public final DBSPType type;

        Field(String name, DBSPType type) {
            super(null);
            this.name = name;
            this.type = type;
        }

        @Override
        public void accept(InnerVisitor visitor) {
            this.type.accept(visitor);
        }
    }

    public final String name;
    public final List<DBSPTypeParameter> typeParameters;
    public final Field[] fields;

    public DBSPStructStruct(String name, List<DBSPTypeParameter> typeParameters, Field... fields) {
        super(null);
        this.name = name;
        this.typeParameters = typeParameters;
        this.fields = fields;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        for (Field f : this.fields)
            f.accept(visitor);
    }
}
