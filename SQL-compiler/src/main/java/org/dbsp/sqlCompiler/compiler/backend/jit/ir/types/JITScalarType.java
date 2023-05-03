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

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.Unimplemented;

import java.util.Objects;

public class JITScalarType extends JITType {
    public final String name;

    protected JITScalarType(String name) {
        this.name = name;
    }

    @Override
    public boolean isScalarType() {
        return true;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public static JITScalarType scalarType(DBSPType type) {
        type = Objects.requireNonNull(ToJitVisitor.resolveWeightType(type));
        if (type.sameType(new DBSPTypeTuple()))
            return JITUnitType.INSTANCE;
        DBSPTypeBaseType base = type.as(DBSPTypeBaseType.class);
        if (base == null)
            throw new RuntimeException("Expected a base type, got " + type);
        switch (base.shortName()) {
            case "b":
                return JITBoolType.INSTANCE;
            case "i16":
                return JITI16Type.INSTANCE;
            case "i32":
                return JITI32Type.INSTANCE;
            case "i64":
                return JITI64Type.INSTANCE;
            case "d":
                return JITF64Type.INSTANCE;
            case "f":
                return JITF32Type.INSTANCE;
            case "s":
                return JITStringType.INSTANCE;
            case "date":
                return JITDateType.INSTANCE;
            case "Timestamp":
                return JITTimestampType.INSTANCE;
            case "u":
                return JITUSizeType.INSTANCE;
            case "i":
                return JITISizeType.INSTANCE;
            default:
                break;
        }
        throw new Unimplemented(type);
    }

    @Override
    public BaseJsonNode asJson() {
        return new TextNode(this.toString());
    }
}
