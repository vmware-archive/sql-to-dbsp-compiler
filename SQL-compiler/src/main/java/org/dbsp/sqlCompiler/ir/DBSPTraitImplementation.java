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
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeParameter;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Represents a simplified form of Rust trait impl.
 */
public class DBSPTraitImplementation extends DBSPNode implements IDBSPInnerNode {
    public final DBSPPath path;
    public final List<DBSPTypeParameter> typeParameters;
    public final DBSPPath type;
    public final List<DBSPTypeConstraint> typeConstraints;
    public final DBSPFunction[] functions;

    public DBSPTraitImplementation(@Nullable Object node, DBSPPath path, List<DBSPTypeParameter> typeParameters,
                                   DBSPPath type, List<DBSPTypeConstraint> typeConstraints, DBSPFunction... functions) {
        super(node);
        this.path = path;
        this.typeParameters = typeParameters;
        this.type = type;
        this.typeConstraints = typeConstraints;
        this.functions = functions;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        this.path.accept(visitor);
        for (DBSPTypeParameter param : this.typeParameters)
            param.accept(visitor);
        this.type.accept(visitor);
        for (DBSPFunction function : this.functions)
            function.accept(visitor);
    }
}
