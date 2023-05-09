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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.TypeCatalog;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.operators.JITOperator;
import org.dbsp.util.IIndentStream;

import java.util.ArrayList;
import java.util.List;

public class JITProgram extends JITNode {
    final List<JITOperator> operators;
    public final TypeCatalog typeCatalog;

    public JITProgram() {
        this.operators = new ArrayList<>();
        this.typeCatalog = new TypeCatalog();
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode nodes = result.putObject("nodes");
        for (JITOperator operator: this.operators) {
            BaseJsonNode opNode = operator.asJson();
            nodes.set(Long.toString(operator.getId()), opNode);
        }
        result.set("layouts", this.typeCatalog.asJson());
        return result;
    }

    public void add(JITOperator source) {
        this.operators.add(source);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        for (JITOperator op: this.operators)
            builder.append(op).newline();
        return builder;
    }
}
