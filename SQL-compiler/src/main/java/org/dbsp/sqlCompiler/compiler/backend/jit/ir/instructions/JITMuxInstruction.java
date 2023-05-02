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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions;

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JITMuxInstruction extends JITInstruction {
    public final JITInstructionReference condition;
    public final JITInstructionReference left;
    public final JITInstructionReference right;

    public JITMuxInstruction(long id,
                             JITInstructionReference condition,
                             JITInstructionReference left,
                             JITInstructionReference right) {
        super(id, "Cond");
        this.condition = condition;
        this.right = right;
        this.left = left;
    }

    @Override
    public BaseJsonNode instructionAsJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("cond", this.condition.getId());
        result.put("lhs", this.left.getId());
        result.put("rhs", this.right.getId());
        return result;
    }

    @Override
    public String toString() {
        return this.id + " " + this.condition + " ? " + this.left + " : " + this.right;
    }
}
