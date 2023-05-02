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

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.cfg;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.IJITId;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITReference;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITInstruction;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions.JITIsNullInstruction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JITBlock extends JITNode implements IJITId {
    final List<JITInstruction> instructions;
    @Nullable
    JITBlockTerminator terminator;
    public final long id;

    public JITBlock(long id) {
        this.id = id;
        this.instructions = new ArrayList<>();
        this.terminator = null;
    }

    void addInstruction(JITInstruction instruction) {
        this.instructions.add(instruction);
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return "Block " + this.id;
    }

    @Override
    public JITReference getReference() {
        return this.getBlockReference();
    }

    public JITBlockReference getBlockReference() { return new JITBlockReference(this.id); }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        result.put("id", this.id);
        ArrayNode body = result.putArray("body");
        for (JITInstruction i: this.instructions) {
            body.add(i.asJson());
        }
        result.set("terminator", Objects.requireNonNull(this.terminator).asJson());
        return result;
    }

    public void add(JITInstruction instruction) {
        if (this.terminator != null)
            throw new RuntimeException("Block already terminated while adding instruction " + instruction);
        this.instructions.add(instruction);
    }

    public void terminate(JITBlockTerminator terminator) {
        if (this.terminator != null)
            throw new RuntimeException("Block already terminated with " + this.terminator);
        this.terminator = terminator;
    }
}
