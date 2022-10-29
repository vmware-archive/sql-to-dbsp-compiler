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
 */

package org.dbsp.sqlCompiler.compiler.visitors;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.util.IModule;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.PrintWriter;

public class ToDotVisitor extends CircuitVisitor implements IModule {
    private final IndentStream stream;

    public ToDotVisitor(IndentStream stream) {
        super(true);
        this.stream = stream;
    }

    @Override
    public boolean preorder(DBSPOperator node) {
        this.stream.append(node.outputName)
                .append(" [ shape=box,label=\"")
                .append(node.toString())
                .append("\" ]")
                .newline();
        for (DBSPOperator input: node.inputs) {
            this.stream.append(input.outputName)
                    .append(" -> ")
                    .append(node.outputName)
                    .append(";")
                    .newline();
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPCircuit circuit) {
        this.stream.append("digraph ")
                .append(circuit.name)
                .append("{")
                .increase();
        return true;
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        this.stream.decrease()
                .append("}")
                .newline();
    }

    public static DBSPCircuit toDot(String fileName, boolean toJpg, DBSPCircuit circuit) {
        try {
            if (Logger.instance.getDebugLevel(ToDotVisitor.class) > 0)
                Logger.instance.append("Writing circuit to ")
                    .append(fileName)
                    .newline();
            File tmp = File.createTempFile("tmp", ".dot");
            PrintWriter writer = new PrintWriter(tmp.getAbsolutePath());
            IndentStream stream = new IndentStream(writer);
            circuit.accept(new ToDotVisitor(stream));
            writer.close();
            if (toJpg)
                Utilities.runProcess(".", "dot", "-T", "jpg",
                        "-o", fileName, tmp.getAbsolutePath());
            else
                //noinspection ResultOfMethodCallIgnored
                tmp.delete();
            return circuit;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
