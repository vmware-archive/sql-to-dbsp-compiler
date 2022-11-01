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
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.List;

public class PassesVisitor extends CircuitVisitor implements IModule {
    public final List<CircuitVisitor> passes;

    public PassesVisitor(CircuitVisitor... passes) {
        super(false);
        this.passes = Linq.list(passes);
    }

    public PassesVisitor(List<CircuitVisitor> passes) {
        super(false);
        this.passes = passes;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        int count = 0;
        if (this.getDebugLevel() >= 3) {
            Logger.instance.from(this, 3)
                    .append("Writing circuit to before.jpg")
                    .newline();
            ToDotVisitor.toDot("0before.jpg", true, circuit);
        }
        ++count;
        for (CircuitVisitor pass: this.passes) {
            Logger.instance.from(this, 1)
                    .append("Executing ")
                    .append(pass.toString())
                    .newline();
            circuit = pass.apply(circuit);
            if (this.getDebugLevel() >= 3) {
                String name = count + pass.toString().replace(" ", "_") + ".jpg";
                Logger.instance.from(this, 3)
                        .append("Writing circuit to ")
                        .append(name)
                        .newline();
                ToDotVisitor.toDot(name, true, circuit);
            }
            ++count;
        }
        return circuit;
    }
}
