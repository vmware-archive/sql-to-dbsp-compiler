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

package org.dbsp.sqlCompiler.compiler;

import javax.annotation.Nullable;

/**
 * Packages options for a compiler from SQL to Rust.
 */
public class CompilerOptions {
    /**
     * Rust file where the output circuit is emitted.
     */
    @Nullable
    public String outputFile;
    /**
     * SQL input script which contains table declarations and view definitions.
     */
    @Nullable
    public String inputFile;
    /**
     * If true the compiler should generate an incremental streaming circuit.
     */
    public boolean incrementalize;
    /**
     * SQL dialect compiled.
     */
    public String dialect;

    public CompilerOptions() {
        this.outputFile = null;
        this.inputFile = null;
        this.incrementalize = false;
        this.dialect = "psql";
    };
}
