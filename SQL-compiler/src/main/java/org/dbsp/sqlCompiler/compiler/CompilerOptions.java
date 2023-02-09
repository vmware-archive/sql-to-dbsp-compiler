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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.calcite.config.Lex;
import org.dbsp.util.SqlLexicalRulesConverter;

import javax.annotation.Nullable;

/**
 * Packages options for a compiler from SQL to Rust.
 */
@SuppressWarnings("CanBeFinal")
// These fields cannot be final, since JCommander writes them through reflection.
public class CompilerOptions {
    /**
     * Options for the optimizer.
     */
    @SuppressWarnings("CanBeFinal")
    public static class Optimizer {
        /**
         * If true the compiler should generate an incremental streaming circuit.
         */
        @Parameter(names = "-i", description = "Generate an incremental circuit")
        public boolean incrementalize = false;
        @Parameter(names = "-O0", description = "Do not optimize")
        public boolean noOptimizations = false;
    }

    /**
     * Options related to input and output.
     */
    @SuppressWarnings("CanBeFinal")
    public static class IO {
        @Parameter(names="-o", description = "Output file; stdout if null")
        @Nullable
        public String outputFile = null;
        /**
         * SQL input script which contains table and view declarations.
         * We expect that declarations are terminated by semicolons.
         */
        @Parameter(names = "-j", description = "Emit JSON instead of Rust")
        public boolean emitJson = false;
        @Parameter(description = "Input file to compile", required = true)
        @Nullable
        public String inputFile = null;
        @Parameter(names = "-f", description = "Name of function to generate")
        public String functionName = "circuit";
        @Parameter(names = "-d",
                   converter = SqlLexicalRulesConverter.class)
        public Lex lexicalRules;

        IO() {
            this.lexicalRules = Lex.ORACLE;
        }
    }

    @Parameter(names = {"-h", "--help", "-"}, help=true, description = "Show this message and exit")
    public boolean help;
    @ParametersDelegate
    public IO ioOptions = new IO();
    @ParametersDelegate
    public Optimizer optimizerOptions = new Optimizer();
}
