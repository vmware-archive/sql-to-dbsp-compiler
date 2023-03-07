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

package org.dbsp.sqlCompiler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.visitors.*;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Main entry point of the SQL compiler.
 */
public class CompilerMain {
    final CompilerOptions options;

    CompilerMain() {
        this.options = new CompilerOptions();
    }

    void parseOptions(String[] argv) {
        JCommander commander = JCommander.newBuilder()
                .addObject(this.options)
                .build();
        commander.setProgramName("sql-to-dbsp");
        try {
            commander.parse(argv);
        } catch (ParameterException ex) {
            commander.usage();
            System.exit(1);
        }
        if (this.options.help) {
            commander.usage();
            System.exit(1);
        }
    }

    void writeToOutput(String program, @Nullable String outputFile) throws IOException {
        PrintStream outputStream;
        if (outputFile == null) {
            outputStream = System.out;
        } else {
            outputStream = new PrintStream(Files.newOutputStream(Paths.get(outputFile)));
        }
        outputStream.print(program);
        outputStream.close();
    }

    InputStream getInputFile(@Nullable String inputFile) throws IOException {
        if (inputFile == null) {
            return System.in;
        } else {
            return Files.newInputStream(Paths.get(inputFile));
        }
    }

    /**
     * Run compiler, return exit code.
     */
    CompilerMessages run() {
        DBSPCompiler compiler = new DBSPCompiler(this.options);
        try {
            InputStream input = this.getInputFile(this.options.ioOptions.inputFile);
            compiler.setEntireInput(this.options.ioOptions.inputFile, input);
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID, false,
                    "Error reading file", e.getMessage());
            return compiler.messages;
        }
        compiler.compileInput();
        if (compiler.hasErrors())
            return compiler.messages;
        if (this.options.ioOptions.emitJsonSchema != null) {
            try {
                PrintStream outputStream = new PrintStream(Files.newOutputStream(Paths.get(this.options.ioOptions.emitJsonSchema)));
                outputStream.println(Objects.requireNonNull(compiler.ios).toPrettyString());
                outputStream.close();
            } catch (IOException e) {
                compiler.reportError(SourcePositionRange.INVALID, false,
                        "Error writing to file", e.getMessage());
                return compiler.messages;
            }
        }

        compiler.optimize();
        DBSPCircuit dbsp = compiler.getFinalCircuit(this.options.ioOptions.functionName);
        String output;
        if (this.options.ioOptions.emitJson) {
            output = ToJSONVisitor.circuitToJSON(dbsp);
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append(ToRustHandleVisitor.generatePreamble());
            String circuit = ToRustHandleVisitor.toRustString(dbsp, this.options.ioOptions.functionName);
            builder.append(circuit);
            output = builder.toString();
        }
        try {
            this.writeToOutput(output, this.options.ioOptions.outputFile);
        } catch (IOException e) {
            compiler.reportError(SourcePositionRange.INVALID,
                    false, "Error writing to file", e.getMessage());
            return compiler.messages;
        }
        return compiler.messages;
    }

    public static CompilerMessages execute(String... argv) {
        CompilerMain main = new CompilerMain();
        main.parseOptions(argv);
        return main.run();
    }

    public static void main(String[] argv) {
        CompilerMessages messages = execute(argv);
        messages.show(System.err);
        System.exit(messages.exitCode);
    }
}
