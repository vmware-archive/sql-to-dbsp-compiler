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
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.optimizer.CircuitOptimizer;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustHandleVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.ToRustVisitor;

import javax.annotation.Nullable;
import java.io.*;

/**
 * Main entry point of the SQL compiler.
 */
public class Main {
    CompilerOptions options;

    Main() {
        this.options = new CompilerOptions();
    }

    void parseOptions(String[] argv) {
        JCommander.newBuilder()
                .addObject(this.options)
                .build()
                .parse(argv);
    }

    void writeToOutput(String program, @Nullable String outputFile) throws FileNotFoundException {
        PrintStream outputStream;
        if (outputFile == null) {
            outputStream = System.out;
        } else {
            outputStream = new PrintStream(new FileOutputStream(outputFile));
        }
        outputStream.print(program);
        outputStream.close();
    }

    String getInputFile(@Nullable String inputFile) throws IOException {
        InputStream stream;
        if (inputFile == null)
            stream = System.in;
        else
            stream = new FileInputStream(inputFile);
        StringBuilder builder = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = br.readLine()) != null)
            builder.append(line)
                    .append(System.lineSeparator());
        return builder.toString();
    }

    void run() throws SqlParseException, IOException {
        DBSPCompiler compiler = new DBSPCompiler(this.options);
        compiler.newCircuit(this.options.ioOptions.functionName);
        String file = this.getInputFile(this.options.ioOptions.inputFile);
        compiler.compileStatements(file);
        DBSPCircuit dbsp = compiler.getResult();
        CircuitOptimizer optimizer = new CircuitOptimizer(this.options.optimizerOptions);
        dbsp = optimizer.optimize(dbsp);
        StringBuilder builder = new StringBuilder();
        builder.append(ToRustHandleVisitor.generatePreamble());
        String circuit = ToRustHandleVisitor.toRustString(dbsp, this.options.ioOptions.functionName);
        builder.append(circuit);
        this.writeToOutput(builder.toString(), this.options.ioOptions.outputFile);
    }

    public static void execute(String... argv) throws IOException, SqlParseException {
        Main main = new Main();
        main.parseOptions(argv);
        main.run();
    }

    public static void main(String[] argv) throws SqlParseException, IOException {
        execute(argv);
    }
}
