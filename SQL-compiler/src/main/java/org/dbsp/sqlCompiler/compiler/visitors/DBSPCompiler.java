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

import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.SourceFileContents;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.optimizer.CircuitOptimizer;
import org.dbsp.sqlCompiler.compiler.sqlparser.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.util.IModule;
import org.dbsp.util.Logger;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * This class compiles SQL statements into DBSP circuits.
 * The protocol is:
 * - create compiler
 * - repeat as much as necessary:
 *   - compile a sequence of SQL statements
 *     (CREATE TABLE, CREATE VIEW)
 *   - get the resulting circuit, starting a new one
 * This protocol allows one compiler to generate multiple independent circuits.
 * The compiler can also compile INSERT statements by simulating their
 * execution and keeping track of the contents of each table.
 * The contents after insertions can be obtained using getTableContents().
 */
public class DBSPCompiler implements IModule {
    enum InputSource {
        /**
         * No data source set yet.
         */
        None,
        /**
         * Data received from stdin.
         */
        Stdin,
        /**
         * Data read from a file.  We read the entire file upfront and then we compile.
         */
        File,
        /**
         * Data received through API calls (compileStatement/s).
         */
        API,
    }

    final CalciteCompiler frontend;
    final CalciteToDBSPCompiler midend;
    public final CompilerOptions options;
    public final CompilerMessages messages;
    public final SourceFileContents sources;
    public InputSource inputSources = InputSource.None;
    /**
     * Circuit produced by the compiler.  Once a circuit has been produced,
     * no more statements can be compiled.
     */
    public @Nullable DBSPCircuit circuit;

    public DBSPCompiler(CompilerOptions options) {
        this.options = options;
        this.frontend = new CalciteCompiler(options);
        this.midend = new CalciteToDBSPCompiler(this.frontend, true, options, this);
        this.messages = new CompilerMessages(this);
        this.sources = new SourceFileContents();
        this.circuit = null;
    }

    /**
     * If argument is true, the inputs to the circuit are generated from the CREATE TABLE
     * statements.  Otherwise they are generated from the LogicalTableScan
     * operations in a view plan.  Default value is 'false'.
     */
    public void setGenerateInputsFromTables(boolean generateInputsFromTables) {
        this.midend.setGenerateInputsFromTables(generateInputsFromTables);
    }

    public void reportError(SourcePositionRange range, boolean warning,
                            String errorType, String message) {
        this.messages.reportError(range, warning, errorType, message);
    }

    /**
     * @param generate
     * If 'false' the next "create view" statements will not generate
     * an output for the circuit
     */
    public void generateOutputForNextView(boolean generate) {
        this.midend.generateOutputForNextView(generate);
    }

    void checkNoCircuit() {
        if (this.circuit != null)
            throw new RuntimeException("No more statements can be compiled once a circuit has been produced");
    }

    void setSource(InputSource source) {
        this.checkNoCircuit();
        if (this.inputSources != InputSource.None &&
                this.inputSources != source)
            throw new RuntimeException("Input data already received from " + this.inputSources);
        this.inputSources = source;
    }

    private void compileInternal(String statements, boolean many, @Nullable String comment) {
        this.checkNoCircuit();
        if (this.inputSources != InputSource.File) {
            // If we read from file we already have read the entire data.
            // Otherwise, we append the statements to the sources.
            this.sources.append(statements);
        }
        try {
            if (many) {
                SqlNodeList nodes = this.frontend.parseStatements(statements);
                for (SqlNode node : nodes) {
                    FrontEndStatement fe = this.frontend.compile(node.toString(), node, null);
                    this.midend.compile(fe);
                }
            } else {
                SqlNode node = this.frontend.parse(statements);
                Logger.INSTANCE.from(this, 2)
                        .append("Parsing result")
                        .newline()
                        .append(node.toString())
                        .newline();
                FrontEndStatement fe = this.frontend.compile(statements, node, comment);
                this.midend.compile(fe);
            }
        } catch (SqlParseException e) {
            this.messages.reportError(e);
        } catch (CalciteContextException e) {
            this.messages.reportError(e);
        } catch (Unimplemented e) {
            this.messages.reportError(e);
        } catch (Throwable e) {
            this.messages.reportError(e);
        }
    }

    public void compileStatement(String statement, @Nullable String comment) {
        this.setSource(InputSource.API);
        this.compileInternal(statement, false, comment);
    }

    public void compileStatements(String program) {
        this.setSource(InputSource.API);
        this.compileInternal(program, true, null);
    }

    public void optimize() {
        if (this.circuit == null) {
            this.circuit = this.getFinalCircuit("tmp");
        }
        CircuitOptimizer optimizer = new CircuitOptimizer(this.options.optimizerOptions);
        this.circuit = optimizer.optimize(circuit);
    }

    public void compileStatement(String statement) {
        this.compileStatement(statement, null);
    }

    public void setEntireInput(@Nullable String filename, InputStream contents) throws IOException {
        if (filename != null)
            this.setSource(InputSource.File);
        else
            this.setSource(InputSource.Stdin);
        this.sources.setEntireInput(filename, contents);
    }

    public void compileInput() {
        if (this.inputSources == InputSource.None)
            throw new RuntimeException("compileInput has been called without calling setEntireInput");
        this.compileInternal(this.sources.getWholeProgram(), true, null);
    }

    public boolean hasErrors() {
        return this.messages.exitCode != 0;
    }

    /**
     * Get the circuit generated by compiling the statements to far.
     * @param name  Name to use for the produced circuit.
     */
    public DBSPCircuit getFinalCircuit(String name) {
        if (this.circuit == null) {
            DBSPPartialCircuit circuit = this.midend.getFinalCircuit();
            this.circuit = circuit.seal(name);
        }
        this.circuit = this.circuit.rename(name);
        return this.circuit;
    }

    /**
     * Get the contents of the tables as a result of all the INSERT statements compiled.
     */
    public TableContents getTableContents() {
        return this.midend.getTableContents();
    }

    public void showErrors(PrintStream stream) {
        this.messages.show(stream);
    }

    /**
     * Throw if any error has been encountered.
     * Displays the errors on stderr as well.
     */
    public void throwOnError() {
        if (this.hasErrors()) {
            this.showErrors(System.err);
            throw new RuntimeException("Error during compilation");
        }
    }
}
