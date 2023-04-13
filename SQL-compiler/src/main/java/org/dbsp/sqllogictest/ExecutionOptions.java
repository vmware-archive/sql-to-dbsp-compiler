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

package org.dbsp.sqllogictest;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.calcite.config.Lex;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqllogictest.executors.*;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;
import java.io.*;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings("CanBeFinal")
public class ExecutionOptions {
    public static class ExecutorValidator implements IParameterValidator {
        private final Set<String> legalExecutors;

        public ExecutorValidator() {
            this.legalExecutors = new HashSet<>();
            this.legalExecutors.add("DBSP");
            this.legalExecutors.add("JDBC");
            this.legalExecutors.add("hybrid");
            this.legalExecutors.add("none");
            this.legalExecutors.add("calcite");
        }

        @Override
        public void validate(String name, String value) throws ParameterException {
            if (this.legalExecutors.contains(value))
                return;
            throw new ParameterException("Illegal executor name " + value + "\n"
                + "Legal values are: " + this.legalExecutors);
        }
    }

    @Parameter(names = "-d", description = "Directory with SLT tests")
    public String sltDirectory = "../../sqllogictest";
    @Parameter(names = "-i", description = "Install the SLT tests if the directory does not exist")
    public boolean install = false;
    @Parameter(names = "-h", description = "Show this help message and exit")
    public boolean help = false;
    @Parameter(names = "-x", description = "Stop at the first encountered query error")
    public boolean stopAtFirstError = false;
    @Parameter(description = "Files or directories with test data")
    List<String> directories = new ArrayList<>();
    @Parameter(names = "-inc", description = "Incremental testing")
    boolean incremental;
    @Parameter(names = "-n", description = "Do not execute, just parse the test files")
    boolean doNotExecute;
    @Parameter(names = "-e", validateWith = ExecutorValidator.class)
    String executor = "none";
    @Parameter(names = "-s", description = "Ignore the status of SQL commands executed")
    boolean validateStatus;
    @Parameter(names = "-b", description = "Load a list of buggy commands to skip from this file")
    @Nullable
    String bugsFile = null;
    // @Parameter(names = "-j", description = "Validate JSON JIT IR representation while compiling")
    // TODO: reenable this when the JIT compiler works properly
    boolean validateJson = false;

    /**
     * Read the list of statements and queries to skip from a file.
     */
    HashSet<String> readBugsFile(String fileName) throws IOException {
        HashSet<String> bugs = new HashSet<>();
        File file = new File(fileName);
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                if (line.startsWith("//"))
                    continue;
                bugs.add(line);
            }
        }
        return bugs;
    }

    final JCommander commander;

    public ExecutionOptions() {
        this.commander = JCommander.newBuilder()
                .addObject(this)
                .build();
        this.commander.setProgramName("slt");
    }

    String jdbcConnectionString() {
        return "jdbc:hsqldb:mem:db";
    }

    public void usage() {
        this.commander.usage();
    }

    String connectionString() {
        return "csv";
    }

    JDBCExecutor jdbcExecutor(HashSet<String> sltBugs) {
        JDBCExecutor jdbc =  new JDBCExecutor(this.jdbcConnectionString());
        jdbc.avoid(sltBugs);
        jdbc.setValidateStatus(this.validateStatus);
        return jdbc;
    }

    SqlSLTTestExecutor getExecutor() throws IOException, SQLException {
        HashSet<String> sltBugs = new HashSet<>();
        if (this.bugsFile != null) {
            sltBugs = this.readBugsFile(this.bugsFile);
        }

        CompilerOptions options = new CompilerOptions();
        options.ioOptions.lexicalRules = Lex.ORACLE;
        options.optimizerOptions.incrementalize = this.incremental;

        switch (this.executor) {
            case "none":
                return new NoExecutor();
            case "DBSP":
                DBSPExecutor dExec = new DBSPExecutor(!this.doNotExecute, this.validateJson, options, connectionString());
                dExec.avoid(sltBugs);
                dExec.setValidateStatus(this.validateStatus);
                return dExec;
            case "JDBC": {
                return this.jdbcExecutor(sltBugs);
            }
            case "calcite": {
                JDBCExecutor jdbc = this.jdbcExecutor(sltBugs);
                CalciteExecutor result = new CalciteExecutor(jdbc);
                result.avoid(sltBugs);
                result.setValidateStatus(this.validateStatus);
                return result;
            }
            case "hybrid": {
                JDBCExecutor jdbc = this.jdbcExecutor(sltBugs);
                DBSP_JDBC_Executor result = new DBSP_JDBC_Executor(
                        jdbc, !this.doNotExecute, this.validateJson,
                        options, connectionString());
                result.avoid(sltBugs);
                result.setValidateStatus(this.validateStatus);
                return result;
            }
            default:
                break;
        }
        throw new UnsupportedException(this.executor);  // unreachable
    }

    public List<String> getDirectories() {
        return this.directories;
    }

    public void parse(String... argv) {
        this.commander.parse(argv);
    }

    @Override
    public String toString() {
        return "ExecutionOptions{" +
                "tests source=" + this.sltDirectory +
                ", install=" + this.install +
                ", directories=" + this.directories +
                ", incremental=" + this.incremental +
                ", execute=" + !this.doNotExecute +
                ", executor=" + this.executor +
                ", stopAtFirstError=" + this.stopAtFirstError +
                '}';
    }
}
