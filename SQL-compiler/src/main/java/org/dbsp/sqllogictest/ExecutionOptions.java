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
import org.dbsp.util.SqlDialectConverter;
import org.dbsp.util.Unimplemented;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

public class ExecutionOptions {
    public static class ExecutorValidator implements IParameterValidator {
        final private Set<String> legalExecutors;

        public ExecutorValidator() {
            this.legalExecutors = new HashSet<>();
            this.legalExecutors.add("DBSP");
            this.legalExecutors.add("JDBC");
            this.legalExecutors.add("hybrid");
            this.legalExecutors.add("none");
        }

        @Override
        public void validate(String name, String value) throws ParameterException {
            if (this.legalExecutors.contains(value))
                return;
            throw new ParameterException("Illegal executor name " + value + "\n"
                + "Legal values are: " + this.legalExecutors);
        }
    }

    @Parameter(description = "Files or directories with test data")
    List<String> directories = new ArrayList<>();
    @Parameter(names = "-i", description = "Incremental testing")
    boolean incremental;
    @Parameter(names = "-n", description = "Do not execute, just parse")
    boolean doNotExecute;
    @Parameter(names = "-e", validateWith = ExecutorValidator.class)
    String executor = "none";
    @Parameter(names = "-d", converter = SqlDialectConverter.class)
    Lex dialect = Lex.ORACLE;
    @Parameter(names = "-u", description = "Name of user to use for database")
    String user = "user";
    @Parameter(names = "-p", description = "Password of user for the database")
    String password = "password";
    @Parameter(names = "-s", description = "Ignore the status of SQL commands executed")
    boolean validateStatus;
    @Parameter(names = "-b", description = "Load a list of buggy commands to skip from this file")
    @Nullable
    String bugsFile = null;
    @Parameter(names = "-l", description = "Input source for DBSP program (choices: csv, db)")
    @Nullable
    String inputSource = "csv";

    static class PostgresPolicy implements AcceptancePolicy {
        @Override
        public boolean accept(List<String> skip, List<String> only) {
            if (only.contains("postgresql"))
                return true;
            if (!only.isEmpty())
                return false;
            return !skip.contains("postgresql");
        }
    }

    static class MySqlPolicy implements AcceptancePolicy {
        @Override
        public boolean accept(List<String> skip, List<String> only) {
            if (only.contains("mysql"))
                return true;
            if (!only.isEmpty())
                return false;
            return !skip.contains("mysql");
        }
    }

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

    String jdbcConnectionString() {
        switch (this.dialect) {
            case MYSQL:
                return "jdbc:mysql://localhost/slt";
            case ORACLE:
                return "jdbc:postgresql://localhost/slt";
            default:
                throw new RuntimeException();
        }
    }

    String connectionString() {
        Objects.requireNonNull(this.inputSource);
        if (this.inputSource.equals("csv")) {
            return "csv";
        } else if (this.inputSource.equals("db")) {
            switch (this.dialect) {
                case ORACLE:
                    return String.format("postgresql://localhost?dbname=slt&user=%s&password=%s", this.user,
                                         this.password);
                case MYSQL:
                default:
                    throw new Unimplemented();
            }
        }
        throw new Unimplemented(String.format("Unsupported input source or DB dialect: inputSource=%s dialect=%s",
                                              this.inputSource, this.dialect));
    }

    AcceptancePolicy getAcceptancePolicy() {
        switch (this.dialect) {
            case MYSQL:
                return new MySqlPolicy();
            case ORACLE:
                return new PostgresPolicy();
            default:
                throw new RuntimeException();
        }
    }

    JDBCExecutor jdbcExecutor(HashSet<String> sltBugs) {
        JDBCExecutor jdbc =  new JDBCExecutor(this.jdbcConnectionString(), this.dialect,
                this.user, this.password);
        jdbc.avoid(sltBugs);
        jdbc.setValidateStatus(this.validateStatus);
        return jdbc;
    }

    SqlSLTTestExecutor getExecutor() throws IOException {
        HashSet<String> sltBugs = new HashSet<>();
        if (this.bugsFile != null) {
            sltBugs = this.readBugsFile(this.bugsFile);
        }

        CompilerOptions options = new CompilerOptions();
        options.ioOptions.dialect = this.dialect;
        options.optimizerOptions.incrementalize = this.incremental;

        switch (this.executor) {
            case "none":
                return new NoExecutor();
            case "DBSP":
                DBSPExecutor dExec = new DBSPExecutor(!this.doNotExecute, options, connectionString());
                dExec.avoid(sltBugs);
                dExec.setValidateStatus(this.validateStatus);
                return dExec;
            case "JDBC": {
                return this.jdbcExecutor(sltBugs);
            }
            case "hybrid": {
                JDBCExecutor jdbc = this.jdbcExecutor(sltBugs);
                DBSP_JDBC_Executor result = new DBSP_JDBC_Executor(jdbc, !this.doNotExecute, options,
                                                                   connectionString());
                result.avoid(sltBugs);
                result.setValidateStatus(this.validateStatus);
                return result;
            }
        }
        throw new UnsupportedException(this.executor);  // unreachable
    }

    public List<String> getDirectories() {
        return this.directories;
    }

    public void parse(String... argv) {
        JCommander.newBuilder()
                .addObject(this)
                .build()
                .parse(argv);
    }

    @Override
    public String toString() {
        return "ExecutionOptions{" +
                "directories=" + this.directories +
                ", incremental=" + this.incremental +
                ", execute=" + !this.doNotExecute +
                ", executor=" + this.executor +
                ", dialect=" + this.dialect +
                '}';
    }
}
