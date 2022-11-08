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

import org.apache.calcite.config.Lex;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqllogictest.executors.*;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Logger;
import org.dbsp.util.UnsupportedException;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

public class ExecutionOptions {
    final List<String> directories;
    boolean incremental;
    boolean execute;
    String executor;
    String dialect;
    String progName;
    String user;
    String password;
    @Nullable
    String bugsFile = null;

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

    final private List<String> legalDialects;
    final private Set<String> legalExecutors;

    void usage() {
        IndentStream stream = new IndentStream(System.out);
        stream.append("Run SqlLogicTests using DBSP")
                .newline()
                .append("Usage:")
                .newline()
                .append(this.progName)
                .append(" options directories")
                .newline()
                .append("where")
                .increase()
                .append("-e executor: Use the specified executor.  Legal executors are")
                .increase()
                .intercalate("\n", this.legalExecutors)
                .decrease()
                .append("[-i]: Incremental testing.")
                .newline()
                .append("[-d dialect]: SQL dialect to use.  One of:")
                .increase()
                .intercalate("\n", this.legalDialects)
                .decrease()
                .append("[-n]: Do not execute, just parse.")
                .newline()
                .append("[-T module]: Increase debug level for specified module.")
                .newline()
                .append("[-u user]: Name of user to use for database; default is ")
                .append(Utilities.singleQuote(this.user))
                .newline()
                .append("[-p module]: Password for database user; default is ")
                .append(Utilities.singleQuote(this.password))
                .newline()
                .append("[-b bugsFile]: Load a list of buggy commands to skip from this file.")
                .newline()
                .append("`directories` is a list of directories or files under ../sqllogictest/test which will be executed")
                .newline()
                .decrease();
        System.exit(1);
    }

    void parseArguments(String... argv) {
        for (int i = 0; i < argv.length; i++) {
            String arg = argv[i];
            if (arg.startsWith("-")) {
                switch (arg) {
                    case "-e":
                        String exec = argv[++i];
                        if (!this.legalExecutors.contains(exec))
                            this.usage();
                        this.executor = exec;
                        break;
                    case "-i":
                        this.incremental = true;
                        break;
                    case "-n":
                        this.execute = false;
                        break;
                    case "-b":
                        this.bugsFile = argv[++i];
                        break;
                    case "-T": {
                        String module = argv[++i];
                        int level = Logger.instance.getDebugLevel(module);
                        Logger.instance.setDebugLevel(module, level + 1);
                        break;
                    }
                    case "-d": {
                        this.dialect = argv[++i];
                        if (!this.legalDialects.contains(this.dialect))
                            this.usage();
                        break;
                    }
                    case "-u": {
                        this.user = argv[++i];
                        break;
                    }
                    case "-p": {
                        this.password = argv[++i];
                        break;
                    }
                    default:
                        System.err.println("Unknown option " + arg);
                        this.usage();
                        break;
                }
            } else {
                this.directories.add(arg);
            }
        }
    }

    public ExecutionOptions(String... argv) {
        this.progName = "";
        this.executor = "";
        this.user = "user0";
        this.password = "password";
        this.legalExecutors = new HashSet<>();
        this.legalExecutors.add("DBSP");
        this.legalExecutors.add("JDBC");
        this.legalExecutors.add("hybrid");
        this.legalExecutors.add("none");
        this.legalDialects = new ArrayList<>();
        this.legalDialects.add("mysql");
        this.legalDialects.add("psql");
        this.dialect = "mysql";
        this.execute = true;

        if (argv.length <= 0)
            this.usage();
        this.progName = argv[0];
        this.directories = new ArrayList<>();
        this.incremental = false;
        try {
            this.parseArguments(argv);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            this.usage();
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
            case "mysql":
                return "jdbc:mysql://localhost/slt";
            case "psql":
                return "jdbc:postgresql://localhost/slt";
            default:
                this.usage();
                return "";
        }
    }

    AcceptancePolicy getAcceptancePolicy() {
        switch (this.dialect) {
            case "mysql":
                return new MySqlPolicy();
            case "psql":
                return new PostgresPolicy();
            default:
                this.usage();
                throw new RuntimeException();
        }
    }

    JDBCExecutor jdbcExecutor(HashSet<String> sltBugs) {
        JDBCExecutor jdbc =  new JDBCExecutor(this.jdbcConnectionString(), this.dialect,
                this.user, this.password);
        jdbc.avoid(sltBugs);
        return jdbc;
    }

    SqlTestExecutor getExecutor() throws IOException {
        HashSet<String> sltBugs = new HashSet<>();
        if (this.bugsFile != null) {
            sltBugs = this.readBugsFile(this.bugsFile);
        }

        CompilerOptions options = new CompilerOptions();
        switch (this.dialect) {
            case "mysql":
                options.dialect = Lex.MYSQL;
                break;
            case "psql":
                options.dialect = Lex.ORACLE;
                break;
            default:
                this.usage();
        }
        options.incrementalize = this.incremental;

        switch (this.executor) {
            case "none":
                return new NoExecutor();
            case "DBSP":
                DBSPExecutor dExec = new DBSPExecutor(this.execute, options);
                dExec.avoid(sltBugs);
                return dExec;
            case "JDBC": {
                return this.jdbcExecutor(sltBugs);
            }
            case "hybrid": {
                JDBCExecutor jdbc = this.jdbcExecutor(sltBugs);
                DBSP_JDBC_Executor result = new DBSP_JDBC_Executor(jdbc, this.execute, options);
                result.avoid(sltBugs);
                return result;
            }
            default: {
                this.usage();
            }
        }
        throw new UnsupportedException(this.executor);  // unreachable
    }

    public List<String> getDirectories() {
        return this.directories;
    }

    @Override
    public String toString() {
        return "ExecutionOptions{" +
                "directories=" + this.directories +
                ", incremental=" + this.incremental +
                ", execute=" + this.execute +
                ", executor=" + this.executor +
                ", dialect=" + this.dialect +
                '}';
    }
}
