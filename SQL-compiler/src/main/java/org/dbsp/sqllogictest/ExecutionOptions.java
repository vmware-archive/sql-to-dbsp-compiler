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

import org.dbsp.sqllogictest.executors.*;
import org.dbsp.util.IndentStream;
import org.dbsp.util.UnsupportedException;

import java.util.*;

public class ExecutionOptions {
    final List<String> directories;
    boolean incremental;
    boolean execute;
    final Set<String> executors;
    String executor;
    String progName;

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
                .intercalate("\n", this.executors)
                .decrease()
                .append("[-i]: Incremental testing.")
                .newline()
                .append("[-n]: Do not execute, jusr parse.")
                .newline()
                .append("`directories` is a list of directories or files under ../sqllogictest/test which will be executed")
                .newline()
                .decrease();
        System.exit(1);
    }

    public ExecutionOptions(String... argv) {
        this.progName = "";
        this.executor = "";
        this.executors = new HashSet<>();
        this.executors.add("DBSP");
        this.executors.add("JDBC");
        this.executors.add("hybrid");
        this.executors.add("none");
        this.execute = true;

        if (argv.length <= 0)
            this.usage();
        this.progName = argv[0];
        this.directories = new ArrayList<>();
        this.incremental = false;

        for (int i = 0; i < argv.length; i++) {
            String arg = argv[i];
            if (arg.startsWith("-")) {
                if (arg.equals("-e")) {
                    String exec = argv[++i];
                    if (!this.executors.contains(exec))
                        this.usage();
                    this.executor = exec;
                } else if (arg.equals("-i")) {
                    this.incremental = true;
                } else if (arg.equals("-n")) {
                    this.execute = false;
                } else {
                    System.err.println("Unknown option " + arg);
                    this.usage();
                }
            } else {
                this.directories.add(arg);
            }
        }
    }

    SqlTestExecutor getExecutor() {
        // Calcite cannot parse this query
        final HashSet<String> calciteBugs = new HashSet<>();
        calciteBugs.add("SELECT DISTINCT - 15 - + - 2 FROM ( tab0 AS cor0 CROSS JOIN tab1 AS cor1 )");
        // Calcite types /0 as not nullable!
        // calciteBugs.add("SELECT - - 96 * 11 * + CASE WHEN NOT + 84 NOT BETWEEN 27 / 0 AND COALESCE ( + 61, + AVG ( 81 ) / + 39 + COUNT ( * ) ) THEN - 69 WHEN NULL > ( - 15 ) THEN NULL ELSE NULL END AS col2");
        // The following two queries trigger a multiplication overflow.
        // Seems like the semantics of overflow is implementation-defined in SQL.
        calciteBugs.add("SELECT DISTINCT - + COUNT( * ) FROM tab1 AS cor0 WHERE NOT - col2 BETWEEN + col0 / 63 + 22 AND + - col2 * - col1 * - col2 * + col2 * + col1 * + - col2 * + + col0");
        calciteBugs.add("SELECT DISTINCT - + COUNT ( * ) FROM tab1 AS cor0 WHERE NOT - col2 BETWEEN + col0 / 63 + 22 AND + - col2 * - col1 * - col2 * + col2 * + col1 * + - col2 * + + col0");

        switch (this.executor) {
            case "none":
                return new NoExecutor();
            case "DBSP":
                DBSPExecutor dExec = new DBSPExecutor(this.execute, this.incremental);
                dExec.avoid(calciteBugs);
                return dExec;
            case "JDBC": {
                return new JDBCExecutor("jdbc:mysql://localhost/slt", "user", "password");
            }
            case "hybrid": {
                JDBCExecutor jdbc = new JDBCExecutor("jdbc:mysql://localhost/slt", "user", "password");
                return new DBSP_JDBC_Executor(jdbc, this.execute, incremental);
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
                '}';
    }
}
