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
 *
 *
 */

package org.dbsp.sqllogictest;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.dbsp.circuit.SqlRuntimeLibrary;
import org.dbsp.sqllogictest.executors.*;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;

/**
 * Execute all SqlLogicTest tests.
 */
public class Main {
    // Following are queries that calcite fails to parse.
    static final String[] skipFiles = {};

    static class NoMySql implements QueryAcceptancePolicy {
        @Override
        public boolean accept(List<String> skip, List<String> only) {
            return !only.contains("mysql") && !skip.contains("postgresql");
        }
    }

    static class MySql implements QueryAcceptancePolicy {
        @Override
        public boolean accept(List<String> skip, List<String> only) {
            return !only.contains("postgresql") && !skip.contains("mysql");
        }
    }

    static class TestLoader extends SimpleFileVisitor<Path> {
        int errors = 0;
        private final SqlTestExecutor executor;
        SqlTestExecutor.TestStatistics statistics;
        private final QueryAcceptancePolicy policy;

        /**
         * Creates a new class that reads tests from a directory tree and executes them.
         * @param executor Program that knows how to generate and run the tests.
         * @param policy   Policy that dictates which tests can be executed.
         */
        TestLoader(SqlTestExecutor executor, QueryAcceptancePolicy policy) {
            this.executor = executor;
            this.statistics = new SqlTestExecutor.TestStatistics();
            this.policy = policy;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            String extension = Utilities.getFileExtension(file.toString());
            String str = file.toString();
            //noinspection RedundantOperationOnEmptyContainer
            for (String d: skipFiles)
                if (str.contains("expr/slt_good_" + d + "."))
                    return FileVisitResult.CONTINUE;
            if (attrs.isRegularFile() && extension != null && extension.equals("test")) {
                // validates the test
                SqlTestFile test = null;
                try {
                    test = new SqlTestFile(file.toString());
                    test.parse(this.policy);
                } catch (Exception ex) {
                    // We can't yet parse all kinds of tests
                    //noinspection UnnecessaryToStringCall
                    System.out.println(ex.toString());
                    this.errors++;
                }
                if (test != null) {
                    try {
                        System.out.println(file);
                        SqlTestExecutor.TestStatistics stats = this.executor.execute(test);
                        this.statistics.add(stats);
                    } catch (SqlParseException | IOException | InterruptedException |
                            SQLException | NoSuchAlgorithmException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            return FileVisitResult.CONTINUE;
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    public static void main(String[] argv) throws IOException {
        // Calcite cannot parse this query
        final HashSet<String> calciteBugs = new HashSet<>();
        calciteBugs.add("SELECT DISTINCT - 15 - + - 2 FROM ( tab0 AS cor0 CROSS JOIN tab1 AS cor1 )");
        // Calcite types /0 as not nullable!
        calciteBugs.add("SELECT - - 96 * 11 * + CASE WHEN NOT + 84 NOT BETWEEN 27 / 0 AND COALESCE ( + 61, + AVG ( 81 ) / + 39 + COUNT ( * ) ) THEN - 69 WHEN NULL > ( - 15 ) THEN NULL ELSE NULL END AS col2");
        HashSet<String> sltBugs = new HashSet<>();
        // The following query is a bug in sqllogictest: the query claims to return
        // an integer for a string column.
        sltBugs.add("SELECT ALL col5 col0 FROM tab0 WHERE + col3 IS NOT NULL");
        sltBugs.add("SELECT ALL col5 col0 FROM tab1 WHERE + col3 IS NOT NULL");
        sltBugs.add("SELECT ALL col5 col0 FROM tab2 WHERE + col3 IS NOT NULL");
        sltBugs.add("SELECT ALL col5 col0 FROM tab3 WHERE + col3 IS NOT NULL");
        sltBugs.add("SELECT ALL col5 col0 FROM tab4 WHERE + col3 IS NOT NULL");
        int batchSize = 500;
        int skipPerFile = 0;
        SqlRuntimeLibrary.instance.writeSqlLibrary( "../lib/genlib/src/lib.rs");
        DBSPExecutor dExec = new DBSPExecutor(true);
        dExec.avoid(calciteBugs);
        SqlTestExecutor executor;
        executor = new NoExecutor();
        executor = dExec;
        JDBCExecutor jdbc = new JDBCExecutor("jdbc:mysql://localhost/slt", "user", "password");
        jdbc.avoid(sltBugs);
        executor = jdbc;
        DBSP_JDBC_Executor hybrid = new DBSP_JDBC_Executor(jdbc, true);
        executor = hybrid;
        String benchDir = "../../sqllogictest/test";
        // These are all the files we support from sqllogictest.
        String[] files = new String[]{
                //"s.test",
                "random/select",  //done
                "random/expr",
                "random/groupby",
                "random/aggregates",
                "select1.test",
                "select2.test",
                "select3.test",
                "select4.test",
                "select5.test",
        };
        if (argv.length > 1)
            files = Utilities.arraySlice(argv, 1);
        QueryAcceptancePolicy policy =
                executor.is(JDBCExecutor.class) ? new MySql() : new NoMySql();
        TestLoader loader = new TestLoader(executor, policy);
        for (String file : files) {
            if (file.startsWith("select"))
                batchSize = Math.min(batchSize, 20);
            if (file.startsWith("select5"))
                batchSize = Math.min(batchSize, 5);
            Path path = Paths.get(benchDir + "/" + file);
            if (executor.is(DBSPExecutor.class))
                executor.to(DBSPExecutor.class).setBatchSize(batchSize, skipPerFile);
            Files.walkFileTree(path, loader);
        }
        System.out.println("Files that could not be not parsed: " + loader.errors);
        System.out.println(loader.statistics);
    }
}
