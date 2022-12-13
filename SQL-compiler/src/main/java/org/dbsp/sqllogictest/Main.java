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
import org.dbsp.sqlCompiler.circuit.SqlRuntimeLibrary;
import org.dbsp.sqllogictest.executors.*;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Execute all SqlLogicTest tests.
 */
public class Main {
    static class TestLoader extends SimpleFileVisitor<Path> {
        int errors = 0;
        private final SqlTestExecutor executor;
        final SqlTestExecutor.TestStatistics statistics;
        private final AcceptancePolicy policy;

        /**
         * Creates a new class that reads tests from a directory tree and executes them.
         * @param executor Program that knows how to generate and run the tests.
         * @param policy   Policy that dictates which operations can be executed.
         */
        TestLoader(SqlTestExecutor executor,
                   AcceptancePolicy policy) {
            this.executor = executor;
            this.statistics = new SqlTestExecutor.TestStatistics();
            this.policy = policy;
        }

        @SuppressWarnings("ConstantConditions")
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            String extension = Utilities.getFileExtension(file.toString());
            int batchSize = 500;
            int skipPerFile = 0;
            String name = file.getFileName().toString();
            if (name.startsWith("select"))
                batchSize = Math.min(batchSize, 20);
            if (name.startsWith("select5"))
                batchSize = Math.min(batchSize, 5);
            if (executor.is(DBSPExecutor.class))
                executor.to(DBSPExecutor.class).setBatchSize(batchSize, skipPerFile);
            if (attrs.isRegularFile() && extension != null && extension.equals("test")) {
                // validates the test
                SLTTestFile test = null;
                try {
                    System.out.println(file);
                    test = new SLTTestFile(file.toString());
                    test.parse(this.policy);
                } catch (Exception ex) {
                    // We can't yet parse all kinds of tests
                    //noinspection UnnecessaryToStringCall
                    System.out.println(ex.toString());
                    this.errors++;
                }
                if (test != null) {
                    try {
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
        SqlRuntimeLibrary.instance.writeSqlLibrary( "../lib/genlib/src/lib.rs");
        String benchDir = "../../sqllogictest/test";
        List<String> files = Linq.list(
                "random/groupby",
                /*
                "random/select",
                "random/expr",
                "random/aggregates", 
                "select1.test",  
                "select2.test",  
                "select3.test",  
                "select4.test",  
                "select5.test",
                "index/orderby", 
                "index/between",
                "index/view/",
                "index/in",      
                "index/delete",  
                "index/commute", 
                "index/orderby_nosort", 
                "index/random",  
                 */
                "evidence"
        );

        String[] args = {
                "-s",                   // do not validate command status
                "-e", "hybrid",         // hybrid executor DBSP + JDBC
                "-b", "psqlsltbugs.txt",// list of tests to exclude
                "-i",                   // incremental (streaming) testing
                "-d", "psql",           // SQL dialect to use
                "-u", "user",           // database user name
                "-p", "password"        // database password
        };
        if (argv.length > 0) {
            args = argv;
        } else {
            List<String> a = new ArrayList<>();
            a.addAll(Linq.list(args));
            a.addAll(files);
            args = a.toArray(new String[0]);
        }
        /*
        Logger.instance.setDebugLevel(JDBCExecutor.class, 3);
        Logger.instance.setDebugLevel(DBSPExecutor.class, 3);
        Logger.instance.setDebugLevel(DBSP_JDBC_Executor.class, 3);
        Logger.instance.setDebugLevel(SLTTestFile.class, 3);
        Logger.instance.setDebugLevel(PassesVisitor.class, 3);
        Logger.instance.setDebugLevel(RemoveOperatorsVisitor.class, 3);
        Logger.instance.setDebugLevel(CalciteCompiler.class, 2);
         */
        ExecutionOptions options = new ExecutionOptions();
        options.parse(args);
        SqlTestExecutor executor = options.getExecutor();
        System.out.println(options);
        AcceptancePolicy policy = options.getAcceptancePolicy();
        TestLoader loader = new TestLoader(executor, policy);
        for (String file : options.getDirectories()) {
            Path path = Paths.get(benchDir + "/" + file);
            Files.walkFileTree(path, loader);
        }
        System.out.println("Files that could not be not parsed: " + loader.errors);
        System.out.println(loader.statistics);
    }
}
