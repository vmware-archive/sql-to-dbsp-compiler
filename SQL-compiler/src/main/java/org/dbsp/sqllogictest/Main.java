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
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.List;

/**
 * Execute all SqlLogicTest tests.
 */
public class Main {
    // Following are queries that calcite fails to parse.
    static final HashSet<String> calciteBugs = new HashSet<>();
    static final String[] skipFiles = {};

    static class NoMySql implements TestAcceptancePolicy {
        @Override
        public boolean accept(List<String> skip, List<String> only) {
            return !only.contains("mysql") && !skip.contains("postgresql");
        }
    }

    static class TestLoader extends SimpleFileVisitor<Path> {
        int errors = 0;
        int tests = 0;
        final int testsInFile;
        final int skipFromFile;
        final ISqlTestExecutor executor;

        /**
         * Creates a new class that reads tests from a directory tree and executes them.
         * @param testsInFile     How many tests in a file.
         * @param skipFromFile    Skip this many tests from each file.
         * @param executor        Program that knows how to generate and run the tests.
         */
        TestLoader(int testsInFile, int skipFromFile, ISqlTestExecutor executor) {
            this.testsInFile = testsInFile;
            this.executor = executor;
            this.skipFromFile = skipFromFile;
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
                    test = new SqlTestFile(file.toString(), new NoMySql());
                    this.tests += test.getTestCount();
                } catch (Exception ex) {
                    // We can't yet parse all kinds of tests
                    //noinspection UnnecessaryToStringCall
                    System.out.println(ex.toString());
                    this.errors++;
                }
                if (test != null) {
                    try {
                        System.out.println(file);
                        test.execute(this.executor, this.testsInFile, this.skipFromFile, calciteBugs);
                    } catch (SqlParseException | IOException | InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            //return FileVisitResult.TERMINATE;
            return FileVisitResult.CONTINUE;
        }
    }

    public static void main(String[] argv) throws IOException {
        // Calcite cannot parse this query
        calciteBugs.add("SELECT DISTINCT - 15 - + - 2 FROM ( tab0 AS cor0 CROSS JOIN tab1 AS cor1 )");       
        // Calcite types /0 as not nullable!
        calciteBugs.add("SELECT - - 96 * 11 * + CASE WHEN NOT + 84 NOT BETWEEN 27 / 0 AND COALESCE ( + 61, + AVG ( 81 ) / + 39 + COUNT ( * ) ) THEN - 69 WHEN NULL > ( - 15 ) THEN NULL ELSE NULL END AS col2");
        int batchSize = 1;
        SqlRuntimeLibrary.instance.writeSqlLibrary( "../lib/genlib/src/lib.rs");
        ISqlTestExecutor executor = new DBSPExecutor(true);
        String files =
            //"../../sqllogictest/test/s.test"
            //"../../sqllogictest/test/random/select"
            //"../../sqllogictest/test/random/expr"
            //"../../sqllogictest/test/random/aggregates/"
            //"../../sqllogictest/test/random/groupby"
            "../../sqllogictest/test/select5.test"
        ;
        if (argv.length > 1)
            files = argv[1];
        Path path = Paths.get(files);
        TestLoader loader = new TestLoader(batchSize, 731, executor);
        Files.walkFileTree(path, loader);
        System.out.println("Could not parse: " + loader.errors);
        System.out.println("Parsed tests: " + String.format("%,3d", loader.tests));
    }
}
