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

package org.dbsp.zetasqltest;

import org.dbsp.util.TestStatistics;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Execute the tests in the Zetasql test suite.
 */
public class Main {
    static String zetaRepo = "../../zetasql/zetasql/compliance/testdata";

    static class TestLoader extends SimpleFileVisitor<Path> {
        int errors = 0;
        final TestStatistics statistics;

        TestLoader() {
            this.statistics = new TestStatistics();
        }

        @SuppressWarnings("ConstantConditions")
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            String extension = Utilities.getFileExtension(file.toString());
            NoExecutor executor = new NoExecutor();
            if (attrs.isRegularFile() && extension != null && extension.equals("test")) {
                ZetaSQLTestFile test;
                try {
                    System.out.println(file);
                    test = ZetaSQLTestFile.parse(file.toString());
                } catch (Exception ex) {
                    System.out.println(ex);
                    this.errors++;
                    throw ex;
                }
                TestStatistics stats = executor.execute(test);
                this.statistics.add(stats);
            }
            return FileVisitResult.CONTINUE;
        }
    }


    public static void main(String[] args) throws IOException {
        //Logger.instance.setDebugLevel(ZetatestVisitor.class, 1);
        Path path = Paths.get(zetaRepo);
        TestLoader loader = new TestLoader();
        Files.walkFileTree(path, loader);
        System.out.println("Files that could not be not parsed: " + loader.errors);
        System.out.println(loader.statistics);
    }
}
