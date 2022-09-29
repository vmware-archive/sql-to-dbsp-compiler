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

package org.dbsp.sqllogictest.executors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqllogictest.SqlTestFile;
import org.dbsp.util.ICastable;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * Interface implemented by a class that knows how to execute a test.
 */
public abstract class SqlTestExecutor implements ICastable {
    static final DecimalFormat df = new DecimalFormat("#,###");
    protected final Set<String> buggyQueries;

    public static class TestStatistics {
        public int failed;
        public int passed;
        public int ignored;

        public void add(TestStatistics stats) {
            this.failed += stats.failed;
            this.passed += stats.passed;
            this.ignored += stats.ignored;
        }

        @Override
        public String toString() {
            return "Passed: " + df.format(this.passed) +
                    "\nFailed: " + df.format(this.failed) +
                    "\nIgnored: " + df.format(this.ignored) +
                    "\n";
        }
    }

    private static long startTime = -1;
    private static int totalTests = 0;
    private long lastTestStartTime;
    long statementsExecuted = 0;
    long queriesExecuted = 0;

    protected SqlTestExecutor() {
        this.buggyQueries = new HashSet<>();
    }

    static long seconds(long end, long start) {
        return (end - start) / 1000000000;
    }

    public void avoid(HashSet<String> calciteBugs) {
        this.buggyQueries.addAll(calciteBugs);
    }

    void reportTime(int tests) {
        long end = System.nanoTime();
        totalTests += tests;
        System.out.println(df.format(tests) + " tests took " +
                df.format(seconds(end, this.lastTestStartTime)) + "s, "
                + df.format(totalTests) + " took " +
                df.format(seconds(end, startTime)) + "s");
    }

    void startTest() {
        this.lastTestStartTime = System.nanoTime();
        if (startTime == -1)
            startTime = lastTestStartTime;
    }

    /**
     * Execute the specified test file.
     */
    public abstract TestStatistics execute(SqlTestFile testFile) throws SqlParseException, IOException, InterruptedException, SQLException, NoSuchAlgorithmException;
}
