/*
 * Copyright 2023 VMware, Inc.
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

import org.dbsp.sqllogictest.ExecutionOptions;
import org.dbsp.sqllogictest.executors.SqlTestExecutor;
import org.dbsp.util.TestStatistics;

/**
 * This class essentially counts the number of tests in a ZetaSQLTestFile.
 * If the code got so far it means that the tests were parsed successfully.
 */
public class NoExecutor extends SqlTestExecutor {
    public NoExecutor() {}

    TestStatistics execute(ZetaSQLTestFile file, ExecutionOptions options) {
        this.startTest();
        TestStatistics result = new TestStatistics(options.stopAtFirstError);
        for (ZetaSQLTest ignored : file.tests) {
            result.ignored++;
        }
        this.reportTime(result.totalTests());
        return result;
    }
}
