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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Interface implemented by a class that knows how to execute a test.
 */
public interface ISqlTestExecutor {
    /**
     * Number of queries added so far.
     */
    int getQueryCount();
    /**
     * Prepare for a new test.
     */
    void reset() throws FileNotFoundException, UnsupportedEncodingException;
    /**
     * Prepare the input tables.
     */
    void prepareTables(SqlTestPrepareTables prepare) throws SqlParseException;

    /**
     * Add code for a query to be tested.
     * @param query   SQL query that is tested.
     * @param inputs  Input data that is inserted in the tables.
     * @param description  Description of the expected output.
     */
    void addQuery(String query,
                  SqlTestPrepareInput inputs,
                  SqlTestOutputDescription description) throws SqlParseException;

    /**
     * Run all the generated tests.
     */
    void run() throws IOException, InterruptedException;
}
