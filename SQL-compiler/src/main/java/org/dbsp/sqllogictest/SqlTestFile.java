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
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Represents the data from a .test file from the
 * SqlLogicTest test framework.
 */
public class SqlTestFile {
    /**
     * Current line number in test file.
     */
    private int lineno;

    public final SqlTestPrepareTables prepareTables;
    public final SqlTestPrepareInput prepareInput;
    public final List<SqlTestQuery> tests;
    private final BufferedReader reader;
    // To support undo for reading
    @Nullable
    private String nextLine;
    private final String testFile;
    private boolean done;

    public SqlTestFile(String testFile, TestAcceptancePolicy policy) throws IOException {
        this.tests = new ArrayList<>();
        this.prepareTables = new SqlTestPrepareTables();
        this.prepareInput = new SqlTestPrepareInput();
        File file = new File(testFile);
        this.reader = new BufferedReader(new FileReader(file));
        this.lineno = 0;
        this.testFile = testFile;
        this.done = false;
        this.parse(policy);
    }

    void error(String message) {
        throw new RuntimeException("File " + this.testFile + "\nError at line " + this.lineno + ": " + message);
    }

    private void undoRead(String line) {
        if (this.nextLine != null)
            throw new RuntimeException("Only one undoRead allowed");
        this.nextLine = line;
    }

    String getNextLine(boolean nullOk) throws IOException {
        String line;
        if (this.nextLine != null) {
            line = this.nextLine;
            this.nextLine = null;
        } else {
            this.lineno++;
            line = this.reader.readLine();
            if (!nullOk && line == null)
                this.error("Test file ends prematurely");
            if (line == null) {
                this.done = true;
                line = "";
            }
        }
        return line.trim();
    }

    String nextLine(boolean nullOk) throws IOException {
        while (true) {
            String line = this.getNextLine(nullOk);
            if (this.done)
                return line;
            // Drop comments
            int sharp = line.indexOf("#");
            if (sharp > 0)
                return line.substring(0, sharp - 1);
            else if (sharp < 0)
                return line;
            // else read one more
        }
    }

    /**
     * Parse the beginning of a SqlLogicTest .test file, which prepares the test data
     */
    private void parsePrepare() throws IOException {
        String line;
        while (!this.done) {
            line = this.nextLine(false);
            if (line.isEmpty())
                continue;
            if (line.startsWith("hash-threshold"))
                continue;

            if (line.startsWith("statement")) {
                boolean ok = line.startsWith("statement ok");
                line = this.nextLine(false);
                StringBuilder statement = new StringBuilder();
                while (!line.isEmpty()) {
                    // TODO: this is wrong, but I can't get the Calcite parser
                    // to accept Postgres-like PRIMARY KEY statements.
                    line = line.replace("PRIMARY KEY", "");
                    statement.append(line);
                    line = this.nextLine(false);
                }

                String stat = statement.toString();
                if (ok) {
                    if (stat.toLowerCase().contains("create table")) {
                        this.prepareTables.add(stat);
                    } else {
                        this.prepareInput.add(stat);
                    }
                }
                // Should we ignore the statements that should produce an error when executed,
                // or ignore the whole test?  Right now we ignore such statements.
            } else {
                this.undoRead(line);
                break;
            }
        }
    }

    /**
     * Parse a query that executes a SqlLogicTest test.
     */
    @SuppressWarnings("SpellCheckingInspection")
    @Nullable
    private SqlTestQuery parseTestQuery(TestAcceptancePolicy policy) throws IOException {
        @Nullable String line = this.nextLine(true);
        if (this.done)
            return null;
        List<String> skip = new ArrayList<>();
        List<String> only = new ArrayList<>();
        while (line.startsWith("onlyif") || line.startsWith("skipif")) {
            boolean sk = line.startsWith("skipif");
            String cond = line.substring("onlyif".length()).trim();
            if (sk)
                skip.add(cond);
            else
                only.add(cond);
            line = this.nextLine(false);
        }

        if (line.startsWith("halt")) {
            this.nextLine(false);
            if (policy.accept(skip, only))
                return null;
            return this.parseTestQuery(policy);
        }

        if (!line.startsWith("query")) {
            this.error("Unexpected line: " + Utilities.singleQuote(line));
        }
        @Nullable SqlTestQuery result = new SqlTestQuery();
        line = line.substring("query".length()).trim();
        if (line.isEmpty())
            this.error("Malformed query description " + line);

        line = result.outputDescription.parseType(line);
        if (line == null)
            this.error("Could not parse output column types");
        assert line != null;
        line = line.trim();
        if (line.isEmpty())
            this.error("Malformed query description " + line);

        line = result.outputDescription.parseOrder(line);
        if (line == null)
            this.error("Did not understand sort order");
        assert line != null;
        line = line.trim();
        if (!line.isEmpty())
            result.setName(line);

        line = this.nextLine(false);
        StringBuilder query = new StringBuilder();
        if (!this.done) {
            while (!line.startsWith("----")) {
                query.append(" ");
                query.append(line);
                line = this.nextLine(false);
            }
        }
        result.setQuery(query.toString().trim());

        if (!this.done) {
            line = this.nextLine(true);
            if (!this.done) {
                if (line.contains("values hashing to")) {
                    int vi = line.indexOf("values hashing to");
                    String number = line.substring(0, vi - 1);
                    int values = Integer.parseInt(number);
                    result.outputDescription.setValueCount(values);
                    line = line.substring(vi + "values hashing to".length()).trim();
                    result.outputDescription.setHash(line);
                    line = this.nextLine(true);
                    if (!this.done && !line.isEmpty())
                        this.error("Expected an empty line between tests: " + Utilities.singleQuote(line));
                } else {
                    result.outputDescription.clearResults();
                    while (!line.isEmpty()) {
                        result.outputDescription.addResultLine(line);
                        line = this.getNextLine(true);
                    }
                }
            } else {
                result.outputDescription.clearResults();
            }
        }

        if (!policy.accept(skip, only)) {
            // Invoke recursively to parse the next query.
            return this.parseTestQuery(policy);
        }
        return result;
    }

    /*
        The Test file format is described at
        https://www.sqlite.org/sqllogictest/doc/tip/about.wiki.

        Here is an example:

        hash-threshold 8

        statement ok
        CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)

        statement ok
        INSERT INTO t1(e,c,b,d,a) VALUES(NULL,102,NULL,101,104)

        statement ok
        INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)

        query I nosort
        SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
          FROM t1
         ORDER BY 1
        ----
        30 values hashing to 3c13dee48d9356ae19af2515e05e6b54
     */
    private void parse(TestAcceptancePolicy policy) throws IOException {
        this.parsePrepare();
        while (!this.done) {
            @Nullable
            SqlTestQuery test = this.parseTestQuery(policy);
            if (test != null)
                this.tests.add(test);
        }
    }

    public int getTestCount() {
        return this.tests.size();
    }

    void run(ISqlTestExecutor executor) throws IOException, InterruptedException {
        long start = System.nanoTime();
        executor.run();
        long end = System.nanoTime();
        System.out.println("Test took " + (end - start) / 1000000000 + " seconds");
        executor.reset();
    }

    /**
     * Execute batch of tests from this file.
     * @param executor    Program that knows how to execute tests.
     * @param batchSize   Number of tests to execute.
     * @param skipFromFile Number of tests to skip.
     * @param calciteBugs Queries that need to be skipped.
     */
    @SuppressWarnings("SameParameterValue")
    void execute(ISqlTestExecutor executor, int batchSize, int skipFromFile, HashSet<String> calciteBugs)
            throws SqlParseException, IOException, InterruptedException {
        int queryCount = 0;
        executor.reset();
        for (SqlTestQuery testQuery : this.tests) {
            if (calciteBugs.contains(testQuery.query)) {
                System.err.println("Skipping query that cannot be handled by Calcite " + testQuery.query);
                continue;
            }
            try {
                // For each query we generate a complete circuit, with all tables, containing the same data.
                executor.createTables(this.prepareTables);
                if (skipFromFile > 0) {
                    skipFromFile--;
                } else {
                    executor.addQuery(testQuery.query, this.prepareInput, testQuery.outputDescription);
                    queryCount++;
                }
            } catch (Throwable ex) {
                System.err.println("Error while compiling " + testQuery.query);
                throw ex;
            }
            if (queryCount > 0 &&
                    queryCount % batchSize == 0) {
                executor.generateCode(0);
                this.run(executor);
                queryCount = 0;
            }
        }
        if ((queryCount % batchSize) != 0) {
            // left overs
            executor.generateCode(0);
            this.run(executor);
        }
    }
}
