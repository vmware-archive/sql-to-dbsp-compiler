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
import java.util.List;

public class SqlTestFile {
    /**
     * Current line number in test file.
     */
    private int lineno;

    @Nullable
    public SqlTestPrepare prepare;
    public final List<SqlTestQuery> tests;
    private final BufferedReader reader;
    // To support undo for reading
    @Nullable
    private String nextLine;
    private final String testFile;
    private boolean done;

    public SqlTestFile(String testFile) throws IOException {
        this.tests = new ArrayList<>();
        File file = new File(testFile);
        this.reader = new BufferedReader(new FileReader(file));
        this.lineno = 0;
        this.testFile = testFile;
        this.done = false;
        this.parse();
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

    private SqlTestPrepare parsePrepare() throws IOException {
        SqlTestPrepare result = new SqlTestPrepare();
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
                if (ok)
                    result.add(line);
                // Should we ignore the  statements where the statement should produce an error
                // or ignore the whole test?
            }

            if (line.startsWith("query")) {
                this.undoRead(line);
                break;
            }
        }
        return result;
    }

    @Nullable
    private SqlTestQuery parseTestQuery() throws IOException {
        String line = this.nextLine(true);
        if (this.done)
            return null;
        while (line.startsWith("onlyif") || line.startsWith("skipif"))
            line = this.nextLine(false);

        if (!line.startsWith("query")) {
            this.error("Unexpected line: " + Utilities.singleQuote(line));
        }
        @Nullable SqlTestQuery result = new SqlTestQuery();
        line = line.substring(5).trim();
        if (line.isEmpty())
            this.error("Malformed query description " + line);

        boolean done = false;
        for (int i = 0; !done && i < line.length(); i++) {
            char c = line.charAt(i);
            switch (c) {
                case ' ':
                    done = true;
                    line = line.substring(i).trim();
                    break;
                case 'I':
                    result.addColumn(SqlTestQuery.ColumnType.Integer);
                    break;
                case 'R':
                    result.addColumn(SqlTestQuery.ColumnType.Real);
                    break;
                case 'T':
                    result.addColumn(SqlTestQuery.ColumnType.String);
                    break;
                default:
                    this.error("Unexpected column type: " + c);
            }
        }

        if (line.isEmpty())
            this.error("Malformed query description " + line);
        if (line.startsWith("nosort")) {
            result.setOrder(SqlTestQuery.SortOrder.None);
            line = line.substring("nosort".length());
        } else if (line.startsWith("rowsort")) {
            result.setOrder(SqlTestQuery.SortOrder.Row);
            line = line.substring("rowsort".length());
        } else if (line.startsWith("valuesort")) {
            result.setOrder(SqlTestQuery.SortOrder.Value);
            line = line.substring("valuesort".length());
        } else {
            this.error("Unexpected sort value in query " + line);
        }
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
        result.setQuery(query.toString());
        if (this.done)
            return result;

        line = this.nextLine(true);
        if (!this.done) {
            if (line.contains("values hashing to")) {
                int vi = line.indexOf("values hashing to");
                String number = line.substring(0, vi - 1);
                int rows = Integer.parseInt(number);
                result.setRowCount(rows);
                line = line.substring(vi + "values hashing to".length()).trim();
                result.setHash(line);
                line = this.nextLine(true);
                if (!this.done && !line.isEmpty())
                    this.error("Expected an empty line between tests: " + Utilities.singleQuote(line));
            } else {
                while (!line.isEmpty()) {
                    result.addResultLine(line);
                    line = this.nextLine(false);
                }
            }
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
    private void parse() throws IOException {
        this.prepare = this.parsePrepare();
        while (!this.done) {
            @Nullable
            SqlTestQuery test = this.parseTestQuery();
            if (test != null)
                this.tests.add(test);
        }
    }

    public int getTestCount() {
        return this.tests.size();
    }

    void execute(ISqlTestExecutor executor) throws SqlParseException {
        if (this.prepare == null)
            return;
        for (SqlTestQuery query: this.tests) {
            executor.reset();
            executor.prepare(this.prepare);
            query.executeAndValidate(executor);
        }
    }
}
