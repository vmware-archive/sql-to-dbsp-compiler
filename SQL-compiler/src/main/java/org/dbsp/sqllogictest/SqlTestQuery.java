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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SqlTestQuery {
    /**
     * Query that is executed.
     */
    private String query;
    /**
     * Types of columns expected in result.
     */
    private final List<ColumnType> columnTypes;
    /**
     * How results are sorted.
     */
    private SortOrder order;
    @Nullable private String name;
    @Nullable private String hash;
    private int valueCount;
    @Nullable
    private List<String> queryResults;

    public void setName(String name) {
        this.name = name;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public void setValueCount(int values) {
        this.valueCount = values;
    }

    public void addResultLine(String line) {
        if (this.queryResults == null)
            this.queryResults = new ArrayList<>();
        this.queryResults.add(line);
        this.valueCount += this.getResultColumnCount();
    }

    public int getResultColumnCount() {
        return this.columnTypes.size();
    }

    public void executeAndValidate(ISqlTestExecutor executor, SqlTestPrepareInput input) throws SqlParseException {
        if (this.hash != null && !this.hash.isEmpty()) {
            // TODO: generate hash-based validator
        }
        executor.executeAndValidate(this.query, input,
                this.queryResults, this.valueCount / this.getResultColumnCount());
    }

    enum ColumnType {
        Integer,
        Real,
        String
    }

    enum SortOrder {
        None,
        Row,
        Value
    }

    SqlTestQuery() {
        this.query = "";
        this.columnTypes = new ArrayList<>();
        this.valueCount = 0;
        this.hash = "";
        this.queryResults = null;
        this.order = SortOrder.None;
    }

    void addColumn(ColumnType type) {
        this.columnTypes.add(type);
    }

    void setOrder(SortOrder order) {
        this.order = order;
    }

    void setQuery(String query) {
        this.query = query;
    }
}
