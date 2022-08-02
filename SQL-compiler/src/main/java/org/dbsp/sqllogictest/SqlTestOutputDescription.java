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

package org.dbsp.sqllogictest;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SqlTestOutputDescription {
    enum ColumnType {
        Integer,
        Real,
        String
    }

    public enum SortOrder {
        None,
        Row,
        Value
    }

    public int valueCount;
    /**
     * Types of columns expected in result.
     */
    public final List<ColumnType> columnTypes;
    @Nullable public String hash;
    /**
     * How results are sorted.
     */
    public SortOrder order;
    @Nullable
    public List<String> queryResults;

    public SqlTestOutputDescription() {
        this.columnTypes = new ArrayList<>();
        this.valueCount = 0;
        this.hash = null;
        this.queryResults = null;
        this.order = SortOrder.None;
    }

    public void clearResults() {
        this.queryResults = new ArrayList<>();
    }

    public void addResultLine(String line) {
        if (this.queryResults == null)
            throw new RuntimeException("queryResults were not initialized");
        this.queryResults.add(line);
        this.valueCount++;
    }

    /**
     * Parse the output type.
     * @param line  A string that starts with the output type.
     * @return      The tail of the string or null on error.
     */
    @Nullable
    String parseType(String line) {
        for (int i = 0; i < line.length(); i++) {
            // Type of result encoded as characters.
            char c = line.charAt(i);
            switch (c) {
                case ' ':
                    return line.substring(i);
                case 'I':
                    this.addColumn(ColumnType.Integer);
                    break;
                case 'R':
                    this.addColumn(ColumnType.Real);
                    break;
                case 'T':
                    this.addColumn(ColumnType.String);
                    break;
                default:
                    return null;
            }
        }
        return "";
    }

    /**
     * Parse the sorting order
     * @param orderDescription  A String that starts with the ordering description
     * @return null on failure, the remaining string if the order is recognized.
     */
    @Nullable
    String parseOrder(String orderDescription) {
        if (orderDescription.startsWith("nosort")) {
            this.order = SortOrder.None;
            return orderDescription.substring("nosort".length());
        } else if (orderDescription.startsWith("rowsort")) {
            this.order = SortOrder.Row;
            return orderDescription.substring("rowsort".length());
        } else if (orderDescription.startsWith("valuesort")) {
            this.order = SortOrder.Value;
            return orderDescription.substring("valuesort".length());
        }
        return null;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public void setValueCount(int values) {
        this.valueCount = values;
    }

    public int getResultColumnCount() {
        return this.columnTypes.size();
    }

    void addColumn(ColumnType type) {
        this.columnTypes.add(type);
    }

    int getExpectedOutputSize() {
        return this.valueCount / this.columnTypes.size();
    }
}
