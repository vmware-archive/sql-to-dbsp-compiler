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

import org.dbsp.sqllogictest.*;
import org.dbsp.util.*;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class JDBCExecutor extends SqlSLTTestExecutor implements IModule {
    public final String db_url;
    @Nullable
    Connection connection;

    // In the end everything is decoded as a string
    static class Row {
        public final List<String> values;
        Row() {
            this.values = new ArrayList<>();
        }
        void add(String v) {
            this.values.add(v);
        }
        @Override
        public String toString() {
            return String.join("\n", this.values);
        }
    }

    static class Rows {
        public List<Row> rows;

        Rows() {
            this.rows = new ArrayList<>();
        }

        void add(Row row) {
            this.rows.add(row);
        }
        @Override
        public String toString() {
            return String.join("\n", Linq.map(this.rows, Row::toString));
        }
        public int size() {
            return this.rows.size();
        }

        public void sort(SqlTestQueryOutputDescription.SortOrder order) {
            switch (order) {
                case None:
                    break;
                case Row:
                    this.rows.sort(new RowComparator());
                    break;
                case Value:
                    this.rows = Linq.flatMap(this.rows,
                            r -> Linq.map(r.values,
                                    r0 -> { Row res = new Row(); res.add(r0); return res; }));
                    this.rows.sort(new RowComparator());
                    break;
            }
        }
    }

    public JDBCExecutor(String db_url) {
        this.db_url = db_url;
        this.connection = null;
    }

    void statement(SqlStatement statement) throws SQLException {
        Logger.INSTANCE.from(this, 1)
                .append(this.statementsExecuted)
                .append(": ")
                .append(statement.statement)
                .newline();
        assert this.connection != null;
        Statement stmt = this.connection.createStatement();
        try {
            stmt.execute(statement.statement);
        } catch (SQLException ex) {
            stmt.close();
            Logger.INSTANCE.from(this, 1)
                    .append("ERROR: ")
                    .append(ex.getMessage())
                    .newline();
            throw ex;
        }
        this.statementsExecuted++;
    }

    void query(SqlTestQuery query, TestStatistics statistics) throws SQLException, NoSuchAlgorithmException {
        assert this.connection != null;
        if (this.buggyOperations.contains(query.query)) {
            System.err.println("Skipping " + query.query);

        }
        Statement stmt = this.connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(query.query);
        this.validate(query, resultSet, query.outputDescription, statistics);
        stmt.close();
        resultSet.close();
        Logger.INSTANCE.from(this, 1)
                .append(statistics.testsRun())
                .append(": ")
                .append(query.query)
                .newline();
    }

    Row getValue(ResultSet rs, String columnTypes) throws SQLException {
        Row row = new Row();
        // Column numbers start from 1
        for (int i = 1; i <= columnTypes.length(); i++) {
            char c = columnTypes.charAt(i - 1);
            switch (c) {
                case 'R':
                    double d = rs.getDouble(i);
                    if (rs.wasNull())
                        row.add("NULL");
                    else
                        row.add(String.format("%.3f", d));
                    break;
                case 'I':
                    try {
                        long integer = rs.getLong(i);
                        if (rs.wasNull())
                            row.add("NULL");
                        else
                            row.add(String.format("%d", integer));
                    } catch (SQLDataException | NumberFormatException ignore) {
                        // This probably indicates a bug in the query, since
                        // the query expects an integer, but the result cannot
                        // be interpreted as such.
                        // unparsable string: replace with 0
                        row.add("0");
                    }
                    break;
                case 'T':
                    String s = rs.getString(i);
                    if (s == null)
                        row.add("NULL");
                    else {
                        StringBuilder result = new StringBuilder();
                        for (int j = 0; j < s.length(); j++) {
                            char sc = s.charAt(j);
                            if (sc < ' ' || sc > '~')
                                sc = '@';
                            result.append(sc);
                        }
                        row.add(result.toString());
                    }
                    break;
                default:
                    throw new RuntimeException("Unexpected column type " + c);
            }
        }
        return row;
    }

    static class RowComparator implements Comparator<Row> {
        @Override
        public int compare(Row o1, Row o2) {
            if (o1.values.size() != o2.values.size())
                throw new RuntimeException("Comparing rows of different lengths");
            for (int i = 0; i < o1.values.size(); i++) {
                int r = o1.values.get(i).compareTo(o2.values.get(i));
                if (r != 0)
                    return r;
            }
            return 0;
        }
    }

    void validate(SqlTestQuery query, ResultSet rs,
                  SqlTestQueryOutputDescription description,
                  TestStatistics statistics)
            throws SQLException, NoSuchAlgorithmException {
        assert description.columnTypes != null;
        Rows rows = new Rows();
        while (rs.next()) {
            Row row = this.getValue(rs, description.columnTypes);
            rows.add(row);
        }
        if (description.valueCount != rows.size() * description.columnTypes.length()) {
            statistics.addFailure(new TestStatistics.FailedTestDescription(
                    query, "Expected " + description.valueCount + " rows, got " +
                    rows.size() * description.columnTypes.length()));
            return;
        }
        rows.sort(description.order);
        Logger.INSTANCE.from(this, 3)
                .append("Result is ")
                .newline()
                .append(rows.toString())
                .newline();
        if (description.queryResults != null) {
            String r = rows.toString();
            String q = String.join("\n", description.queryResults);
            if (!r.equals(q)) {
                statistics.addFailure(new TestStatistics.FailedTestDescription(
                        query, "Output differs: " + r + " vs " + q));
                return;
            }
        }
        if (description.hash != null) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            String repr = rows + "\n";
            md.update(repr.getBytes());
            byte[] digest = md.digest();
            String hash = Utilities.toHex(digest);
            if (!description.hash.equals(hash)) {
                statistics.addFailure(new TestStatistics.FailedTestDescription(
                        query, "Hash of data does not match expected value"));
                return;
            }
        }
        statistics.passed++;
    }

    List<String> getTableList() throws SQLException {
        List<String> result = new ArrayList<>();
        assert this.connection != null;
        DatabaseMetaData md = this.connection.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", new String[] { "TABLE" });
        while (rs.next()) {
            String tableName = rs.getString(3);
            if (tableName.equals("PUBLIC"))
                // The catalog table in HSQLDB
                continue;
            result.add(tableName);
        }
        rs.close();
        return result;
    }

    List<String> getViewList() throws SQLException {
        List<String> result = new ArrayList<>();
        assert this.connection != null;
        DatabaseMetaData md = this.connection.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", new String[] { "VIEW" });
        while (rs.next()) {
            String tableName = rs.getString(3);
            result.add(tableName);
        }
        rs.close();
        return result;
    }

    void dropAllTables() throws SQLException {
        assert this.connection != null;
        List<String> tables = this.getTableList();
        for (String tableName: tables) {
            String del = "DROP TABLE " + tableName;
            Logger.INSTANCE.from(this, 2).append(del).newline();
            Statement drop = this.connection.createStatement();
            drop.execute(del);
            drop.close();
        }
    }

    void dropAllViews() throws SQLException {
        assert this.connection != null;
        List<String> tables = this.getViewList();
        for (String tableName: tables) {
            String del = "DROP VIEW IF EXISTS " + tableName + " CASCADE";
            Logger.INSTANCE.from(this, 2).append(del).newline();
            Statement drop = this.connection.createStatement();
            drop.execute(del);
            drop.close();
        }
    }

    public void establishConnection() throws SQLException {
        this.connection = DriverManager.getConnection(this.db_url, "", "");
        assert this.connection != null;
        Statement statement = this.connection.createStatement();
        // Enable postgres compatibility
        statement.execute("SET DATABASE SQL SYNTAX PGS TRUE");
    }

    public void closeConnection() throws SQLException {
        assert this.connection != null;
        this.connection.close();
    }

    @Override
    public TestStatistics execute(SLTTestFile file, ExecutionOptions options)
            throws SQLException, NoSuchAlgorithmException {
        this.startTest();
        this.establishConnection();
        this.dropAllTables();
        TestStatistics result = new TestStatistics(options.stopAtFirstError);
        for (ISqlTestOperation operation: file.fileContents) {
            try {
                SqlStatement stat = operation.as(SqlStatement.class);
                if (stat != null) {
                    this.statement(stat);
                } else {
                    SqlTestQuery query = operation.to(SqlTestQuery.class);
                    this.query(query, result);
                }
            } catch (SQLException ex) {
                System.err.println("Error while processing #" + result.testsRun() + " " + operation);
                throw ex;
            }
        }
        this.closeConnection();
        this.reportTime(result.passed);
        return result;
    }
}
