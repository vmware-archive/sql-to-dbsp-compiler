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
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class JDBCExecutor extends SqlTestExecutor {
    private static final boolean debug = false;
    public final String db_url;
    public final String user;
    public final String password;
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

    public JDBCExecutor(String db_url, String user, String password) {
        this.db_url = db_url;
        this.user = user;
        this.password = password;
        this.connection = null;
    }

    void statement(SqlStatement statement) throws SQLException {
        assert this.connection != null;
        Statement stmt = this.connection.createStatement();
        stmt.execute(statement.statement);
        stmt.close();
        if (debug)
            System.out.println(statement.statement);
    }

    void query(SqlTestQuery query, int queryNo) throws SQLException, NoSuchAlgorithmException {
        assert this.connection != null;
        Statement stmt = this.connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(query.query);
        this.validate(query.query, queryNo, resultSet, query.outputDescription);
        stmt.close();
        resultSet.close();
        if (debug)
            System.out.println(query.query);
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
                    long integer = rs.getLong(i);
                    if (rs.wasNull())
                        row.add("NULL");
                    else
                        row.add(String.format("%d", integer));
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

    void validate(String query, int queryNo,
                  ResultSet rs, SqlTestQueryOutputDescription description)
            throws SQLException, NoSuchAlgorithmException {
        assert description.columnTypes != null;
        Rows rows = new Rows();
        while (rs.next()) {
            Row row = this.getValue(rs, description.columnTypes);
            rows.add(row);
        }
        if (description.valueCount != rows.size() * description.columnTypes.length())
            throw new RuntimeException("Expected " + description.valueCount + " got " +
                    rows.size() * description.columnTypes.length());
        rows.sort(description.order);
        if (description.queryResults != null) {
            String r = rows.toString();
            String q = String.join("\n", description.queryResults);
            if (!r.equals(q))
                throw new RuntimeException("Output differs: " + r + " vs " + q);
        }
        if (description.hash != null) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            String repr = rows + "\n";
            md.update(repr.getBytes());
            byte[] digest = md.digest();
            String hash = DatatypeConverter.printHexBinary(digest).toLowerCase();
            if (!description.hash.equals(hash))
                throw new RuntimeException(query + " #" + queryNo +
                        ": Hash of data does not match");
        }
    }

    void dropAllTables() throws SQLException {
        assert this.connection != null;
        Statement stmt = this.connection.createStatement();
        ResultSet rs = stmt.executeQuery("SHOW TABLES");
        while (rs.next()) {
            String tableName = rs.getString(1);
            String del = "DROP TABLE " + tableName;
            Statement drop = this.connection.createStatement();
            drop.execute(del);
            drop.close();
            if (debug)
                System.out.println(del);
        }
        rs.close();
        stmt.close();
    }

    @Override
    public TestStatistics execute(SqlTestFile file) throws SQLException, NoSuchAlgorithmException {
        this.startTest();
        this.connection = DriverManager.getConnection(this.db_url, this.user, this.password);
        assert this.connection != null;
        this.dropAllTables();
        TestStatistics result = new TestStatistics();
        for (ISqlTestOperation operation: file.fileContents) {
            SqlStatement stat = operation.as(SqlStatement.class);
            if (stat != null) {
                this.statement(stat);
            } else {
                SqlTestQuery query = operation.to(SqlTestQuery.class);
                this.query(query, result.passed);
                result.passed++;
            }
        }
        this.connection.close();
        this.reportTime(result.passed);
        return result;
    }
}
