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

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqllogictest.*;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class JDBCExecutor extends SqlTestExecutor implements IModule {
    public final String db_url;
    public final String user;
    public final String password;
    public final String dialect;
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

    public JDBCExecutor(String db_url, String dialect, String user, String password) {
        this.db_url = db_url;
        this.user = user;
        this.password = password;
        this.dialect = dialect;
        this.connection = null;
    }

    void statement(SqlStatement statement) throws SQLException {
        Logger.instance.from(this, 1)
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
            Logger.instance.from(this, 1)
                    .append("ERROR: ")
                    .append(ex.getMessage())
                    .newline();
            throw ex;
        }
        this.statementsExecuted++;
    }

    boolean query(SqlTestQuery query, int queryNo) throws SQLException, NoSuchAlgorithmException {
        assert this.connection != null;
        if (this.buggyOperations.contains(query.query)) {
            System.err.println("Skipping " + query.query);
            return false;
        }
        Statement stmt = this.connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(query.query);
        this.validate(query.query, queryNo, resultSet, query.outputDescription);
        stmt.close();
        resultSet.close();
        this.queriesExecuted++;
        Logger.instance.from(this, 1)
                .append(this.queriesExecuted)
                .append(": ")
                .append(query.query)
                .newline();
        return true;
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
            String hash = Utilities.toHex(digest);
            if (!description.hash.equals(hash))
                throw new RuntimeException(query + " #" + queryNo +
                        ": Hash of data does not match");
        }
    }

    /*
     Calcite cannot parse DDL statements in all dialects.
     For example, it has no support for MySQL CREATE TABLE statements
     which indicate the primary key for each column.
     So to handle these we let JDBC execute the statement, then
     we retrieve the table schema and make up a new statement
     in a Calcite-friendly syntax.  This implementation does not
     preserve primary keys, but this does not seem important right now.
     */
    public String generateCreateStatement(String table) throws SQLException {
        assert this.connection != null;
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ");
        builder.append(table);
        builder.append("(");

        Statement stmt = this.connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " WHERE 1 = 0");
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 0; i < meta.getColumnCount(); i++) {
            JDBCType columnType = JDBCType.valueOf(meta.getColumnType(i + 1));
            int n = meta.isNullable(i + 1);
            String colName = meta.getColumnName(i + 1);

            if (i > 0)
                builder.append(", ");
            builder.append(colName);
            builder.append(" ");

            boolean nullable;
            if (n == ResultSetMetaData.columnNullable)
                nullable = true;
            else if (n == ResultSetMetaData.columnNullableUnknown)
                throw new RuntimeException("Unknown column nullability");
            else
                nullable = false;
            switch (columnType) {
                case INTEGER:
                    builder.append("INTEGER");
                    break;
                case REAL:
                case DOUBLE:
                    builder.append("DOUBLE");
                    break;
                case VARCHAR:
                case LONGVARCHAR:
                    builder.append("VARCHAR");
                    break;
                default:
                    throw new RuntimeException("Unexpected column type " + columnType);
            }
            if (!nullable)
                builder.append(" NOT NULL");
        }
        rs.close();
        builder.append(")");
        return builder.toString();
    }

    public DBSPZSetLiteral getTableContents(String table) throws SQLException {
        List<DBSPExpression> rows = new ArrayList<>();
        assert this.connection != null;
        Statement stmt1 = this.connection.createStatement();
        ResultSet rs = stmt1.executeQuery("SELECT * FROM " + table);
        ResultSetMetaData meta = rs.getMetaData();
        DBSPType[] colTypes = new DBSPType[meta.getColumnCount()];
        for (int i1 = 0; i1 < meta.getColumnCount(); i1++) {
            JDBCType columnType = JDBCType.valueOf(meta.getColumnType(i1 + 1));
            int n = meta.isNullable(i1 + 1);
            boolean nullable;
            if (n == ResultSetMetaData.columnNullable)
                nullable = true;
            else if (n == ResultSetMetaData.columnNullableUnknown)
                throw new RuntimeException("Unknown column nullability");
            else
                nullable = false;
            switch (columnType) {
                case INTEGER:
                    colTypes[i1] = DBSPTypeInteger.signed32.setMayBeNull(nullable);
                    break;
                case REAL:
                case DOUBLE:
                    colTypes[i1] = DBSPTypeDouble.instance.setMayBeNull(nullable);
                    break;
                case VARCHAR:
                case LONGVARCHAR:
                    colTypes[i1] = DBSPTypeString.instance.setMayBeNull(nullable);
                    break;
                default:
                    throw new RuntimeException("Unexpected column type " + columnType);
            }
        }
        while (rs.next()) {
            DBSPExpression[] cols = new DBSPExpression[colTypes.length];
            for (int i = 0; i < colTypes.length; i++) {
                DBSPExpression exp;
                DBSPType type = colTypes[i];
                if (type.is(DBSPTypeInteger.class)) {
                    int value = rs.getInt(i + 1);
                    if (rs.wasNull())
                        exp = DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true));
                    else
                        exp = new DBSPIntegerLiteral(value, type.mayBeNull);
                } else if (type.is(DBSPTypeDouble.class)) {
                    double value = rs.getDouble(i + 1);
                    if (rs.wasNull())
                        exp = DBSPLiteral.none(DBSPTypeDouble.instance.setMayBeNull(true));
                    else
                        exp = new DBSPDoubleLiteral(value, type.mayBeNull);
                } else {
                    String s = rs.getString(i + 1);
                    if (s == null)
                        exp = DBSPLiteral.none(DBSPTypeString.instance.setMayBeNull(true));
                    else
                        exp = new DBSPStringLiteral(s, type.mayBeNull);
                }
                cols[i] = exp;
            }
            DBSPTupleExpression row = new DBSPTupleExpression(cols);
            rows.add(row);
        }
        rs.close();
        if (rows.size() == 0)
            return new DBSPZSetLiteral(
                    new DBSPTypeZSet(new DBSPTypeTuple(colTypes)));
        return new DBSPZSetLiteral(rows.toArray(new DBSPExpression[0]));
    }

    List<String> getStringResults(String query) throws SQLException {
        List<String> result = new ArrayList<>();
        assert this.connection != null;
        Statement stmt = this.connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            String tableName = rs.getString(1);
            result.add(tableName);
        }
        rs.close();
        return result;
    }

    List<String> getTableList() throws SQLException {
        switch (this.dialect) {
            case "mysql":
                return this.getStringResults("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");
            case "psql":
                return this.getStringResults("SELECT tableName FROM pg_catalog.pg_tables\n" +
                        "    WHERE schemaname != 'information_schema' AND\n" +
                        "    schemaname != 'pg_catalog'");
            default:
                throw new UnsupportedOperationException(this.dialect);
        }
    }

    List<String> getViewList() throws SQLException {
        switch (this.dialect) {
            case "mysql":
                return this.getStringResults("SHOW FULL TABLES WHERE Table_type = 'VIEW'");
            case "psql":
                return this.getStringResults("SELECT table_name \n" +
                        "FROM information_schema.views \n" +
                        "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \n");
            default:
                throw new UnsupportedOperationException(this.dialect);
        }
    }



    void dropAllTables() throws SQLException {
        assert this.connection != null;
        List<String> tables = this.getTableList();
        for (String tableName: tables) {
            String del = "DROP TABLE " + tableName;
            Logger.instance.from(this, 2).append(del).newline();
            Statement drop = this.connection.createStatement();
            drop.execute(del);
            drop.close();
        }
    }

    void dropAllViews() throws SQLException {
        assert this.connection != null;
        List<String> tables = this.getViewList();
        for (String tableName: tables) {
            String del = "DROP VIEW " + tableName;
            Logger.instance.from(this, 2).append(del).newline();
            Statement drop = this.connection.createStatement();
            drop.execute(del);
            drop.close();
        }
    }

    public void establishConnection() throws SQLException {
        this.connection = DriverManager.getConnection(this.db_url, this.user, this.password);
        assert this.connection != null;
    }

    public void closeConnection() throws SQLException {
        assert this.connection != null;
        this.connection.close();
    }

    @Override
    public TestStatistics execute(SqlTestFile file) throws SQLException, NoSuchAlgorithmException {
        this.startTest();
        this.establishConnection();
        this.dropAllTables();
        TestStatistics result = new TestStatistics();
        for (ISqlTestOperation operation: file.fileContents) {
            try {
                SqlStatement stat = operation.as(SqlStatement.class);
                if (stat != null) {
                    this.statement(stat);
                } else {
                    SqlTestQuery query = operation.to(SqlTestQuery.class);
                    boolean executed = this.query(query, result.passed);
                    if (executed) {
                        result.passed++;
                    } else {
                        result.ignored++;
                    }
                }
            } catch (SQLException ex) {
                System.err.println("Error while processing #" + result.passed + " " + operation);
                throw ex;
            }
        }
        this.closeConnection();
        this.reportTime(result.passed);
        return result;
    }
}
