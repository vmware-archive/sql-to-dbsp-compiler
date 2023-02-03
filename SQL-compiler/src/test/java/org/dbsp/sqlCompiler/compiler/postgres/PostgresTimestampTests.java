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

package org.dbsp.sqlCompiler.compiler.postgres;

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Linq;
import org.junit.Test;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/timestamp.out
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresTimestampTests extends BaseSQLTests {
    // Calcite is not very flexible with regards to timestamp formats
    public DBSPCompiler compileQuery(String query, boolean optimize) throws SqlParseException {
        String data =
                "CREATE TABLE TIMESTAMP_TBL (d1 timestamp(2) without time zone)\n;" +
                                                                                         // -- Postgres v6.0 standard output format
                "INSERT INTO TIMESTAMP_TBL VALUES ('1970-01-01 00:00:00');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('epoch');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01 1997 PST');
                                                                                         // -- Variations on Postgres v6.1 standard output format
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.000001');\n" +   // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.000001 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.999999');\n" +   // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.999999 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.4');\n" +        // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.4 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.5');\n" +        // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.5 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.6');\n" +        // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.6 1997 PST');
                                                                                         // -- ISO 8601 format
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02');\n" +                   // INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02 03:04:05');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02 03:04:05');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01-08');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01-0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01 -08:00');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 -0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 17:32:01 -07:00');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2001-09-22 18:19:20');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2001-09-22T18:19:20');
                                                                                         // POSIX format (note that the timezone abbrev is just decoration here)
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 08:14:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 08:14:01 GMT+8');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 13:14:02');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 13:14:02 GMT-1');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 12:14:03');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 12:14:03 GMT-2');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 03:14:04');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 03:14:04 PST+8');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 02:14:05');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 02:14:05 MST+7:00');
                                                                                         // -- Variations for acceptable input formats
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997 -0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:00');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 5:32PM 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997/02/10 17:32:01-0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb-10-1997 17:32:01 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('02-10-1997 17:32:01 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 PST');
                                                                                         // set datestyle to ymd;
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('97FEB10 5:32:01PM UTC');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('97/02/10 17:32:01 UTC');
                                                                                         // reset datestyle
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997.041 17:32:01 UTC'); // 41st day
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 America/New_York');
                                                                                         // -- this fails (even though TZ is a no-op, we still look it up)
                                                                                         // INSERT INTO TIMESTAMP_TBL VALUES ('19970710 173201 America/Does_not_exist');
                                                                                         // ERROR:  time zone "america/does_not_exist" not recognized
                                                                                         // LINE 1: INSERT INTO TIMESTAMP_TBL VALUES ('19970710 173201 America/D...
                                                                                         // -- Check date conversion and date arithmetic
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 18:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 18:32:01 PDT');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-11 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 11 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-12 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 12 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-13 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 13 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-14 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 14 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-15 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 15 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1997');
                                                                                         // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0097 BC');
                "INSERT INTO TIMESTAMP_TBL VALUES ('0097-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0097');
                "INSERT INTO TIMESTAMP_TBL VALUES ('0597-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0597');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1097-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1097');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1697-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1697');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1797-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1797');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1897-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1897');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2097-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 2097');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-02-28 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 28 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-02-29 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 29 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-03-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Mar 01 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-12-30 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 30 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-28 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 28 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-29 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 29 17:32:01 1997'); // illegal
                                                                                         // ERROR:  date/time field value out of range: "Feb 29 17:32:01 1997"
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-03-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Mar 01 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-12-30 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 30 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1999-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1999');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-01-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2000');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 2000');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2001-01-01 17:32:01')\n";            // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2001');
        CompilerOptions options = new CompilerOptions();
        options.optimizerOptions.noOptimizations = !optimize;
        DBSPCompiler compiler = new DBSPCompiler(options);
        // So that queries that do not depend on the input still
        // have circuits with inputs.
        compiler.setGenerateInputsFromTables(true);
        compiler.compileStatements(data);
        compiler.compileStatement(query);
        return compiler;
    }

    @SuppressWarnings("SameParameterValue")
    void testQuery(String query, DBSPZSetLiteral expectedOutput, boolean optimize) throws SqlParseException {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query, optimize);
        DBSPCircuit circuit = getCircuit(compiler);
        DBSPZSetLiteral input = compiler.getTableContents().getTableContents("TIMESTAMP_TBL");
        InputOutputPair streams = new InputOutputPair(input, expectedOutput);
        this.addRustTestCase(circuit, streams);
    }

    static SimpleDateFormat[] inputFormats = {
            new SimpleDateFormat("EEE MMM d HH:mm:ss.SSS yyyy"),
            new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy")
    };
    static SimpleDateFormat outputFormats = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

    /**
     * Convert a date from a format like Sat Feb 16 17:32:01 1996 to
     * a format like 1996-02-16 17:32:01
     */
    static DBSPLiteral convertDate(@Nullable String date) {
        if (date == null)
            return DBSPLiteral.none(DBSPTypeTimestamp.instance.setMayBeNull(true));
        for (SimpleDateFormat input: inputFormats) {
            try {
                Date converted = input.parse(date);
                String out = outputFormats.format(converted);
                return new DBSPTimestampLiteral(out, true);
            } catch (Exception ignored) {
            }
        }
        throw new RuntimeException("Could not parse " + date);
    }

    @Test
    public void testTS() throws SqlParseException {
        String[] data = {
                "Thu Jan 01 00:00:00 1970",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 00:00:00 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                //"Tue Feb 16 17:32:01 0097 BC",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001",
                null
        };
        String query = "SELECT d1 FROM TIMESTAMP_TBL";
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }

    @Test
    public void testGt() throws SqlParseException {
        String[] data = {
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 > timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }
    
    @Test
    public void testLt() throws SqlParseException {
        String[] data = {
                "Thu Jan 01 00:00:00 1970",
                //"Tue Feb 16 17:32:01 0097 BC",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 < timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }

    @Test
    public void testEq() throws SqlParseException {
        String[] data = {
                "Thu Jan 02 00:00:00 1997"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 = timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }

    @Test
    public void testNe() throws SqlParseException {
        String[] data = {
                "Thu Jan 01 00:00:00 1970",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                //"Tue Feb 16 17:32:01 0097 BC",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001",
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 != timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }

    @Test
    public void testLeq() throws SqlParseException {
        String[] data = {
                "Thu Jan 01 00:00:00 1970",
                "Thu Jan 02 00:00:00 1997",
                //"Tue Feb 16 17:32:01 0097 BC",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 <= timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }

    @Test
    public void testGte() throws SqlParseException {
        String[] data = {
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 00:00:00 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 >= timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }

    static int intervalToSeconds(String interval) {
        String orig = interval;
        Pattern days = Pattern.compile("^(\\d+) days?(.*)");
        Pattern hours = Pattern.compile("^\\s*(\\d+) hours?(.*)");
        Pattern minutes = Pattern.compile("\\s*(\\d+) mins?(.*)");
        Pattern seconds = Pattern.compile("\\s*(\\d+)([.](\\d+))? secs?(.*)");
        Pattern ago = Pattern.compile("\\s*ago(.*)");

        int result = 0;
        if (interval.equals("0")) {
            interval = "";
        } else {
            Matcher m = days.matcher(interval);
            if (m.matches()) {
                int d = Integer.parseInt(m.group(1));
                result += d * 86400;
                interval = m.group(2);
            }

            m = hours.matcher(interval);
            if (m.matches()) {
                int h = Integer.parseInt(m.group(1));
                result += h * 3600;
                interval = m.group(2);
            }

            m = minutes.matcher(interval);
            if (m.matches()) {
                int mm = Integer.parseInt(m.group(1));
                result += mm * 60;
                interval = m.group(2);
            }

            m = seconds.matcher(interval);
            if (m.matches()) {
                int s = Integer.parseInt(m.group(1));
                result += s;
                interval = m.group(4);
            }

            m = ago.matcher(interval);
            if (m.matches()) {
                interval = m.group(1);
                result = -result;
            }
        }
        //System.out.println(orig + "->" + result + ": " + interval);
        if (!interval.isEmpty())
            throw new RuntimeException("Could not parse interval " + orig);
        return result;
    }

    @Test
    public void diff() throws SqlParseException {
        String[] data = {
                "9863 days ago",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 2 secs",
                "39 days 17 hours 32 mins 1.4 secs",
                "39 days 17 hours 32 mins 1.5 secs",
                "39 days 17 hours 32 mins 1.6 secs",
                "0",
                "3 hours 4 mins 5 secs",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "159 days 17 hours 32 mins 1 sec",
                "1724 days 18 hours 19 mins 20 secs",
                "1168 days 8 hours 14 mins 1 sec",
                "1168 days 13 hours 14 mins 2 secs",
                "1168 days 12 hours 14 mins 3 secs",
                "1168 days 3 hours 14 mins 4 secs",
                "1168 days 2 hours 14 mins 5 secs",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "159 days 18 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "40 days 17 hours 32 mins 1 sec",
                "41 days 17 hours 32 mins 1 sec",
                "42 days 17 hours 32 mins 1 sec",
                "43 days 17 hours 32 mins 1 sec",
                "44 days 17 hours 32 mins 1 sec",
                "45 days 17 hours 32 mins 1 sec",
                "45 days 17 hours 32 mins 1 sec",
                "308 days 6 hours 27 mins 59 secs ago",
                "307 days 6 hours 27 mins 59 secs ago",
                "306 days 6 hours 27 mins 59 secs ago",
                "2 days 6 hours 27 mins 59 secs ago",
                "1 day 6 hours 27 mins 59 secs ago",
                "6 hours 27 mins 59 secs ago",
                "57 days 17 hours 32 mins 1 sec",
                "58 days 17 hours 32 mins 1 sec",
                "362 days 17 hours 32 mins 1 sec",
                "363 days 17 hours 32 mins 1 sec",
                "1093 days 17 hours 32 mins 1 sec",
                "1094 days 17 hours 32 mins 1 sec",
                "1459 days 17 hours 32 mins 1 sec",
                "1460 days 17 hours 32 mins 1 sec"
        };

        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(d == null ? DBSPLiteral.none(DBSPTypeInteger.signed32.setMayBeNull(true)) :
                        new DBSPI32Literal(-intervalToSeconds(d) / 60, true)), DBSPExpression.class);
        String query = "SELECT TIMESTAMPDIFF(MINUTE, d1, timestamp '1997-01-02') AS diff\n" +
                "   FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01'";
        this.testQuery(query, new DBSPZSetLiteral(results), true);
    }

    void testQueryTwice(String query, DBSPTimestampLiteral expectedValue) throws SqlParseException {
        // by running with optimizations disabled we test runtime evaluation
        DBSPZSetLiteral zset = new DBSPZSetLiteral(new DBSPTupleExpression(expectedValue));
        this.testQuery(query, zset, false);
        this.testQuery(query, zset, true);
    }

    // @Test
    // TODO: Postgres dives a different result from Calcite!
    public void testWeek() throws SqlParseException {
        // This is called DATE_TRUNC in postgres
        String query = "SELECT FLOOR(timestamp '2004-02-29 15:44:17.71393' TO WEEK) AS week_trunc";
        this.testQueryTwice(query, new DBSPTimestampLiteral("2004-02-23 00:00:00", false));
    }
}