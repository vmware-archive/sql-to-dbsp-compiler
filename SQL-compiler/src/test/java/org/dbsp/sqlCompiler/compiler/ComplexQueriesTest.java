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

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.CircuitFunctionRewriter;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.ThreeOperandVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("SpellCheckingInspection")
public class ComplexQueriesTest extends BaseSQLTests {
    @Test
    public void smallTaxiTest() {
        String ddl = "CREATE TABLE green_tripdata\n" +
                "(\n" +
                "        lpep_pickup_datetime TIMESTAMP NOT NULL,\n" +
                "        lpep_dropoff_datetime TIMESTAMP NOT NULL,\n" +
                "        pickup_location_id BIGINT NOT NULL,\n" +
                "        dropoff_location_id BIGINT NOT NULL,\n" +
                "        trip_distance DOUBLE PRECISION,\n" +
                "        fare_amount DOUBLE PRECISION \n" +
                ")";
        String query =
                "SELECT\n" +
                        "*,\n" +
                        "COUNT(*) OVER(\n" +
                        "                PARTITION BY  pickup_location_id\n" +
                        "                ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) ) \n" +
                        "                -- 1 hour is 3600  seconds\n" +
                        "                RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip\n" +
                        "FROM green_tripdata";
        DBSPCompiler compiler = testCompiler();
        compiler.setGenerateInputsFromTables(true);
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        this.addRustTestCase(getCircuit(compiler));
    }

    @Test
    public void testArray() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "ID INTEGER,\n"
                + "VALS INTEGER ARRAY,\n"
                + "VALVALS VARCHAR(10) ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT *, CARDINALITY(VALS), ARRAY[ID, 5], VALS[1] FROM ARR_TABLE";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(circuit);
    }

    // @Test
    public void testUnnest() {
        // TODO: not yet implemented.
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "ID INTEGER,\n"
                + "VALS INTEGER ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT ID, VAL FROM ARR_TABLE, UNNEST(VALS) AS VAL";
        Logger.INSTANCE.setDebugLevel(CalciteToDBSPCompiler.class, 3);
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(circuit);
    }

    //@Test
    public void test2DArray() {
        // TODO: not yet implemented
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT *, CARDINALITY(VALS), VALS[1] FROM ARR_TABLE";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(circuit);
    }

    @Test
    public void simpleOver() {
        String query = "CREATE TABLE green_tripdata\n" +
                "(\n" +
                "    lpep_pickup_datetime TIMESTAMP NOT NULL,\n" +
                "    lpep_dropoff_datetime TIMESTAMP NOT NULL,\n" +
                "    pickup_location_id BIGINT NOT NULL,\n" +
                "    dropoff_location_id BIGINT NOT NULL,\n" +
                "    trip_distance DOUBLE PRECISION,\n" +
                "    fare_amount DOUBLE PRECISION\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW FEATURES as SELECT\n" +
                "    *,\n" +
                "    COUNT(*) OVER(\n" +
                "      PARTITION BY  pickup_location_id\n" +
                "      ORDER BY extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) )\n" +
                "      -- 1 hour is 3600  seconds\n" +
                "      RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip\n" +
                "FROM green_tripdata;\n";
        DBSPCompiler compiler = testCompiler();
        compiler.setGenerateInputsFromTables(true);
        compiler.compileStatements(query);
        DBSPCircuit circuit = getCircuit(compiler);
        ThreeOperandVisitor tav = new ThreeOperandVisitor();
        CircuitFunctionRewriter rewriter = new CircuitFunctionRewriter(tav);
        circuit = rewriter.apply(circuit);
        this.addRustTestCase(circuit);
    }

    @Test
    public void demographicsTest() {
        String query =
                "CREATE TABLE demographics (\n" +
                "    cc_num FLOAT64 NOT NULL,\n" +
                "    first STRING,\n" +
                "    gender STRING,\n" +
                "    street STRING,\n" +
                "    city STRING,\n" +
                "    state STRING,\n" +
                "    zip INTEGER,\n" +
                "    lat FLOAT64,\n" +
                "    long FLOAT64,\n" +
                "    city_pop INTEGER,\n" +
                "    job STRING,\n" +
                "    dob STRING\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE transactions (\n" +
                "    trans_date_trans_time TIMESTAMP NOT NULL,\n" +
                "    cc_num FLOAT64 NOT NULL,\n" +
                "    merchant STRING,\n" +
                "    category STRING,\n" +
                "    amt FLOAT64,\n" +
                "    trans_num STRING,\n" +
                "    unix_time INTEGER NOT NULL,\n" +
                "    merch_lat FLOAT64,\n" +
                "    merch_long FLOAT64,\n" +
                "    is_fraud INTEGER\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW features as\n" +
                "    SELECT\n" +
                "        DAYOFWEEK(trans_date_trans_time) AS d,\n" +
                "        TIMESTAMPDIFF(YEAR, trans_date_trans_time, CAST(dob as TIMESTAMP)) AS age,\n" +
                "        ST_DISTANCE(ST_POINT(long,lat), ST_POINT(merch_long,merch_lat)) AS distance,\n" +
                "        -- TIMESTAMPDIFF(MINUTE, trans_date_trans_time, last_txn_date) AS trans_diff,\n" +
                "        AVG(amt) OVER(\n" +
                "            PARTITION BY   CAST(cc_num AS NUMERIC)\n" +
                "            ORDER BY unix_time\n" +
                "            -- 1 week is 604800  seconds\n" +
                "            RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS\n" +
                "        avg_spend_pw,\n" +
                "        AVG(amt) OVER(\n" +
                "            PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "            ORDER BY unix_time\n" +
                "            -- 1 month(30 days) is 2592000 seconds\n" +
                "            RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS\n" +
                "        avg_spend_pm,\n" +
                "        COUNT(*) OVER(\n" +
                "            PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "            ORDER BY unix_time\n" +
                "            -- 1 day is 86400  seconds\n" +
                "            RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS\n" +
                "        trans_freq_24,\n" +
                "        category,\n" +
                "        amt,\n" +
                "        state,\n" +
                "        job,\n" +
                "        unix_time,\n" +
                "        city_pop,\n" +
                "        merchant,\n" +
                "        is_fraud\n" +
                "    FROM (\n" +
                "        SELECT t1.*, t2.*\n" +
                "               -- , LAG(trans_date_trans_time, 1) OVER (PARTITION BY t1.cc_num  ORDER BY trans_date_trans_time ASC) AS last_txn_date\n" +
                "        FROM  transactions AS t1\n" +
                "        LEFT JOIN  demographics AS t2\n" +
                "        ON t1.cc_num = t2.cc_num);";
        DBSPCompiler compiler = testCompiler();
        compiler.setGenerateInputsFromTables(true);
        compiler.compileStatements(query);
        DBSPZSetLiteral[] inputs = new DBSPZSetLiteral[] {
                new DBSPZSetLiteral(new DBSPTupleExpression(
                        new DBSPDoubleLiteral(0.0),
                        new DBSPStringLiteral("First", true),
                        new DBSPStringLiteral("Male", true),
                        new DBSPStringLiteral("Street", true),
                        new DBSPStringLiteral("City", true),
                        new DBSPStringLiteral("State", true),
                        new DBSPI32Literal(94043, true),
                        //new DBSPDoubleLiteral(128.0, true),
                        DBSPLiteral.none(DBSPTypeDouble.NULLABLE_INSTANCE),
                        new DBSPDoubleLiteral(128.0, true),
                        new DBSPI32Literal(100000, true),
                        new DBSPStringLiteral("Job", true),
                        new DBSPStringLiteral("2020-02-20", true)
                        )),
                new DBSPZSetLiteral(new DBSPTupleExpression(
                        new DBSPTimestampLiteral("2020-02-20 10:00:00", false),
                        new DBSPDoubleLiteral(0.0, false),
                        new DBSPStringLiteral("Merchant", true),
                        new DBSPStringLiteral("Category", true),
                        new DBSPDoubleLiteral(10.0, true),
                        new DBSPStringLiteral("Transnum", true),
                        new DBSPI32Literal(1000),
                        new DBSPDoubleLiteral(128.0, true),
                        new DBSPDoubleLiteral(128.0, true),
                        new DBSPI32Literal(0, true)
                ))
        };
        DBSPZSetLiteral[] outputs = new DBSPZSetLiteral[] {};
        InputOutputPair ip = new InputOutputPair(inputs, outputs);
        this.addRustTestCase(getCircuit(compiler), ip);
    }

    @Test
    public void taxiTest() {
        String ddl = "CREATE TABLE green_tripdata\n" +
                "(\n" +
                "        lpep_pickup_datetime TIMESTAMP NOT NULL,\n" +
                "        lpep_dropoff_datetime TIMESTAMP NOT NULL,\n" +
                "        pickup_location_id BIGINT NOT NULL,\n" +
                "        dropoff_location_id BIGINT NOT NULL,\n" +
                "        trip_distance DOUBLE PRECISION,\n" +
                "        fare_amount DOUBLE PRECISION \n" +
                ")";
        String query =
                "SELECT\n" +
                        "*,\n" +
                        "COUNT(*) OVER(\n" +
                        "                PARTITION BY  pickup_location_id\n" +
                        "                ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) ) \n" +
                        "                -- 1 hour is 3600  seconds\n" +
                        "                RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip,\n" +
                        "AVG(fare_amount) OVER(\n" +
                        "                PARTITION BY  pickup_location_id\n" +
                        "                ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) ) \n" +
                        "                -- 1 hour is 3600  seconds\n" +
                        "                RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS mean_fare_window_1h_pickup_zip,\n" +
                        "COUNT(*) OVER(\n" +
                        "                PARTITION BY  dropoff_location_id\n" +
                        "                ORDER BY  extract (EPOCH from  CAST (lpep_dropoff_datetime AS TIMESTAMP) ) \n" +
                        "                -- 0.5 hour is 1800  seconds\n" +
                        "                RANGE BETWEEN 1800  PRECEDING AND 1 PRECEDING ) AS count_trips_window_30m_dropoff_zip,\n" +
                        "case when extract (ISODOW from  CAST (lpep_dropoff_datetime AS TIMESTAMP))  > 5 then 1 else 0 end as dropoff_is_weekend\n" +
                        "FROM green_tripdata";
        DBSPCompiler compiler = testCompiler();
        compiler.setGenerateInputsFromTables(true);
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        this.addRustTestCase(getCircuit(compiler));
    }

    @Test
    // Not yet supported because of LAG
    public void fraudDetectionTest() {
        // fraudDetection-352718.cc_data.demo_
        String ddl0 = "CREATE TABLE demographics (\n" +
                " cc_num FLOAT64 NOT NULL,\n" +
                " first STRING,\n" +
                " gender STRING,\n" +
                " street STRING,\n" +
                " city STRING,\n" +
                " state STRING,\n" +
                " zip INTEGER,\n" +
                " lat FLOAT64,\n" +
                " long FLOAT64,\n" +
                " city_pop INTEGER,\n" +
                " job STRING,\n" +
                " dob DATE\n" +
                ")";
        String ddl1 = "CREATE TABLE transactions (\n" +
                " trans_date_trans_time TIMESTAMP NOT NULL,\n" +
                " cc_num FLOAT64,\n" +
                " merchant STRING,\n" +
                " category STRING,\n" +
                " amt FLOAT64,\n" +
                " trans_num STRING,\n" +
                " unix_time INTEGER NOT NULL,\n" +
                " merch_lat FLOAT64,\n" +
                " merch_long FLOAT64,\n" +
                " is_fraud INTEGER\n" +
                ")";
        String query = "SELECT\n" +
                "    DAYOFWEEK(trans_date_trans_time) AS d,\n" +
                "    TIMESTAMPDIFF(YEAR, trans_date_trans_time, CAST(dob as TIMESTAMP)) AS age,\n" +
                "    ST_DISTANCE(ST_POINT(long,lat), ST_POINT(merch_long,merch_lat)) AS distance,\n" +
                "    -- TIMESTAMPDIFF(MINUTE, trans_date_trans_time, last_txn_date) AS trans_diff,\n" +
                "    AVG(amt) OVER(\n" +
                "                PARTITION BY   CAST(cc_num AS NUMERIC)\n" +
                "                ORDER BY unix_time\n" +
                "                -- 1 week is 604800  seconds\n" +
                "                RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS\n" +
                "avg_spend_pw,\n" +
                "      AVG(amt) OVER(\n" +
                "                PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "                ORDER BY unix_time\n" +
                "                -- 1 month(30 days) is 2592000 seconds\n" +
                "                RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS\n" +
                "avg_spend_pm,\n" +
                "    COUNT(*) OVER(\n" +
                "                PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "                ORDER BY unix_time\n" +
                "                -- 1 day is 86400  seconds\n" +
                "                RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS\n" +
                "trans_freq_24,\n" +
                "  category,\n" +
                "    amt,\n" +
                "    state,\n" +
                "    job,\n" +
                "    unix_time,\n" +
                "    city_pop,\n" +
                "    merchant,\n" +
                "    is_fraud\n" +
                "  FROM (\n" +
                "          SELECT t1.*, t2.*\n" +
                "          --,    LAG(trans_date_trans_time, 1) OVER (PARTITION BY t1.cc_num\n" +
                "          -- ORDER BY trans_date_trans_time ASC) AS last_txn_date\n" +
                "          FROM  transactions AS t1\n" +
                "          LEFT JOIN  demographics AS t2\n" +
                "          ON t1.cc_num =t2.cc_num)";
        DBSPCompiler compiler = testCompiler();
        compiler.setGenerateInputsFromTables(true);
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl0);
        compiler.compileStatement(ddl1);
        compiler.compileStatement(query);
        this.addRustTestCase(getCircuit(compiler));
    }
}
