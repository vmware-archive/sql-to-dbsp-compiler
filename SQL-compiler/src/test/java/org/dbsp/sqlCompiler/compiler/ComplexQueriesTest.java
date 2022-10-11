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

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.visitors.DBSPCompiler;
import org.junit.Assert;
import org.junit.Test;

public class ComplexQueriesTest extends BaseSQLTests {
    public String fixup(String query) {
        return query.replace("FLOAT64", "DOUBLE")
                .replace("STRING", "VARCHAR");
    }

    //@Test
    public void fraudDetectionTest() throws SqlParseException {
        // frauddetection-352718.cc_data.demo_
        String ddl0 = "CREATE TABLE demographics (\n" +
                "  cc_num FLOAT64,\n" +
                "  first STRING,\n" +
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
                "  trans_date_trans_time TIMESTAMP,\n" +
                "  cc_num FLOAT64,\n" +
                "  merchant STRING,\n" +
                " category STRING,\n" +
                " amt FLOAT64,\n" +
                " trans_num STRING,\n" +
                " unix_time INTEGER,\n" +
                " merch_lat FLOAT64,\n" +
                " merch_long FLOAT64,\n" +
                " is_fraud INTEGER\n" +
                ")";
        String query = "SELECT\n" +
                "    DAYOFWEEK(trans_date_trans_time) AS d,\n" +
                "    DATEDIFF(YEAR, CAST(trans_date_trans_time AS DATE), dob) AS age,\n" +
                "    ST_DISTANCE(ST_GEOGPOINT(long,lat), ST_GEOGPOINT(merch_long,\n" +
                "merch_lat)) AS distance,\n" +
                "    TIMESTAMP_DIFF(trans_date_trans_time, last_txn_date , MINUTE) AS trans_diff,\n" +
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
                "          SELECT t1.*,t2.* EXCEPT(cc_num),\n" +
                "              LAG(trans_date_trans_time) OVER (PARTITION BY t1.cc_num\n" +
                "ORDER BY trans_date_trans_time ASC) AS last_txn_date,\n" +
                "          FROM  `frauddetection-352718.cc_data.train_raw`  t1\n" +
                "          LEFT JOIN  `frauddetection-352718.cc_data.demographics`  t2\n" +
                "ON t1.cc_num =t2.cc_num)";
        DBSPCompiler compiler = new DBSPCompiler().newCircuit("circuit");
        compiler.setGenerateInputsFromTables(true);
        query = "CREATE VIEW V AS (" + query + ")";
        ddl0 = this.fixup(ddl0);
        ddl1 = this.fixup(ddl1);
        compiler.compileStatement(ddl0, null);
        compiler.compileStatement(ddl1, null);
        compiler.compileStatement(query, null);
        Assert.assertNotNull(compiler.getResult());
    }
}
