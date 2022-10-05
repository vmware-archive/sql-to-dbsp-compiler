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

import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.midend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.midend.TableContents;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqllogictest.SqlStatement;
import org.dbsp.sqllogictest.SqlTestFile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a hybrid test executor which keeps all the state in a
 * database using JDBC and executes all queries using DBSP>
 */
public class DBSP_JDBC_Executor extends DBSPExecutor {
    private final JDBCExecutor statementExecutor;

    /**
     * @param execute If true the tests are executed, otherwise they are only compiled to Rust.
     */
    public DBSP_JDBC_Executor(JDBCExecutor executor, boolean execute) {
        super(execute);
        this.statementExecutor = executor;
    }

    DBSPFunction createInputFunction(CalciteToDBSPCompiler compiler, TableContents transaction)
            throws SQLException {
        List<String> tables = this.statementExecutor.getTableList();
        DBSPZSetLiteral[] tuple = new DBSPZSetLiteral[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            String table = tables.get(i);
            DBSPZSetLiteral lit = this.statementExecutor.getTableContents(table);
            if (lit == null)
                throw new RuntimeException("No input found for table " + table);
            tuple[i] = lit;
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(tuple);
        return new DBSPFunction("input", new ArrayList<>(),
                result.getType(), result);
    }

    public boolean statement(SqlStatement statement) throws SQLException {
        this.statementExecutor.statement(statement);
        String command = statement.statement.toLowerCase();
        if (command.contains("create table") || command.contains("drop table"))
            super.statement(statement);
        return true;
    }

    @Override
    public TestStatistics execute(SqlTestFile file)
            throws SqlParseException, IOException, InterruptedException, SQLException {
        this.statementExecutor.establishConnection();
        this.statementExecutor.dropAllTables();
        return super.execute(file);
    }
}
