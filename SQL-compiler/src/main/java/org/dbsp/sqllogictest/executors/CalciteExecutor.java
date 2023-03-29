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

package org.dbsp.sqllogictest.executors;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelRunner;
import org.dbsp.sqlCompiler.compiler.sqlparser.Catalog;
import org.dbsp.sqllogictest.*;
import org.dbsp.util.Logger;
import org.dbsp.util.TestStatistics;

import javax.sql.DataSource;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.dbsp.sqlCompiler.compiler.sqlparser.CalciteCompiler.TYPE_SYSTEM;

// TODO: this is work in progress

public class CalciteExecutor extends SqlSLTTestExecutor {
    private final JDBCExecutor statementExecutor;
    private final CalciteConnection calciteConnection;
    private static final String POSTGRESQL_SCHEMA = "";
    private final Planner planner;
    private final SqlToRelConverter converter;

    public CalciteExecutor(JDBCExecutor statementExecutor, String user, String password) throws SQLException {
        this.statementExecutor = statementExecutor;
        // Build our connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        // Unwrap our connection using the CalciteConnection
        this.calciteConnection = connection.unwrap(CalciteConnection.class);
        Catalog catalog = new Catalog("schema");
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        DataSource postgresDataSource = JdbcSchema.dataSource(
                "jdbc:postgresql://localhost/slt",
                "org.postgresql.Driver",
                user,
                password
        );
        // Attach our Postgres Jdbc Datasource to our Root Schema
        JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, POSTGRESQL_SCHEMA, postgresDataSource, null, null);
        rootSchema.add(POSTGRESQL_SCHEMA, jdbcSchema);
        SqlParser.Config parserConfig = SqlParser.config();
        Frameworks.ConfigBuilder config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(parserConfig)
                .context(Contexts.of(calciteConnection.config()));
        this.planner = Frameworks.getPlanner(config.build());

        Properties connConfigProp = new Properties();
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(connConfigProp);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(TYPE_SYSTEM);
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                Objects.requireNonNull(rootSchema.unwrap(CalciteSchema.class)),
                Collections.singletonList(catalog.schemaName), typeFactory, connectionConfig);

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withTypeCoercionEnabled(true)
                .withDefaultNullCollation(connectionConfig.defaultNullCollation());
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.STANDARD),
                catalogReader,
                typeFactory,
                validatorConfig
        );

        // This planner does not do anything.
        // We use a series of planner stages later to perform the real optimizations.
        RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        planner.setExecutor(RexUtil.EXECUTOR);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config();
        this.converter = new SqlToRelConverter(
                (type, query, schema, path) -> null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    static final String regexCreate = "create\\s+table\\s+(\\w+)";
    static final Pattern patCreate = Pattern.compile(regexCreate);
    static final String regexDrop = "drop\\s+table\\s+(\\w+)";
    static final Pattern patDrop = Pattern.compile(regexDrop);

    boolean statement(SqlStatement statement) throws SQLException {
        this.statementExecutor.statement(statement);
        return true;
    }

    boolean query(SqlTestQuery query, int queryNo) throws SQLException, SqlParseException {
        Logger.INSTANCE.from(this, 1)
                .append("Executing query ")
                .append(query.query)
                .newline();
        SqlNode node = planner.parse(query.query);
        RelRoot relRoot = this.converter.convertQuery(node, true, true);

        final RelRunner runner = this.calciteConnection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(relRoot.rel);
        ps.execute();
        ResultSet resultSet = ps.getResultSet();
        return true;
    }

    @Override
    public TestStatistics execute(SLTTestFile file)
            throws SqlParseException, IOException, InterruptedException, SQLException, NoSuchAlgorithmException {
        this.statementExecutor.establishConnection();
        this.statementExecutor.dropAllViews();
        this.statementExecutor.dropAllTables();

        TestStatistics result = new TestStatistics();
        for (ISqlTestOperation operation: file.fileContents) {
            SqlStatement stat = operation.as(SqlStatement.class);
            if (stat != null) {
                boolean status;
                try {
                    if (this.buggyOperations.contains(stat.statement)) {
                        Logger.INSTANCE.from(this, 1)
                                .append("Skipping buggy test ")
                                .append(stat.statement)
                                .newline();
                        status = stat.shouldPass;
                    } else {
                        status = this.statement(stat);
                    }
                } catch (SQLException ex) {
                    Logger.INSTANCE.from(this, 1)
                            .append("Statement failed ")
                            .append(stat.statement)
                            .newline();
                    status = false;
                }
                this.statementsExecuted++;
                if (this.validateStatus &&
                        status != stat.shouldPass)
                    throw new RuntimeException("Statement " + stat.statement + " status " + status + " expected " + stat.shouldPass);
            } else {
                SqlTestQuery query = operation.to(SqlTestQuery.class);
                if (this.buggyOperations.contains(query.query)) {
                    Logger.INSTANCE.from(this, 1)
                            .append("Skipping buggy test ")
                            .append(query.query)
                            .newline();
                    result.ignored++;
                    continue;
                }
                boolean executed = this.query(query, result.passed);
                if (executed) {
                    result.passed++;
                } else {
                    result.ignored++;
                }
            }
        }
        this.statementExecutor.closeConnection();
        this.reportTime(result.passed);
        Logger.INSTANCE.from(this, 1)
                .append("Finished executing ")
                .append(file.toString())
                .newline();
        return result;
    }
}