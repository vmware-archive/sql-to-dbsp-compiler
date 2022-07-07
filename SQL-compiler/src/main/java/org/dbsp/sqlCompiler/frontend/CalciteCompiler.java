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

package org.dbsp.sqlCompiler.frontend;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.dbsp.util.Unimplemented;
import org.dbsp.util.UnsupportedException;

import java.util.Collections;
import java.util.Properties;

/**
 * This class wraps up the Calcite SQL compiler.
 * It provides a simple service: Given a sequence of SQL query strings,
 * including a complete set of Data Definition Language statements,
 * it returns a Calcite representation for each statement.
 */
@SuppressWarnings("FieldCanBeLocal")
public class CalciteCompiler {
    private final SqlParser.Config parserConfig;
    private final SqlValidator validator;
    private final Catalog simple;
    private final boolean debug = true;
    private final SqlToRelConverter converter;
    private final SqlSimulator simulator;
    public final RelOptCluster cluster;
    public final RelDataTypeFactory typeFactory;

    private final CalciteProgram program = new CalciteProgram();

    // Adapted from https://www.querifylabs.com/blog/assembling-a-query-optimizer-with-apache-calcite
    public CalciteCompiler() {
        Properties connConfigProp = new Properties();
        connConfigProp.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        connConfigProp.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        connConfigProp.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(connConfigProp);
        // Add support for DDL language
        this.parserConfig = SqlParser.config().withParserFactory(SqlDdlParserImpl.FACTORY);
        this.typeFactory = new JavaTypeFactoryImpl();
        this.simple = new Catalog("schema");
        this.simulator = new SqlSimulator(this.simple);
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(simple.schemaName, this.simple);
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema, Collections.singletonList(simple.schemaName), this.typeFactory, connectionConfig);

        SqlOperatorTable operatorTable = SqlOperatorTables.chain(
                SqlStdOperatorTable.instance()
        );

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withTypeCoercionEnabled(true)
                .withConformance(connectionConfig.conformance())
                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                .withIdentifierExpansion(true);

        this.validator = SqlValidatorUtil.newValidator(
                operatorTable,
                catalogReader,
                this.typeFactory,
                validatorConfig
        );
        RelOptPlanner planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(connectionConfig)
        );
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        this.cluster = RelOptCluster.create(
                planner,
                new RexBuilder(this.typeFactory)
        );

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);
        this.converter = new SqlToRelConverter(
                (type, query, schema, path) -> null,
                this.validator,
                catalogReader,
                this.cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    /**
     * Given a SQL statement returns a SqlNode - a calcite AST
     * representation of the query.
     * @param sql  SQL query to compile
     */
    private SqlNode parse(String sql) throws SqlParseException {
        // This is a weird API - the parser depends on the query string!
        SqlParser sqlParser = SqlParser.create(sql, this.parserConfig);
        return sqlParser.parseStmt();
    }

    /**
     * Simulate the execution of a DDL statement.
     * @return A data structure summarizing the result of the simulation.
     */
    private SimulatorResult simulate(SqlNode node) {
        return this.simulator.execute(node);
    }

    /**
     * Given a SQL query statement it compiles.
     * If the statement is a DDL statement (CREATE TABLE, CREATE VIEW),
     * the result is added to the generated "program".
     * This function can be invoked multiple times to produce a program that computes
     * multiple views.  It should be invoked for both
     * - the table definition statements (CREATE TABLE X AS ...), one for each input table
     * - the view definition statements (CREATE VIEW V AS ...), one for each output view
     * If the statement is a DML statement (INSERT), this returns a representation
     * of the statement, but does *not* execute it.
     * @param sqlStatement  SQL statement to compile.
     */
    public SimulatorResult compile(String sqlStatement) throws SqlParseException {
        SqlNode node = this.parse(sqlStatement);
        if (SqlKind.DDL.contains(node.getKind())) {
            SimulatorResult result = this.simulate(node);
            TableDDL table = result.as(TableDDL.class);
            if (table != null) {
                this.program.addInput(table);
                return table;
            }
            ViewDDL view = result.as(ViewDDL.class);
            if (view != null) {
                RelRoot relRoot = this.converter.convertQuery(view.query, true, true);
                // Dump plan
                if (this.debug)
                    System.out.println(
                        RelOptUtil.dumpPlan("[Logical plan]", relRoot.rel,
                            SqlExplainFormat.TEXT,
                            SqlExplainLevel.NON_COST_ATTRIBUTES));
                view.setCompiledQuery(relRoot);
                if (!relRoot.collation.getKeys().isEmpty()) {
                    throw new UnsupportedException("ORDER BY", relRoot);
                }
                this.program.addView(view);
                return view;
            }
        }

        if (SqlKind.DML.contains(node.getKind())) {
            SimulatorResult result = this.simulate(node);
            UpdateStatment stat = result.as(UpdateStatment.class);
            if (stat != null) {
                TableDDL tbl = this.program.getInputTable(stat.table);
                assert tbl != null;
                RelRoot values = this.converter.convertQuery(stat.data, true, true);
                stat.setTranslation(values.rel);
                return stat;
            }
        }

        throw new Unimplemented(node);
    }

    public CalciteProgram getProgram() {
        return this.program;
    }
}
