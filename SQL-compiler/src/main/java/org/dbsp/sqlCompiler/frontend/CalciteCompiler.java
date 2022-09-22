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
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.util.Linq;
import org.dbsp.util.TranslationException;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
    private final SqlToRelConverter converter;
    private final SqlSimulator simulator;
    public final RelOptCluster cluster;
    public final RelDataTypeFactory typeFactory;
    @Nullable
    private CalciteProgram program;
    private final SqlToRelConverter.Config converterConfig;
    private static final boolean debug = false;
    private final RelOptPlanner optimizer;

    /**
     * Policy which decides whether to run the busy join optimization.
     * @param rootRel Current plan.
     */
    public static boolean avoidBushyJoin(RelNode rootRel) {
        class OuterJoinFinder extends RelVisitor {
            public int outerJoinCount = 0;
            public int joinCount = 0;
            @Override public void visit(RelNode node, int ordinal,
                                        @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
                if (node instanceof Join) {
                    Join join = (Join)node;
                    ++joinCount;
                    if (join.getJoinType().isOuterJoin())
                    ++outerJoinCount;
                }
                super.visit(node, ordinal, parent);
            }

            void run(RelNode node) {
                this.go(node);
            }
        }

        OuterJoinFinder finder = new OuterJoinFinder();
        finder.run(rootRel);
        // Bushy join optimization fails when the query contains outer joins.
        return (finder.outerJoinCount > 0) || (finder.joinCount < 4);
    }

    static HepProgram createProgram(RelOptRule... rules) {
        HepProgramBuilder builder = new HepProgramBuilder();
        for (RelOptRule r: rules)
            builder.addRuleInstance(r);
        return builder.build();
    }

    /**
     * We do program-dependent optimization, since some optimizations
     * are buggy and don't always work.
     */
    static List<HepProgram> getOptimizationStages(RelNode rel) {
        HepProgram constantFold = createProgram(
                CoreRules.FILTER_REDUCE_EXPRESSIONS,
                CoreRules.PROJECT_REDUCE_EXPRESSIONS,
                CoreRules.JOIN_REDUCE_EXPRESSIONS,
                CoreRules.WINDOW_REDUCE_EXPRESSIONS,
                CoreRules.CALC_REDUCE_EXPRESSIONS);
        // Remove empty collections
        HepProgram removeEmpty = createProgram(
                PruneEmptyRules.UNION_INSTANCE,
                PruneEmptyRules.INTERSECT_INSTANCE,
                PruneEmptyRules.MINUS_INSTANCE,
                PruneEmptyRules.PROJECT_INSTANCE,
                PruneEmptyRules.FILTER_INSTANCE,
                PruneEmptyRules.SORT_INSTANCE,
                PruneEmptyRules.AGGREGATE_INSTANCE,
                PruneEmptyRules.JOIN_LEFT_INSTANCE,
                PruneEmptyRules.JOIN_RIGHT_INSTANCE,
                PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
        HepProgram distinctAaggregates = createProgram(
                // Convert DISTINCT aggregates into separate computations and join the results
                CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN);
        HepProgram multiJoins = new HepProgramBuilder()
                // Join order optimization
                .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                .addMatchOrder(HepMatchOrder.BOTTOM_UP)
                .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
                .addRuleInstance(CoreRules.PROJECT_MULTI_JOIN_MERGE)
                .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY)
                .build();
        HepProgram mergeNodes = createProgram(
                CoreRules.PROJECT_MERGE,
                CoreRules.MINUS_MERGE,
                CoreRules.UNION_MERGE,
                CoreRules.AGGREGATE_MERGE,
                CoreRules.INTERSECT_MERGE);
        // Remove unused code
        HepProgram remove = createProgram(
                CoreRules.AGGREGATE_REMOVE,
                CoreRules.UNION_REMOVE,
                CoreRules.PROJECT_REMOVE,
                CoreRules.PROJECT_JOIN_JOIN_REMOVE,
                CoreRules.PROJECT_JOIN_REMOVE
                );
            if (avoidBushyJoin(rel))
                return Linq.list(constantFold, removeEmpty, distinctAaggregates, mergeNodes, remove);
            return Linq.list(constantFold, removeEmpty, distinctAaggregates, multiJoins, mergeNodes, remove);
            /*
        return Linq.list(
                CoreRules.PROJECT_JOIN_TRANSPOSE)
                CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
                CoreRules.AGGREGATE_UNION_AGGREGATE,
                CoreRules.JOIN_PUSH_EXPRESSIONS,
                CoreRules.JOIN_CONDITION_PUSH,
                CoreRules.PROJECT_FILTER_TRANSPOSE,
                CoreRules.PROJECT_SET_OP_TRANSPOSE,
        );
         */
    }

    // Adapted from https://www.querifylabs.com/blog/assembling-a-query-optimizer-with-apache-calcite
    public CalciteCompiler() {
        this.program = null;
        Properties connConfigProp = new Properties();
        connConfigProp.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        connConfigProp.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        connConfigProp.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(connConfigProp);
        // Add support for DDL language
        this.parserConfig = SqlParser.config()
                .withParserFactory(SqlDdlParserImpl.FACTORY)
                .withConformance(SqlConformanceEnum.BABEL);
        this.typeFactory = new JavaTypeFactoryImpl();
        this.simple = new Catalog("schema");
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

        // The optimizer does not seem to be invoked anywhere.
        this.optimizer = new HepPlanner(new HepProgramBuilder().build());
        /*
        RelOptPlanner planner = new VolcanoPlanner(
                    RelOptCostImpl.FACTORY,
                    Contexts.of(connectionConfig));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
         */
        this.cluster = RelOptCluster.create(
                this.optimizer,
                new RexBuilder(this.typeFactory)
        );

        this.converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withDecorrelationEnabled(true)
                .withExpand(true);
        this.converter = new SqlToRelConverter(
                (type, query, schema, path) -> null,
                this.validator,
                catalogReader,
                this.cluster,
                StandardConvertletTable.INSTANCE,
                this.converterConfig
        );
        this.simulator = new SqlSimulator(this.simple, this.typeFactory, this.validator);
    }

    public static String getPlan(RelNode rel) {
        return RelOptUtil.dumpPlan("[Logical plan]", rel,
                SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES);
    }

    public RexBuilder getRexBuilder() {
        return this.cluster.getRexBuilder();
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

    public void startCompilation() {
        this.program = new CalciteProgram();
    }

    RelNode optimize(RelNode rel) {
        if (debug)
            System.out.println("Before optimizer " + getPlan(rel));

        for (HepProgram program: getOptimizationStages(rel)) {
            HepPlanner planner = new HepPlanner(program);
            planner.setRoot(rel);
            rel = planner.findBestExp();
            if (debug)
                System.out.println("After optimizer stage " + getPlan(rel));
        }
        RelBuilder relBuilder = this.converterConfig.getRelBuilderFactory().create(
                cluster, null);
        // This converts correlated sub-queries into standard joins.
        rel = RelDecorrelator.decorrelateQuery(rel, relBuilder);
        if (debug)
            System.out.println("After decorrelator " + getPlan(rel));
        return rel;
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
        if (this.program == null)
            throw new RuntimeException("Did you call startCompilation? Program is null");
        this.program.setQuery(sqlStatement);
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
                relRoot = relRoot.withRel(this.optimize(relRoot.rel));
                view.setCompiledQuery(relRoot);
                this.program.addView(view);
                return view;
            }
        }

        if (SqlKind.DML.contains(node.getKind())) {
            SimulatorResult result = this.simulate(node);
            TableModifyStatement stat = result.as(TableModifyStatement.class);
            if (stat != null) {
                TableDDL tbl = this.program.getInputTable(stat.table);
                if (tbl == null)
                    throw new TranslationException("Could not find translation", stat.table);
                RelRoot values = this.converter.convertQuery(stat.data, true, true);
                values = values.withRel(this.optimize(values.rel));
                stat.setTranslation(values.rel);
                return stat;
            }
        }

        throw new Unimplemented(node);
    }

    public CalciteProgram getProgram() {
        return Objects.requireNonNull(this.program);
    }
}
