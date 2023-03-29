package org.dbsp.sqlCompiler.compiler.backend.visitors;

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Base class for Inner visitors which rewrite expressions and statements.
 * This class recurses over the structure of expressions and statements
 * and if any fields have changed builds a new version of the same expression
 * or statement.
 */
public abstract class InnerExpressionRewriteVisitor
        extends InnerVisitor
        implements Function<IDBSPInnerNode, IDBSPInnerNode>, IModule {

    protected InnerExpressionRewriteVisitor(boolean visitSuper) {
        super(visitSuper);
    }

    /**
     * Result produced by the last preorder invocation.
     */
    @Nullable
    protected IDBSPInnerNode lastResult;

    IDBSPInnerNode getResult() {
        return Objects.requireNonNull(this.lastResult);
    }

    @Override
    @Nullable
    public IDBSPInnerNode apply(@Nullable IDBSPInnerNode dbspNode) {
        if (dbspNode == null)
            return null;
        this.startVisit();
        dbspNode.accept(this);
        this.endVisit();
        return this.getResult();
    }

    /**
     * Replace the 'old' IR node with the 'newOp' IR node.
     * @param old     only used for debugging.
     */
    protected void map(IDBSPInnerNode old, IDBSPInnerNode newOp) {
        Logger.INSTANCE.from(this, 1)
                .append(this.toString())
                .append(":")
                .append(old.toString())
                .append(" -> ")
                .append(newOp.toString())
                .newline();
        this.lastResult = newOp;
    }

    @Override
    public boolean preorder(IDBSPInnerNode node) {
        this.map(node, node);
        return false;
    }

    protected DBSPExpression getResultExpression() {
        return this.getResult().to(DBSPExpression.class);
    }

    @Nullable
    protected DBSPExpression transformN(@Nullable DBSPExpression expression) {
        if (expression == null)
            return null;
        expression.accept(this);
        return this.getResultExpression();
    }

    protected DBSPExpression transform(DBSPExpression expression) {
        expression.accept(this);
        return this.getResultExpression();
    }

    protected DBSPStatement transform(DBSPStatement statement) {
        statement.accept(this);
        return this.getResult().to(DBSPStatement.class);
    }

    @Override
    public boolean preorder(DBSPLiteral expression) {
        this.map(expression, expression);
        return false;
    }

    @Override
    public boolean preorder(DBSPAsExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPExpression result = expression;
        if (source != expression.source) {
            result = new DBSPAsExpression(source, expression.getNonVoidType());
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyExpression expression) {
        DBSPExpression[] arguments = Linq.map(expression.arguments, this::transform, DBSPExpression.class);
        DBSPExpression function = this.transform(expression.function);
        DBSPExpression result = expression;
        if (function != expression.function || Linq.different(arguments, expression.arguments))
            result = new DBSPApplyExpression(function, expression.getType(), arguments);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPApplyMethodExpression expression) {
        DBSPExpression[] arguments = Linq.map(expression.arguments, this::transform, DBSPExpression.class);
        DBSPExpression function = this.transform(expression.function);
        DBSPExpression self = this.transform(expression.self);
        DBSPExpression result = expression;
        if (function != expression.function ||
                self != expression.self ||
                Linq.different(arguments, expression.arguments))
            result = new DBSPApplyMethodExpression(function, self, arguments);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPAssignmentExpression expression) {
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPExpression result = expression;
        if (left != expression.left || right != expression.right)
            result = new DBSPAssignmentExpression(left, right);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        DBSPExpression left = this.transform(expression.left);
        DBSPExpression right = this.transform(expression.right);
        DBSPExpression result = expression;
        if (left != expression.left || right != expression.right)
            result = new DBSPBinaryExpression(expression.getNode(), expression.getNonVoidType(),
                    expression.operation, left, right, expression.primitive);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        List<DBSPStatement> body = Linq.map(expression.contents, this::transform);
        DBSPExpression last = this.transformN(expression.lastExpression);
        DBSPExpression result = expression;
        if (Linq.different(body, expression.contents) || last != expression.lastExpression) {
            result = new DBSPBlockExpression(body, last);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPBorrowExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression) {
            result = source.borrow(expression.mut);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPCastExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPExpression result = expression;
        if (source != expression.source) {
            result = source.cast(expression.destinationType);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        DBSPExpression body = this.transform(expression.body);
        DBSPExpression result = expression;
        if (body != expression.body)
            result = body.closure(expression.parameters);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPComparatorExpression expression) {
        this.map(expression, expression);
        return false;
    }

    @Override
    public boolean preorder(DBSPDerefExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression)
            result = new DBSPDerefExpression(source);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPEnumValue expression) {
        this.map(expression, expression);
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression)
            result = source.field(expression.fieldNo);
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPForExpression expression) {
        DBSPExpression iterated = this.transform(expression.iterated);
        DBSPExpression body = this.transform(expression.block);
        DBSPBlockExpression block;
        if (body.is(DBSPBlockExpression.class))
            block = body.to(DBSPBlockExpression.class);
        else
            block = new DBSPBlockExpression(Linq.list(), body);
        DBSPExpression result = expression;
        if (iterated != expression.iterated ||
                block != expression.block) {
            result = new DBSPForExpression(expression.pattern, iterated, block);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPIfExpression expression) {
        DBSPExpression cond = this.transform(expression.condition);
        DBSPExpression positive = this.transform(expression.positive);
        DBSPExpression negative = this.transform(expression.negative);
        DBSPExpression result = expression;
        if (cond != expression.condition || positive != expression.positive || negative != expression.negative) {
            result = new DBSPIfExpression(expression.getNode(), cond, positive, negative);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPMatchExpression expression) {
        List<DBSPExpression> caseExpressions =
                Linq.map(expression.cases, c -> this.transform(c.result));
        DBSPExpression matched = this.transform(expression.matched);
        DBSPExpression result = expression;
        if (matched != expression.matched ||
                Linq.different(caseExpressions, Linq.map(expression.cases, c -> c.result))) {
            List<DBSPMatchExpression.Case> newCases = Linq.zipSameLength(expression.cases, caseExpressions,
                    (c0, e) -> new DBSPMatchExpression.Case(c0.against, e));
            result = new DBSPMatchExpression(matched, newCases, expression.getNonVoidType());
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPPathExpression expression) {
        this.map(expression, expression);
        return false;
    }

    @Override
    public boolean preorder(DBSPQualifyTypeExpression expression) {
        DBSPExpression source = this.transform(expression.expression);
        DBSPExpression result = expression;
        if (source != expression.expression) {
            result = new DBSPQualifyTypeExpression(source, expression.types);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPRangeExpression expression) {
        @Nullable DBSPExpression left = this.transformN(expression.left);
        @Nullable DBSPExpression right = this.transformN(expression.right);
        DBSPExpression result = expression;
        if (left != expression.left || right != expression.right)
            result = new DBSPRangeExpression(left, right,
                    expression.endInclusive, expression.getNonVoidType());
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPRawTupleExpression expression) {
        DBSPExpression[] fields = Linq.map(expression.fields, this::transform, DBSPExpression.class);
        DBSPExpression result = expression;
        if (Linq.different(fields, expression.fields)) {
            result = new DBSPRawTupleExpression(fields);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPSortExpression expression) {
        this.map(expression, expression);
        return false;
    }

    @Override
    public boolean preorder(DBSPStructExpression expression) {
        DBSPExpression function = this.transform(expression.function);
        DBSPExpression[] arguments = Linq.map(expression.arguments, this::transform, DBSPExpression.class);
        DBSPExpression result = expression;
        if (Linq.different(arguments, expression.arguments)) {
            result = new DBSPStructExpression(function, expression.getNonVoidType(), arguments);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        DBSPExpression[] fields = Linq.map(expression.fields, this::transform, DBSPExpression.class);
        DBSPExpression result = expression;
        if (Linq.different(fields, expression.fields)) {
            result = new DBSPTupleExpression(fields);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression expression) {
        DBSPExpression source = this.transform(expression.source);
        DBSPExpression result = expression;
        if (source != expression.source) {
            result = new DBSPUnaryExpression(expression.getNode(), expression.getNonVoidType(),
                    expression.operation, source);
        }
        this.map(expression, result);
        return false;
    }

    @Override
    public boolean preorder(DBSPVariablePath expression) {
        this.map(expression, expression);
        return false;
    }

    /////////////////// statements

    @Override
    public boolean preorder(DBSPExpressionStatement statement) {
        DBSPExpression expression = this.transform(statement.expression);
        DBSPStatement result = statement;
        if (expression != statement.expression)
            result = new DBSPExpressionStatement(expression);
        this.map(statement, result);
        return false;
    }

    public boolean preorder(DBSPLetStatement statement) {
        DBSPExpression init = this.transformN(statement.initializer);
        DBSPStatement result = statement;
        if (init != statement.initializer) {
            if (init != null)
                result = new DBSPLetStatement(statement.variable, init);
            else
                result = new DBSPLetStatement(statement.variable, statement.type, statement.mutable);
        }
        this.map(statement, result);
        return false;
    }
}
