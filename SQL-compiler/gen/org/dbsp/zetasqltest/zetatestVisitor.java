// Generated from /home/mbudiu/git/sql-to-dbsp/SQL-compiler/src/main/java/org/dbsp/zetasqltest/zetatest.g4 by ANTLR 4.10.1
package org.dbsp.zetasqltest;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link zetatestParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface zetatestVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link zetatestParser#test}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTest(zetatestParser.TestContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#setup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetup(zetatestParser.SetupContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#separator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSeparator(zetatestParser.SeparatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(zetatestParser.QueryContext ctx);
}