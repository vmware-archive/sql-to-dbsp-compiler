// Generated from /home/mbudiu/git/sql-to-dbsp/SQL-compiler/src/main/java/org/dbsp/zetasqltest/zetatest.g4 by ANTLR 4.10.1
package org.dbsp.zetasqltest;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link zetatestParser}.
 */
public interface zetatestListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link zetatestParser#test}.
	 * @param ctx the parse tree
	 */
	void enterTest(zetatestParser.TestContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#test}.
	 * @param ctx the parse tree
	 */
	void exitTest(zetatestParser.TestContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#setup}.
	 * @param ctx the parse tree
	 */
	void enterSetup(zetatestParser.SetupContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#setup}.
	 * @param ctx the parse tree
	 */
	void exitSetup(zetatestParser.SetupContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#separator}.
	 * @param ctx the parse tree
	 */
	void enterSeparator(zetatestParser.SeparatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#separator}.
	 * @param ctx the parse tree
	 */
	void exitSeparator(zetatestParser.SeparatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(zetatestParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(zetatestParser.QueryContext ctx);
}