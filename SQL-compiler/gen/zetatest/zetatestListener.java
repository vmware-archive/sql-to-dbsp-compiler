// Generated from java-escape by ANTLR 4.11.1
package zetatest;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link zetatestParser}.
 */
public interface zetatestListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link zetatestParser#tests}.
	 * @param ctx the parse tree
	 */
	void enterTests(zetatestParser.TestsContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#tests}.
	 * @param ctx the parse tree
	 */
	void exitTests(zetatestParser.TestsContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#nonEmptyTests}.
	 * @param ctx the parse tree
	 */
	void enterNonEmptyTests(zetatestParser.NonEmptyTestsContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#nonEmptyTests}.
	 * @param ctx the parse tree
	 */
	void exitNonEmptyTests(zetatestParser.NonEmptyTestsContext ctx);
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
	 * Enter a parse tree produced by {@link zetatestParser#dashes}.
	 * @param ctx the parse tree
	 */
	void enterDashes(zetatestParser.DashesContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#dashes}.
	 * @param ctx the parse tree
	 */
	void exitDashes(zetatestParser.DashesContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#equals}.
	 * @param ctx the parse tree
	 */
	void enterEquals(zetatestParser.EqualsContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#equals}.
	 * @param ctx the parse tree
	 */
	void exitEquals(zetatestParser.EqualsContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#lines}.
	 * @param ctx the parse tree
	 */
	void enterLines(zetatestParser.LinesContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#lines}.
	 * @param ctx the parse tree
	 */
	void exitLines(zetatestParser.LinesContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#line}.
	 * @param ctx the parse tree
	 */
	void enterLine(zetatestParser.LineContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#line}.
	 * @param ctx the parse tree
	 */
	void exitLine(zetatestParser.LineContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatestParser#result}.
	 * @param ctx the parse tree
	 */
	void enterResult(zetatestParser.ResultContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatestParser#result}.
	 * @param ctx the parse tree
	 */
	void exitResult(zetatestParser.ResultContext ctx);
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