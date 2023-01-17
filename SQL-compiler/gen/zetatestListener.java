// Generated from java-escape by ANTLR 4.11.1
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link zetatest}.
 */
public interface zetatestListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link zetatest#tests}.
	 * @param ctx the parse tree
	 */
	void enterTests(zetatest.TestsContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#tests}.
	 * @param ctx the parse tree
	 */
	void exitTests(zetatest.TestsContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#test}.
	 * @param ctx the parse tree
	 */
	void enterTest(zetatest.TestContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#test}.
	 * @param ctx the parse tree
	 */
	void exitTest(zetatest.TestContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#lines}.
	 * @param ctx the parse tree
	 */
	void enterLines(zetatest.LinesContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#lines}.
	 * @param ctx the parse tree
	 */
	void exitLines(zetatest.LinesContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#macros}.
	 * @param ctx the parse tree
	 */
	void enterMacros(zetatest.MacrosContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#macros}.
	 * @param ctx the parse tree
	 */
	void exitMacros(zetatest.MacrosContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#macro}.
	 * @param ctx the parse tree
	 */
	void enterMacro(zetatest.MacroContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#macro}.
	 * @param ctx the parse tree
	 */
	void exitMacro(zetatest.MacroContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(zetatest.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(zetatest.QueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#result}.
	 * @param ctx the parse tree
	 */
	void enterResult(zetatest.ResultContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#result}.
	 * @param ctx the parse tree
	 */
	void exitResult(zetatest.ResultContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#typedvalue}.
	 * @param ctx the parse tree
	 */
	void enterTypedvalue(zetatest.TypedvalueContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#typedvalue}.
	 * @param ctx the parse tree
	 */
	void exitTypedvalue(zetatest.TypedvalueContext ctx);
	/**
	 * Enter a parse tree produced by {@link zetatest#line}.
	 * @param ctx the parse tree
	 */
	void enterLine(zetatest.LineContext ctx);
	/**
	 * Exit a parse tree produced by {@link zetatest#line}.
	 * @param ctx the parse tree
	 */
	void exitLine(zetatest.LineContext ctx);
}