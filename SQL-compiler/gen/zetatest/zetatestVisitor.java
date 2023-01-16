// Generated from java-escape by ANTLR 4.11.1
package zetatest;
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
	 * Visit a parse tree produced by {@link zetatestParser#tests}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTests(zetatestParser.TestsContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#nonEmptyTests}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonEmptyTests(zetatestParser.NonEmptyTestsContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#test}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTest(zetatestParser.TestContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#dashes}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDashes(zetatestParser.DashesContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#equals}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEquals(zetatestParser.EqualsContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#lines}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLines(zetatestParser.LinesContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#line}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLine(zetatestParser.LineContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#result}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResult(zetatestParser.ResultContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatestParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(zetatestParser.QueryContext ctx);
}