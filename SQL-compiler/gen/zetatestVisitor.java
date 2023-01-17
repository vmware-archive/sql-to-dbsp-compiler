// Generated from java-escape by ANTLR 4.11.1
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link zetatest}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface zetatestVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link zetatest#tests}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTests(zetatest.TestsContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#test}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTest(zetatest.TestContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#lines}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLines(zetatest.LinesContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#macros}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMacros(zetatest.MacrosContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#macro}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMacro(zetatest.MacroContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(zetatest.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#result}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResult(zetatest.ResultContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#typedvalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypedvalue(zetatest.TypedvalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link zetatest#line}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLine(zetatest.LineContext ctx);
}