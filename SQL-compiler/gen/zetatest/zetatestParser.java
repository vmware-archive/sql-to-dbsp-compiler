// Generated from java-escape by ANTLR 4.11.1
package zetatest;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class zetatestParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.11.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3;
	public static final int
		RULE_tests = 0, RULE_nonEmptyTests = 1, RULE_test = 2, RULE_dashes = 3, 
		RULE_equals = 4, RULE_lines = 5, RULE_line = 6, RULE_result = 7, RULE_query = 8;
	private static String[] makeRuleNames() {
		return new String[] {
			"tests", "nonEmptyTests", "test", "dashes", "equals", "lines", "line", 
			"result", "query"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'--'", "'\\n'", "'=='"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "java-escape"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public zetatestParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TestsContext extends ParserRuleContext {
		public NonEmptyTestsContext nonEmptyTests() {
			return getRuleContext(NonEmptyTestsContext.class,0);
		}
		public TestsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tests; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterTests(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitTests(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitTests(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TestsContext tests() throws RecognitionException {
		TestsContext _localctx = new TestsContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_tests);
		try {
			setState(20);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case EOF:
				enterOuterAlt(_localctx, 1);
				{
				}
				break;
			case T__0:
			case T__1:
			case T__2:
				enterOuterAlt(_localctx, 2);
				{
				setState(19);
				nonEmptyTests();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NonEmptyTestsContext extends ParserRuleContext {
		public TestContext test() {
			return getRuleContext(TestContext.class,0);
		}
		public EqualsContext equals() {
			return getRuleContext(EqualsContext.class,0);
		}
		public NonEmptyTestsContext nonEmptyTests() {
			return getRuleContext(NonEmptyTestsContext.class,0);
		}
		public NonEmptyTestsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonEmptyTests; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterNonEmptyTests(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitNonEmptyTests(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitNonEmptyTests(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonEmptyTestsContext nonEmptyTests() throws RecognitionException {
		NonEmptyTestsContext _localctx = new NonEmptyTestsContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_nonEmptyTests);
		try {
			setState(27);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(22);
				test();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(23);
				test();
				setState(24);
				equals();
				setState(25);
				nonEmptyTests();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TestContext extends ParserRuleContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public DashesContext dashes() {
			return getRuleContext(DashesContext.class,0);
		}
		public ResultContext result() {
			return getRuleContext(ResultContext.class,0);
		}
		public TestContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_test; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterTest(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitTest(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitTest(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TestContext test() throws RecognitionException {
		TestContext _localctx = new TestContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_test);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(29);
			query();
			setState(30);
			dashes();
			setState(31);
			result();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DashesContext extends ParserRuleContext {
		public DashesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dashes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterDashes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitDashes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitDashes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DashesContext dashes() throws RecognitionException {
		DashesContext _localctx = new DashesContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_dashes);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(33);
			match(T__0);
			setState(34);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class EqualsContext extends ParserRuleContext {
		public EqualsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_equals; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterEquals(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitEquals(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitEquals(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EqualsContext equals() throws RecognitionException {
		EqualsContext _localctx = new EqualsContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_equals);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(36);
			match(T__2);
			setState(37);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LinesContext extends ParserRuleContext {
		public LineContext line() {
			return getRuleContext(LineContext.class,0);
		}
		public LinesContext lines() {
			return getRuleContext(LinesContext.class,0);
		}
		public LinesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lines; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterLines(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitLines(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitLines(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LinesContext lines() throws RecognitionException {
		LinesContext _localctx = new LinesContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_lines);
		try {
			setState(43);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(40);
				line();
				setState(41);
				lines();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LineContext extends ParserRuleContext {
		public LineContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_line; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterLine(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitLine(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitLine(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LineContext line() throws RecognitionException {
		LineContext _localctx = new LineContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_line);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(48);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1+1 ) {
					{
					{
					setState(45);
					matchWildcard();
					}
					} 
				}
				setState(50);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			}
			setState(51);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ResultContext extends ParserRuleContext {
		public LinesContext lines() {
			return getRuleContext(LinesContext.class,0);
		}
		public ResultContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_result; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterResult(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitResult(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitResult(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResultContext result() throws RecognitionException {
		ResultContext _localctx = new ResultContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_result);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(53);
			lines();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public LinesContext lines() {
			return getRuleContext(LinesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof zetatestListener ) ((zetatestListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof zetatestVisitor ) return ((zetatestVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_query);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(55);
			lines();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u0003:\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0001\u0000\u0001\u0000\u0003\u0000\u0015\b\u0000\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001\u001c\b\u0001"+
		"\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0005\u0003\u0005,\b\u0005\u0001\u0006\u0005\u0006"+
		"/\b\u0006\n\u0006\f\u00062\t\u0006\u0001\u0006\u0001\u0006\u0001\u0007"+
		"\u0001\u0007\u0001\b\u0001\b\u0001\b\u00010\u0000\t\u0000\u0002\u0004"+
		"\u0006\b\n\f\u000e\u0010\u0000\u00004\u0000\u0014\u0001\u0000\u0000\u0000"+
		"\u0002\u001b\u0001\u0000\u0000\u0000\u0004\u001d\u0001\u0000\u0000\u0000"+
		"\u0006!\u0001\u0000\u0000\u0000\b$\u0001\u0000\u0000\u0000\n+\u0001\u0000"+
		"\u0000\u0000\f0\u0001\u0000\u0000\u0000\u000e5\u0001\u0000\u0000\u0000"+
		"\u00107\u0001\u0000\u0000\u0000\u0012\u0015\u0001\u0000\u0000\u0000\u0013"+
		"\u0015\u0003\u0002\u0001\u0000\u0014\u0012\u0001\u0000\u0000\u0000\u0014"+
		"\u0013\u0001\u0000\u0000\u0000\u0015\u0001\u0001\u0000\u0000\u0000\u0016"+
		"\u001c\u0003\u0004\u0002\u0000\u0017\u0018\u0003\u0004\u0002\u0000\u0018"+
		"\u0019\u0003\b\u0004\u0000\u0019\u001a\u0003\u0002\u0001\u0000\u001a\u001c"+
		"\u0001\u0000\u0000\u0000\u001b\u0016\u0001\u0000\u0000\u0000\u001b\u0017"+
		"\u0001\u0000\u0000\u0000\u001c\u0003\u0001\u0000\u0000\u0000\u001d\u001e"+
		"\u0003\u0010\b\u0000\u001e\u001f\u0003\u0006\u0003\u0000\u001f \u0003"+
		"\u000e\u0007\u0000 \u0005\u0001\u0000\u0000\u0000!\"\u0005\u0001\u0000"+
		"\u0000\"#\u0005\u0002\u0000\u0000#\u0007\u0001\u0000\u0000\u0000$%\u0005"+
		"\u0003\u0000\u0000%&\u0005\u0002\u0000\u0000&\t\u0001\u0000\u0000\u0000"+
		"\',\u0001\u0000\u0000\u0000()\u0003\f\u0006\u0000)*\u0003\n\u0005\u0000"+
		"*,\u0001\u0000\u0000\u0000+\'\u0001\u0000\u0000\u0000+(\u0001\u0000\u0000"+
		"\u0000,\u000b\u0001\u0000\u0000\u0000-/\t\u0000\u0000\u0000.-\u0001\u0000"+
		"\u0000\u0000/2\u0001\u0000\u0000\u000001\u0001\u0000\u0000\u00000.\u0001"+
		"\u0000\u0000\u000013\u0001\u0000\u0000\u000020\u0001\u0000\u0000\u0000"+
		"34\u0005\u0002\u0000\u00004\r\u0001\u0000\u0000\u000056\u0003\n\u0005"+
		"\u00006\u000f\u0001\u0000\u0000\u000078\u0003\n\u0005\u00008\u0011\u0001"+
		"\u0000\u0000\u0000\u0004\u0014\u001b+0";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}