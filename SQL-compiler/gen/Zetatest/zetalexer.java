// Generated from java-escape by ANTLR 4.11.1
package Zetatest;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class zetalexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.11.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		LINE_COMMENT=1, NEWLINE=2, CHAR=3, MINUS=4, OPEN_BRACKET=5, CLOSED_BRACKET=6, 
		EQUAL=7, INT=8;
	public static final int
		RESULT=1;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE", "RESULT"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"LINE_COMMENT", "NEWLINE", "CHAR", "MINUS", "OPEN_BRACKET", "CLOSED_BRACKET", 
			"EQUAL", "INT"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, null, null, null, "'--'", "'['", "']'", "'=='"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "LINE_COMMENT", "NEWLINE", "CHAR", "MINUS", "OPEN_BRACKET", "CLOSED_BRACKET", 
			"EQUAL", "INT"
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


	public zetalexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "zetalexer.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\u0004\u0000\b:\u0006\uffff\uffff\u0006\uffff\uffff\u0002\u0000\u0007"+
		"\u0000\u0002\u0001\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007"+
		"\u0003\u0002\u0004\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007"+
		"\u0006\u0002\u0007\u0007\u0007\u0001\u0000\u0001\u0000\u0005\u0000\u0015"+
		"\b\u0000\n\u0000\f\u0000\u0018\t\u0000\u0001\u0000\u0001\u0000\u0001\u0000"+
		"\u0001\u0000\u0001\u0001\u0003\u0001\u001f\b\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0007\u0003\u0007"+
		"4\b\u0007\u0001\u0007\u0004\u00077\b\u0007\u000b\u0007\f\u00078\u0001"+
		"\u0016\u0000\b\u0002\u0001\u0004\u0002\u0006\u0003\b\u0004\n\u0005\f\u0006"+
		"\u000e\u0007\u0010\b\u0002\u0000\u0001\u0003\u0001\u0000\r\r\u0001\u0000"+
		"\n\n\u0001\u000009<\u0000\u0002\u0001\u0000\u0000\u0000\u0000\u0004\u0001"+
		"\u0000\u0000\u0000\u0000\u0006\u0001\u0000\u0000\u0000\u0000\b\u0001\u0000"+
		"\u0000\u0000\u0000\n\u0001\u0000\u0000\u0000\u0000\f\u0001\u0000\u0000"+
		"\u0000\u0001\u000e\u0001\u0000\u0000\u0000\u0001\u0010\u0001\u0000\u0000"+
		"\u0000\u0002\u0012\u0001\u0000\u0000\u0000\u0004\u001e\u0001\u0000\u0000"+
		"\u0000\u0006\"\u0001\u0000\u0000\u0000\b$\u0001\u0000\u0000\u0000\n)\u0001"+
		"\u0000\u0000\u0000\f+\u0001\u0000\u0000\u0000\u000e-\u0001\u0000\u0000"+
		"\u0000\u00103\u0001\u0000\u0000\u0000\u0012\u0016\u0005#\u0000\u0000\u0013"+
		"\u0015\t\u0000\u0000\u0000\u0014\u0013\u0001\u0000\u0000\u0000\u0015\u0018"+
		"\u0001\u0000\u0000\u0000\u0016\u0017\u0001\u0000\u0000\u0000\u0016\u0014"+
		"\u0001\u0000\u0000\u0000\u0017\u0019\u0001\u0000\u0000\u0000\u0018\u0016"+
		"\u0001\u0000\u0000\u0000\u0019\u001a\u0005\n\u0000\u0000\u001a\u001b\u0001"+
		"\u0000\u0000\u0000\u001b\u001c\u0006\u0000\u0000\u0000\u001c\u0003\u0001"+
		"\u0000\u0000\u0000\u001d\u001f\u0007\u0000\u0000\u0000\u001e\u001d\u0001"+
		"\u0000\u0000\u0000\u001e\u001f\u0001\u0000\u0000\u0000\u001f \u0001\u0000"+
		"\u0000\u0000 !\u0007\u0001\u0000\u0000!\u0005\u0001\u0000\u0000\u0000"+
		"\"#\b\u0001\u0000\u0000#\u0007\u0001\u0000\u0000\u0000$%\u0005-\u0000"+
		"\u0000%&\u0005-\u0000\u0000&\'\u0001\u0000\u0000\u0000\'(\u0006\u0003"+
		"\u0001\u0000(\t\u0001\u0000\u0000\u0000)*\u0005[\u0000\u0000*\u000b\u0001"+
		"\u0000\u0000\u0000+,\u0005]\u0000\u0000,\r\u0001\u0000\u0000\u0000-.\u0005"+
		"=\u0000\u0000./\u0005=\u0000\u0000/0\u0001\u0000\u0000\u000001\u0006\u0006"+
		"\u0002\u00001\u000f\u0001\u0000\u0000\u000024\u0005-\u0000\u000032\u0001"+
		"\u0000\u0000\u000034\u0001\u0000\u0000\u000046\u0001\u0000\u0000\u0000"+
		"57\u0007\u0002\u0000\u000065\u0001\u0000\u0000\u000078\u0001\u0000\u0000"+
		"\u000086\u0001\u0000\u0000\u000089\u0001\u0000\u0000\u00009\u0011\u0001"+
		"\u0000\u0000\u0000\u0006\u0000\u0001\u0016\u001e38\u0003\u0006\u0000\u0000"+
		"\u0002\u0001\u0000\u0002\u0000\u0000";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}