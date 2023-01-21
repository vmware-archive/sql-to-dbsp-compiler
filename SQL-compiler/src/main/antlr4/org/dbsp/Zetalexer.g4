lexer grammar Zetalexer;

LINE_COMMENT: '#' .*? '\n' -> skip;
NEWLINE : [\r]? [\n];
CHAR    : ~ '\n';
DASHDASH   : '--' NEWLINE -> mode(RESULT);
OPEN_BRACKET : '[' ;
CLOSED_BRACKET : ']' ;

mode RESULT;
EQUAL   : '==' '\r'? '\n' -> mode(DEFAULT_MODE);
INT   : DecimalNumeral;
RESULT_DASHDASH : '--' NEWLINE;
WS: [\r\t\n ] -> skip;
WITH: 'WITH FEATURES:';
INT64: 'INT64';
INT32: 'INT32';
UINT32: 'UINT32';
UINT64: 'UINT64';
DOUBLE: 'DOUBLE';
FLOAT: 'FLOAT';
STRUCT: 'STRUCT';
ARRAY: 'ARRAY';
DATE: 'DATE';
BOOL: 'BOOL';
NULL: 'NULL';
BYTES: 'BYTES';
STRING: 'STRING';
TIMESTAMP: 'TIMESTAMP';
TIME: 'TIME';
NUMERIC: 'NUMERIC';
GEOGRAPHY: 'GEOGRAPHY';
DATETIME: 'DATETIME';
BIGNUMERIC: 'BIGNUMERIC';
ENUM: 'ENUM';
DOT: '.' ;
LESS: '<';
GREATER: '>';
COMMA: ',';
DASH: '-';
L_BRACKET: '[';
R_BRACKET: ']';
L_BRACE: '{';
R_BRACE: '}';
L_PARENS: '(';
R_PARENS: ')';
FALSE: 'false';
TRUE: 'true';
ERROR: 'ERROR:' -> mode(DEFAULT_MODE);
POINT: 'POINT';
FeatureDescription: 'WITH FEATURES:' .*? '\n';
fragment D: [0-9];
DATEVALUE: D D D D '-' D D '-' D D;
TIMEVALUE: D D ':' D D ':' D D ('.' D+)?;
TimestampLiteral: DATEVALUE ' ' TIMEVALUE '+' D D;

// Most of the stuff below is lifted from the Java grammar and lexer at
// https://github.com/antlr/grammars-v4/blob/master/java/java8/Java8Lexer.g4

fragment
DecimalNumeral
	:	'0'
	|	NonZeroDigit Digits?
	;

fragment
Digit
	:	'0'
	|	NonZeroDigit
	;

fragment
NonZeroDigit
	:	[1-9]
	;

fragment
Digits
	:	Digit+
	;

FloatingPointLiteral
	:	DecimalFloatingPointLiteral
	| 'nan'
	| '-'? 'inf'
	;

fragment
DecimalFloatingPointLiteral
	:	Digits '.' Digits? ExponentPart?
	|	'.' Digits ExponentPart?
	|	Digits ExponentPart
	|	Digits
	;

fragment
ExponentPart
	:	ExponentIndicator SignedInteger
	;

fragment
ExponentIndicator
	:	[eE]
	;

fragment
SignedInteger
	:	Sign? Digits
	;

fragment
Sign
	:	[+-]
	;

StringLiteral
	:	'"' StringCharacters? '"'
	;

fragment
StringCharacters
	:	StringCharacter+
	;

fragment
StringCharacter
	:	~["\\\r\n]
	|	EscapeSequence
	;

fragment
EscapeSequence
	:	'\\' [btnfr"'\\]
    |   UnicodeEscape
	;

fragment
UnicodeEscape
    :   '\\' 'u'+  HexDigit HexDigit HexDigit HexDigit
    ;

fragment
HexDigit
	:	[0-9a-fA-F]
	;

BytesLiteral
    : 'b' '\'' (~'\'')* '\''
    | 'b' StringLiteral
    ;

ID : [a-zA-Z_][a-zA-Z0-9_]* ;

