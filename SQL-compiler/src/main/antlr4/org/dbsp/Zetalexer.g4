lexer grammar Zetalexer;

LINE_COMMENT: '#' .*? '\n' -> skip;
NEWLINE : [\r]? [\n];
CHAR    : ~ '\n';
MINUS   : '--' NEWLINE -> mode(RESULT);
OPEN_BRACKET : '[' ;
CLOSED_BRACKET : ']' ;

mode RESULT;
EQUAL   : '==' '\r'? '\n' -> mode(DEFAULT_MODE);
INT   : DecimalNumeral;
WS: [\r\t\n ] -> skip;
INT64: 'INT64';
DOUBLE: 'DOUBLE';
STRUCT: 'STRUCT';
ARRAY: 'ARRAY';
DATE: 'DATE';
BOOL: 'BOOL';
NULL: 'NULL';
BYTES: 'BYTES';
STRING: 'STRING';
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
DATEVALUE: [0-9][0-9][0-9][0-9] '-' [0-9][0-9] '-' [0-9][0-9];
ID : [a-zA-Z_][a-zA-Z0-9_]* ;

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
    |   UnicodeEscape // This is not in the spec but prevents having to preprocess the input
	;

// This is not in the spec but prevents having to preprocess the input
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