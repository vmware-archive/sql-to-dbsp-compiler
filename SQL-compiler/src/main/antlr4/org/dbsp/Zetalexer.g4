lexer grammar Zetalexer;

LINE_COMMENT: '#' .*? '\n' -> skip;
NEWLINE : [\r]? [\n];
CHAR    : ~ '\n';
MINUS   : '--' NEWLINE -> mode(RESULT);
OPEN_BRACKET : '[' ;
CLOSED_BRACKET : ']' ;

mode RESULT;
EQUAL   : '==' '\r'? '\n' -> mode(DEFAULT_MODE);
INT   : [0-9]+;
WS: [\r\t\n ] -> skip;
INT64: 'INT64';
STRUCT: 'STRUCT';
ARRAY: 'ARRAY';
DATE: 'DATE';
BOOL: 'BOOL';
LESS: '<';
GREATER: '>';
COMMA: ',';
DASH: '-';
L_BRACKET: '[';
R_BRACKET: ']';
L_BRACE: '{';
R_BRACE: '}';
FALSE: 'false';
TRUE: 'true';
DATEVALUE: [0-9][0-9][0-9][0-9] '-' [0-9][0-9] '-' [0-9][0-9];


