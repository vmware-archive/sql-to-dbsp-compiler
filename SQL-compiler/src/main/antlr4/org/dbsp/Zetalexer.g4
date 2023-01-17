lexer grammar Zetalexer;

LINE_COMMENT: '#' .*? '\n' -> skip;
NEWLINE : [\r]? [\n];
CHAR    : ~ '\n';
MINUS   : '--' -> mode(RESULT);
OPEN_BRACKET : '[' ;
CLOSED_BRACKET : ']' ;

mode RESULT;
EQUAL   : '==' -> mode(DEFAULT_MODE);
INT   : '-'? [0-9]+;
