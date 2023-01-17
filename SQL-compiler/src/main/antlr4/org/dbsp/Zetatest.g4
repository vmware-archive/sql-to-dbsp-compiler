parser grammar Zetatest;

options { tokenVocab = Zetalexer; }

tests:   test (EQUAL test)* NEWLINE? EOF;
test:    macros query MINUS NEWLINE result;

lines: line (NEWLINE line)*;

macros: macro*;
macro: OPEN_BRACKET line CLOSED_BRACKET;
query: lines NEWLINE;
result:     typedvalue;
typedvalue: lines;

line: CHAR* ;

/*
//typedvalue: sqltype sqlvalue;
sqltype: arraytype
       | structtype
       | datetype
       | inttype
       ;

inttype: 'INT64' ;
structtype: 'STRUCT' '<' typeArguments '>' ;
typeArguments: sqltype (',' sqltype)* ;
arraytype: 'ARRAY' '<' sqltype '>' ;
datetype: 'DATE' ;

sqlvalue: //intvalue
     //| datevalue
     | arrayvalue
     | structvalue
     ;

arrayvalue: '[' sqlvalue (',' sqlvalue)* ']' ;
structvalue: '{' sqlvalue (',' sqlvalue)* '}' ;
//datevalue: intvalue '-' intvalue '-' intvalue ;
*/


