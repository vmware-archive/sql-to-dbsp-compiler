parser grammar Zetatest;

/* Grammar for parsing a ZetaSQL test */

options { tokenVocab = Zetalexer; }

tests:   test (EQUAL test)* NEWLINE? EOF;
test:    query MINUS result;

/* Query part: parse whole lines */
query: line (NEWLINE line)*;
line: CHAR* ;

/* Result part: parse in detail */
result:     typedvalue;
typedvalue: sqltype sqlvalue;
sqltype: arraytype
       | structtype
       | datetype
       | inttype
       | booltype
       ;

booltype: BOOL ;
inttype: INT64 ;
structtype: STRUCT LESS typeArguments GREATER ;
typeArguments: sqltype (COMMA sqltype)* ;
arraytype: ARRAY LESS sqltype GREATER ;
datetype: DATE ;

sqlvalue: intvalue
     | datevalue
     | arrayvalue
     | structvalue
     | boolvalue
     ;

arrayvalue: L_BRACKET sqlvalue (COMMA sqlvalue)* R_BRACKET ;
structvalue: L_BRACE sqlvalue (COMMA sqlvalue)* R_BRACE ;
datevalue: DATEVALUE ;
intvalue: DASH? INT;
boolvalue: FALSE | TRUE ;
