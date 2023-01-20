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
       | doubletype
       | stringtype
       | bytestype
       ;

bytestype: BYTES ;
doubletype: DOUBLE ;
booltype: BOOL ;
inttype: INT64 ;
stringtype: STRING ;
structtype: STRUCT LESS fields GREATER ;
fields: optNamedSqlType (COMMA optNamedSqlType)* ;
arraytype: ARRAY LESS sqltype? GREATER ;
datetype: DATE ;

sqlvalue: intvalue
     | floatvalue
     | datevalue
     | arrayvalue
     | structvalue
     | boolvalue
     | nullvalue
     | stringvalue
     | bytesvalue
     ;

optNamedSqlType: (ID|NULL)? sqltype ;
arrayvalue: arraytype? L_BRACKET sqlvalue (COMMA sqlvalue)* R_BRACKET
          | arraytype L_PARENS NULL R_PARENS
          ;
structvalue: L_BRACE sqlvalue (COMMA sqlvalue)* R_BRACE ;
datevalue: DATEVALUE ;
intvalue: DASH? INT;
boolvalue: FALSE | TRUE ;
stringvalue: StringLiteral ;
nullvalue: NULL ;
floatvalue: FloatingPointLiteral ;
bytesvalue: BytesLiteral ;