parser grammar Zetatest;

/* Grammar for parsing a ZetaSQL test */

options { tokenVocab = Zetalexer; }

tests:   test (EQUAL test)* EQUAL? NEWLINE? EOF;
test:    macro* query ((DASHDASH|RESULT_DASHDASH) result)+;

/* Query part: parse whole lines */
macro: LEFT_BRACKET NB* RIGHT_BRACKET;
query: line (NEWLINE line)*;
line: (NB|LEFT_BRACKET|RIGHT_BRACKET)* ;

/* Result part: parse in detail */
result:     feature* typedvalue;
feature: FeatureDescription;
typedvalue: sqltype sqlvalue
          | error
          | valueNotSpecified
          ;

error: ERROR line (NEWLINE line)*;
valueNotSpecified : UpdateTheTestOutput ;

sqltype: arraytype
       | structtype
       | datetype
       | int64type
       | uint64type
       | int32type
       | uint32type
       | booltype
       | doubletype
       | floattype
       | stringtype
       | bytestype
       | timestamptype
       | enumtype
       | numerictype
       | bignumerictype
       | timetype
       | datetimetype
       | geographytype
       | prototype
       | intervaltype
       ;

bytestype: BYTES ;
floattype: FLOAT ;
doubletype: DOUBLE ;
booltype: BOOL ;
int64type: INT64 ;
uint64type: UINT64 ;
int32type: INT32 ;
uint32type: UINT32 ;
stringtype: STRING ;
numerictype: NUMERIC ;
timetype: TIME ;
datetimetype: DATETIME ;
intervaltype: INTERVAL ;
geographytype: GEOGRAPHY ;
bignumerictype: BIGNUMERIC ;
enumtype: ENUM LESS names GREATER ;
timestamptype: TIMESTAMP ;
structtype: STRUCT LESS fields GREATER ;
prototype: PROTO LESS names GREATER ;
fields: (optNamedSqlType (COMMA optNamedSqlType)*)? ;
arraytype: ARRAY LESS sqltype? GREATER ;
datetype: DATE ;
names: ID (DOT ID)*;

sqlvalue: intvalue
     | floatvalue
     | datevalue
     | arrayvalue
     | structvalue
     | boolvalue
     | nullvalue
     | stringvalue
     | bytesvalue
     | timestampvalue
     | enumvalue
     | timevalue
     | datetimevalue
     | st_pointvalue
     | protovalue
     | intervalvalue
     ;

optNamedSqlType: (ID|NULL|sqltype)? sqltype ;
arrayvalue: arraytype? L_BRACKET sqlvalue? (COMMA sqlvalue)* R_BRACKET
          | arraytype L_PARENS NULL R_PARENS
          ;
number: intvalue
      | floatvalue
      ;

protofieldvalue: (ID COLON)? sqlvalue
               | L_BRACKET names R_BRACKET COLON sqlvalue
               ;
protovalue: (L_BRACKET names R_BRACKET)? L_BRACE protofieldvalue* R_BRACE ;
structvalue: L_BRACE (sqlvalue (COMMA sqlvalue)*)? R_BRACE ;
datevalue: DATEVALUE ;
intvalue: DASH? INT;
boolvalue: FALSE | TRUE ;
stringvalue: StringLiteral ;
nullvalue: NULL ;
floatvalue: '-'? FloatingPointLiteral ;
bytesvalue: BytesLiteral ;
timestampvalue: TimestampLiteral ;
datetimevalue: DATEVALUE TIMEVALUE;
timevalue: TIMEVALUE;
st_pointvalue: POINT L_PARENS number number R_PARENS;
intervalvalue: INTPAIR '-'? INT (INTERVALTIMEVALUE|TIMEVALUE);
enumvalue: ID ;