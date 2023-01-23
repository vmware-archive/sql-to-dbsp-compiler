parser grammar Zetatest;

/* Grammar for parsing a ZetaSQL test */

options { tokenVocab = Zetalexer; }

tests:   test (EQUAL test)* EQUAL? NEWLINE? EOF;
test:    query ((DASHDASH|RESULT_DASHDASH) result)+;

/* Query part: parse whole lines */
query: line (NEWLINE line)*;
line: CHAR* ;

error: ERROR line (NEWLINE line)*;

/* Result part: parse in detail */
result:     feature* typedvalue;
feature: FeatureDescription;
typedvalue: sqltype sqlvalue
          | error
          ;
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
geographytype: GEOGRAPHY ;
bignumerictype: BIGNUMERIC ;
enumtype: ENUM LESS ID (DOT ID)* GREATER ;
timestamptype: TIMESTAMP ;
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
     | timestampvalue
     | enumvalue
     | timevalue
     | datetimevalue
     | st_pointvalue
     ;

optNamedSqlType: (ID|NULL)? sqltype ;
arrayvalue: arraytype? L_BRACKET sqlvalue? (COMMA sqlvalue)* R_BRACKET
          | arraytype L_PARENS NULL R_PARENS
          ;
number: intvalue
      | floatvalue
      ;
structvalue: L_BRACE sqlvalue (COMMA sqlvalue)* R_BRACE ;
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
enumvalue: ID ;