grammar zetatest;

tests
   : // EMPTY
   | nonEmptyTests
   ;

nonEmptyTests
   : test
   | test equals nonEmptyTests
   ;

test: query dashes result
    ;

dashes
   : '--' '\n'
   ;

equals
   : '==' '\n'
   ;

lines: // EMPTY
   | line lines
   ;

line: .*? '\n'
   ;

result
   : lines
   ;

query
   : lines
   ;