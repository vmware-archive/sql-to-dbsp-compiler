# Testing the SQL compiler

This directory contains a Java implementation of the tools from
sqllogictest: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
for testing SQL programs.

We assume that the sqllogictest source tree is installed in ../sqllogictest
with respect to the root directory of the compiler project
(we only need the .test files).  One way the source tree can be obtained
is from the git mirror: https://github.com/gregrahn/sqllogictest.git

The model of SQLLogicTest has to be adapted for testing DBSP.
In SQLLogicTest a test is composed of a series of alternating SQL
DDL (data definition language) and DML statements (data modification language)
(CREATE TABLE, INSERT VALUES), and queries (SELECT).
The statements can fail in a test.

Since DBSP is not a database, but a streaming system, we have to turn around
this model. 
* The DDL statements to create tables are compiled into definitions of
circuit inputs.
* The DML INSERT statements are converted into input-generating functions.
  In the absence of a database we cannot really execute statements that  
  are supposed to fail.
* Some DML statements like DELETE based on a WHERE clause cannot be compiled at all
* The queries are converted into DDL VIEW create statements, which are
compiled into circuits.

So a SqlLogicTest script is turned into multiple DBSP tests, each of which creates
a circuit, feeds it one input, reads the output, and validates it, executing exactly
one transaction.  No incremental or streaming aspects are tested.