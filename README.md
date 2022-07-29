# SQL to DBSP compiler

This repository holds the source code for a compiler translating SQL view definitions into DBSP circuits.
DBSP is implemented in Rust in the repository
https://github.com/vmware/database-stream-processor

The SQL compiler is based on the Apache Calcite compiler infrastructure https://calcite.apache.org/

## Dependencies

You need Java 8 to build the compiler, and the maven (mvn) Java build program.
Maven will take care of installing all required Java dependencies.

The testing programs use sqllogictest -- see the (section on testing)[#testing]

## Running

To run the tests:

$> cd SQL-compiler
$> ./run-tests.sh

## Incremental view maintenance

The DBSP runtime is optimized for performing incremental view maintenance.  In consequence,
DBSP programs in SQL are expressed as VIEWS, or *standing queries*.  A view is a function
of one or more tables and other views.

For example, the following query defines a view:

```SQL
CREATE VIEW V AS SELECT * FROM T WHERE T.age > 18
```

In order to interpret this query the compiler needs to have been given
a definition of table (or view) T.  The table T should be defined
using a SQL Data Definition Language (DDL) statement, e.g.:

```SQL
CREATE TABLE T
(
    name    VARCHAR,
    age     INT,
    present BOOLEAN
)
```

The compiler must be given the table definition first, and then the
view definition.  The compiler generates a library which will
incrementally maintain the view `V` when presented with changes to
table `T`.

```
                                           table changes
                                                V
tables -----> SQL-to-DBSP compiler ------> DBSP circuit
views                                           V
                                           view changes
```

## Compiler architecture

Compilation proceeds in several stages:

- the SQL DDL statements are parsed using the calcite SQL parser (function `CalciteCompiler.compile`),
  generating an IR representation using the Calcite `SqlNode` data types
- the SQL IR tree is validated, optimized, and converted to the Calcite IR representation using `RelNode`
  (function CaciteCompiler.compile)
- The result of this stage is a `CalciteProgram` data structure, which packages together the definition
  of all tables and views that are being compiled
- The `CalciteToDBSPCompiler.compile` converts a `CalciteProgram` data structure into a `DBSPCircuit`
  data structure.
- The `circuit.toRustString()` method of a circuit can be used to generate Rust.
- The CalciteToDBSPCompiler makes use of two additional compilers:
  - `ExpressionCompiler` converts Calcite row expressions (`RexNode`) into DBSP expressions (`DBSPExpression`)
  - `TypeCompiler` converts Calcite types (`RelDataType`) into DBSP types (`DBSPType`)

## Testing

One of the means of testing the compiler is using sqllogictests:
https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki.

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
* The validation rules in SQLLogicTest are compiled into Rust functions that compare
the outputs of queries.

So a SqlLogicTest script is turned into multiple DBSP tests, each of which creates
a circuit, feeds it one input, reads the output, and validates it, executing exactly
one transaction.  No incremental or streaming aspects are tested currently.
