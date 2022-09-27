# SQL to DBSP compiler

This repository holds the source code for a compiler translating SQL
view definitions into DBSP circuits.  DBSP is a framework for
implementing incremental, streaming, (and non-streaming) queries.
DBSP is implemented in Rust in the repository
https://github.com/vmware/database-stream-processor

The SQL compiler is based on the Apache Calcite compiler
infrastructure https://calcite.apache.org/

## Dependencies

You need Java 8 to build the compiler, and the maven (mvn) Java build program.
Maven will take care of installing all required Java dependencies.

The testing programs use sqllogictest -- see the (section on testing)[#testing]

## Running

To run the tests:

$> cd SQL-compiler
$> ./run-tests.sh

## Incremental view maintenance

The DBSP runtime is optimized for performing incremental view
maintenance.  In consequence, DBSP programs in SQL are expressed as
VIEWS, or *standing queries*.  A view is a function of one or more
tables and other views.

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
view definition.  The compiler generates a Rust library which
implements the query as a function: given the input data, it produces
the output data.

In the future the compiler generates a library which will
incrementally maintain the view `V` when presented with changes to
table `T`:

```
                                           table changes
                                                V
tables -----> SQL-to-DBSP compiler ------> DBSP circuit
views                                           V
                                           view changes
```

## Compiler architecture

Compilation proceeds in several stages:

- SQL statements are parsed using the calcite SQL parser (function `CalciteCompiler.compile`),
  generating an IR representation using the Calcite `SqlNode` data types
  We handle the following kinds of statements:
  - DDL statements such as `CREATE TABLE` which define inputs of the computation
  - DDL statements such as `CREATE VIEW` which define outputs of the computation
  - DML statements such as `INSERT INTO TABLE` which define insertions or deletions from inputs
- the SQL IR tree is validated, optimized, and converted to the Calcite `RelNode` representation
  (function CaciteCompiler.compile)
- The result of this stage is a `CalciteProgram` data structure, which packages together all the
  views that are being compiled (multiple views can be maintained simultaneously)
- The `CalciteToDBSPCompiler.compile` converts a `CalciteProgram` data structure into a `DBSPCircuit`
  data structure.
- The `circuit` can be serialized as Rust using the `ToRustString` visitor.
-

## Testing

### Unit tests

Unit tests are written using JUnit and test pointwise parts of the compiler.
They can be executed usign `mvn test`.

### SQL logic tests

One of the means of testing the compiler is using sqllogictests:
https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki.

We assume that the sqllogictest source tree is installed in ../sqllogictest
with respect to the root directory of the compiler project
(we only need the .test files).  One way the source tree can be obtained
is from the git mirror: https://github.com/gregrahn/sqllogictest.git

We have implemented a general-purpose parser and testing framework for
running SqlLogicTest programs, in the `org.dbsp.sqllogictest` package.
The framework parses SqlLogicTest files and creates an internal
representation of these files.  The files are executed by "test executors".

We have multiple executors:

#### The `NoExecutor` test executor

This executor does not really run any tests.  But it can still be used
by the test loading mechanism to check that we correctly parse all
SQL logic test files.

#### The `DBSPExecutor`

The model of SQLLogicTest has to be adapted for testing using DBSP.
Since DBSP is not a database, but a streaming system, some SQL
statements are ignored (e.g., `CREATE INDEX`) and some other
cannot be supported (e.g., `CREATE UNIQUE INDEX`).

* The DDL statements to create tables are compiled into definitions of
circuit inputs.
* The DML INSERT statements are converted into input-generating functions.
  In the absence of a database we cannot really execute statements that
  are supposed to fail, so we ignore such statements.
* Some DML statements like DELETE based on a WHERE clause cannot be compiled at all
* The queries are converted into DDL VIEW create statements, which are
compiled into circuits.
* The validation rules in SQLLogicTest are compiled into Rust functions that compare
the outputs of queries.

So a SqlLogicTest script is turned into multiple DBSP tests, each of
which creates a circuit, feeds it one input, reads the output, and
validates it, executing exactly one transaction.  No incremental or
streaming aspects are tested currently.

#### The `JDBC` executor (under construction).

This executor parallels the standard ODBC executor written in C by
sending the statements and queries to a database to be executed.  Any
database that supports JDBC and can handle the correct syntax of the
queries can be used.

#### The hybrid `DBSP_DB_Executor` (under construction)

This executor is a combination of the DBSP executor and the JDBC
executor, using a real database to store data in tables, but using
DBSP as a query engine.
