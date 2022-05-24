# SQL to DBSP compiler

This repository holds the source code for a compiler translating SQL view definitions into DBSP circuits.
DBSP is implemented in Rust in the repository
https://github.com/vmware/database-stream-processor

The SQL compiler is based on the Apache Calcite compiler infrastructure https://calcite.apache.org/

## Incremental view maintenance

The DBSP runtime is optimized for performing incremental view maintenance.  In consequence,
DBSP programs in SQL are expressed as VIEWS, or *standing queries*.  A view is a function
of one or more tables and other views.

For example, the following query defines a view:

```SQL
CREATE VIEW V AS SELECT * FROM T WHERE T.age > 18
```

In order to interpret this query the compiler needs to have a definition of table (or view) T.
The table T should be defined using a SQL Data Definition Language (DDL) statement, e.g.:

```SQL
CREATE TABLE T
(
    name    VARCHAR,
    age     INT,
    present BOOLEAN
)
```

The compiler must be given the table definition first, and then the view definition.
The compiler generates a library which will incrementally maintain the view `V` when 
presented with changes to table `T`.

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

In the future we will insert an additional IR in the CalciteToDBSP compiler which will be 
used to incrementalize and optimize the generated circuits.

We will insert a new compilation IR 