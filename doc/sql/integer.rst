Integer Operations
==================

There are four supported integer datatypes, ``TINYINT``, ``SMALLINT``,
``INTEGER``, and ``BIGINT``.  These are represented as two's
complement values, and computations on these types obey the standard
two's complement semantics, including overflow.

The legal operations are ``+`` (plus, unary and binary), ``-`` (minus,
unary and binary), ``*`` (multiplication), ``/`` (division), ``%``
(modulus).

Division or modulus by zero return ``NULL``.

SQL performs a range of implicit casts when operating on values with
different types.

TODO: document the implicit casts.

Predefined functions
--------------------

``ABS(value)`` return absolute value.
``MOD(left, right)`` same as ``left % right``.

Operations not supported
------------------------

Non-deterministic functions, such as ``RAND`` cannot be supported in
DBSP.