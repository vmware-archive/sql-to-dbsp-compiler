Semantics of built-in SQL operations
====================================

Nullable types
--------------

A type is nullable if it can represent the ``NULL`` value.  For input
tables the nullability of a column is declared explicitly.  For
intermediate results and output views the compiler infers the
nullability of each column using type inference rules.

Most SQL operations are defined for nullable types.  Our compiler
follows the SQL standard in this respect.  Most operations (e.g.,
``+``), when applied a ``NULL`` operand will produce a ``NULL``
value.

Boolean Operations
------------------

We support the following Boolean operations: ``OR``, ``AND``, ``NOT``,
``IS FALSE``, ``IS NOT FALSE``, ``IS TRUE``, ``IS NOT TRUE``.  Notice
that not all Boolean operations produce ``NULL`` results when an
operand is ``NULL``.

Comparison Operations
---------------------

The following operations can take operands with multiple data types
but always return a Boolean value (sometimes nullable):

``=`` (equality test), ``<>`` (inequality test), ``>`` (greater than),
``<`` less than, ``>=`` (greater or equal), ``<=`` (less or equal),
``IS NULL`` (true if operand is ``NULL``), ``IS NOT NULL`` (true if
operand is not ``NULL``), ``<=>`` (result is not nullable; equality
check that treats ``NULL`` values as equal), ``IS DISTINCT FROM``
(result is not nullable; check if two values are not equal, treating
``NULL`` as equal), ``IS NOT DISTINCT FROM`` (result is not nullable;
check if two values are the same, treating ``NULL`` values as equal),
``BETWEEN ... AND`` (inclusive at both endpoints), ``NOT BETWEEN
... AND`` (not inclusive at either endpoint), ``IN`` (checks whether
value appears in a list or set), ``<OP> ANY`` (check if any of the
values in a set compares properly), ``<OP> ALL`` (check if all the
values in a set compare properly), ``EXISTS (query)`` (check whether
query results has at least one row), ``UNIQUE(query)`` (result of a
query contains no duplicates -- ignoring ``NULL`` values).

Integer Arithmetic
------------------

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

There are many built-in numeric functions as well, listed in
:ref:`predefined_functions`.

Decimal data type
-----------------

Our implementation of decimal supports 96 bits of precision.

Floating-point arithmetic
-------------------------

We support the standard IEEE floating point datatypes ``FLOAT`` and
``DOUBLE``, including all their special values: `NAN`, `INFINITY`.

TODO: How does NAN compare?

String operations
-----------------

``||`` is string concatenation.

Date/time operations
--------------------

``DATE`` literals have the form ``DATE 'YYYY-MM-DD'``.

``TIMESTAMP`` literals have the form ``TIMESTAMP 'YYYY-MM-DD
HH:MM:SS.FFF'``, where the fractional part is optional.

Values of type ``DATE`` or ``TIMESTAMP`` can be compared using `=`,
`<>`, `!=`, `<`, `>`, `<=`, `>=`, `<=>`, ``BETWEEN`.

``EXTRACT(<unit> FROM datetime)`` where ``<unit>`` is one of
``MILLENNIUM``, ``CENTURY``, ``DECADE``, ``YEAR``, ``QUARTER``,
``MONTH``, ``WEEK`` [#]_, ``DOY`` (day of year, between 1 and 366),
``DOW`` (day of week, with Sunday being 1 and Saturday being 7),
``ISODOW`` (ISO day of the week, with Monday 1 and Sunday 7), ``DAY``,
``HOUR``, ``MINUTE``, ``SECOND``, ``EPOCH`` (Unix epoch timestamp in
seconds since 1970/01/01).  Result is always a ``BIGINT`` value.

.. [#] Note that the definition of "week" is quite involved: ''The year's
  first week is the week containing the first Thursday of the year or
  either the week containing the 4th of January or either the week that
  begins between 29th of Dec. and 4th of Jan.''  The week number is
  thus a value between 0 and 53.

``YEAR(date)`` is an abbreviation for ``EXTRACT(YEAR FROM date)``.

``MONTH(date)`` is an abbreviation for ``EXTRACT(MONTH FROM date)``.

``DAYOFMONTH(date)`` is an abbreviation for ``EXTRACT(DAY FROM
date)``.

``DAYOFWEEK(date)`` is an abbreviation for ``EXTRACT(DOW FROM
date)``.

``HOUR(date)`` is an abbreviation for ``EXTRACT(HOUR FROM date)``.
For dates it always returns 0, since dates have no time component.

``MINUTE(date)`` is an abbreviation for ``EXTRACT(MINUTE FROM date)``.
For dates it always returns 0, since dates have no time component.

``SECOND(date)`` is an abbreviation for ``EXTRACT(SECOND FROM date)``.
For dates it always returns 0, since dates have no time component.

The following abbreviations

``FLOOR(datetime TO <unit>)``, where ``<unit>`` is as above.

``CEIL(datetime TO <unit>)``, where ``<unit>`` is as above.

Important unsupported operations
--------------------------------

Since DBSP is a *deterministic* query engine, it cannot offer support
for any function that depends on the current time.  So the following
are *not* supported: ``LOCALTIME``, ``LOCALTIMESTAMP``,
``CURRENT_TIME``, ``CURRENT_DATE``, ``CURRENT_TIMESTAMP``.
