Supported Data Types
====================

The compiler supports the following SQL data types:

- ``BOOLEAN``, represented as a Boolean value
- ``TINYINT``, represented as an 8-bit signed integer using two's
  complement
- ``SMALLINT``, represented as a 16-bit signed integer using two's
  complement
- ``INTEGER``, or ``INT``, represented as a 32-bit signed integer,
  using two's complement
- ``BIGINT``, represented as a 64-bit signed integer, using two's
  complement
- ``DECIMAL(precision, scale)``, a high precision fixed-point type,
  with a precision (number of decimal digits after decimal point) and
  a scale (total number of decimall digits)
- ``NUMERIC``, a high-precision fixed-point type
- ``FLOAT``, an IEEE 32-bit floating point number
- ``DOUBLE``, an IEEE 64-bit floating point number
- ``VARCHAR(n)``, or ``CHAR(n)``, or ``CHARACTER(n)``, or ``CHARACTER
  VARYING(n)``, a string value with maximum fixed width
- ``NULL``, a type comprising only the ``NULL`` value
- ``INTERVAL``, a SQL interval.  Two types of intervals are supported:
  long intervals (comprising years and months), and short intervals,
  comprising days, hours, minutes, seconds.
- ``TIMESTAMP``, a SQL timestamp without a timezone.  A timestamp
  represents a value containing a date and a time, with a precision up
  to a millisecond.
- ``DATE``, a SQL date without a timezone.  A date represents a value
  containing a date (year, month, day).
- ``GEOMETRY``: geographic data type

A suffix of ``NULL`` or ``NOT NULL`` can be appended to a type name to
indicate the nullability.  A type with no suffix is not nullable by
default.
