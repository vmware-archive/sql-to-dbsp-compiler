Date/time operations
====================

``DATE`` literals have the form ``DATE 'YYYY-MM-DD'``.

``TIME`` literals have the form ``TIME 'HH:MM:SS.FFF``, where the
fractional part is optional.

A timestamp contains both a date and a time.  ``TIMESTAMP`` literals
have the form ``TIMESTAMP 'YYYY-MM-DD HH:MM:SS.FFF'``, where the
fractional part is optional.

Values of type ``DATE``, ``TIME``, and ``TIMESTAMP`` can be compared
using ``=``, ``<>``, ``!=``, ``<``, ``>``, ``<=``, ``>=``, ``<=>``,
``BETWEEN``; the result is a Boolean.

The following arithmetic operations are supported:

.. list-table:: Arithmetic on date/time values
  :header-rows: 1

  * - operation
    - result type
    - Explanation
  * - ``DATE`` + ``INTEGER``
    - ``DATE``
    - add a number of days to a date
  * - ``DATE`` + ``INTERVAL``
    - ``TIMESTAMP``
    - add an interval to a date
  * - ``DATE`` +  ``TIME``
    - ``TIMESTAMP``
    - create a timestamp from parts
  * - ``INTERVAL`` + ``INTERVAL`` (of the same kind)
    - ``INTERVAL``
    - add two intervals; both must have the same type
  * - ``TIMESTAMP`` + ``INTERVAL``
    - ``TIMESTAMP``
    - Add an interval to a timestamp
  * - ``TIME`` + ``INTERVAL`` (short)
    - ``TIME``
    - Add an interval to a time
  * - ``-`` ``INTERVAL``
    - ``INTERVAL``
    - Negate an interval
  * - ``DATE`` - ``DATE``
    - ``INTERVAL`` (long)
    - Compute the interval between two dates
  * - ``DATE`` - ``INTEGER``
    - ``DATE``
    - Subtract a number of days from a date
  * - ``DATE`` - ``INTERVAL``
    - ``DATE``
    - Subtract an interval from a date
  * - ``TIME`` - ``TIME``
    - ``INTERVAL`` (short)
    - Compute the difference between two times
  * - ``TIME`` - ``INTERVAL`` (short)
    - ``TIME``
    - Subtract an interval from a time
  * - ``TIMESTAMP`` - ``INTERVAL``
    - ``TIMESTAMP``
    - Subtract an interval from a timestamp
  * - ``INTERVAL`` - ``INTERVAL`` (of the same kind)
    - ``INTERVAL``
    - Subtract two intervals
  * - ``TIMESTAMP`` - ``TIMESTAMP``
    - ``INTERVAL`` (long)
    - Subtract two timestamps, convert result into days
  * - ``INTERVAL`` * ``DOUBLE``
    - ``INTERVAL``
    - Multiply an interval by a scalar
  * - ``INTERVAL`` / ``DOUBLE``
    - ``INTERVAL``
    - Divide an interval by a scalar

The following are legal time units:

.. list-table:: Time units
  :header-rows: 1

  * - Time unit
    - Meaning
  * - ``MILLENNIUM``
    - A thousand years
  * - ``CENTURY``
    - A hundred years; a number between 1 and 10
  * - ``DECADE``
    - Ten years; a number between 1 and 10
  * - ``YEAR``
    - One year; can be positive or negative
  * - ``QUARTER``,
    - 1/4 of a year; a number between 1 and 4
  * - ``MONTH``
    - One month; a number between 1 and 12
  * - ``WEEK``
    - Seven days.  The definition of "week" is quite involved: ''The year's
      first week is the week containing the first Thursday of the year or
      either the week containing the 4th of January or either the week that
      begins between 29th of Dec. and 4th of Jan.''  The week number is
      thus a value between 0 and 53.
  * - ``DOY``
    - Day of year, a number between 1 and 366
  * - ``DOW``
    - Day of week, with Sunday being 1 and Saturday being 7
  * - ``ISODOW``
    - ISO day of the week, with Monday 1 and Sunday 7
  * - ``DAY``
    - A day within a month, a number between 1 and 31
  * - ``HOUR``
    - An hour within a day, a number between 0 and 23
  * - ``MINUTE``
    - A minute within an hour, a number between 0 and 59
  * - ``SECOND``
    - A second within a minute, a number between 0 and 59
  * - ``EPOCH``
    - Unix epoch timestamp in seconds since 1970/01/01.

The following operations are available on dates:

``EXTRACT(<unit> FROM date)`` where ``<unit>`` is a time unit, as
described above.  Result is always a ``BIGINT`` value.

The following abbreviations can be used as well:

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

``FLOOR(datetime TO <unit>)``, where ``<unit>`` is a time unit.

``CEIL(datetime TO <unit>)``, where ``<unit>`` is a time unit.

Important unsupported operations
--------------------------------

Since DBSP is a *deterministic* query engine, it cannot offer support
for any function that depends on the current time.  So the following
are *not* supported: ``LOCALTIME``, ``LOCALTIMESTAMP``,
``CURRENT_TIME``, ``CURRENT_DATE``, ``CURRENT_TIMESTAMP``.
