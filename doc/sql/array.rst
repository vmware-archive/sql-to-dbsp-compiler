Operations on arrays
====================

An array type can be created by applying the ``ARRAY`` suffix to
another type.  For example ``INT ARRAY`` is an array of integers.
Array indexes start from 1.  Array sizes are limited to 2^31 elements.
Array elements may be nullable types, e.g., ``INT ARRAY NULL``.
Multidimensional arrays are possible, e.g. ``VARCHAR ARRAY ARRAY``
is a two-dimensional array.

Predefined functions on array values
------------------------------------

.. list-table:: Predefined functions on array values
   :header-rows: 1

   * - Function
     - Description
   * - ``ARRAY`` ``[`` value [, value]* ``]``
     - creates an array from a list of expressions that evaluate to
       values of the same type.
   * - ``array[index]``
     - where ``index`` is an expression that evaluates to an integer,
       and ``array`` is an expression that evaluates to an array.
       Returns the element at the specified position.  If the index
       is out of bounds, the result is ``NULL``.
   * - ``CARDINALITY(array)``
     - Returns the size of the array expression (number of elements).
   * - ``ELEMENT(array)``
     - Returns the single element of an array.  If the array
       has zero elements, returns ``NULL``.  If the array has more
       than one element, it causes a runtime exception.  (In the
       future we should probably replace the runtime exception with
       a ``NULL`` result for this case.)
