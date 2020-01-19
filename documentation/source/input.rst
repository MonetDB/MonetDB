***********************
Giving input to MonetDB
***********************

This chapter discusses what kinds of input MonetDB accepts as syntactically
correct.

Literals
========


Strings
-------

String literals are denoted by enclosing a sequence of UTF-8 characters between
single quotes: ``'``. MonetDB allows the use of different quote sequences that
control the interpretation of the contents of the string.

.. note:: The quote sequences are *not* case sensitive, i.e. the strings
   ``E'abc'`` and ``e'abc'``, are exactly the same.

The available modes
are as follows:

Enhanced strings

  Enhanced strings are enclosed between ``E'`` and ``'``. Within them various
  C-like escape sequences are valid::

    sql>SELECT E'Newline chars\nare interpreted like in C-strings';
    +-------------------------------------------------+
    | single_value                                    |
    +=================================================+
    | Newline chars                                   |
    : are interpreted like in C-strings               :
    +-------------------------------------------------+
    1 tuple

  .. warning::
    By default MonetDB interprets strings in this mode, i.e. if you use just
    single quotes to enclose a string it will be interpreted like this.

Raw strings

  Raw strings are enclosed between the lexemes ``R'`` and ``'``. In this mode
  all characters are interpreted literally except for the single quote character
  (``'``) that needs to be escaped by writing it twice. This mode is what the
  SQL specifies as strings::

    sql>SELECT R'Backslash doesn''t have a special meaning here: \n';
    +---------------------------------------------------+
    | single_value                                      |
    +===================================================+
    | Backslash doesn't have a special meaning here: \n |
    +---------------------------------------------------+
    1 tuple

Blobs
  Strings enclosed in ``X'`` and ``'``, are intended for input of binary
  blobs. Only hexadecimal digits (case insensitive) are allowed in this mode,
  and the string must have an even number of characters. Every hexadecimal digit
  pair is interpreted as one byte::

   sql>SELECT X'12EEff';
   +---------------------------------------------------+
   | single_value                                      |
   +===================================================+
   | 12EEFF                                            |
   +---------------------------------------------------+
   1 tuple

   sql>SELECT X'1';
   incorrect blob 1 in: "select X'1';"

Unicode sequences

  Strings enclosed between ``U&'`` and ``'``, are interpreted as unicode
  sequences::

   sql>select U&'\000a';
   +--------------+
   | single_value |
   +==============+
   |              |
   +--------------+
   1 tuple
   sql>select U&'\0061';
   +--------------+
   | single_value |
   +==============+
   | a            |
   +--------------+
   1 tuple
   sql>select U&'\00a';
   Bad Unicode string in: "select U&'\00a';"
   sql>select U&'\00oa';
   Bad Unicode string in: "select U&'\00oa';"


Numbers
-------

Comments
--------

You can write comments in three different ways:

SQL line comments

  These start with two hyphens: ``--`` and extend to the end of the line.

Python line comments

  Anything between the hash character ``#`` and the end of the line is ignored
  by MonetDB.

C block comments

  MonetDB also ignores anything that is written between the lexemes ``/*`` and
  ``*/``.

Identifiers
===========
