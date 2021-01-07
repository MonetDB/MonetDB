.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

********************
Developer's Handbook
********************

Testing
=======

MonetDB uses a custom program ``Mtest.py`` to run tests. Each test is an SQL,
Python or MAL program along with its standard output and standard error streams.
``Mtest.py`` runs each of these programs, captures the standard output and the
standard error, and compares them to the *stable* streams. If any differences are
encountered then the test has failed.

Tests in the codebase
---------------------

Tests are usually ``.sql``, ``.py`` or ``.malC`` files and the stable streams
are stored in ``.stable.out`` and ``.stable.err`` files with the same name in
the same directory. For example in the directory ``sql/test/json/Tests`` we find
the files:

* ``jsonkeyarray.Bug-6858.sql``
* ``jsonkeyarray.Bug-6858.stable.out``
* ``jsonkeyarray.Bug-6858.stable.err``

These three files define the test ``jsonkeyarray.Bug-6858``.

In any directory with tests you will also find a file named ``All``. This is an
index of all the tests in the directory. If a test is not mentioned in the
``All`` file, it will not run.

Running tests
-------------

The `canonical` name of a test is its path relative to the root of the source
tree (``$src_root``), with the ``Tests`` component and the suffix removed. For
instance the above test's canonical name is
``sql/test/json/jsonkeyarray.Bug-6858``.

In order to run a single test from ``$src_root`` or any of its descendants, use
its canonical name as the argument of ``Mtest.py``::

  [$src_root]$ Mtest.py sql/test/json/jsonkeyarray.Bug-6858

Alternatively, from within the directory where the test is defined you can run
the test by giving the last component of its canonical name as the argument to
``Mtest.py``::

  [$src_root/sql/test/json/Tests/]$ Mtest.py jsonkeyarray.Bug-6858

Another way to run tests is running a number of them together using the
canonical name of a group. For example to run all the tests in the ``json``
group of the ``sql`` component use::

  [$src_root]$ Mtest.py sql/test/json/

This will run all the tests defined in the file
``$src_root/sql/test/json/Tests/All``.

The last way to run a group of tests is from within the
directory where they reside::

  [$src_root/sql/test/json/Tests]$ Mtest.py .


Adding sqllogic test
--------------------

See `<https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki/>`_ for detail information 
on how to structure sqllogic test if you desire to make one by hand. We have extended the 
sqllogic protocol a bit further::

    skipif <system>
    onlyif <system>

    statement (ok|ok rowcount|error) [arg]
    query (I|T|R)+ (nosort|rowsort|valuesort|python)? [arg]
          I: integer; T: text (string); R: real (decimal)
          nosort: do not sort
          rowsort: sort rows
          valuesort: sort individual values
          python some.python.function: run data through function (MonetDB extension)
    hash-threshold number
    halt

Alternatively ``.sql`` scripts can be converted to sqllogic tests (.test) with ``Mconvert.py``.
For example::

    $Mconvert.py  --auto <module>/Tests <convert_me>.sql

All new tests need to be placed in the appropriate test folder and their name respectively in the
index ``All`` file.

Python tests API
----------------

We are using ``pymonetdb`` client in our testing infrastructure heavily. All .py tests needs to log errors in ``stderror``
and exit abnormally if failure is present. To ease up writing testing scripts the ``SQLTestCase`` class from ``MonetDBtesting`` 
module can be utilized. Following is an example of the ``SQLTestCase`` API::

    from MonetDBtesting.sqltest import SQLTestCase

    from decimal import Decimal

    with SQLTestCase() as tc:
        # using default connection context
        tc.connect()
        # insert into non-existing table
        tc.execute('insert into foo values (888.42), (444.42);').assertFailed(err_code='42S02')
        tc.execute('create table foo (salary decimal(10,2));').assertSucceeded()
        tc.execute('insert into foo values (888.42), (444.42);').assertSucceeded().assertRowCount(2)
        tc.execute('select * from foo;').assertSucceeded().assertDataResultMatch([(Decimal('888.42'),), (Decimal('444.42'),)])

For more examples check out tests in ``sql/test/Users/Tests``.
