.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

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


Adding a new test
-----------------

Python tests API
----------------
