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

* ``jsonkeyarray.sql``
* ``jsonkeyarray.stable.out``
* ``jsonkeyarray.stable.err``

Running tests
-------------

Adding a new test
-----------------

Python tests API
----------------
