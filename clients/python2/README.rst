.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.

==========================================
The MonetDB MAPI and SQL client python API
==========================================


Introduction
============

This is the new native python client API.  This API is cross-platform,
and doesn't depend on any monetdb libraries.  It has support for
python 2.5+ and is Python DBAPI 2.0 compatible.


Installation
============

To install the MonetDB python API run the following command from the
python source directory::

 # python setup.py install

That's all, now you are ready to start using the API.


Documentation
=============

The python code is well documented, so if you need to find
documentation you should have a look at the source code.  Below is an
interactive example on how to use the monetdb SQL API which should get
you started quite fast.


Examples
========

There are some examples in the 'examples' folder, but here are is a
line by line example of the SQL API::

 > # import the SQL module
 > import monetdb.sql
 > 
 > # set up a connection. arguments below are the defaults
 > connection = monetdb.sql.connect(username="monetdb", password="monetdb", hostname="localhost", database="demo")
 > 
 > # create a cursor
 > cursor = connection.cursor()
 > 
 > # increase the rows fetched to increase performance (optional)
 > cursor.arraysize = 100
 >
 > # execute a query (return the number of rows to fetch)
 > cursor.execute('SELECT * FROM tables')
 26
 >
 > # fetch only one row
 > cursor.fetchone()
 [1062, 'schemas', 1061, None, 0, True, 0, 0]
 >
 > # fetch the remaining rows
 > cursor.fetchall()
 [[1067, 'types', 1061, None, 0, True, 0, 0],
  [1076, 'functions', 1061, None, 0, True, 0, 0],
  [1085, 'args', 1061, None, 0, True, 0, 0],
  [1093, 'sequences', 1061, None, 0, True, 0, 0],
  [1103, 'dependencies', 1061, None, 0, True, 0, 0],
  [1107, 'connections', 1061, None, 0, True, 0, 0],
  [1116, '_tables', 1061, None, 0, True, 0, 0],
  ...
  [4141, 'user_role', 1061, None, 0, True, 0, 0],
  [4144, 'auths', 1061, None, 0, True, 0, 0],
  [4148, 'privileges', 1061, None, 0, True, 0, 0]]
 >
 > # Show the table meta data
 > cursor.description 
 [('id', 'int', 4, 4, None, None, None),
  ('name', 'varchar', 12, 12, None, None, None),
  ('schema_id', 'int', 4, 4, None, None, None),
  ('query', 'varchar', 168, 168, None, None, None),
  ('type', 'smallint', 1, 1, None, None, None),
  ('system', 'boolean', 5, 5, None, None, None),
  ('commit_action', 'smallint', 1, 1, None, None, None),
  ('temporary', 'tinyint', 1, 1, None, None, None)]

 
If you would like to communicate with the database at a lower level
you can use the MAPI library::

 > from monetdb import mapi
 > server = mapi.Server()
 > server.connect(hostname="localhost", port=50000, username="monetdb", password="monetdb", database="demo", language="sql")
 > server.cmd("sSELECT * FROM tables;")
 ...

