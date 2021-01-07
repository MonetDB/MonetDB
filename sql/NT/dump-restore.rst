.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

Dumping a MonetDB SQL database
------------------------------

Start MonetDB Server (msqlserver.bat) and MonetDB Client (mclient.bat)
as usual.

In the mclient shell, type the following three commands.  Type them
without any extra white space:

::
	>.../dump.sql
	\D
	>

That is, first a line consisting of a greater than symbol followed by
the absolute (!) path name of the file in which you want to store the
dump.  This will redirect future output to the file mentioned.  Then a
line consisting of just backslash-capital D.  This command does the
actual dump which is, because of the previous line, stored in the dump
file.  And finally a line with just a greater than symbol.  This
closes the file and redirects the output again to the screen.

[Note: if you don't use the absolute path name of a file, the file
will be created in the directory where the mclient was started.  On
Windows this is at the location where the program was installed, and
therefore using an absolute path name is highly recommended.]

It is also possible to dump from the command line.  Start a MonetDB
Server, and then issue the command:

::
	msqldump.bat -umonetdb > dump.sql

You will need to provide the password (monetdb).

This command will connect to the MonetDB Server and dump the database
into the file dump.sql in the current directory.


Restoring a MonetDB SQL database
--------------------------------

After having dumped the database per the preceding instructions, it is
possible to restore the database using the following commands.

Start MonetDB Server (msqlserver.bat) and MonetDB Client (mclient.bat)
as usual.

In the mclient shell, type the following command:

::
	<.../dump.sql

That is, a less than symbol followed by the absolute (!) path name of
the dump file that was produced using the dump instructions.   Again,
unless you use an absolute path name, the file name is relative to
where the mclient was started, which on Windows may not be where
you expect.

It is also possible to restore from the command line.  Start a MonetDB
Server, and then issue the command:

::
	mclient.bat -lsql -umonetdb < dump.sql

You will need to provide the password (monetdb).


Online documentation
--------------------

For more information visit web pages:
.. Dump-Restore Guide: https://www.monetdb.org/Documentation/UserGuide/DumpRestore
.. msqldump-man-page: https://www.monetdb.org/Documentation/msqldump-man-page
.. mclient-man-page: https://www.monetdb.org/Documentation/mclient-man-page
.. mserver5-man-page: https://www.monetdb.org/Documentation/mserver5-man-page

