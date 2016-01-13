.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

Dumping the SQL database
------------------------

Start the SQL Server and SQL Client as usual.

In the SQL Client, type the following three commands.  Type them
without any extra white space::

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
will be created in the directory where the SQL Client was started.  On
Windows this is at the location where the program was installed, and
therefore using an absolute path name is highly recommended.]

It is also possible to dump from the command line.  Start a SQL
Server, and then issue the command

::
	msqldump -umonetdb -Pmonetdb > dump.sql

This command will connect to the SQL Server and dump the database into
the file dump.sql in the current directory.

Restoring the SQL database
--------------------------

After having dumped the database per the preceding instructions, it is
possible to restore the database using the following commands.

Start the SQL Server and SQL Client as usual.

In the SQL Client, type the following command.

::
	<.../dump.sql

That is, a less than symbol followed by the absolute (!) path name of
the dump file that was produced using the dump instructions.

It is also possible to restore from the command line.  Start a SQL
Server, and then issue the command

::
	mclient -lsql -umonetdb -Pmonetdb < dump.sql
