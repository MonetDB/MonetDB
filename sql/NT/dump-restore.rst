.. The contents of this file are subject to the MonetDB Public License
.. Version 1.1 (the "License"); you may not use this file except in
.. compliance with the License. You may obtain a copy of the License at
.. http://www.monetdb.org/Legal/MonetDBLicense
..
.. Software distributed under the License is distributed on an "AS IS"
.. basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
.. License for the specific language governing rights and limitations
.. under the License.
..
.. The Original Code is the MonetDB Database System.
..
.. The Initial Developer of the Original Code is CWI.
.. Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
.. Copyright August 2008-2014 MonetDB B.V.
.. All Rights Reserved.

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
