========
MSQLDUMP
========

NAME
====

msqldump - dump a MonetDB/SQL database

SYNOPSIS
========

**msqldump** [ *options* ] [ *dbname* ]

DESCRIPTION
===========

MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, and an SQL front end.

*Msqldump* is the program to dump an MonetDB/SQL database. The dump can
be used to populate a new MonetDB/SQL database.

Before *msqldump* starts parsing command line options, it reads a
configuration file. If the environment variable **DOTMONETDBFILE** is
set and not empty, it reads the file pointed to by that variable. If set
but empty, no configuration file is read. If unset, *msqldump* first
looks for a file *.monetdb* in the current working directory, and if
that doesn't exist, it looks for a file *monetdb* in the XDG
configuration directory (**$XDG_CONFIG_HOME** which defaults to
**$HOME/.config** if not set), and finally for a file *.monetdb* in the
current user's home directory. This file can contain defaults for the
flags **user**, **password**, **host**, and **port**. To disable reading
the *.monetdb* file, set the variable **DOTMONETDBFILE** to the empty
string in the environment.

OPTIONS
=======

**--help** (**-?**)
   Print usage information and exit.

**--database=**\ *database* (**-d** *database*)
   Specify the name of the database to connect to. The **-d** can be
   omitted if it is the last option.

**--host=**\ *hostname* (**-h** *hostname*)
   Specify the name of the host on which the server runs (default:
   **localhost**).

**--port=**\ *portnr* (**-p** *portnr*)
   Specify the portnumber of the server (default: 50000).

**--user=**\ *user* (**-u** *user*)
   Specify the user to connect as. If this flag is absent, the client
   will ask for a user name.

**--describe** (**-D**)
   Only dump the database schema.

**--inserts** (**-N**)
   When dumping the table data, use INSERT INTO statements, rather than
   COPY INTO + CSV values. INSERT INTO statements are more portable, and
   necessary when the load of the dump is processed by e.g. a JDBC
   application.

**--noescape** (**-e**)
   When dumping the table data, use the NO ESCAPE option on the COPY
   INTO query.

**--functions** (**-f**)
   Only dump functions definitions.

**--table=**\ [\ *schema*\ **.**\ ]\ *table* (**-t** [\ *schema*\ **.**]\ *table*\ )
   Only dump the specified table. If *schema* is not specified, the
   user's current schema is used. When either *schema* or *table*
   contains percent characters, all tables matching the (SQL) search
   pattern are dumped.

**--output=**\ *filename* (**-o** *filename*\ )
   Write the dump to the specified file. If *filename* contains a
   recognized compression scheme suffix, the file will be compressed
   using that scheme.

**--outputdir=**\ *directory* (**-O** *directory*\ )
   Write the dump to the file **dump.sql** in the specified directory
   which is created if it does not exist. The data of the tables will be
   stored in separate CSV files in the directory. If the
   **--compression** option is used, the CSV files will be compressed
   using the specified compression scheme. If both the **--outputdir**
   option and the **--output** option are specified, the last one is
   used. The **--outputdir** option is not compatible with the
   **--inserts** option.

**--compression=**\ *extension* (**-x** *extension*\ )
   Compress the CSV files produced with the **--outputdir** option using
   the specified compression scheme. Depending on how the program is
   compiled, the following compression schemes are recognized: **gz**,
   **bz2**, **xz**, and **lz4**.

**--quiet** (**-q**)
   Don't print the welcome message.

**--Xdebug** (**-X**)
   Trace network interaction between *mclient* and the server.

SEE ALSO
========

*mclient*\ (1), *mserver5*\ (1)
