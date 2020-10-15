=======
MCLIENT
=======

NAME
====

mclient --- the MonetDB command-line tool

SYNOPSIS
========

| **mclient** [ *options* ] [ *file or database* [ *file* ... ] ]
| **mclient** **--help**

DESCRIPTION
===========

MonetDB is a database management system that is developed from a
main-memory perspective with use of a fully decomposed storage model,
automatic index management, extensibility of data types and search
accelerators, and an SQL front end.

*Mclient* is the command-line interface to the MonetDB server.

If the **--statement=**\ *query* (**-s** *query*) option is given, the
query is executed. If any files are listed after the options, queries
are read from the files and executed. The special filename **-** refers
to standard input. Note that if there is both a **--statement** option
and filename arguments, the query given with **--statement** is executed
first. If no **--statement** option is given and no files are specified
on the command line, *mclient* reads queries from standard input.

When reading from standard input, if standard input is a terminal or if
the **--interactive** (**-i**) option is given, *mclient* interprets
lines starting with **\\** (backslash) specially. See the section
BACKSLASH COMMANDS below.

Before *mclient* starts parsing command line options, it reads a
configuration file. If the environment variable **DOTMONETDBFILE** is
set and not empty, it reads the file pointed to by that variable. If set
but empty, no configuration file is read. If unset, *mclient* first
looks for a file *.monetdb* in the current working directory, and if
that doesn't exist, it looks for a file *monetdb* in the XDG
configuration directory (**$XDG_CONFIG_HOME** which defaults to
**$HOME/.config** if not set), and finally for a file *.monetdb* in the
current user's home directory. This file can contain defaults for the
flags **user**, **password**, **language**, **database**,
**save_history**, **format**, **host**, **port**, and **width**. For
example, an entry in a *.monetdb* file that sets the default language
for *mclient* to mal looks like this: **language=mal**. To disable
reading the *.monetdb* file, set the variable **DOTMONETDBFILE** to the
empty string in the environment.

OPTIONS
=======

General Options
---------------

**--help** (**-?**)
   Print usage information and exit.

**--version** (**-v**)
   Print version information and exit.

**--encoding=**\ *encoding* (**-E** *encoding*)
   Specify the character encoding of the input. The option applies to
   both the standard input of *mclient* and to the argument of the
   **--statement** (**-s**) option but not to the contents of files
   specified on the command line (except for **-** which refers to
   standard input) or files specified using the **\\<** command (those
   must be encoded using UTF-8). The default encoding is taken from the
   locale.

**--language=**\ *language* (**-l** *language*)
   Specify the query language. The following languages are recognized:
   **mal** and **sql**. A unique prefix suffices. When the
   **--language** option is omitted, the default of **sql** is assumed.

**--database=**\ *database* (**-d** *database*)
   Specify the name or URI of the database to connect to. The **-d** can
   be omitted if an equally named file does not exist in the current
   directory. As such, the first non-option argument will be interpreted
   as database to connect to if the argument does not exist as file.
   Valid URIs are as returned by \`monetdb discover`, see
   *monetdb*\ (1), and look like
   **mapi:monetdb://**\ *hostname*\ **:**\ *port*\ **/**\ *database*.

**--host=**\ *hostname* (**-h** *hostname*)
   Specify the name of the host on which the server runs (default:
   **localhost**). When the argument starts with a forward slash (/),
   host is assumed to be the directory where the UNIX sockets are stored
   for platforms where these are supported.

**--port=**\ *portnr* (**-p** *portnr*)
   Specify the portnumber of the server (default: **50000**).

**--interactive** (**-i**)
   When reading from standard input, interpret lines starting with
   **\\** (backslash) specially. See the section BACKSLASH COMMANDS
   below.

**--timer=**\ *timermode* (**-t** *timermode*)
   | The *timer* command controls the format of the time reported for
     queries. The default mode is **none** which turns off timing
     reporting. The timer mode **clock** reports the client-side
     wall-clock time ("**clk**") in a human-friendly format. The timer
     mode **performance** reports client-side wall-clock time
     ("**clk**") as well as detailed server-side timings, all in
     milliseconds (ms): the time to parse the SQL query, optimize the
     logical relational plan and create the initial physical (MAL) plan
     ("**sql**"); the time to optimize the physical (MAL) plan
     ("**opt**"); the time to execute the physical (MAL) plan
     ("**run**"). All timings are reported on stderr.
   | **Note** that the client-measured wall-clock time is reported per
     query **only** when options **--interactive** or **--echo** are
     used, because only then does *mclient* send individual lines
     (statements) of the SQL script to the server. Otherwise, when
     *mclient* sends the SQL script in large(r) batch(es), only the
     total wall-clock time per batch is measured and reported. The
     server-measured detailed performance timings are always measured
     and reported per query.

**--user=**\ *user* (**-u** *user*)
   Specify the user to connect as. If this flag is absent, the client
   will ask for a user name, unless a default was found in the
   *.monetdb* or **$DOTMONETDBFILE** file.

**--format=**\ *format* (**-f** *format*)
   Specify the output format. The possible values are **sql**,
   **expanded**, **x**, **csv**, **tab**, **raw**, **xml**, **trash**,
   and **rowcount**. **csv** is comma-separated values; **tab** is
   tab-separated values; **raw** is no special formatting (data is
   dumped the way the server sends it to the client); **sql** is a
   pretty format which is meant for human consumption where columns are
   clearly shown; **expanded** and **x** are synonyms and are another
   pretty format meant for human consumption where column values are
   printed in full and below each other; **xml** is a valid (in the XML
   sense) document; **trash** does not render any output, enabling
   performance measurements free of any output rendering/serialization
   costs; and **rowcount** is a variation on **trash** where only the
   number of affected rows is printed. In addition to plain **csv**, two
   other forms are possible. **csv=**\ *c* uses *c* as column separator;
   **csv+**\ *c* uses *c* as column separator and produces a single
   header line in addition to the data.

**--echo** (**-e**)
   Echo the query. Note that using this option slows down processing.

**--history** (**-H**)
   If compiled with the *readline*\ (3) library, load and save the
   command line history (default off).

**--log=**\ *logfile* (**-L** *logfile*)
   Save client/server interaction in the specified file.

**--statement=**\ *stmt* (**-s** *stmt*)
   Execute the specified query. The query is run before any queries from
   files specified on the command line are run.

**--timezone** (**-z**)
   Do not tell the client's timezone to the server.

**--Xdebug** (**-X**)
   Trace network interaction between *mclient* and the server.

**--pager=**\ *cmd* (**-\|** *cmd*)
   Send query output through the specified *cmd*. One *cmd* is started
   for each query. Note that the **\|** will have to be quoted or else
   the shell will interpret it.

SQL Options
-----------

**--null=**\ *nullstr* (**-n** *nullstr*)
   Set the string to be used as NULL representation when using the sql,
   csv, or tab output formats. If not used, NULL values are represented
   by the string "null" in the sql output format, and as the empty
   string in the csv and tab output formats. Note that an argument is
   required, so in order to use the empty string, use **-n ""** (with
   the space) or **--null=**.

**--autocommit** (**-a**)
   Switch autocommit mode off. By default, autocommit mode is on.

**--allow-remote** (**-R**)
   Allow remote content (URLs) in the **COPY INTO** *table* **FROM**
   *file* **ON CLIENT** ... query. Remote content is retrieved by
   *mclient*.

**--rows=**\ *nr* (**-r** *nr*)
   If specified, query results will be paged by an internal pager at the
   specified number of lines.

**--width=**\ *nr* (**-w** *nr*)
   Specify the width of the screen. The default is the (initial) width
   of the terminal.

**--dump** (**-D**)
   Create an SQL dump.

**--inserts** (**-N**)
   Use INSERT INTO statements instead of COPY INTO + CSV values when
   dumping the data of a table. This option can be used when trying to
   load data from MonetDB into another database, or when e.g. JDBC
   applications are used to reload the dump.

BACKSLASH COMMANDS
==================

General Commands
----------------

**\\?**
   Show a help message explaining the backslash commands.

**\\q**
   Exit *mclient*.

**\\<** *file*
   Read input from the named *file*.

**\\>** *file*
   Write output to the named *file*. If no *file* is specified, write to
   standard output.

**\\\|** *command*
   Pipe output to the given *command*. Each query is piped to a new
   invocation of the *command*. If no *command* is given, revert to
   writing output to standard output.

**\\h**
   Show the *readline*\ (3) history.

**\\L** *file*
   Log client/server interaction in the given *file*. If no *file* is
   specified, stop logging information.

**\\X**
   Trace what *mclient* is doing. This is mostly for debugging purposes.

**\\e**
   Echo the query in SQL formatting mode.

**\\f** *format*
   Use the specified *format* mode to format the output. Possible modes
   the same as for the **--format** (**-f**) option.

**\\w** *width*
   Set the maximum page width for rendering in the **sql** formatting
   mode. If *width* is **-1**, the page width is unlimited, when *width*
   is **0**, use the terminal width. If *width* is greater than **0**,
   use the given width.

**\\r** *rows*
   Use an internal pager using *rows* per page. If *rows* is **-1**,
   stop using the internal pager.

SQL Commands
------------

**\\D**
   Dump the complete database. This is equivalent to using the program
   *msqldump*\ (1).

**\\D** *table*
   Dump the given *table*.

**\\d**
   Alias for \\dvt.

**\\d[Stvsfn]+**
   List database objects of the given type. Multiple type specifiers can
   be used at the same time. The specifiers *S*, *t*, *v*, *s*, *f* and
   *n* stand for System, table, view, sequence, function and schema
   respectively. Note that *S* simply switches on viewing system catalog
   objects, which is orthogonal to the other specifiers.

**\\d[Stvsfn]+** *object*
   Describe the given *object* in the database using SQL statements that
   reconstruct the object. The same specifiers as above can be used,
   following the same rules. When no specifiers are given, **vt** is
   assumed. The object can be given with or without a schema, separated
   by a dot. The object name can contain the wildcard characters **\***
   and **\_** that represent zero or more, and exactly one character
   respectively. An object name is converted to lowercase, unless the
   object name is quoted by double quotes (**"**). Examples of this, are
   e.g. *\*.mytable*, *tabletype\**, or *"myschema.FOO"*. Note that
   wildcard characters do not work in quoted objects. Quoting follows
   SQL quoting rules. Arbitrary parts can be quoted, and two quotes
   following each other in a quoted string represent the quote itself.

**\\A**
   Enable auto commit mode.

**\\a**
   Disable auto commit mode.

EXAMPLES
========

Efficiently import data from a CSV (comma-separated values) file into a
table. The file must be readable by the server. *$file* is the absolute
path name of the file, *$table* is the name of the table, *$db* is the
name of the database.

::

 mclient -d $db -s "COPY INTO $table FROM '$file' USING DELIMITERS ',',E'\\n','\"'"

Efficiently import data from a CSV file into a table when the file is to
be read by *mclient* (e.g. the server has no access to the file).
*$file* is the (absolute or relative) path name of the file, *$table* is
the name of the table, *$db* is the name of the database.

::

 mclient -d $db -s "COPY INTO $table FROM STDIN USING DELIMITERS ',',E'\\n','\"'" - < $file

Note that in this latter case, if a count of records is supplied, it
should be at least as large as the number of records actually present in
the CSV file. This, because otherwise the remainder of the file will be
interpreted as SQL queries.

Another, easier method to have the client read the file content is as
follows::

 mclient -d $db -s "COPY INTO $table FROM '$file' ON CLIENT USING DELIMITERS ',',E'\\n',\"'"

In this case the value of *$file* can be a path name relative to the
directory in which *mclient* was started. If, in addition, the option
**--allow-remote** is passed to *mclient*, the *$file* in the above
query can also be a URL. It then has to have the form
*schema*\ **://**\ *string*\ **,** *e*.\ *g*.,
*https://www.example.org/dumpdata.csv*.

See https://www.monetdb.org/Documentation/Manuals/SQLreference/CopyInto
for more information about the COPY INTO query.

SEE ALSO
========

*msqldump*\ (1), *mserver5*\ (1)
