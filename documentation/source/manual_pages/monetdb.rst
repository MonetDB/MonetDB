NAME
====

monetdb - control a MonetDB Database Server instance

SYNOPSIS
========

**monetdb** [ *monetdb_options* ] *command* [ *command_options* ] [
*command_args* ]

DESCRIPTION
===========

*Monetdb* allows an administrator of the MonetDB Database Server to
perform various operations on the databases in the server. It relies on
*monetdbd*\ (1) running in the background for all operations.

OPTIONS
=======

*Monetdb_options* affect all commands and control the general behavior
of *monetdb*.

**-q**
   Suppresses all standard progress messages, only writing output to
   stderr if an error occurred.

**-h** *hostname*
   Connect to *hostname* instead of attempting a connection over the
   local UNIX socket. This allows *monetdb* to connect to a remote
   *monetdbd*\ (1). The use of this option requires **-P** (see below).
   If *hostname* starts with a forward slash (/), *hostname* is assumed
   to be the directory where the UNIX sockets are stored. In that case,
   the **-P** option is not allowed.

**-p** *port*
   Connects to the given portnumber instead of the default (50000).
   Requires **-h** to be given as option too.

**-P** *passphrase*
   Specifies the passphrase necessary to login to a remote
   *monetdbd*\ (1). This option requires -h to be given as well. A bad
   passphrase causes *monetdb* to fail to login, and hence fail to
   perform any remote action.

**-v**
   Show version, equal to **monetdb version**.

COMMANDS
========

The commands for the *monetdb* utility are **create**, **destroy**,
**lock**, **release**, **status**, **start**, **stop**, **kill**,
**profilerstart**, **profilerstop**, **snapshot**, **set**, **get**,
**inherit**, **discover**, **help**, and **version**. The commands
facilitate adding, removing, maintaining, starting and stopping a
database inside the MonetDB Database Server.

For all commands, database arguments can be glob-like expressions. This
allows to do wildcard matches. For details on the syntax, see
*EXPRESSIONS*.

**create** [**-m** *pattern*] [**-p** *password*\ **]**\ *database*\ **[**\ *database*\ **...]**
   Initializes a new database in the MonetDB Database Server. A database
   created with this command makes it available under its database name,
   but not yet for use by clients, as the database is put into
   maintenance mode. This allows the database administrator to perform
   initialization steps before releasing it to users, unless the **-p**
   argument is supplied. See also **monetdb lock**. The name of the
   database must match the expression [A-Za-z0-9-_]+.

   **-m**\ *pattern*
      With the **-m** flag, instead of creating a database, a
      multiplex-funnel is created. See section *MULTIPLEX-FUNNEL* in
      *monetdbd*\ (1). The pattern argument is not fully the same as a
      pattern for connecting or discovery. Each parallel target for the
      multiplex-funnel is given as
      *username*\ **+**\ *password*\ **@**\ *pattern* sequence,
      separated by commas. Here the *pattern* is an ordinary pattern as
      would be used for connecting to a database, and can hence also be
      just the name of a database.

   **-p**\ *password*
      The **-p** flag allows to create a database with the given
      password for the monetdb user. Since this protects the database
      from being accessed via well-known credentials, the created
      database is not locked after creation. This way, a new database
      can be created and used right away using the password supplied.

**destroy [-f]**\ *database*\ **[**\ *database*\ **...]**
   Removes the given *database*, including all its data and logfiles.
   Once destroy has completed, all data is lost. Be careful when using
   this command.

   **-f**
      By default, a confirmation question is asked, however the **-f**
      option, when provided, suppresses this question and removal is
      executed right away. Note that without this option you cannot
      destroy a running database, bring it down first using the **stop**
      command.

**lock**\ *database*\ **[**\ *database*\ **...]**
   Puts the given database in maintenance mode. A database under
   maintenance can only be connected to by an administrator account (by
   default the *monetdb* account). A database which is under maintenance
   is not started automatically by *monetdbd*\ (1), the MonetDB Database
   Server, when clients request for it. Use the **release** command to
   bring the database back for normal usage. To start a database which
   is under maintenance for administrator access, the **start** command
   can be used.

**release**\ *database*\ **[**\ *database*\ **...]**
   Brings back a database from maintenance mode. A released database is
   available again for normal use by any client, and is started on
   demand. Use the **lock** command to take a database under
   maintenance.

**status [-lc] [-s**\ *states*\ **] [**\ *database*\ **...]**
   Shows the state of the given database, or, when none given, all known
   databases. Three modes control the level of detail in the displayed
   output. By default a condensed one-line output per database format is
   used. This output resembles pretty much the output of various
   *xxxstat* programs, and is ideal for quickly gaining an overview of
   the system state. The output is divided into four columns, **name**,
   **state**, **health**, and **remarks**. The **state** column contains
   two characters that identify the state of the database, based on
   Booting (starting up), Running, Stopped, Crashed and Locked (under
   maintenance). This is followed by the uptime when running. The
   **health** column contains the percentage of successful starts and
   stops, followed by the average uptime. The **remarks** column can
   contain arbitrary information about the database state, but usually
   contains the URI the database can be connected to.

   **-c**
      The **-c** flag shows the most used properties of a database. This
      includes the state of the database (running, crashed, stopped),
      whether it is under maintenance or not, the crash averages and
      uptime statistics. The crash average is the number of times the
      database has crashed over the last 1, 15 or 30 starts. The lower
      the average, the healthier the database is.

   **-l**
      Triggered by the **-l** flag, a long listing is used. This listing
      spans many rows with on each row one property and its value
      separated by a colon (**:**). The long listing includes all
      information that is available.

   **-s**
      The **-s** flag controls which databases are being shown, matching
      their state. The required argument to this flag can be a
      combination of any of the following characters. Note that the
      order in which they are put also controls the order in which the
      databases are printed. **b**, **r**, **s**, **c**, and **l** are
      used to print a starting up (booting), started (running), stopped,
      crashed and locked database respectively. The default order which
      is used when the **-s** flag is absent, is **rbscl.**

**start [-a]**\ *database*\ **[**\ *database*\ **...]**

**stop [-a]**\ *database*\ **[**\ *database*\ **...]**

**kill [-a]**\ *database*\ **[**\ *database*\ **...]**

Starts, stops or kills the given database, or, when **-a** is supplied,
all known databases. The **kill** command immediately terminates the
database by sending the SIGKILL signal. Any data that hasn't been
committed will be lost. This command should only be used as last resort
for a database that doesn't respond any more. It is more common to use
the **stop** command to stop a database. This will first attempt to stop
the database, waiting for **exittimeout** seconds and if that fails,
kill the database. When using the **start** command, *monetdb*\ (1) will
output diagnostic messages if the requested action failed. When
encountering an error, one should always consult the logfile of
*monetdbd*\ (1) for more details. For the **kill** command a diagnostic
message indicating the database has crashed is always emitted, due to
the nature of that command. Note that in combination with **-a** the
return code of *monetdb*\ (1) indicates failure if one of the databases
had a failure, even though the operation on other databases was
successful.

**profilerstart**\ *database*\ **[**\ *database*\ **...]**

**profilerstop**\ *database*\ **[**\ *database*\ **...]**

Starts or stops the collection of profiling logs for the given database.
The property **profilerlogpath** must be set for the given database, and
it should point to a directory where the logs will be gathered. The
filenames of the logs have the format:
*proflog_<database>_YYYY-MM-DD_HH-MM-SS.json* where the last part is the
date and time when the collection started. Please note that a file
recording the pid of the profiler is written in the log directory,
therefore each database needs to have a different **profilerlogpath**
value.

**monetdb snapshot write**\ *dbname*
   Takes a snapshot of the given database and writes it to stdout.

**monetdb snapshot create [-t**\ *targetfile*\ **]**\ *dbname*\ **[**\ *dbname*\ **..]**
   Takes a snapshot of the given databases. Here, *dbname* can be either
   the name of a single database or a pattern such as *staging\**
   indicating multiple databases to snapshot. Unless **-t** is given,
   the snapshots are written to files named
   *<snapshotdir>/<dbname>_<YYYY><MM><DD>T<HH><MM>UTC<snapshotcompression>*
   where *snapshotdir* is a *monetdbd* setting that has to be configured
   explicitly using **monetdbd set** and *snapshotcompression* is
   another **monetdbd** setting which defaults to *.tar.lz4* or *.tar*.
   If **-t** is given, only a single database can be snapshotted and the
   snapshot is written to *targetfile*, a file on the server which must
   be somewhere under *snapshotdir* but which does not have to follow
   any particular naming convention.

**monetdb snapshot list [**\ *dbname*\ **..]**
   Lists the snapshots for the given databases, or all databases if none
   is given, showing the snapshot id, the time the snapshot was taken
   and the (compressed) size of the snapshot file. Only snapshots
   following the naming convention described under **monetdb snapshot
   create** are listed. The snapshot id is of the form
   *dbname*\ **@**\ *tag* where the tags are numbers starting at 1 for
   the most recent snapshot of a database, 2 for the next most recent,
   etc. For clarity, the first snapshot for each database shows the full
   snapshot id (*dbname*\ **@1) and** older snapshots for the same
   database are listed just as @2, @3, etc.

**monetdb snapshot restore [-f]**\ *snapshotid*\ **[**\ *dbname*\ **]**
   Restores a database from the given snapshot, where *snapshotid* is
   either a path on the server or *name*\ **@**\ *tag*\ **as listed by**
   **monetdb snapshot** **list.** The optional *dbname* argument sets
   the name of the newly created database. It can be omitted unless
   *snapshotid* is a full path. When **-f** is given, no confirmation is
   asked when overwriting an existing database.

**monetdb snapshot destroy [-f]**\ *name*\ **@**\ *tag*\ **..**
   Delete the listed snapshots from the *snapshotdir* directory. When
   **-f** is given, no confirmation is asked.

**monetdb snapshot destroy [-f] -r**\ *N*\ *dbname*\ **..**
   Delete all but the *N* latest snapshots for the given databases.
   Again, *dbname* can be a pattern such as *staging\** or even *\** to
   work on all snapshotted databases. When **-f** is given, no
   confirmation is asked.

**get <all \|**\ *property*\ **[,**\ *property*\ **[,..]]> [**\ *database*\ **...]**
   Prints the requested properties, or all known properties, for the
   given database. For each property its source and value are printed.
   Source indicates where the current value comes from, e.g. the
   configuration file, or a local override.

**set**\ *property*\ **=**\ *value*\ *database*\ **[**\ *database*\ **...]**
   Sets property to value for the given database. For a list of
   properties, run **monetdb get all**. Most properties require the
   database to be stopped when set.

   **shared=<yes|no\|**\ *tag*\ **>**
      Defines if and how the database is being announced to other
      monetdbds or not. If not set to **yes** or **no** the database is
      simply announced or not. Using a string, called *tag* the database
      is shared using that tag, allowing for more sophisticated usage.
      For information about the tag format and use, see section *REMOTE
      DATABASES* in the *monetdbd*\ (1) manpage. Note that this property
      can be set for a running database, and that a change takes
      immediate effect in the network.

   **nthreads=**\ *number*
      Defines how many worker threads the server should use to perform
      main processing. Normally, this number equals the number of
      available CPU cores in the system. Reducing this number forces the
      server to use less parallelism when executing queries, or none at
      all if set to **1**.

   **optpipe=**\ *string*
      Each server operates with a given optimizer pipeline. While the
      default usually is the best setting, for some experimental uses
      the pipeline can be changed. See the *mserver5*\ (1) manpage for
      available pipelines. Changing this setting is discouraged at all
      times.

   **readonly=**\ <**yes**\ \|\ **no**>
      Defines if the database has to be started in readonly mode.
      Updates are rejected in this mode, and the server employs some
      read-only optimizations that can lead to improved performance.

   **nclients=**\ *number*
      Sets the maximum amount of clients that can connect to this
      database at the same time. Setting this to a high value is
      discouraged. A multiplex-funnel may be more performant, see
      *MULTIPLEX-FUNNEL* below.

   **raw_strings=**\ <**yes**\ \|\ **no**>
      Defines how the server interprets literal strings. See the
      *mserver5*\ (1) manpage for more details.

**inherit**\ *property*\ *database*\ **[**\ *database*\ **...]**
   Like set, but unsets the database-local value, and reverts to inherit
   from the default again.

**discover [**\ *expression*\ **]**
   Returns a list of remote monetdbds and database URIs that were
   discovered by *monetdbd*\ (1). All databases listed can be connected
   to via the local MonetDB Database Server as if it were local
   databases using their database name. The connection is redirected or
   proxied based on configuration settings. If *expression* is given,
   only those discovered databases are returned for which their URI
   matches the expression. The expression syntax is described in the
   section *EXPRESSIONS*. Next to database URIs the hostnames and ports
   for monetdbds that allow to be controlled remotely can be found in
   the discover list masked with an asterisk. These entries can easily
   be filtered out using an expression (e.g. "mapi:monetdb:*") if
   desired. The control entries come in handy when one wants to get an
   overview of available monetdbds in e.g. a local cluster. Note that
   for *monetdbd* to announce its control port, the *mero_controlport*
   setting for that *monetdbd* must be enabled in the configuration
   file.

**-h**

**help [**\ *command*\ **]**

Shows general help, or short help for a given command.

**-v**

**version**

Shows the version of the *monetdb* utility.

EXPRESSIONS
===========

For various options, typically database names, expressions can be used.
These expressions are limited shell-globbing like, where the \* in any
position is expanded to an arbitrary string. The \* can occur multiple
times in the expression, allowing for more advanced matches. Note that
the empty string also matches the \*, hence "de*mo" can return "demo" as
match. To match the literal '*' character, one has to escape it using a
backslash, e.g. "\*".

RETURN VALUE
============

The *monetdb* utility returns exit code **0** if it successfully
performed the requested command. An error caused by user input or
database state is indicated by exit code **1**. If an internal error in
the utility occurs, exit code **2** is returned.

SEE ALSO
========

*monetdbd*\ (1), *mserver5*\ (1)
