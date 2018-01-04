.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

How To Start with MonetDB
=========================

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.

This document helps you compile and install the MonetDB suite from
scratch on Unix-like systems (this includes of course Linux, but also
MacOS X and Cygwin).  This document is meant to be used when you want
to compile and install from Mercurial source.  When you use the prepared tar
balls, some of the steps described here should be skipped.

In case you prefer installing a pre-compiled binary distribution,
please check out `the binary distribution`__.

This document assumes that you are planning on compiling and
installing MonetDB on a Unix-like system (e.g., Linux, IRIX, Solaris,
AIX, Mac OS X/Darwin, or CYGWIN).  For compilation and installation on
a native Windows system (NT, 2000, XP) see the instructions in the
file `buildtools/doc/windowsbuild.rst`__.

__ http://dev.monetdb.org/downloads/
__ Windows-Installation.html

The Suite
---------

The following are the most important parts of the MonetDB software suite:

buildtools
	Tools used only for building the other parts of the suite.
	These tools are only needed when building from Mercurial.  When
	building from the source distribution (i.e. the tar balls),
	you do not need this.

common
	Fundamental libraries used in the other parts of the suite.

clients
	Libraries and programs to communicate with the server(s) that
	are part of the suite.

monetdb5
	The MAL-based server.  This can be used with and is
	recommended for SQL.

sql
	The SQL front-end built on top of MonetDB5.


Prerequisites
-------------

Mercurial
	You only need this if you are building directly from our version
	control system.  If you start with the source distribution from `our
	download page`__ you don't need Mercurial.

	You need to have a working Mercurial (hg) and clone the main
	repository from: http://dev.monetdb.org/hg

Python
	MonetDB uses Python (version 2.0.0 or better) during
	configuration of the software.  See http://www.python.org/ for
	more information.  (It must be admitted, version 2.0.0 is
	ancient and has not recently been tested, we currently use
	2.6 and newer.)

autoconf/automake/libtool
	MonetDB uses GNU autoconf__ (>= 2.60) and automake__ (>= 1.10)
	during the Bootstrap_ phase, and libtool__ (>= 1.5) during the
	Make_ phase.  autoconf and automake are not needed when you
	start with the source distribution.

iconv
	A macrofile `iconv.m4` is expected in `/usr/share/aclocal/`.
	On Ubuntu, you can search with `apt-file` what provides these
	files:

	``$ apt-file search iconv.m4``
	gettext: /usr/share/aclocal/iconv.m4
	gnulib: /usr/share/gnulib/m4/iconv.m4

	The .m4 that usually works is in gettext. Simply run,

	``$ sudo apt-get install gettext``

	On Fedora, you can search with `yum`:

	``$ yum provides /usr/share/aclocal/iconv.m4``

	This shows the file is provided by the gettext-devel package.
	Run

	``$ sudo yum install gettext-devel``

standard software development tools
	To compile MonetDB, you also need to have the following
	standard software development tools installed and ready for
	use on you system:

	- a C compiler (e.g. GNU's ``gcc``);
	- GNU ``make`` (``gmake``) (native ``make`` on, e.g., IRIX and
	  Solaris usually don't work);
	- a lexical analyzer generator (e.g., ``lex`` or ``flex``);
	- a parser generator (e.g., ``yacc`` or ``bison``).

	If ``yacc`` and ``bison`` are missing, you won't be able to
	build the SQL front end.

	The following are optional.  They are checked for during
	configuration and if they are missing, the feature is just
	missing:

	- perl
	- php

libxml2
	The XML parsing library `libxml2`__ is used by the xml module
	of monetdb5.

	MonetDB5 cannot be compiled without libxml2.  Current Linux
	distributions all come with libxml2.

pcre
	The Perl Compatible Regular Expressions library `pcre`__ is
	used by monetdb5 and sql.  Most prominently, complex SQL LIKE
	expressions are evaluated with help of the pcre library.

openssl
	The `OpenSSL`__ toolkit implementing SSL v2/v3 and TLS
	protocols is used for its with full-strength world-wide
	cryptography functions.  The client-server login procedures
	make use of these functions.

__ http://dev.monetdb.org/downloads/sources/
__ http://www.gnu.org/software/autoconf/
__ http://www.gnu.org/software/automake/
__ http://www.gnu.org/software/libtool/
__ http://www.xmlsoft.org/
__ http://www.pcre.org/
__ http://www.openssl.org/

Space Requirements
~~~~~~~~~~~~~~~~~~

The packages take about this much space:

==========  =======  =======  =======
 Package    Source   Build    Install
==========  =======  =======  =======
buildtools  1.5 MB   8 MB     2.5 MB
common      2 MB     21 MB    4 MB
clients     9 MB     25 MB    10 MB
monetdb5    26 MB    46 MB    12 MB
sql         100 MB   22.5 MB  8 MB
==========  =======  =======  =======


Getting the Software
--------------------

There are two ways to get the source code:

(1) checking it out from the Mercurial repository on dev.monetdb.org;
(2) downloading the pre-packaged source distribution from
    `our download page`__.

The following instructions first describe how to check out the source
code from the Mercurial repository; in case you downloaded
the pre-packaged source distribution, you can skip this section and
proceed to `Bootstrap, Configure and Make`_.

__ http://dev.monetdb.org/downloads/

Mercurial clone
~~~~~~~~~~~~~~~

This command should be done once.  It makes an initial copy of the
development sources on your computer.

::

 hg clone http://dev.monetdb.org/hg/MonetDB

This will create the directory MonetDB in your current working directory
with underneath all subcomponents.


Bootstrap, Configure and Make
-----------------------------

In case you checked out the Mercurial version, you have to run
``bootstrap`` first; in case you downloaded the pre-packaged source
distribution, you should skip ``bootstrap`` and start with ``configure``
(see `Configure`_).

Bootstrap
~~~~~~~~~

This step is only needed when building from Mercurial.

In the top-level directory of the package type the command::

 ./bootstrap

Configure
~~~~~~~~~

Then in any directory (preferably a *new, empty* directory and *not*
in the ``MonetDB`` top-level directory) give the command::

 .../configure [<options>]

where ``...`` is replaced with the (absolute or relative) path to the
``MonetDB`` top-level directory.

The directory where you execute ``configure`` is the place where all
intermediate source and object files are generated during compilation
via ``make``.  It is useful to have this be a new directory so that
there is an easy way to remove all intermediates in case you want to
rebuild (just empty or remove the directory).

By default, MonetDB is installed in ``/usr/local``.  To choose another
target directory, you need to call

::

 .../configure --prefix=<prefixdir> [<options>]

Some other useful ``configure`` options are:

--enable-debug          enable full debugging
			default=[see `Configure defaults and
			recommendations`_ below]
--enable-optimize       enable extra optimization
			default=[see `Configure defaults and
			recommendations`_ below]
--enable-assert         enable assertions in the code
			default=[see `Configure defaults and
			recommendations`_ below]
--enable-strict         enable strict compiler flags
			default=[see `Configure defaults and
			recommendations`_ below]

You can also add options such as ``CC=<compiler>`` to specify the
compiler and compiler flags to use.

Use ``configure --help`` to find out more about ``configure`` options.

Configure defaults and recommendations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For convenience of both developers and users, we use the following
defaults for the configure options.

When compiling from Mercurial sources (as mainly done by developers)::

 --enable-strict --enable-assert --enable-debug --disable-optimize

When compiling from the pre-packages source distribution::

 --disable-strict --disable-assert --disable-debug --disable-optimize

When building a binary distribution, we use::

 --disable-strict --disable-assert --disable-debug --enable-optimize

IMPORTANT NOTE:

Since ``--enable-optimize=yes`` is not the default for any case except
binary packages, it is *strongly recommended* to (re)compile everything from
scratch, *explicitly configured* with

::

 --enable-debug=no --enable-assert=no --enable-optimize=yes

in case you want to run any performance experiments with MonetDB!

Please note:
``--enable-X=yes`` is equivalent to ``--enable-X``, and
``--enable-X=no``  is equivalent to ``--disable-X``.

Make
~~~~

In the same directory (where you called ``configure``) give the
command

::

 make

to compile the source code.  Please note that parallel make
runs (e.g. ``make -j2``) are fully supported.

Install
~~~~~~~

Give the command

::

 make install

By default (if no ``--prefix`` option was given to ``configure`` above),
this will install in ``/usr/local``.  Make sure you have appropriate
privileges.


Testing the Installation
~~~~~~~~~~~~~~~~~~~~~~~~

This step is optional.

Make sure that *prefix*/bin is in your ``PATH``.  Then
in the package top-level directory issue the command

::

 Mtest.py -r [--package=<package>]

where *package* is one of ``clients``, ``monetdb5`` or ``sql``
(the ``--package=<package>`` option can be omitted when
using a Mercurial checkout; see

::

 Mtest.py --help

for more options).

You need write permissions in part of the installation directory for
this command: it will create subdirectories ``var/dbfarm`` and
``Tests``, although there are options to ``Mtest.py`` to change the
paths.

Usage
-----

The MonetDB5 engine can be used interactively or as a
server.  The SQL back-end can only be used as server.

To run MonetDB5 interactively, just run::

 mserver5

A more pleasant environment can be had by using the system as a server
and using ``mclient`` to interact with the system.  In that case it is
easiest to start ``monetdbd`` and create, start, stop, remove, etc.
databases using the ``monetdb`` tool.

When MonetDB5 is started interactively, it automatically starts the MAL
server in addition to the interactive "console".

With ``mclient``, you get a text-based interface that supports
command-line editing and a command-line history.  The latter can even
be stored persistently to be re-used after stopping and restarting
``mclient``; see

::

 mclient --help

for details.

At the ``mclient`` prompt some extra commands are available.  Type
a single question mark to get a list of options.  Note that one of the
options is to read input from a file using ``\<``.

Troubleshooting
---------------

``bootstrap`` fails if any of the requisite programs cannot be found
or is an incompatible version.

``bootstrap`` adds files to the source directory, so it must have
write permissions.

``configure`` will fail if certain essential programs cannot be found
or certain essential tasks (such as compiling a C program) cannot be
executed.  The problem will usually be clear from the error message.

E.g., if ``configure`` cannot find package XYZ, it is either not
installed on your machine, or it is not installed in places that
``configure`` searches (i.e., ``/usr``, ``/usr/local``).  In the first
case, you need to install package XYZ before you can ``configure``,
``make``, and install MonetDB.  In the latter case, you need to tell
``configure`` via ``--with-XYZ=<DIR>`` where to find package XYZ on
your machine.  ``configure`` then looks for the header files in
<DIR>/include, and for the libraries in <DIR>/lib.

In case one of ``bootstrap``, ``configure``, or ``make`` fails ---
especially after a ``hg pull -u``, or after you changed some code
yourself --- try the following steps (in this order; if you are using
the pre-packaged source distribution, you can skip steps 2 and 3):

0) In case only ``make`` fails, you can try running::

	make clean

   in your build directory and proceed with step 5; however, if ``make``
   then still fails, you have to re-start with step 1.
1) Clean up your whole build directory (i.e., the one where you ran
   ``configure`` and ``make``) by going there and running::

	make maintainer-clean

   In case your build directory is different from your source
   directory, you are advised to remove the whole build directory.
2) Go to the top-level source directory and run::

	./de-bootstrap

   and type ``y`` when asked whether to remove the listed files.  This
   will remove all the files that were created during ``bootstrap``.
   Only do this with sources obtained through Mercurial.
3) In the top-level source directory, re-run::

	./bootstrap

   Only do this with sources obtained through Mercurial.
4) In the build-directory, re-run::

	/path/to/configure

   as described above.
5) In the build-directory, re-run::

	make
	make install

   as described above.

If this still does not help, please contact us.

Reporting Problems
------------------

Bugs and other problems with compiling or running MonetDB should be
reported using our `bug tracking system`__ (preferred) or
emailed to info@monetdb.org.  Please make sure that you give a *detailed*
description of your problem!

__ http://bugs.monetdb.org/
