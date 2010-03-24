.. The contents of this file are subject to the MonetDB Public License
.. Version 1.1 (the "License"); you may not use this file except in
.. compliance with the License. You may obtain a copy of the License at
.. http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
.. Copyright August 2008-2010 MonetDB B.V.
.. All Rights Reserved.

Building MonetDB On Windows
+++++++++++++++++++++++++++

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.

In this document we describe how to build the MonetDB suite of
programs on Windows using the sources from our source repository at
SourceForge__.  This document is mainly targeted at building on
Windows XP on a 32-bit architecture, but there are notes throughout
about building on Windows XP x64 which is indicated with Windows64.

__ http://sourceforge.net/projects/monetdb/

Introduction
============

The MonetDB suite of programs consists of a number of components which
we will describe briefly here.  The general rule is that the
components should be compiled and installed in the order given here,
although some components can be compiled and installed in a different
order.  Unless you know the inter-component dependencies, it is better
to stick to this order.  Also note that before the next component is
built, the previous ones need to be installed.  The section names are
the names of the CVS modules on SourceForge.

buildtools
----------

The buildtools component is required in order to build the sources
from the CVS repository.  If you get the pre-packaged sources
(i.e. the one in tar balls), you don't need the buildtools component
(although this has not been tested on Windows).

MonetDB
-------

Also known as the MonetDB Common component contains the database
kernel, i.e. the heart of MonetDB, and some generally useful
libraries.  This component is required.

clients
-------

Also known as the MonetDB Client component contains a library which
forms the basis for communicating with the MonetDB server components,
and some interface programs that use this library to communicate with
the server.  This component is required.

MonetDB4
--------

The deprecated (but still used) database server MonetDB4 Server.  This
component is still required for the MonetDB XQuery (pathfinder)
component.  This is the old server which uses MIL (the MonetDB
Interface Language) as programming interface.  This component is only
required if you need MIL or if you need the MonetDB XQuery component.

MonetDB5
--------

The MonetDB5 Server component is the new database server.  It uses MAL
(the MonetDB Algebra Language) as programming interface.  This
component is required if you need MAL or if you need the MonetDB SQL
component.

sql
---

Also known as MonetDB SQL, this component provides an SQL frontend to
MonetDB5.  This component is required if you need SQL support.

pathfinder
----------

Also known as MonetDB XQuery, this component provides an XQuery query
engine on top of a relational database.  You can store XML documents
in the database and query these documents using XQuery.  This
component is required if you need XML/XQuery support.

java
----

Also known as MonetDB Java, this component provides both the MonetDB
JDBC driver and the XRPC wrapper.  This component is optional.

geom
----

The geom component provides a module for the MonetDB SQL frontend.
This component is optional.

testing
-------

The testing component contains some files and programs we use for
testing the MonetDB suite.  This component is optional.

Prerequisites
=============

In order to compile the MonetDB suite of programs, several other
programs and libraries need to be installed.  Some further programs
and libraries can be optionally installed to enable optional
features.  The required programs and libraries are listed in this
section, the following section lists the optional programs and
libraries.

CVS (Concurrent Version System)
-------------------------------

All sources of the MonetDB suite of programs are stored using CVS__ at
SourceForge__.  You will need CVS to get the sources.  We use CVS
under Cygwin__, but any other version will do as well.

__ http://www.cvshome.org/
__ http://sourceforge.net/projects/monetdb/
__ http://www.cygwin.com/

Compiler
--------

The suite can be compiled using one of the following compilers:

- Microsoft Visual Studio .NET 2003 (also known as Microsoft Visual Studio 7);
- Microsoft Visual Studio 2005 (also known as Microsoft Visual Studio 8);
- Intel(R) C++ Compiler 9.1 (which actually needs one of the above);
- Intel(R) C++ Compiler 10.1 (which also needs one of the Microsoft compilers).

Note that the pathfinder component can currently not be compiled with
any of the Microsoft compilers.  It can be compiled with the Intel
compiler.

Not supported anymore (but probably still possible) are the GNU C
Compiler gcc under Cygwin__.  Using that, it (probably still) is
possible to build a version that runs using the Cygwin DLLs, but also
a version that uses the MinGW__ (Minimalist GNU for Windows) package.
This is not supported and not further described here.

__ http://www.cygwin.com/
__ http://www.mingw.org/

Python
------

Python__ is needed for creating the configuration files that the
compiler uses to determine which files to compile.  Python can be
downloaded from http://www.python.org/.  Just download and install the
Windows binary distribution.

On Windows64 you can use either the 32-bit or 64-bit version of
Python.

__ http://www.python.org/

Bison
-----

Bison is a reimplementation of YACC (Yet Another Compiler Compiler), a
program to convert a grammar into working code.

A version of Bison for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Packages
link on the left and then on Bison, and get the Setup file and install
it.

Flex
----

Flex is a fast lexical analyzer generator.

A version of Flex for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Packages
link on the left and then on Flex, and get the Setup file and install
it.

Pthreads
--------

Get a Windows port of pthreads from
ftp://sources.redhat.com/pub/pthreads-win32/.  You can download the
latest pthreads-\*-release.exe which is a self-extracting archive.
Extract it, and move or copy the contents of the Pre-built.2 folder to
``C:\Pthreads`` (so that you end up with folders ``C:\Pthreads\lib`` and
``C:\Pthreads\include``).

On Windows64, in a command interpreter, run ``nmake clean VC`` in the
extracted ``pthreads.2`` folder with the Visual Studio environment set
to the appropriate values, e.g. by executing the command ``Open Visual
Studio 2005 x64 Win64 Command Prompt``.  Then copy the files
``pthreadVC2.dll`` and ``pthreadVC2.lib`` to ``C:\Pthreads\lib``.

.. _diff:

Diff
----

Diff is a program to compare two versions of a file and list the
differences.  This program is not used during the build process, but
only during testing.  As such it is not a strict prerequisite.

A version of Diff for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Packages
link on the left and then on DiffUtils (note the name), and get the
Setup file and install it.

Patch
-----

Patch is a program to apply the output of diff_ to the original.  This
program is not used during the build process, but only for testing,
and then only to approve results that were different from what was
expected.  As such it is not a strict prerequisite.

A version of Patch for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Packages link on
the left and then on Patch, and get the Setup file and install it.

PsKill
------

PsKill is a program to kill (terminate) processes.  This program is
only used during testing to terminate tests that take too long.

PsKill is part of the `Windows Sysinternals`__.  Go to the Process
Utilities, and get the PsKill package.  PsKill is also part of the
PsTools package and the Sysinternals Suite, so you can get those
instead.  Extract the archive, and make sure that the folder is in
your ``Path`` variable when you run the tests.

__ http://www.microsoft.com/technet/sysinternals/default.mspx

PCRE (Perl Compatible Regular Expressions)
------------------------------------------

The PCRE__ library is used to extend the string matching capabilities
of MonetDB.  The PCRE library is required for the MonetDB5 component.

Download the source from http://www.pcre.org/.  In order to build the
library, you will need a program called ``cmake`` which you can
download from http://www.cmake.org/.  Follow the Download link and get
the Win32 Installer, install it, and run it.  It will come up with a
window where you have to fill in the location of the source code and
where to build the binaries.  Fill in where you extracted the PCRE
sources, and some other directory (I used a ``build`` directory which
I created within the PCRE source tree).  You need to configure some
PCRE build options.  I chose to do build shared libs, to match
newlines with the ``ANYCRLF`` option, and to do have UTF-8 support and
support for Unicode properties.  When you're satisfied with the
options, click on Configure, and then on Generate.  Then in the build
directory you've chosen, open the PCRE.sln file with Visual Studio,
and build and install.  Make sure you set the Solution Configuration
to Release if you want to build a releasable version of the MonetDB
suite.  The library will be installed in ``C:\Program Files\PCRE``.

For Windows64, select the correct compiler (``Visual Studio 9 2008
Win64``) and proceed normally.  When building the 32 bit version on
Windows64, choose ``C:/Program Files (x86)/PCRE`` for the
``CMAKE_INSTALL_PREFIX`` value, otherwise choose ``C:/Program Files/PCRE``.

__ http://www.pcre.org/

OpenSSL
-------

The OpenSSL__ library is used during authentication of a MonetDB
client program with the MonetDB server.  The OpenSSL library is
required for the MonetDB5 component, and hence implicitly required for
the clients component when it needs to talk to a MonetDB5 server.

Download the source from http://www.openssl.org/.  We used the latest
stable version (0.9.8k).  Follow the instructions in the file
``INSTALL.W32`` or ``INSTALL.W64``.

Fix the ``OPENSSL`` definitions in ``MonetDB\NT\winrules.msc`` so that
they refer to the location where you installed the library and call
``nmake`` with the extra parameter ``HAVE_OPENSSL=1``.

__ http://www.openssl.org/

libxml2
-------

Libxml2__ is the XML C parser and toolkit of Gnome.

This library is only a prerequisite for the pathfinder component,
although the MonetDB5 component can also make use of it.

The home of the library is http://xmlsoft.org/.  But Windows binaries
can be gotten from http://www.zlatkovic.com/libxml.en.html.  Click on
Win32 Binaries on the right, and download libxml2, iconv, and zlib.
Install these in e.g. ``C:\``.

Note that we hit a bug in version 2.6.31 of libxml2.  See the
bugreport__.  Use version 2.6.30 or 2.6.32.

On Windows64 you will have to compile libxml2 yourself (with its
optional prerequisites iconv_ and zlib_, for which see below).

Edit the file ``win32\Makefile.msvc`` and change the one occurrence of
``zdll.lib`` to ``zlib1.lib``, and then run the following commands in
the ``win32`` subdirectory, substituting the correct locations for the
iconv and zlib libraries::

 cscript configure.js compiler=msvc prefix=C:\libxml2-2.6.30.win64 ^
  include=C:\iconv-1.11.win64\include;C:\zlib-1.2.3.win64\include ^
  lib=C:\iconv-1.11.win64\lib;C:\zlib-1.2.3.win64\lib iconv=yes zlib=yes
 nmake /f Makefile.msvc
 nmake /f Makefile.msvc install

After this, you may want to move the file ``libxml2.dll`` from the
``lib`` directory to the ``bin`` directory.

__ http://xmlsoft.org/
__ https://sourceforge.net/tracker/index.php?func=detail&aid=1899258&group_id=56967&atid=482468

geos (Geometry Engine Open Souce)
---------------------------------

Geos__ is a library that provides geometric functions.  This library
is only a prerequisite for the geom component.

There are no Windows binaries available (not that I looked very hard),
so to get the software, you will have to get the source and build it
yourself.

Get the source tar ball from http://trac.osgeo.org/geos/#Download and
extract somewhere.  All the versions I have tried (up to 3.1.1) miss
one essential file to build on Windows, so in addition get the file
``nmake.opt`` from http://svn.osgeo.org/geos/branches/3.1/ and copy it
to the top of the extracted source directory.  Then build using::

 nmake /f makefile.vc

Then install the library somewhere, e.g. in ``C:\geos-3.1.win32``::

 mkdir C:\geos-3.1.win32
 mkdir C:\geos-3.1.win32\lib
 mkdir C:\geos-3.1.win32\bin
 mkdir C:\geos-3.1.win32\include
 mkdir C:\geos-3.1.win32\include\geos
 copy source\geos_c_i.lib C:\geos-3.1.win32\lib
 copy source\geos_c.dll C:\geos-3.1.win32\bin
 copy source\headers C:\geos-3.1.win32\include
 copy source\headers\geos C:\geos-3.1.win32\include\geos
 copy capi\geos_c.h C:\geos-3.1.win32\include

__ http://geos.refractions.net/

Optional Packages
=================

.. _iconv:

iconv
-----

Iconv__ is a program and library to convert between different
character encodings.  We only use the library.

The home of the program and library is
http://www.gnu.org/software/libiconv/, but Windows binaries can be
gotten from the same site as the libxml2 library:
http://www.zlatkovic.com/libxml.en.html.  Click on Win32 Binaries on
the right, and download iconv.  Install in e.g. ``C:\``.  Note that
these binaries are quite old (libiconv-1.9.2, last I looked).

On Windows64 you will have to compile iconv yourself.  Get the source
from the `iconv website`__ and extract somewhere.  Note that with the
1.12 release, the libiconv developers removed support for building
with Visual Studio but require MinGW instead, which means that there
is no support for Windows64.  In other words, get the latest 1.11 release.

Build using the commands::

 nmake -f Makefile.msvc NO_NLS=1 DLL=1 MFLAGS=-MD PREFIX=C:\iconv-1.11.win64
 nmake -f Makefile.msvc NO_NLS=1 DLL=1 MFLAGS=-MD PREFIX=C:\iconv-1.11.win64 install

Fix the ``ICONV`` definitions in ``MonetDB\NT\winrules.msc`` so that
they refer to the location where you installed the library and call
``nmake`` with the extra parameter ``HAVE_ICONV=1``.

__ http://www.gnu.org/software/libiconv/
__ http://www.gnu.org/software/libiconv/#downloading

.. _zlib:

zlib
----

Zlib__ is a compression library which is optionally used by both
MonetDB and the iconv library.  The home of zlib is
http://www.zlib.net/, but Windows binaries can be gotten from the same
site as the libxml2 library: http://www.zlatkovic.com/libxml.en.html.
Click on Win32 Binaries on the right, and download zlib.  Install in
e.g. ``C:\``.

On Windows64 you will have to compile zlib yourself.  Get the source
from the `zlib website`__ and extract somewhere.  Open the Visual
Studio 6 project file ``projects\visualc6\zlib.dsw`` and click on
``Yes To All`` to convert to the version of Visual Studio which you
are using.  Then add a x64 Solution Platform by selecting ``Build`` ->
``Confguration Manager...``, in the new window, in the pull down menu
under ``Active solution platform:`` select ``<New...>``.  In the pop
up window select ``x64`` for the new platform, copying the settings
from ``Win32`` and click on ``OK``.  Set the ``Active solution
configuration`` to ``DLL Release`` and click on ``Close``.  Then build
by selecting ``Build`` -> ``Build Solution``.  Create the directory
where you want to install the binaries, e.g. ``C:\zlib-1.2.3.win64``,
and the subdirectories ``bin``, ``include``, and ``lib``.  Copy the
files ``zconf.h`` and ``zlib.h`` to the newly created ``include``
directory.  Copy the file
``projects\visualc6\win32_dll_release\zlib1.lib`` to the new ``lib``
directory, and copy the file
``projects\visualc6\win32_dll_release\zlib1.dll`` to the new ``bin``
directory.

Fix the ``LIBZ`` definitions in ``MonetDB\NT\winrules.msc`` so that
they refer to the location where you installed the library and call
``nmake`` with the extra parameter ``HAVE_LIBZ=1``.

__ http://www.zlib.net/
__ http://www.zlib.net/

bzip2
-----

Bzip2__ is compression library which is optionally used by MonetDB.
The home of bzip2 is http://www.bzip.org/.  The executable which is
referenced on the download page is an executable of the command-line
program, but since we need the library, you will have to build it
yourself.

Get the source tar ball and extract it somewhere.  The sources
contains a file ``makefile.msc`` which can be used to build the
executable, but it needs some tweaking in order to build a DLL.  Apply
the following patches to the files ``makefile.msc`` and ``bzlib.h``
(lines starting with ``-`` should be replaced with lines starting with
``+``)::

 --- makefile.msc.orig   2007-01-03 03:00:55.000000000 +0100
 +++ makefile.msc        2009-10-13 13:15:49.343022600 +0200
 @@ -17,11 +17,11 @@
  all: lib bzip2 test
 
  bzip2: lib
 -       $(CC) $(CFLAGS) -o bzip2 bzip2.c libbz2.lib setargv.obj
 -       $(CC) $(CFLAGS) -o bzip2recover bzip2recover.c
 +       $(CC) $(CFLAGS) /Febzip2.exe bzip2.c libbz2.lib setargv.obj
 +       $(CC) $(CFLAGS) /Febzip2recover.exe bzip2recover.c
 
  lib: $(OBJS)
 -       lib /out:libbz2.lib $(OBJS)
 +       $(CC) /MD /LD /Felibbz2.dll $(OBJS) /link
 
  test: bzip2
	 type words1
 @@ -59,5 +59,5 @@
	 del sample3.tst
 
  .c.obj: 
 -       $(CC) $(CFLAGS) -c $*.c -o $*.obj
 +       $(CC) $(CFLAGS) -c $*.c /Fe$*.obj
 
 --- bzlib.h.orig        2007-12-09 13:34:39.000000000 +0100
 +++ bzlib.h     2009-10-13 13:54:15.013743800 +0200
 @@ -82,12 +82,12 @@
  #      undef small
  #   endif
  #   ifdef BZ_EXPORT
 -#   define BZ_API(func) WINAPI func
 -#   define BZ_EXTERN extern
 +#   define BZ_API(func) func
 +#   define BZ_EXTERN extern __declspec(dllexport)
  #   else
     /* import windows dll dynamically */
 -#   define BZ_API(func) (WINAPI * func)
 -#   define BZ_EXTERN
 +#   define BZ_API(func) func
 +#   define BZ_EXTERN extern __declspec(dllimport)
  #   endif
  #else
  #   define BZ_API(func) func

After this, compile using ``nmake -f makefile.msc`` and copy the files
``bzlib.h``, ``libbz2.dll``, and ``libbz2.lib`` to a location where
the MonetDB build process can find them,
e.g. ``C:\bzip2-1.0.5.win32``.

Fix the ``LIBBZ2`` definitions in ``MonetDB\NT\winrules.msc`` so that
they refer to the location where you installed the library and call
``nmake`` with the extra parameter ``HAVE_LIBBZ2=1``.

__ http://www.bzip.org/

Perl
----

Perl__ is only needed to create an interface that can be used from a
Perl program to communicate with a MonetDB server.

We have used ActiveState__'s ActivePerl__ distribution (release
5.10.0.1003).  Just install the 32 or 64 bit version and compile the
clients component with the additional ``nmake`` flags ``HAVE_PERL=1
HAVE_PERL_DEVEL=1 HAVE_PERL_SWIG=1`` (the latter flag only if SWIG_
is also installed).

__ http://www.perl.org/
__ http://www.activestate.com/
__ http://www.activestate.com/Products/activeperl/

PHP
---

PHP__ is only needed to create an interface that can be used from a PHP
program to communicate with a MonetDB server.

Download the Windows installer and source
package of PHP 5 from http://www.php.net/.
Install the binary package and extract the sources somewhere (e.g. as
a subdirectory of the binary installation).

In order to get MonetDB to compile with these sources a few changes
had to be made to the sources:

- In the file ``Zend\zend.h``, move the line
  ::

   #include <stdio.h>

  down until just *after* the block where ``zend_config.h`` is
  included.
- In the file ``main\php_network.h``, delete the line
  ::

   #include "arpa/inet.h"

We have no support yet for Windows64.

__ http://www.php.net/

.. _SWIG:

SWIG (Simplified Wrapper and Interface Generator)
-------------------------------------------------

We use SWIG__ to build interface files for Perl and Python.  You can
download SWIG from http://www.swig.org/download.html.  Get the latest
swigwin ZIP file and extract it somewhere.  It contains the
``swig.exe`` binary.

__ http://www.swig.org/

Java
----

If you want to build the java component of the MonetDB suite, you need
Java__.  Get Java from http://java.sun.com/, but make sure you do
*not* get the latest version.  Get the Java Development Kit 1.5.  Our
current JDBC driver is not compatible with Java 1.6 yet, and the XRPC
wrapper is not compatible with Java 1.4 or older.

In addition to the Java Development Kit, you will also need Apache Ant
which is responsible for the actual building of the driver.

__ http://java.sun.com/

Apache Ant
----------

`Apache Ant`__ is a program to build other programs.  This program is
only used by the java component of the MonetDB suite.

Get the Binary Distribution from http://ant.apache.org/, and extract
the file somewhere.

__ http://ant.apache.org/

Build Environment
=================

Placement of Sources
--------------------

For convenience place the various MonetDB packages in sibling
subfolders.  You will need at least:

- buildtools
- MonetDB
- clients
- one or both of MonetDB4, MonetDB5

Optionally:

- sql (requires MonetDB5)
- pathfinder (requires MonetDB4)

Apart from buildtools, all packages contain a subfolder ``NT`` which
contains a few Windows-specific source files.  Like on Unix/Linux, we
recommend to build in a new directory which is not part of the original
source tree.  On Windows, this build directory must be a sibling of the
aforementioned ``NT`` directory.

Build Process
-------------

We use a command window ``cmd.exe`` (also known as ``%ComSpec%``) to
execute the programs to build the MonetDB suite.  We do not use the
point-and-click interface that Visual Studio offers.  In fact, we do
not have project files that would support building using the Visual
Studio point-and-click interface.

We use a number of environment variables to tell the build process
where other parts of the suite can be found, and to tell the build
process where to install the finished bits.

In addition, you may need to edit some of the ``NT\rules.msc`` files
(each component has one), or the file ``NT\winrules.msc`` in the
MonetDB component which is included by all ``NT\rules.msc`` files.

Environment Variables
---------------------

Compiler
~~~~~~~~

Make sure that the environment variables that your chosen compiler
needs are set.  A convenient way of doing that is to use the batch
files that are provided by the compilers:

- Microsoft Visual Studio .NET 2003 (also known as Microsoft Visual
  Studio 7)::

   call "%ProgramFiles%\Microsoft Visual Studio .NET 2003\Common7\Tools\vsvars32.bat"

- Microsoft Visual Studio 2005 (also known as Microsoft Visual Studio
  8)::

   call "%ProgramFiles%\Microsoft Visual Studio 8\Common7\Tools\vsvars32.bat"

- Intel(R) C++ Compiler 10.1.013::

   call "C:%ProgramFiles%\Intel\Compiler\C++\10.1.013\IA32\Bin\iclvars.bat"

When using the Intel compiler, you also need to set the ``CC`` and
``CXX`` variables::

 set CC=icl -Qstd=c99 -GR- -Qsafeseh-
 set CXX=icl -Qstd=c99 -GR- -Qsafeseh-

(These are the values for the 10.1 version, for 9.1 replace
``-Qstd=c99`` with ``-Qc99``.)

Internal Variables
~~~~~~~~~~~~~~~~~~

- ``MONETDB_SOURCE`` - source folder of the MonetDB component
- ``CLIENTS_SOURCE`` - source folder of the clients component
- ``MONETDB4_SOURCE`` - source folder of the MonetDB4 component
- ``MONETDB5_SOURCE`` - source folder of the MonetDB5 component
- ``SQL_SOURCE`` - source folder of the sql component
- ``PATHFINDER_SOURCE`` - source folder of the pathfinder component

- ``MONETDB_BUILD`` - build folder of the MonetDB component (sibling of ``%MONETDB_SOURCE%\NT``)
- ``CLIENTS_BUILD`` - build folder of the clients component (sibling of ``%CLIENTS_SOURCE%\NT``)
- ``MONETDB4_BUILD`` - build folder of the MonetDB4 component (sibling of ``%MONETDB4_SOURCE%\NT``)
- ``MONETDB5_BUILD`` - build folder of the MonetDB5 component (sibling of ``%MONETDB5_SOURCE%\NT``)
- ``SQL_BUILD`` - build folder of the sql component (sibling of ``%SQL_SOURCE%\NT``)
- ``PATHFINDER_BUILD`` - build folder of the pathfinder component (sibling of ``%PATHFINDER_SOURCE%\NT``)

- ``MONETDB_PREFIX`` - installation folder of the MonetDB component
- ``CLIENTS_PREFIX`` - installation folder of the clients component
- ``MONETDB4_PREFIX`` - installation folder of the MonetDB4 component
- ``MONETDB5_PREFIX`` - installation folder of the MonetDB5 component
- ``SQL_PREFIX`` - installation folder of the sql component
- ``PATHFINDER_PREFIX`` - installation folder of the pathfinder component

We recommend that the various ``PREFIX`` environment variables all
point to the same location (all contain the same value) which is
different from the source and build folders.

PATH and PYTHONPATH
~~~~~~~~~~~~~~~~~~~

Extend your ``Path`` variable to contain the various folders where you
have installed the prerequisite and optional programs.  The ``Path``
variable is a semicolon-separated list of folders which are searched
in succession for commands that you are trying to execute (note, this
is an example: version numbers may differ)::

 rem Python is required
 set Path=C:\Python25;C:\Python25\Scripts;%Path%
 rem Bison and Flex (and Diff)
 set Path=%ProgramFiles%\GnuWin32\bin;%Path%
 rem for testing: pskill
 set Path=%ProgramFiles%\PsTools;%Path%
 rem Java is optional, set JAVA_HOME for convenience
 set JAVA_HOME=%ProgramFiles%\Java\jdk1.5.0_13
 set Path=%JAVA_HOME%\bin;%ProgramFiles%\Java\jre1.5.0_13\bin;%Path%
 rem Apache Ant is optional, but required for Java compilation
 set Path=%ProgramFiles%\apache-ant-1.7.0\bin;%Path%
 rem SWIG is optional
 set Path=%ProgramFiles%\swigwin-1.3.31;%Path%

In addition, during the build process we need to execute some programs
that were built and installed earlier in the process, so we need to
add those to the ``Path`` as well.  In addition, we use Python to
execute some Python programs which use Python modules that were
also installed earlier in the process, so we need to add those to the
``PYTHONPATH`` variable::

 set Path=%BUILDTOOLS_PREFIX%\bin;%Path%
 set Path=%BUILDTOOLS_PREFIX%\Scripts;%Path%
 set PYTHONPATH=%BUILDTOOLS_PREFIX%\Lib\site-packages;%PYTHONPATH%

Here the variable ``BUILDTOOLS_PREFIX`` represents the location where
the buildtools component is installed.  This variable is not used
internally, but only used here as a shorthand.

For testing purposes it may be handy to add some more folders to the
``Path``.  To begin with, all DLLs that are used also need to be found
in the ``Path``, various programs are used during testing, such as
diff (from GnuWin32) and php, and Python modules that were installed
need to be found by the Python interpreter::

 rem Pthreads DLL
 set Path=C:\Pthreads\lib;%Path%
 rem PCRE DLL
 set Path=C:\Program Files\PCRE\bin;%Path%
 rem PHP binary
 set Path=C:\Program Files\PHP;%Path%
 if not "%MONETDB_PREFIX%" == "%SQL_PREFIX%" set Path=%SQL_PREFIX%\bin;%SQL_PREFIX%\lib;%SQL_PREFIX%\lib\MonetDB4;%Path%
 set Path=%MONETDB4_PREFIX%\lib\MonetDB4;%Path%
 if not "%MONETDB_PREFIX%" == "%MONETDB4_PREFIX%" set Path=%MONETDB4_PREFIX%\bin;%MONETDB4_PREFIX%\lib;%Path%
 if not "%MONETDB_PREFIX%" == "%CLIENTS_PREFIX%" set Path=%CLIENTS_PREFIX%\bin;%CLIENTS_PREFIX%\lib;%Path%
 set Path=%MONETDB_PREFIX%\bin;%MONETDB_PREFIX%\lib;%Path%

 set PYTHONPATH=%CLIENTS_PREFIX%\share\MonetDB\python;%PYTHONPATH%
 set PYTHONPATH=%MONETDB_PREFIX%\share\MonetDB\python;%PYTHONPATH%
 set PYTHONPATH=%SQL_PREFIX%\share\MonetDB\python;%PYTHONPATH%

Compilation
-----------

Building and Installing Buildtools
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The buildtools component needs to be built and installed first::

 cd ...\buildtools
 nmake /nologo /f Makefile.msc "prefix=%BUILDTOOLS_PREFIX%" install

where, again, the ``BUILDTOOLS_PREFIX`` variable represents the
location where the buildtools component is to be installed.

Building and Installing the Other Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The other components of the MonetDB suite are all built and installed
in the same way.  Do note the order in which the components need to be
built and installed: MonetDB, clients, MonetDB4/MonetDB5,
sql/pathfinder.  There is no dependency between MonetDB4 and MonetDB5.
MonetDB4 is a prerequisite for pathfinder, and pathfinder can use
MonetDB5 (there is some very preliminary support).  Sql requires
MonetDB5.

For each of the components, do the following::

 mkdir ...\<component>\BUILD_DIR
 cd ...\<component>\BUILD_DIR
 nmake /nologo /f ..\NT\Makefile NEED_MX=1 ... "prefix=%..._PREFIX%"
 nmake /nologo /f ..\NT\Makefile NEED_MX=1 ... "prefix=%..._PREFIX%" install

Here the first ``...`` needs to be replaced by a list of parameters
that tells the system which of the optional programs and libraries are
available.  The following parameters are possible:

- ``DEBUG=1`` - compile with extra debugging information
- ``NDEBUG=1`` - compile without extra debugging information (this is
  used for creating a binary release);
- ``HAVE_ICONV=1`` - the iconv library is available;
- ``HAVE_JAVA=1`` - Java and Apache Ant are both available;
- ``HAVE_LIBXML2=1`` - the libxml2 library is available;
- ``HAVE_MONETDB4=1`` - for sql and pathfinder: MonetDB4 was compiled
  and installed;
- ``HAVE_MONETDB5=1`` - for sql and pathfinder: MonetDB5 was compiled
  and installed;
- ``HAVE_MONETDB5_XML=1`` - for sql and pathfinder: MonetDB5 was compiled
  with the xml2 library available (HAVE_LIBXML2=1), and hence provides XML
  support (i.e., module xml);
- ``HAVE_MONETDB5_RDF=1`` - for sql and pathfinder: MonetDB5 was compiled
  with the raptor library available (HAVE_RAPTOR=1), and hence provides RDF
  support (i.e., module rdf);
- ``HAVE_RAPTOR=1`` - the raptor library is available;
- ``HAVE_NETCDF=1`` - the netcdf library is available;
- ``HAVE_OPENSSL=1`` - the OpenSSL library is available;
- ``HAVE_PERL=1`` - Perl is available;
- ``HAVE_PERL_DEVEL=1`` - Perl development is possible (include files
  and libraries are available--also need ``HAVE_PERL=1``);
- ``HAVE_PERL_SWIG=1`` - Perl development is possible and SWIG is
  available (also need ``HAVE_PERL=1``);
- ``HAVE_PHP=1`` - PHP is available;
- ``HAVE_PROBXML=1`` - compile in support for probabilistic XML (an
  experimental extension to the pathfinder component);
- ``HAVE_PYTHON=1`` - Python is available.

In addition, you can add a parameter which points to a file with extra
definitions for ``nmake``.  This is very convenient to define where
all packages were installed that the build process depends on since
you then don't have to edit any of the ``rules.msc`` files in the
source tree:

- ``"MAKE_INCLUDEFILE=..."`` - file with extra ``nmake`` definitions.

It is recommended to at least put the ``MAKE_INCLUDEFILE`` parameter
with argument in double quotes to protect any spaces that may appear
in the file name.

The contents of the file referred to with the ``MAKE_INCLUDEFILE``
parameter may contain something like::

 bits=32
 PTHREAD_INCS=-IC:\Pthreads\include
 PTHREAD_LIBS=C:\Pthreads\lib\pthreadVC2.lib
 PHP_SRCDIR=C:\Program Files\PHP\php-5.2.6
 PHP_INSTDIR=C:\Program Files\PHP
 LIBPERL=C:\Perl
 LIBPCRE=C:\Program Files\PCRE
 LIBICONV=C:\iconv-1.11.win32
 LIBZLIB=C:\zlib-1.2.3.win32
 LIBXML2=C:\libxml2-2.6.32+.win32

Building Installers
~~~~~~~~~~~~~~~~~~~

Installers can be built either using the full-blown Visual Studio user
interface or on the command line.  To use the user interface, open one
or more of the files ``MonetDB4-XQuery-Installer.sln``,
``MonetDB5-SQL-Installer.sln``, ``MonetDB-ODBC-Driver.sln``, and
``MonetDB5-Geom-Module.sln`` in the
installation folder and select ``Build`` -> ``Build Solution``.  To
use the command line, execute one or more of the commands in the
installation folder::

 devenv MonetDB4-XQuery-Installer.sln /build
 devenv MonetDB5-SQL-Installer.sln /build
 devenv MonetDB-ODBC-Driver.sln /build
 devenv MonetDB5-Geom-Module.sln /build

In both cases, use the solutions (``.sln`` files) that are
appropriate.

There is an annoying bug in Visual Studio on Windows64 that affects
the MonetDB5-Geom-Module installer.  The installer contains code to
check the registry to find out where MonetDB5/SQL is installed.  The
bug is that the 64 bit installer will check the 32-bit section of the
registry.  The code can be fixed by editing the generated installer
(``.msi`` file) using e.g. the program ``orca`` from Microsoft.  Open
the installer in ``orca`` and locate the table ``RegLocator``.  In the
Type column, change the value from ``2`` to ``18`` and save the file.
Alternatively, use the following Python script to fix the ``.msi``
file::

 # Fix a .msi (Windows Installer) file for a 64-bit registry search.
 # Microsoft refuses to fix a bug in Visual Studio so that for a 64-bit
 # build, the registry search will look in the 32-bit part of the
 # registry instead of the 64-bit part of the registry.  This script
 # fixes the .msi to look in the correct part.

 import msilib
 import sys
 import glob

 def fixmsi(f):
     db = msilib.OpenDatabase(f, msilib.MSIDBOPEN_DIRECT)
     v = db.OpenView('UPDATE RegLocator SET Type = 18 WHERE Type = 2')
     v.Execute(None)
     v.Close()
     db.Commit()

 if __name__ == '__main__':
     for f in sys.argv[1:]:
	 for g in glob.glob(f):
	     fixmsi(g)
