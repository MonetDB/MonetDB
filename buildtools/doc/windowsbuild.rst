.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 2008-2015 MonetDB B.V.

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.

Building MonetDB On Windows
+++++++++++++++++++++++++++

In this document we describe how to build the MonetDB suite of
programs on Windows using the sources from `our source repository`__.
This document is mainly targeted at building on Windows on a 32-bit
architecture, but there are notes throughout about building on Windows
on a 64-bit architecture which is indicated with Windows64.  We have
successfully built on Windows XP, Windows Server, and Windows 7.

.. _MonetDB: http://dev.monetdb.org/hg/MonetDB/

__ MonetDB_

Introduction
============

The MonetDB suite of programs consists of a number of components which
we will describe briefly here.  The section names are the names of the
top-level folders in the Mercurial clone.

Note that in branches up to and including Oct2010 the build process
was different.  This document describes the build process for the
branch this document is part of.  Use the command ``hg branch`` to
find out the name of the branch.

buildtools
----------

The buildtools component contains tools that are used to build the
other components.  This component is required, but not all parts of
this component are required for all configurations.

common
------

Also known as the MonetDB Common component contains some generally
useful libraries.  This component is required.

gdk
---

Also known as the Goblin Database Kernel contains the database kernel,
i.e. the heart of MonetDB.  This component is required.

clients
-------

Also known as the MonetDB Client component contains a library which
forms the basis for communicating with the MonetDB server components,
and some interface programs that use this library to communicate with
the server.  Additionally, this component contains modules for some
other languages (Python, Perl, PHP, Ruby) to enable communication with
the server from programs written in those languages.  The Python and
Ruby modules can be built separately; the PHP module does not need to
be built.  This component is required.

monetdb5
--------

The MonetDB5 Server component is the database server.  It uses MAL
(the MonetDB Algebra Language) as programming interface.  This
component is required.

sql
---

Also known as MonetDB SQL, this component provides an SQL frontend to
MonetDB5.  This component is required if you need SQL support.

tools
-----

The tools component contains two parts.  The mserver part is the
actual database server binary and is required.  The merovingian part
is not used on Windows.

geom
----

The geom component provides a module for the MonetDB SQL frontend.
This component is optional.

java
----

Also known as MonetDB Java, this component provides the MonetDB JDBC
driver.  This component is optional.

testing
-------

The testing component contains some files and programs we use for
testing the MonetDB suite.  This component is optional.

Prerequisites
=============

In order to compile the MonetDB suite of programs, several other
programs and libraries need to be installed.  Some further programs
and libraries can be optionally installed to enable optional features.
The required programs and libraries are listed in this section, the
following section lists the optional programs and libraries.

Chocolatey
----------

Although Chocolatey_ is not a prerequisite per se, it makes
installing and maintaining some of the other prerequisites a lot
easier.  Therefore we recommend installing chocolatey.  Instructions
are on their website__.

We have installed the following programs using Chocolatey_::

  choco install ActivePerl ant cmake ruby
  choco install python2 python2-x86_32 python3 python3-x86_32

.. _Chocolatey: https://chocolatey.org/

__ Chocolatey_

Mercurial (a.k.a. HG)
---------------------

All sources of the MonetDB suite of programs are stored using
Mercurial_ at our server__.  You will need Mercurial to get the
sources.  We use Mercurial under Cygwin_, but any other version will
do as well.

Once Mercurial is installed and configured, you can get the sources
using the command

::

 hg clone http://dev.monetdb.org/hg/MonetDB/

This will create a folder ``MonetDB`` that contains everything.

You can update the sources using (from within the above-mentioned
``MonetDB`` folder)

::

 hg pull -u

.. _Mercurial: http://mercurial.selenic.com/
.. _Cygwin: http://www.cygwin.com/

__ MonetDB_


Compiler
--------

The suite can be compiled using one of the following compilers:

- Microsoft Visual Studio .NET 2003 (also known as Microsoft Visual Studio 7);
- Microsoft Visual Studio 2005 (also known as Microsoft Visual Studio 8);
- Microsoft Visual Studio 2008 (also known as Microsoft Visual Studio 9.0);
- Microsoft Visual Studio 2010 (also known as Microsoft Visual Studio 10.0);
- Intel(R) C++ Compiler 9.1 (which actually needs one of the above);
- Intel(R) C++ Compiler 10.1 (which also needs one of the Microsoft compilers);
- Intel(R) C++ Compiler 11.1 (which also needs one of the Microsoft compilers).

Not supported anymore (but probably still possible) are the GNU C
Compiler gcc under Cygwin__.  Using that, it (probably still) is
possible to build a version that runs using the Cygwin DLLs, but also
a version that uses the MinGW__ (Minimalist GNU for Windows) package.
This is not supported and not further described here (in either case,
the build process would be much more like Unix than what is described
here).

We currently use Microsoft Visual Studio 2010 and Intel(R) C++
Compiler XE 13.1.2.190, the latter using Microsoft Visual Studio
10.0.  Older versions haven't been tried in a long time.

__ http://www.cygwin.com/
__ http://www.mingw.org/

Python
------

Python_ is needed for creating the configuration files that the
compiler uses to determine which files to compile.  Python can be
downloaded from http://www.python.org/.  Just download and install the
Windows binary distribution.

Note that you can use either or both Python2 and Python3, and on 64
bit architectures, either the 32 bit or 64 bit version of Python.  All
these versions are fine for building the MonetDB suite.  However, if
you want to create an installer for the Python component using
``python setup.py bdist_wininst``, you need to use the version of
Python for which you're building the installer.  It is possible to
install all versions.  Using Chocolatey_ you can do::

  choco install python2 python3 
  choco install python2-x86_32 python3-x86_32

The latter command only on 64 bit architectures to install the 32 bit
verions.

.. _Python: http://www.python.org/

Bison
-----

Bison is a reimplementation of YACC (Yet Another Compiler Compiler), a
program to convert a grammar into working code.

A version of Bison for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Packages
link on the left and then on Bison, and get the Setup file and install
it.

However, we use the version of bison that comes with Cygwin__.

__ http://www.cygwin.com/

Diff
----

Diff is a program to compare two versions of a file and list the
differences.  This program is not used during the build process, but
only during testing.  As such it is not a strict prerequisite.

A version of Diff for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Packages link on
the left and then on DiffUtils (note the name), and get the Setup file
and install it.

Patch
-----

Patch is a program to apply the output of diff_ to the original.  This
program is not used during the build process, but only for testing,
and then only to approve results that were different from what was
expected.  As such it is not a strict prerequisite.

A version of Patch for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Packages link on
the left and then on Patch, and get the Setup file and install it.

PCRE (Perl Compatible Regular Expressions)
------------------------------------------

The PCRE__ library is used to extend the string matching capabilities
of MonetDB.  The PCRE library is required for the monetdb5 component.

Download the source from http://www.pcre.org/.  In order to build the
library, you will need a program called ``cmake`` which you can
download from http://www.cmake.org/.  Follow the Download link and get
the Win32 Installer, install it, and run it.  It will come up with a
window where you have to fill in the location of the source code and
where to build the binaries.  Fill in where you extracted the PCRE
sources, and some other folder (I used a ``build`` folder which I
created within the PCRE source tree), then click on the Configure
button.  This pops up a dialog to choose the compiler.  I chose Visual
Studio 10 2010.

You need to configure some PCRE build options.  I chose to do build
shared libs, to match newlines with the ``ANYCRLF`` option, and to do
have UTF-8 support and support for Unicode properties.  When you're
satisfied with the options, click on Generate.  Then in the build
folder you've chosen, open the PCRE.sln file with Visual Studio, and
build and install.  Make sure you set the Solution Configuration to
Release if you want to build a releasable version of the MonetDB
suite.  By default the library will be installed in ``C:\Program
Files\PCRE``.

For Windows64, select the correct compiler (``Visual Studio 10 2010
Win64``) and proceed normally.  When building the 32 bit version on
Windows64, choose ``C:/Program Files (x86)/PCRE`` for the
``CMAKE_INSTALL_PREFIX`` value, otherwise choose ``C:/Program
Files/PCRE``.

In order to get a version number in the DLL that is produced, we added
a file ``version.rc`` to the sources for the ``pcre`` subproject.  The
contents of the file are::

 #include <Windows.h>
 VS_VERSION_INFO	VERSIONINFO
 FILEVERSION		8,37,0,0	// change as appropriate
 PRODUCTVERSION		8,37,0,0	// change as appropriate
 FILEFLAGSMASK		0x3fL
 FILEFLAGS		0
 FILEOS			VOS_NT_WINDOWS32
 FILETYPE		VFT_DLL
 FILESUBTYPE		VFT2_UNKNOWN
 BEGIN
   BLOCK "StringFileInfo"
   BEGIN
   END
 END

__ http://www.pcre.org/

OpenSSL
-------

The OpenSSL__ library is used during authentication of a MonetDB
client program with the MonetDB server.  The OpenSSL library is
required for the MonetDB5 component, and hence implicitly required for
the clients component when it needs to talk to a MonetDB5 server.

Download the source from http://www.openssl.org/.  We used the latest
stable version (1.0.2d).  Follow the instructions in the file
``INSTALL.W32`` or ``INSTALL.W64``.  We used the option
``enable-static-engine`` as described in the instructions.

.. The actual commands used were::
   perl Configure VC-WIN32 no-asm enable-static-engine --prefix=C:\Libraries\openssl-1.0.2d.win32
   ms\do_ms.bat
   nmake /f ms\ntdll.mak
   nmake /f ms\ntdll.mak install
   and::
   perl Configure VC-WIN64A enable-static-engine --prefix=C:\Libraries\openssl-1.0.2d.win64
   ms\do_win64a.bat
   nmake /f ms\ntdll.mak
   nmake /f ms\ntdll.mak install
   For the debug versions, use ``debug-VC-WIN32`` and
   ``debug-VC-WIN64A`` and edit the file ``ms/ntdll.mak`` to add a
   ``d`` to the definitions of ``O_SSL``, ``O_CRYPTO``, ``L_SSL``, and
   ``L_CRYPTO`` before building.

Fix the ``LIBOPENSSL`` definition in ``NT\rules.msc`` so that it
refers to the location where you installed the library and call
``nmake`` with the extra parameter ``HAVE_OPENSSL=1``.

__ http://www.openssl.org/

libxml2
-------

Libxml2__ is the XML C parser and toolkit of Gnome.

The home of the library is http://xmlsoft.org/.  But Windows binaries
can be gotten from http://www.zlatkovic.com/libxml.en.html.  Click on
Win32 Binaries on the right, and download libxml2, iconv, and zlib.
Install these in e.g. ``C:\Libraries``.

Note that we hit a bug in version 2.6.31 of libxml2.  See the
bugreport__.  Use version 2.6.30 or 2.6.32 or later.

On Windows64 you will have to compile libxml2 yourself (with its
optional prerequisites iconv_ and zlib_, for which see below).

Run the following commands in the ``win32`` subfolder, substituting
the correct locations for the iconv and zlib libraries::

 cscript configure.js compiler=msvc prefix=C:\Libraries\libxml2-2.9.2.win64 ^
  include=C:\Libraries\iconv-1.11.1.win64\include;C:\Libraries\zlib-1.2.8.win64\include ^
  lib=C:\Libraries\iconv-1.11.1.win64\lib;C:\Libraries\zlib-1.2.8.win64\lib ^
  iconv=yes zlib=yes vcmanifest=yes
 nmake /f Makefile.msvc
 nmake /f Makefile.msvc install

Note that there are a few minor problems with the 2.7.8 distribution.
Edit the file ``win32\Makefile.msvc`` and remove the ``+`` from the
start of the three lines that contain one.  Also, unless you use an
older compiler, remove the line that contains ``/OPT:NOWIN98``.
Visual Studio 2010 will give an error with this option, and Visual
Studio 2008 a warning.

In the 2.9.2 there is another problem.  In the ``win32\configure.js``
file near the top, change ``configure.in`` to ``configure.ac``.

.. For a debug version, add ``debug=yes cruntime=/MDd`` to the
   ``cscript`` command and edit the file ``Makefile.msvc`` to add a
   ``d`` to the definitions of ``XML_SO``, ``XML_IMP``, ``XML_A``, and
   ``XML_A_DLL``.  Also add ``d`` to the ``zlib.lib`` and ``iconv.lib``.

In order to get a version number in the DLL that is produced, we added
a file ``version.rc`` in the ``win32`` folder.  The contents of the
file are::

 #include <Windows.h>
 VS_VERSION_INFO	VERSIONINFO
 FILEVERSION		2,9,2,0		// change as appropriate
 PRODUCTVERSION		2,9,2,0		// change as appropriate
 FILEFLAGSMASK		0x3fL
 FILEFLAGS		0
 FILEOS			VOS_NT_WINDOWS32
 FILETYPE		VFT_DLL
 FILESUBTYPE		VFT2_UNKNOWN
 BEGIN
   BLOCK "StringFileInfo"
   BEGIN
   END
 END

To use it, we also added ``version.res`` after ``$(XML_OBJS)`` in
``win32\Makefile.msvc`` both in the list of dependencies of
``$(BINDIR)\$(XML_SO)`` and in the command to produce said file.

After this, you may want to move the file ``libxml2.dll`` from the
``lib`` folder to the ``bin`` folder.

__ http://xmlsoft.org/
__ http://bugs.monetdb.org/1600

geos (Geometry Engine Open Souce)
---------------------------------

Geos__ is a library that provides geometric functions.  This library
is only a prerequisite for the geom component.

There are no Windows binaries available (not that I looked very hard),
so to get the software, you will have to get the source and build it
yourself.

Get the source tar ball from http://trac.osgeo.org/geos/#Download and
extract somewhere.  You can follow the instructions in e.g. `Building
on Windows with NMake`__.  I did find one problem with this procedure:
you will need to run the ``autogen.bat`` script despite what it says
in the instructions.

We needed to make a few changes to the file ``nmake.opt``.  We needed
to add a blurb for the version of ``nmake`` that we were using.  Look
at the version number of ``nmake /P`` and adapt the closest match.  We
also uncommented the section defining ``MSVC_VLD_DIR`` in order to
compile a debug version.

For newer versions of Visual Studio, we also needed to add a line::

   #include <algorithm>

to the files::

   src\algorithm\LineIntersector.cpp
   src\geom\LineSegment.cpp
   src\io\WKTWriter.cpp
   src\operation\buffer\OffsetCurveSetBuilder.cpp

.. The actual commands were::
   autogen.bat
   nmake /f makefile.vc

.. On Windows64, add ``WIN64=YES`` to the nmake command line.

.. For a debug build, add ``BUILD_DEBUG=YES`` to the ``nmake`` command
   line.

In order to get a version number in the DLL that is produced, we added
a file ``version.rc`` in the ``src`` folder.  The contents of the
file are::

 #include <Windows.h>
 VS_VERSION_INFO	VERSIONINFO
 FILEVERSION		3,4,2,0		// change as appropriate
 PRODUCTVERSION		3,4,2,0		// change as appropriate
 FILEFLAGSMASK		0x3fL
 FILEFLAGS		0
 FILEOS			VOS_NT_WINDOWS32
 FILETYPE		VFT_DLL
 FILESUBTYPE		VFT2_UNKNOWN
 BEGIN
   BLOCK "StringFileInfo"
   BEGIN
   END
 END

To use it, we also added ``version.res`` at the end of the definition
of the ``OBJ`` macro and to the list of dependencies of and the
command for ``$(CDLLNAME)`` in ``src\Makefile.vc``.

After this, install the library somewhere, e.g. in
``C:\Libraries\geos-3.4.2.win32``::

 mkdir C:\Libraries\geos-3.4.2.win32
 mkdir C:\Libraries\geos-3.4.2.win32\lib
 mkdir C:\Libraries\geos-3.4.2.win32\bin
 mkdir C:\Libraries\geos-3.4.2.win32\include
 mkdir C:\Libraries\geos-3.4.2.win32\include\geos
 copy src\geos_c_i.lib C:\Libraries\geos-3.4.2.win32\lib
 copy src\geos_c.dll C:\Libraries\geos-3.4.2.win32\bin
 copy include C:\Libraries\geos-3.4.2.win32\include
 copy include\geos C:\Libraries\geos-3.4.2.win32\include\geos
 copy capi\geos_c.h C:\Libraries\geos-3.4.2.win32\include

__ http://geos.refractions.net/
__ http://trac.osgeo.org/geos/wiki/BuildingOnWindowsWithNMake

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
the right, and download iconv.  Install in e.g. ``C:\Libraries\``.  Note that
these binaries are quite old (libiconv-1.9.2, last I looked).

On Windows64 you will have to compile iconv yourself.  Get the source
from the `iconv website`__ and extract somewhere.  Note that with the
1.12 release, the libiconv developers removed support for building
with Visual Studio but require MinGW instead, which means that there
is no support for Windows64.  In other words, get the latest 1.11
release.

When compiling with Visual Studio 12.0, delete the file
``windows\stdbool.h`` since this version of VS includes its own copy
(older versions didn't).

Build using the commands::

 nmake /f Makefile.msvc NO_NLS=1 DLL=1 MFLAGS=-MD PREFIX=C:\Libraries\iconv-1.11.1.win64 IIPREFIX=C:\\Libraries\\iconv-1.11.1.win64
 nmake /f Makefile.msvc NO_NLS=1 DLL=1 MFLAGS=-MD PREFIX=C:\Libraries\iconv-1.11.1.win64 IIPREFIX=C:\\Libraries\\iconv-1.11.1.win64 install

.. Before the install, run the commands::
   cd lib
   mt /nologo /manifest iconv.dll.manifest /outputresource:iconv.dll;2
   cd ..\libcharset\lib
   mt /nologo /manifest charset.dll.manifest /outputresource:charset.dll;2
   cd ..\..

.. For the debug version, use options ``MFLAGS=-MDd DEBUG=1`` and
   change the ``Makefile.msvc`` files to include a ``d`` for the DLLs.

Fix the ``ICONV`` definitions in ``NT\rules.msc`` so that they refer
to the location where you installed the library and call ``nmake``
with the extra parameter ``HAVE_ICONV=1``.

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
e.g. ``C:\Libraries\``.  Note that the at the time of writing, the precompiled
version lags behind: it is version 1.2.3, whereas 1.2.8 is current.

On Windows64 you will have to compile zlib yourself.  Get the source
from the `zlib website`__ and extract somewhere.  Then compile using::

 nmake /f win32\Makefile.msc OBJA=inffast.obj

Create the folder where you want to install the binaries,
e.g. ``C:\Libraries\zlib-1.2.8.win64``, and the subfolders ``bin``,
``include``, and ``lib``.  Copy the files ``zconf.h`` and ``zlib.h``
to the newly created ``include`` folder.  Copy the file ``zdll.lib``
to the new ``lib`` folder, and copy the file ``zlib1.dll`` to the new
``bin`` folder.

Fix the ``LIBZ`` definitions in ``NT\rules.msc`` so that they refer to
the location where you installed the library and call ``nmake`` with
the extra parameter ``HAVE_LIBZ=1``.

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

In order to get a version number in the DLL that is produced, we added
a file ``version.rc`` in the top-level folder.  The contents of the
file are::

 #include <Windows.h>
 VS_VERSION_INFO	VERSIONINFO
 FILEVERSION		1,0,6,0		// change as appropriate
 PRODUCTVERSION		1,0,6,0		// change as appropriate
 FILEFLAGSMASK		0x3fL
 FILEFLAGS		0
 FILEOS			VOS_NT_WINDOWS32
 FILETYPE		VFT_DLL
 FILESUBTYPE		VFT2_UNKNOWN
 BEGIN
   BLOCK "StringFileInfo"
   BEGIN
   END
 END

To use it, we also added ``version.res`` to the list of dependencies of and the
command for ``lib`` in ``makefile.msc``.

After this, compile using ``nmake /f makefile.msc`` and copy the files
``bzlib.h``, ``libbz2.dll``, and ``libbz2.lib`` to a location where
the MonetDB build process can find them,
e.g. ``C:\Libraries\bzip2-1.0.6.win32``.

.. Before copying the files, run the command::
   mt /nologo /manifest libbz2.dll.manifest /Outputresource:libbz2.dll;2

.. For a debug build, change the definition of CFLAGS to contain
   ``-MDd -D_DEBUG -Od`` instead of ``-MD -Ox``, and change all
   occurences of ``libbz2.dll`` and ``libbz2.lib`` to ``libbz2d.dll``
   and ``libbz2d.lib``.

Fix the ``LIBBZ2`` definitions in ``NT\rules.msc`` so that they refer
to the location where you installed the library and call ``nmake``
with the extra parameter ``HAVE_LIBBZ2=1``.

__ http://www.bzip.org/

Libatomic_ops
-------------

`Atomic Ops`__ is a library that provides semi-portable access to
hardware-provided atomic memory update operations on a number of
architectures.  We optionally uses this to implement thread-safe
access to a number of variables and for the implementation of locks.

To install, it suffices to copy the file ``src\atomic_ops.h`` and the
folder ``src\atomic_ops`` to the installation location in
e.g. ``C:\Libraries``.

Fix the ``LIBATOMIC_OPS`` definition in ``NT\rules.msc`` so that it
refers to the location where you installed the header files and call
``nmake`` with the extra parameter ``HAVE_ATOMIC_OPS=1``.

__ https://github.com/ivmai/libatomic_ops

Perl
----

There is a Perl__ interface that can be used from a Perl program to
communicate with a MonetDB server.  This interface is written
completely in Perl, so there is no compilation involved.  This means
that no installation of Perl is required for building, but only for
testing.

We have used ActiveState__'s ActivePerl__ distribution (release
5.12.1.1201).  Just install the 32 or 64 bit version.

__ http://www.perl.org/
__ http://www.activestate.com/
__ http://www.activestate.com/Products/activeperl/

PHP
---

There is a PHP__ interface that can be used from a PHP program to
communicate with a MonetDB server.  This interface is written
completely in PHP, so there is no compilation involved.  This means
that no installation of PHP is required for building, but only for
testing.

Download the Windows installer and source package of PHP 5 from
http://www.php.net/.  Install the binary package and extract the
sources somewhere (e.g. as a subfolder of the binary installation).

__ http://www.php.net/

Java
----

If you want to build the java component of the MonetDB suite, you need
Java__.  Get Java from http://java.sun.com/, but make sure you do
*not* get the latest version.  Get the Java Development Kit 1.5.  Our
current JDBC driver is not compatible with Java 1.6 yet.

In addition to the Java Development Kit, you will also need `Apache Ant`_
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

Place the sources in a location with enough free space.  On Windows,
you can either build inside the ``NT`` subdirectory, or in an empty
directory that you create inside the top level of the source tree.
This means that all intermediate files will also be located on the
same drive.

Currently, the sources take up about 1.1 GB, the build takes up
another 0.2 to 0.6 GB (depending on compiler and compiler options),
and the installation takes up between 30 MB and 0.1 GB (again,
depending on compiler and compiler options).  The installation can be
on a different drive than sources and build.

At the top level of the source tree there is a subfolder ``NT`` which
contains a few Windows-specific source files.  Like on Unix/Linux, we
recommend building in a new folder which is not part of the original
source tree.  On Windows, this build folder must be a sibling of the
aforementioned ``NT`` folder.

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

In addition, you may need to edit some of the ``NT\rules.msc`` file.
(We actually override the values in ``NT\rules.msc`` using
command-line options to ``nmake``, including an option
``MAKE_INCLUDEFILE=...`` where ``...`` is the name of a file which
contains further assignments to ``nmake`` variables.  See below__.)

__ make_includefile_

Environment Variables
---------------------

Compiler
~~~~~~~~

Make sure that the environment variables that your chosen compiler
needs are set.  A convenient way of doing that is to use the batch
files that are provided by the compilers.  This is most easily done by
using the appropriate entry from the Start Menu, e.g. ``Start Menu``
-> ``All Programs`` -> ``Microsoft Visual Studio 2010`` -> ``Visual
Studio Tools`` -> ``Visual Studio x64 Win64 Command Prompt (2010)``
(this is for a 64-bit build on a 64-bit version of the operating
system).

When using the Intel compiler, you also need to set the ``CC`` and
``CXX`` variables::

 set CC=icl -Qstd=c99 -GR- -Qsafeseh-
 set CXX=icl -Qstd=c99 -GR- -Qsafeseh-

(These are the values for the 10.1 and 11.1 versions, for 9.1 replace
``-Qstd=c99`` with ``-Qc99``.)

Internal Variables
~~~~~~~~~~~~~~~~~~

- ``SOURCE`` - source folder of the MonetDB suite
- ``BUILD`` - build folder of the MonetDB suite (sibling of ``%SOURCE%\NT``)
- ``PREFIX`` - installation folder of the MonetDB suite

We recommend that the ``PREFIX`` environment variable points to a
location that is different from the source and build folders.

PATH and PYTHONPATH
~~~~~~~~~~~~~~~~~~~

Extend your ``Path`` variable to contain the various folders where you
have installed the prerequisite and optional programs.  The ``Path``
variable is a semicolon-separated list of folders which are searched
in succession for commands that you are trying to execute (note, this
is an example: version numbers may differ)::

 rem Python is required
 set Path=C:\Python27;%Path%
 rem Bison (and Diff)
 set Path=%ProgramFiles%\GnuWin32\bin;%Path%
 rem Java is optional, set JAVA_HOME for convenience
 set JAVA_HOME=%ProgramFiles%\Java\jdk1.5.0_16
 set Path=%JAVA_HOME%\bin;%ProgramFiles%\Java\jre1.5.0_16\bin;%Path%
 rem Apache Ant is optional, but required for Java compilation
 set Path=%ProgramFiles%\apache-ant-1.7.1\bin;%Path%

For testing purposes it may be handy to add some more folders to the
``Path``.  This includes the ``bin`` and ``lib`` folders of the
installation, and all DLLs for the libraries used by the build.  Also,
various programs are used during testing, such as diff (from GnuWin32)
and php, and Python modules that were installed need to be found by
the Python interpreter::

 rem PCRE DLL
 set Path=C:\Program Files\PCRE\bin;%Path%
 rem PHP binary
 set Path=C:\Program Files\PHP;%Path%
 rem assuming we're testing MonetDB5 or SQL:
 set Path=%PREFIX%\lib\MonetDB5;%Path%
 set Path=%PREFIX%\bin;%PREFIX%\lib;%Path%
 rem Python module search path
 set PYTHONPATH=%PREFIX%\lib\site-packages;%PYTHONPATH%

Compilation
-----------

Building and Installing
~~~~~~~~~~~~~~~~~~~~~~~

To build and install the whole suite, go to your build folder (assumed
to be a sibling of the top-level ``NT`` folder) and execute the
command::

 nmake /nologo /f ..\NT\Makefile "prefix=%PREFIX%" ...
 nmake /nologo /f ..\NT\Makefile "prefix=%PREFIX%" ... install

The ``...`` needs to be replaced by a list of parameters that tells
the system which of the optional programs and libraries are available
and which components are to be built.  The following parameters are
possible:

- ``DEBUG=1`` - compile with extra debugging information
- ``NDEBUG=1`` - compile without extra debugging information (this is
  used for creating a binary release);
- ``HAVE_MONETDB5=1`` - include the MonetDB5 component;
- ``HAVE_SQL=1`` - include the sql component;
- ``HAVE_GEOM=1`` - include the geom component;
- ``HAVE_JAVA=1`` - include the java component (only use if Java and
  Apache Ant are both available);
- ``HAVE_TESTING=1`` - include the testing component;
- ``HAVE_PYTHON=1`` - include the Python component;
- ``HAVE_ICONV=1`` - the iconv library is available;
- ``HAVE_OPENSSL=1`` - the OpenSSL library is available;
- ``HAVE_PERL=1`` - include the Perl component.

In addition, you can add a parameter which points to a file with extra
definitions for ``nmake``.  This is very convenient to define where
all packages were installed that the build process depends on since
you then don't have to edit the ``rules.msc`` file in the source tree:

.. _make_includefile:

- ``"MAKE_INCLUDEFILE=..."`` - file with extra ``nmake`` definitions.

It is recommended to at least put the ``MAKE_INCLUDEFILE`` parameter
with argument in double quotes to protect any spaces that may appear
in the file name.  The file name should be an absolute path name.

The contents of the file referred to with the ``MAKE_INCLUDEFILE``
parameter may contain something like::

 bits=32
 LIBPERL=C:\Perl
 LIBPCRE=C:\Program Files\PCRE
 LIBICONV=C:\Libraries\iconv-1.11.win32
 LIBZLIB=C:\Libraries\zlib-1.2.8.win32
 LIBXML2=C:\Libraries\libxml2-2.9.2.win32

Building Installers
~~~~~~~~~~~~~~~~~~~

Installers can be built either using the full-blown Visual Studio user
interface or on the command line.  To use the user interface, open one
or more of the files ``MonetDB5-SQL-Installer.sln``,
``MonetDB-ODBC-Driver.sln``, and ``MonetDB5-Geom-Module.sln`` in the
installation folder and select ``Build`` -> ``Build Solution``.  To use
the command line, execute one or more of the commands in the
installation folder::

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
