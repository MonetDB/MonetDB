.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

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

.. _MonetDB: https://dev.monetdb.org/hg/MonetDB/

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
the server.  This component is required.

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

 hg clone https://dev.monetdb.org/hg/MonetDB/

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

- Microsoft Visual Studio 2015 or newer;
- Intel(R) C++ Compiler 9.1;
- Intel(R) C++ Compiler 10.1;
- Intel(R) C++ Compiler 11.1.

Not supported anymore are the GNU C Compiler gcc under Cygwin__.
Using that, it would be possible to build a version that runs using
the Cygwin DLLs, but also a version that uses the MinGW__ (Minimalist
GNU for Windows) package.  This is not supported and not further
described here (in either case, the build process would be much more
like Unix than what is described here).

We currently use Microsoft Visual Studio Community 2015, Microsoft
Visual Studio Community 2017, and Intel(R) C++ Compiler XE 13.1.2.190,
the latter using Microsoft Visual Studio Community 2015.  Older
versions of Visual Studio will not work since they do not support the
C-99 standard.

__ http://www.cygwin.com/
__ http://www.mingw.org/

Python
------

Python_ is needed for creating the configuration files that the
compiler uses to determine which files to compile.  Python can be
downloaded from http://www.python.org/.  Just download and install the
Windows binary distribution.

.. Say something about py2integration.

Note that you can use either or both Python2 and Python3, and on 64
bit architectures, either the 32 bit or 64 bit version of Python.  All
these versions are fine for building the MonetDB suite.  It is
possible to install all versions.  Using Chocolatey_ you can do::

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
download from http://www.cmake.org/ or by using Chocolatey_.  Follow
the Download link and get the Win32 Installer, install it, and run it.
It will come up with a window where you have to fill in the location
of the source code and where to build the binaries.  Fill in where you
extracted the PCRE sources, and some other folder (I used a ``build``
folder which I created within the PCRE source tree), then click on the
Configure button.  This pops up a dialog to choose the compiler.  I
chose Visual Studio 14 2015.

You need to configure some PCRE build options.  I chose to do build
shared libs, to match newlines with the ``ANYCRLF`` option, and to do
have UTF-8 support and support for Unicode properties.  When you're
satisfied with the options, click on Generate.  Then in the build
folder you've chosen, open the PCRE.sln file with Visual Studio, and
build and install.  Make sure you set the Solution Configuration to
Release if you want to build a releasable version of the MonetDB
suite.  By default the library will be installed in ``C:\Program
Files\PCRE``.

For Windows64, select the correct compiler (``Visual Studio 14 2015
Win64``) and proceed normally.  When building the 32 bit version on
Windows64, choose ``C:/Program Files (x86)/PCRE`` for the
``CMAKE_INSTALL_PREFIX`` value, otherwise choose ``C:/Program
Files/PCRE``.

In order to get a version number in the DLL that is produced, we added
a file ``version.rc`` to the sources for the ``pcre`` subproject.  The
contents of the file are::

 #include <Windows.h>
 LANGUAGE		LANG_ENGLISH, SUBLANG_ENGLISH_US
 VS_VERSION_INFO	VERSIONINFO
 FILEVERSION		8,41,0,0	// change as appropriate
 PRODUCTVERSION		8,41,0,0	// change as appropriate
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
client program with the MonetDB server.  The only part of the OpenSSL
library that is used is some of the hash functions, it is not used to
secure communication between client and server processes.  The OpenSSL
library is required for the MonetDB5 component, and hence implicitly
required for the clients component when it needs to talk to a MonetDB5
server.

Download the source from http://www.openssl.org/.  We used the latest
stable version (1.1.0g).  Follow the instructions in the file
``NOTES.WIN``.

.. The actual commands used were::
   perl Configure VC-WIN32 no-asm --prefix=C:\Libraries\openssl-1.1.0g.win32
   nmake
   nmake install
   and::
   perl Configure VC-WIN64A no-asm --prefix=C:\Libraries\openssl-1.1.0g.win64
   nmake
   nmake install

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

On Windows64 you will have to compile libxml2 yourself (with its
optional prerequisites iconv_ and zlib_, for which see below).

Run the following commands in the ``win32`` subfolder, substituting
the correct locations for the iconv and zlib libraries::

 cscript configure.js compiler=msvc prefix=C:\Libraries\libxml2-2.9.8.win64 ^
  include=C:\Libraries\iconv-1.15.win64\include;C:\Libraries\zlib-1.2.11.win64\include ^
  lib=C:\Libraries\iconv-1.15.win64\lib;C:\Libraries\zlib-1.2.11.win64\lib ^
  iconv=yes zlib=yes vcmanifest=yes
 nmake /f Makefile.msvc
 nmake /f Makefile.msvc install

We needed to edit the file ``win32\Makefile.msvc`` and change
``iconv.lib`` to ``iconv.dll.lib``.

__ http://xmlsoft.org/

geos (Geometry Engine Open Souce)
---------------------------------

Geos__ is a library that provides geometric functions.  This library
is only a prerequisite for the geom component.

There are no Windows binaries available (not that I looked very hard),
so to get the software, you will have to get the source and build it
yourself.

Get the source tar ball from http://trac.osgeo.org/geos/#Download and
extract somewhere.  You can follow the instructions in e.g. `Building
on Windows with NMake`__.

We needed to make a few changes to the file ``nmake.opt``.  We needed
to add a blurb for the version of ``nmake`` that we were using.  Look
at the version number of ``nmake /P`` and adapt the closest match.

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

In order to get a version number in the DLL that is produced, we added
a file ``version.rc`` in the ``src`` folder.  The contents of the
file are::

 #include <Windows.h>
 LANGUAGE		LANG_ENGLISH, SUBLANG_ENGLISH_US
 VS_VERSION_INFO	VERSIONINFO
 FILEVERSION		3,6,2,0		// change as appropriate
 PRODUCTVERSION		3,6,2,0		// change as appropriate
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
``C:\Libraries\geos-3.6.2.win32``::

 mkdir C:\Libraries\geos-3.6.2.win32
 mkdir C:\Libraries\geos-3.6.2.win32\lib
 mkdir C:\Libraries\geos-3.6.2.win32\bin
 mkdir C:\Libraries\geos-3.6.2.win32\include
 mkdir C:\Libraries\geos-3.6.2.win32\include\geos
 copy src\geos_c_i.lib C:\Libraries\geos-3.6.2.win32\lib
 copy src\geos_c.dll C:\Libraries\geos-3.6.2.win32\bin
 copy include C:\Libraries\geos-3.6.2.win32\include
 copy include\geos C:\Libraries\geos-3.6.2.win32\include\geos
 copy capi\geos_c.h C:\Libraries\geos-3.6.2.win32\include

__ http://geos.refractions.net/
__ http://trac.osgeo.org/geos/wiki/BuildingOnWindowsWithNMake

Optional Packages
=================

.. _zlib:

zlib
----

Zlib__ is a compression library which is optionally used by both
MonetDB and the iconv library.  The home of zlib is
http://www.zlib.net/, but Windows binaries can be gotten from the same
site as the libxml2 library: http://www.zlatkovic.com/libxml.en.html.
Click on Win32 Binaries on the right, and download zlib.  Install in
e.g. ``C:\Libraries\``.  Note that the at the time of writing, the precompiled
version lags behind: it is version 1.2.8, whereas 1.2.11 is current.

On Windows64 you will have to compile zlib yourself.  Get the source
from the `zlib website`__ and extract somewhere.  There are Visual
Studio project files for building the library.  They produce a library
called ``zlibwapi.dll``.  We haven't tried using this library.
Instead we built using the following command::

 nmake /f win32\Makefile.msc

Create the folder where you want to install the binaries,
e.g. ``C:\Libraries\zlib-1.2.11.win64``, and the subfolders ``bin``,
``include``, and ``lib``.  Copy the files ``zconf.h`` and ``zlib.h``
to the newly created ``include`` folder.  Copy the file ``zdll.lib``
to the new ``lib`` folder, and copy the file ``zlib1.dll`` to the new
``bin`` folder.

Fix the ``LIBZ`` definitions in ``NT\rules.msc`` so that they refer to
the location where you installed the library and call ``nmake`` with
the extra parameter ``HAVE_LIBZ=1``.

__ http://www.zlib.net/
__ http://www.zlib.net/

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
from the `iconv website`__ and extract somewhere.

Follow the instructions for building native binaries using the MS
Visual C/C++ tool chain in the file ``README.windows``.  We installed
the following extra Cygwin packages in order to build successfully:
``mingw64-i686-binutils``, ``mingw64-i686-gcc-core``,
``mingw64-x86_64-binutils``, ``mingw64-x86_64-gcc-core``,
``cygwin32-binutils``, and ``cygwin32-gcc-core``.

.. The commands used (where INCLUDE and LIB are as in the Developer
   Command Prompt and PATH contains the directory where cl.exe can be
   found; for 64 bit use --host=x86_64-w64-mingw32 and adapt LIB, PATH
   and prefix)::
   INCLUDE='C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\INCLUDE;'\
   'C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\ATLMFC\INCLUDE;'\
   'C:\Program Files (x86)\Windows Kits\10\include\10.0.14393.0\ucrt;'\
   'C:\Program Files (x86)\Windows Kits\NETFXSDK\4.6.1\include\um;'\
   'C:\Program Files (x86)\Windows Kits\10\include\10.0.14393.0\shared;'\
   'C:\Program Files (x86)\Windows Kits\10\include\10.0.14393.0\um;'\
   'C:\Program Files (x86)\Windows Kits\10\include\10.0.14393.0\winrt;'
   LIB='C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\LIB;'\
   'C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\ATLMFC\LIB;'\
   'C:\Program Files (x86)\Windows Kits\10\lib\10.0.14393.0\ucrt\x86;'\
   'C:\Program Files (x86)\Windows Kits\NETFXSDK\4.6.1\lib\um\x86;'\
   'C:\Program Files (x86)\Windows Kits\10\lib\10.0.14393.0\um\x86;'
   PATH="/cygdrive/c/Program Files (x86)/Microsoft Visual Studio 14.0/VC/bin:$PATH"
   export INCLUDE LIB PATH
   win32_target=_WIN32_WINNT_WIN7
   prefix=/cygdrive/c/Libraries/iconv-1.15.win32-vs2015
   PATH="$prefix/bin:$PATH"
   ./configure --host=i686-w64-mingw32 --prefix=$prefix \
	       CC="$PWD/build-aux/compile cl -nologo" \
	       CFLAGS="-MD" \
	       CXX="$PWD/build-aux/compile cl -nologo" \
	       CXXFLAGS="-MD" \
	       CPPFLAGS="-D_WIN32_WINNT=$win32_target -I$prefix/include" \
	       LDFLAGS="-L$prefix/lib" \
	       LD="link" \
	       NM="dumpbin -symbols" \
	       STRIP=":" \
	       AR="$PWD/build-aux/ar-lib lib" \
	       RANLIB=":" &&
   make &&
   # make check &&
   make install DESTDIR=$HOME/tmp &&
   rsync -av $HOME/tmp/c/Libraries/ /c/Libraries/

Fix the ``ICONV`` definitions in ``NT\rules.msc`` so that they refer
to the location where you installed the library and call ``nmake``
with the extra parameter ``HAVE_ICONV=1``.

__ http://www.gnu.org/software/libiconv/
__ http://www.gnu.org/software/libiconv/#downloading

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
 LANGUAGE		LANG_ENGLISH, SUBLANG_ENGLISH_US
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
- ``HAVE_TESTING=1`` - include the testing component;
- ``HAVE_PYTHON=1`` - include the Python component;
- ``HAVE_ICONV=1`` - the iconv library is available;
- ``HAVE_OPENSSL=1`` - the OpenSSL library is available;

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
 LIBPCRE=C:\Program Files\PCRE
 LIBICONV=C:\Libraries\iconv-1.11.win32
 LIBZLIB=C:\Libraries\zlib-1.2.8.win32
 LIBXML2=C:\Libraries\libxml2-2.9.2.win32

Building Installers
~~~~~~~~~~~~~~~~~~~

The installers are built using the WiX Toolset.  The WiX Toolset can
be installed using Chocolatey.

The Python scripts ``mksqlwxs.py`` and ``mkodbcwxs.py`` in the ``NT``
subdirectory are used to create the files
``MonetDB5-SQL-Installer.wxs`` and ``MonetDB-ODBC-Installer.wxs``.
This happens as part of the normal build process.

These files then need to be processed using the ``candle`` command
from the WiX Toolset::

  candle.exe -nologo -arch x64 MonetDB5-SQL-Installer.wxs

Use ``-arch x86`` for 32 bit Windows.

This command produces a file ``MonetDB5-SQL-Installer.wixobj`` which
needs to be processed with the ``light`` command from the toolset::

  light.exe -nologo -sice:ICO03 -sice:ICE60 -sice:ICE82 -ext WixUIExtension MonetDB5-SQL-Installer.wixobj

The same for the ODBC driver.
