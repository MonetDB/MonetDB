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
- Intel(R) C++ Compiler 9.1 (which actually needs one of the above).

Note that the pathfinder component can currently not be compiled with
any of the Microsoft compilers.  It can be compiled with the Intel
compiler.

Not supported anymore (but probably still possible) are the GNU C
Compiler gcc under Cygwin__.  Using that it (probably still) is possible
to build a version that runs using the Cygwin DLLs, but also a version
that uses the MinGW__ (Minimalist GNU for Windows) package.  This is
not further described here.

__ http://www.cygwin.com/
__ http://www.mingw.org/

Python
------

Python__ is needed for creating the configuration files that the
compiler uses to determine which files to compile.  Python can be
downloaded from http://www.python.org/.  Just download and install the
Windows binary distribution.

On Windows64 you can use either the 32-bit or 64-bit version of Python
for creating the configuration files, but you will need the 64-bit
version if you want to create the optional Python client modules.

__ http://www.python.org/

Bison
-----

Bison is a reimplementation of YACC (Yet Another Compiler Compiler), a
program to convert a grammar into working code.

A version of Bison for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Download Packages
link on the left and then on Bison, and get the Setup file and install
it.

Flex
----

Flex is a fast lexical analyzer generator.

A version of Flex for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Download Packages
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
``pthreadsVC2.dll`` and ``pthreadsVC2.lib`` to ``C:\Pthreads\lib``.

Diff
----

Diff is a program to compare two versions of a file and list the
differences.  This program is not used during the build process, but
only during testing.  As such it is not a strict prerequisite.

A version of Diff for Windows can be gotten from the GnuWin32 project
at http://gnuwin32.sourceforge.net/.  Click on the Download Packages
link on the left and then on DiffUtils (note the name), and get the
Setup file and install it.

PsKill
------

PsKill is a program to kill (terminate) processes.  This program is
only used during testing to terminate tests that take too long.

PsKill is part of the `Windows Sysinternals`__.  Go to the Process
Utilities, and get the PsKill package.  PsKill is also part of the
PsTools package, so you can get that instead.  Extract the archive,
and make sure that the folder is in your ``Path`` variable when you
run the tests.

__ http://www.microsoft.com/technet/sysinternals/default.mspx

libxml2
-------

Libxml2__ is the XML C parser and toolkit of Gnome.

This library is only a prerequisite for the pathfinder component.

The home of the library is http://xmlsoft.org/.  But Windows binaries
can be gotten from http://www.zlatkovic.com/libxml.en.html.  Click on
Win32 Binaries on the right, and download libxml2, iconv, and zlib.
Install these in e.g. ``C:\``.

On Windows64 you will have to compile libxml2 yourself (with its
optional prerequisites iconv and zlib).

__ http://xmlsoft.org/

Optional Packages
=================

Perl
----

Perl__ is only needed to create an interface that can be used from a
Perl program to communicate with a MonetDB server.

We haven't tried Perl ourselves yet on Windows, so no help here.
ActiveState's ActivePerl at http://www.activestate.com looks like a
promising distribution.

__ http://www.perl.org/

PHP
---

PHP__ is only needed to create an interface that can be used from a PHP
program to communicate with a MonetDB server.

Download the Windows binaries in a ZIP package (i.e. not the Windows
installer) and the source package of PHP 5 from http://www.php.net/.
Unzip the binaries into e.g. ``C:\php-5`` (For PHP-5, the ZIP file
does not contain a top-level directory, so create a new directory,
e.g. ``C:\php-5``, and unzip the files there; for PHP-4, the ZIP file
does contain a top-level directory, so you can unzip directly into
``C:\``.)  Also extract the source distribution somewhere, e.g. into
the directory where the ZIP package was extracted.

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

PCRE (Perl Compatible Regular Expressions)
------------------------------------------

The PCRE__ library is used to extend the string matching capabilities
of MonetDB.

Download the source from http://www.pcre.org/.  In order to build the
library, you will need a program called ``cmake`` which you can
download from http://www.cmake.org/.  Follow the Download link and get
the Win32 Installer, install it, and run it.  It will come up with a
window where you have to fill in the location of the source code and
where to build the binaries.  Fill in where you extracted the PCRE
sources, and some other directory (I used a ``build`` directory which
I created within the PCRE source tree).  You need to configure some
PCRE build options.  I chose to do build shared libs, and to do have
UTF-8 support and support for Unicode properties.  When you're
satisfied with the options, click on Configure, and then on OK.  Then
in the build directory you've chosen, open the PCRE.sln file with
Visual Studio, and build and install.  Make sure you set the Solution
Configuration to Release if you want to build a releasable version of
the MonetDB suite.  The library will be installed in ``C:\Program
Files\PCRE``.

For Windows64, select the correct compiler (``Visual Studio 8 2005
Win64``) and proceed normally.  When building the 32 bit version on
Windows64, choose ``C:/Program Files (x86)/PCRE`` for the
``CMAKE_INSTALL_PREFIX`` value.

__ http://www.pcre.org/

SWIG (Simplified Wrapper and Interface Generator)
-------------------------------------------------

We use SWIG__ to build interface files for Perl and Python.  You can
download SWIG from http://www.swig.org/download.html.  Get the latest
swigwin ZIP file and extract it somewhere.  It contains the
``swig.exe`` binary.

__ http://www.swig.org/

Java
----

The most important use of Java__ that we make is for building the JDBC
driver.  Get Java from http://java.sun.com/, but make sure you do
*not* get the latest version.  Get the Java Development Kit 1.5.  Our
current JDBC driver is not compatible with Java 1.6 yet, and the XRPC
wrapper which is part of the pathfinder component is not compatible
with Java 1.4 or older.

In addition to the Java Development Kit, you will also need Apache Ant
which is responsible for the actual building of the driver.

__ http://java.sun.com/

Apache Ant
----------

`Apache Ant`__ is a program to build other programs.

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

- sql (requires MonetDB4 or MonetDB5--MonetDB5 is recommended)
- pathfinder (requires MonetDB4)

Apart from buildtools, all packages contain a subfolder ``NT`` which
contains a few Windows-specific source files, and which is the
directory in which the Windows version is built.  (On Unix/Linux we
recommend to build in a new directory which is not part of the source
tree, but on Windows we haven't made this separation.)

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

- Intel(R) C++ Compiler 9.1::

   call "%ProgramFiles%\Intel\Compiler\C++\9.1\IA32\Bin\iclvars.bat"

Internal Variables
~~~~~~~~~~~~~~~~~~

- ``MONETDB_SOURCE`` - source folder of the MonetDB package
- ``CLIENTS_SOURCE`` - source folder of the clients package
- ``MONETDB4_SOURCE`` - source folder of the MonetDB4 package
- ``MONETDB5_SOURCE`` - source folder of the MonetDB5 package
- ``SQL_SOURCE`` - source folder of the sql package
- ``PATHFINDER_SOURCE`` - source folder of the pathfinder package

- ``MONETDB_BUILD`` - build folder of the MonetDB package (i.e. ``%MONETDB_SOURCE%\NT%``)
- ``CLIENTS_BUILD`` - build folder of the clients package (i.e. ``%CLIENTS_SOURCE%\NT%``)
- ``MONETDB4_BUILD`` - build folder of the MonetDB4 package (i.e. ``%MONETDB4_SOURCE%\NT%``)
- ``MONETDB5_BUILD`` - build folder of the MonetDB5 package (i.e. ``%MONETDB5_SOURCE%\NT%``)
- ``SQL_BUILD`` - build folder of the sql package (i.e. ``%SQL_SOURCE%\NT%``)
- ``PATHFINDER_BUILD`` - build folder of the pathfinder package (i.e. ``%PATHFINDER_SOURCE%\NT%``)

- ``MONETDB_PREFIX`` - installation folder of the MonetDB package
- ``CLIENTS_PREFIX`` - installation folder of the clients package
- ``MONETDB4_PREFIX`` - installation folder of the MonetDB4 package
- ``MONETDB5_PREFIX`` - installation folder of the MonetDB5 package
- ``SQL_PREFIX`` - installation folder of the sql package
- ``PATHFINDER_PREFIX`` - installation folder of the pathfinder package

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
 set Path=C:\php-5;%Path%
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
MonetDB5 (there is some very preliminary support).  Sql requires one
or both of MonetDB4 and MonetDB5 (the latter is recommended).

For each of the components, do the following::

 cd ...\<component>\NT
 nmake /nologo NEED_MX=1 ... "prefix=%..._PREFIX%"
 nmake /nologo NEED_MX=1 ... "prefix=%..._PREFIX%" install

Here the first ``...`` needs to be replaced by a list of parameters
that tell the system which of the optional programs and libraries are
available.  The following parameters are possible:

- ``DEBUG=1`` - compile with extra debugging information
- ``NDEBUG=1`` - compile without extra debugging information (this is
  used for creating a binary release);
- ``HAVE_JAVA=1`` - Java and Apache Ant are both available;
- ``HAVE_MONETDB4=1`` - for sql and pathfinder: MonetDB4 was compiled
  and installed;
- ``HAVE_MONETDB5=1`` - for sql and pathfinder: MonetDB5 was compiled
  and installed;
- ``HAVE_PERL=1`` - Perl is available;
- ``HAVE_PERL_DEVEL=1`` - Perl development is possible (include files
  and libraries are available--also need ``HAVE_PERL=1``);
- ``HAVE_PERL_SWIG=1`` - Perl development is possible and SWIG is
  available (also need ``HAVE_PERL=1``);
- ``HAVE_PHP=1`` - PHP is available;
- ``HAVE_PROBXML=1`` - compile in support for probabilistic XML (an
  experimental extension to the pathfinder component);
- ``HAVE_PYTHON=1`` - Python is available;
- ``HAVE_PYTHON_SWIG=1`` - Python and SWIG are both available (also
  need ``HAVE_PYTHON=1``).
