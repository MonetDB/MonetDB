****************************
Building MonetDB from source
****************************

Summary
=======

For cmake, you should always build the code in a separate directory, say
"build". This directory should be outside of the source code tree. The
results of the build are stored in this directory. The location on the
filesystem is not important, as long as you have permissions to write in
that location.

Assuming the MonetDB source code is checked out in directory
"/path/to/monetdb/source", and if you have all the required packages
(see below) to build MonetDB, these are the set of commands to build and
install it from source. Install is one of the predefined targets
[``install``, ``test``, ``mtest``]. When you test MonetDB, you will
likely not want to install it in the default location, the standard GNU
directory structure. So you may want to set the install prefix variable
when generating the build system, using ``-DCMAKE_INSTALL_PREFIX``::

  mkdir build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=/path/to/install/monetdb /path/to/monetdb/source
  cmake --build .
  cmake --build . --target install


Prerequisites
=============

PATH settings: None
ROle of clients?? How to install

## Testing

For testing, you likely don't want to install in the default location,
so you need to add the installation prefix parameter to the cmake
command. But you do not need any configuration to run mtest (on
Linux). Just run the command::

  cmake --build . --target mtest

Configuration options
=====================

The way options interact with building of the MonetDB source has
fundamentally changed from the way this was done using the autotools
buildsystem. Now almost all options are on by default. And these options
mostly control library detection. In the old system, it was possible to
build a subset of the codebase. For example, you could choose not to
build the sql part. Now the every part of the code is build, as long as
the dependent libraries are detected. And by default, the system would
try to detect all dependent libraries. If your system does not have a
required library, that section of the code will not be build. Only if
you want to prevent the build of a certain section, you could use the
option to prevent that a dependency is detected.

Evidently there are several options to control as illustrated in
``$SOURCE/cmake/monetdb-options.cmake``

The important once to choose from are ``-DCMAKE_BUILD_TYPE``, which
takes the value Release or Debug.  The former creates the binary ready
for shipping, including all compiler optimizations that come with it.
The Debug mode is necessary if you plan to debug the binary and needs
access to the symbol tables.  This build type also typically leads to a
slower execution time, because also all kinds of assertions are being
checked.

Other relevant properties are also ``-DASSERT=ON`` and ``-DSTRICT=ON``,
used in combination with a Debug build, e.g.::

  CONFIGURE_OPTIONS="-DCMAKE_BUILD_TYPE=Debug -DASSERT=ON -DSTRICT=ON"
  mkdir build
  cd build
  cmake $CONFIGURE_OPTIONS -DCMAKE_INSTALL_PREFIX=/path/to/install/monetdb /path/to/monetdb/source
  cmake --build .
  cmake --build . --target install

Explain the role of cmake --build . --target mtest
In particular how to call it from anywhere in the tree

Platform specifics
==================

The packages required to build MonetDB from source depends mostly on the
operating system environment.  They are specified in the corresponding
README files,

README-Debian .... which version

README-Fedora .... Which version


Windows
=======

MacOS
=====

How to start
============
