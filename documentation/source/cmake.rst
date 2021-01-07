.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

=====
CMake
=====

``Introduction``
================

``Packaging``
=============
The source packages are located at https://www.monetdb.org/downloads/sources/Apr2019-SP1/

RPM
===

The source rpm's are located in https://www.monetdb.org/downloads/Fedora/source/ and are shared by all rpm based repositories. The debug packages are located in https://www.monetdb.org/downloads/Fedora/debug/ and https://www.monetdb.org/downloads/epel/debug/ The binary packages are located in https://www.monetdb.org/downloads/Fedora/ and https://www.monetdb.org/downloads/epel/

DEB
===

The source packages are located in https://www.monetdb.org/downloads/deb/dists/stretch/monetdb/source/ and the binary packages are located at https://www.monetdb.org/downloads/deb/ The source package setup for debian is totally different than for rpm's.

MSI
===

The windows packages are located at https://www.monetdb.org/downloads/Windows/Apr2019-SP1/

Mac OS X
========

The Mac OS X packages are downloaded from https://www.monetdb.org/downloads/MacOSX/ This repository contains a binary tarball and a pkg package. This last one is generated using the "MonetDB.pkgproj" file in the "MacOSX" directory in the source repository. The repository itself is only a download folder, there is no scripting needed to generate the repository itself. The third option to install monetdb on MacOS is to use homebrew. The relevant file is "https://github.com/MonetDB/homebrew-core/blob/master/Formula/monetdb.rb", the repository is in the MonetDB github organization and is a clone of the homebrew repository. This file will need to be changes after switching to cmake. The difficult part will likely be to keep this backwards compatible.

``Releasing``
=============

``Versioning``
==============

``Source Code Management``
==========================

``Testing``
===========

``CMake code structure``
========================

Main file
=========

CmakeLists.txt

options
versions
includes
check features
install directory setup
build header files
add subdirectories
packaging
testing

Options
=======

In cmake, options can have the value ON or OFF. In the autotools system, many configure options where called "enable_xxxx" with the value true or false. To make the sure the cmake command "sounds logical" when read from the commandline, we choose to use short descriptions for the options. For example "enable_cintegration=true" becomes "-DCINTEGRATION=ON".

The idea behind the defaults for the options also has changed from the autotools build system. Previously the idea was to limit the part of the code that was build in order to speed up the compilation. Parts of the code that were not required where only build when explicitly enabled. In the cmake build system this has been reversed. Now as much as possible is build, since compiling everything is now fast enough. In the cmake build system, every optional part is build, as long as the dependent libraries are detected.

It is no longer possible to disable building major parts of the code, such as sql or mapi. And setting a certain option to off really means that you disable detecting the required libraries. For example, "-DODBC=OFF" means don't try detect the odbc development libraries. That implies that the odbc library will not be build.

Most of the library detection is don in the "monetdb-findpackages" cmake file. We want to do this in such a way that we don't need a lot of logic. So for example, for the geom module library we only need to detect the geos library:::

  if(GEOM)
    find_package(Geos)
  endif()

The find_packages function should define a "GEOS_FOUND" variable that is used throughout the rest of the cmake code. If you want to include a cmake file conditionally on the detection of a library, you should use that variable, for example in "geom/monetdb5/CMakeLists.txt" we use "if(GEOS_FOUND)". The only exception is in the monetdb_config.h file. Since the source code still uses legacy variables, usually "HAVE_xxx", we define them using this pattern:::

  #cmakedefine HAVE_GEOM @GEOS_FOUND@

This should give the correct result, because of the way "boolean" values, in combination with empty and non-existing variables work. The only situation where this might go wrong is when the variable explicitly has to have the value 1. This way, we cleanup as much of the legacy variables as possible from the build system. If the build system is setup correctly, the "HAVE_xxxx" variables should not be needed in most cases, because the code will only be build if the value = 1.

Build type
""""""""""

Install prefix
""""""""""""""

Verbose
"""""""

Debug find
""""""""""

Prefix path
"""""""""""

Toolchain files
"""""""""""""""

Custom targets
==============

Help
""""

install
"""""""

test
""""

mtest
"""""

Installing
==========

To configure an install of a target, you have to define two things, where to install the file and the component that the file belongs to. And this has to be done for all the files that are part of the target. See the following example from the streams library to get an idea on how something would look:::

  install(TARGETS
    stream
    EXPORT streamTargets
    RUNTIME
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT stream
    LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    COMPONENT stream
    ARCHIVE
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    COMPONENT stream
    PUBLIC_HEADER
    COMPONENT streamdev
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    COMPONENT streamdev)

When defining the destination, always use the "CMAKE\_INSTALL\_???DIR" variables, not the "CMAKE\_INSTALL\_FULL\_???DIR" ones. See https://cmake.org/cmake/help/latest/module/GNUInstallDirs.html for the details, but the important part is that you should let CMake handle the "CMAKE\_INSTALL\_PREFIX". Only when the value is used inside the C code, or python scripts, you want to use the actual absolute paths.

Unspecified
===========

- clients/odbc/samples

``Legacy``
==========

Numpy detection
===============

Since the Python3::Numpy target is not available before version 3.14, we need an alternative. We use a NumPy detection function from a github project: https://raw.githubusercontent.com/fperazzi/davis-2017/master/cmake/FindNumPy.cmake This is MIT licensed, so we can include this in MonetDB. We change the python detection to detect python3, instead of python2.

shp.h define conflicts
======================

We now disable some defines in sql/backends/monet5/vaults/shp/shp.h that were defined in monetdb_config.h because they conflict with defines in gdal library header files.::

  /* these were previously defined in monetdb_config.h */
  #undef HAVE_DLFCN_H
  #undef HAVE_FCNTL_H
  #undef HAVE_ICONV
  #undef HAVE_STRINGS_H

kvm library
===========

The kvm library is only used on \*bsd. We already remove the header detection from cmakelists.txt: "find_path(HAVE_KVM_H "kvm.h")".

WIX packages
============

You need to install the wixtoolset (with chocolaty): ::

  choco install wixtoolset

Then you can run the following command from the build directory: ::

  "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\CPack" -G WIX -C Debug --config CPackConfig.cmake

This will create the "msi" file.
