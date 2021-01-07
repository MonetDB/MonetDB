The MonetDB Database System
===========================

The MonetDB database was originally developed by the `CWI`__ `database
research group`__ (see https://www.monetdb.org/).  Development has now
shifted to the spin-off company `MonetDB Solutions`__.

Via the MonetDB project we have brought the MonetDB system in open
source, where it is accessible at https://www.monetdb.org/Downloads/.
Even though development happens mostly in a company, the MonetDB
database system will remain open source.  It is available under the
`Mozilla Public License 2.0`__.

The MonetDB database system is a high-performance database kernel for
query-intensive applications. The MonetDB source can be found at our
`Mercurial server`__. There is also a `github mirror`__ that is updated
once an hour.

.. _CWI: https://www.cwi.nl/
__ CWI_

.. _DA: https://www.cwi.nl/research/groups/database-architectures
__ DA_

.. _solutions: https://www.monetdbsolutions.com
__ solutions_

.. _mpl: http://mozilla.org/MPL/2.0/
__ mpl_

.. _MonetDB: https://dev.monetdb.org/hg/MonetDB/
__ MonetDB_

.. _github: https://github.com/MonetDB/MonetDB
__ github_

Building
--------

MonetDB is built using the ``cmake`` program.  It is recommended to
build in a directory that is not inside the source tree.  In order to
build, use the following commands when inside your build directory::

  cmake [options] /path/to/monetdb/source
  cmake --build .
  cmake --build . --target install

In order to install into a different directory than the default
``/usr/local``, add the option ``-DCMAKE_INSTALL_PREFIX``::

  cmake -DCMAKE_INSTALL_PREFIX=/path/to/install/monetdb /path/to/monetdb/source
  cmake --build .
  cmake --build . --target install

Build Options
.............

There are many options that can be used to select how MonetDB is to be
built.  Options can be turned ``ON`` or ``OFF`` using a ``-D`` flag on
the first of the ``cmake`` command lines.  Except when specified
otherwise, options are ``ON`` when the relevant libraries can be found.
Available options are:

==============  ===============================================================================================
Option          Explanation
==============  ===============================================================================================
ASSERT          Enable asserts (default=ON for development sources, OFF for tarball installation)
CINTEGRATION    Enable support for C UDFs (default=ON except on Windows)
CMAKE_SUMMARY   Show a summary of the cmake configuration (for debug purposes, default=OFF)
CMAKE_UNITTEST  Build and run the unittest for the build system (default=OFF)
FITS            Enable support for FITS
GEOM            Enable support for geom module
INT128          Enable support for 128-bit integers
NETCDF          Enable support for netcdf
ODBC            Compile the MonetDB ODBC driver
PY3INTEGRATION  Enable support for Python 3 integration into MonetDB
RINTEGRATION    Enable support for R integration into MonetDB
SANITIZER       Enable support for the GCC address sanitizer (default=OFF)
SHP             Enable support for ESRI Shapefiles
STRICT          Enable strict compiler flags (default=ON for development sources, OFF for tarball installation)
TESTING         Enable support for testing
WITH_BZ2        Include bz2 support
WITH_CMOCKA     Include cmocka support (default=OFF)
WITH_CRYPTO     Only in very some special cases we build without crypto dependencies
WITH_CURL       Include curl support
WITH_LZMA       Include lzma support
WITH_PCRE       Include pcre support
WITH_PROJ       Include proj support
WITH_READLINE   Include readline support
WITH_UUID       Include uuid support
WITH_VALGRIND   Include valgrind support
WITH_XML2       Include xml2 support
WITH_ZLIB       Include zlib support
==============  ===============================================================================================

Required packages
.................

On Fedora, the following packages are required:
``bison``, ``cmake``, ``gcc``, ``openssl-devel``, ``pkgconf``,
``python3``.

The following packages are optional but recommended:
``bzip2-devel``, ``libuuid-devel``, ``pcre-devel``, ``readline-devel``,
``xz-devel``, ``zlib-devel``.

The following packages are optional:
``cfitsio-devel``, ``gdal-devel``, ``geos-devel``, ``libasan``,
``libcurl-devel``, ``libxml2-devel``, ``netcdf-devel``, ``proj-devel``,
``python3-devel``, ``python3-numpy``, ``R-core-devel``,
``unixODBC-devel``, ``valgrind-devel``.

On Ubuntu and Debian the following packages are required:
``bison``, ``cmake``, ``gcc``, ``libssl-dev``, ``pkg-config``,
``python3``.

The following packages are optional but recommended:
``libbz2-dev``, ``uuid-dev``, ``libpcre3-dev``, ``libreadline-dev``,
``liblzma-dev``, ``zlib1g-dev``.

The following packages are optional:
``libasan5``, ``libcfitsio-dev``, ``libcurl4-gnutls-dev``,
``libgdal-dev``, ``libgeos-dev``, ``libnetcdf-dev``, ``libproj-dev``,
``libxml2-dev``, ``python3-dev``, ``python3-numpy``, ``r-base-dev``,
``unixodbc-dev``, ``valgrind``.

``cmake`` must be at least version 3.12, ``python`` must be at least
version 3.5.

Bugs
----

We of course hope there aren't any, but if you do find one, you can
report bugs in our `github`__ repository.

Please note that we do not accept github Pull Requests. See the
`developers`__ page for instructions.

.. _github: https://github.com/MonetDB/MonetDB/issues
__ github_

.. _developers: https://www.monetdb.org/Developers
__ developers_

Copyright Notice
================

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
