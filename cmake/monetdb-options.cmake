#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

option(RELEASE_VERSION
  "Use release values for version variables"
  OFF)

# In case of .hg/.git we assume a development
if(EXISTS "${CMAKE_SOURCE_DIR}/.hg" OR EXISTS "${CMAKE_SOURCE_DIR}/.git")
  set(DEVELOPMENT TRUE)
else()
  set(DEVELOPMENT FALSE)
endif()

option(TESTING
  "Enable support for testing"
  ON)

cmake_dependent_option(CINTEGRATION
  "Enable support for C UDFs (default=ON)"
  ON
  "NOT WIN32"
  OFF)

option(WITH_SQLPARSE
  "Compile and install the sqlparse utility (default=ON)"
  ON)

option(WITH_RTREE
  "Enable support for rtrees (librtree; default=ON)"
  ON)

option(PY3INTEGRATION
  "Enable support for Python 3 integration into MonetDB (default=ON)"
  ON)

option(RINTEGRATION
  "Enable support for R integration into MonetDB (default=ON)"
  ON)

option(FITS
  "Enable support for FITS (default=ON)"
  ON)

option(FORCE_COLORED_OUTPUT
  "Force colored compiler output (GCC and Clang only; default=OFF)"
  OFF)

option(GEOM
  "Enable support for geom module (default=ON)"
  ON)

option(INT128
  "Enable support for 128-bit integers (default=ON)"
  ON)

option(NETCDF
  "Enable support for netcdf (default=ON)"
  ON)

option(ODBC
  "Compile the MonetDB ODBC driver (default=ON)"
  ON)

cmake_dependent_option(SHP
  "Enable support for ESRI Shapefiles (default=ON)"
  ON
  "GEOM"
  OFF)

option(SANITIZER
  "Enable support for the GCC address sanitizer (default=OFF)"
  OFF)

option(UNDEFINED
  "Enable support for the GCC undefined sanitizer (default=OFF)"
  OFF)

option(PGOTRAIN
  "Enable support for the profile generated optimization training (default=OFF)"
  OFF)

option(PGOBUILD
  "Enable support for the profile generated optimization build (using obtained data) (default=OFF)"
  OFF)

option(STRICT
  "Enable strict compiler flags (default=ON for development sources, OFF for tarball installation)"
  "${DEVELOPMENT}")

option(TAGS
  "Enable tags usage (ctags and/or cscope) )"
  OFF)

option(ASSERT
  "Enable asserts (default=ON for development sources, OFF for tarball installation)"
  "${DEVELOPMENT}")

option(WITH_BZ2
  "Include bz2 support"
  ON)

option(WITH_CURL
  "Include curl support"
  ON)

option(WITH_LZMA
  "Include lzma support"
  ON)

option(WITH_XML2
  "Include xml2 support"
  ON)

option(WITH_CMOCKA
  "Include cmocka support"
  OFF)

option(WITH_LZ4
  "Include lz4 support"
  ON)

option(WITH_PROJ
  "Include proj support"
  ON)

option(WITH_READLINE
  "Include readline support"
  ON)

option(WITH_PCRE
  "Include pcre support"
  ON)

option(WITH_VALGRIND
  "Include valgrind support"
  ON)

option(WITH_ZLIB
  "Include zlib support"
  ON)

option(WITH_OPENSSL
  "Include TLS support"
  ON)

option(CMAKE_SUMMARY
  "Show a summary of the cmake configuration (for debug purposes)"
  OFF)

option(CMAKE_UNITTESTS
  "Build and run the unittest for the build system"
  OFF)

option(MONETDB_STATIC
  "Enable static compilation mode"
  OFF)

option(WITH_UDF
  "Include UDF support"
  ON)

option(WITH_VAULTS
  "Include UDF support"
  ON)

option(WITH_MEROVINGIAN
  "Build merovingian and friends"
  ON)

option(WITH_MSERVER5
  "Build mserver5"
  ON)

option(WITH_MAPI_CLIENT
  "Build mapi clients(mclient, msqldump)"
  ON)
