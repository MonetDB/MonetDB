#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
#]]

# Detect required packages
find_package(BISON REQUIRED)
find_package(Iconv)
find_package(Threads)

if(${CMAKE_VERSION} VERSION_LESS "3.14.0")
  find_package(Python3 COMPONENTS Interpreter Development)
  find_package(NumPy)
else()
  find_package(Python3 COMPONENTS Interpreter Development NumPy)
endif()

if(WITH_LZMA)
  find_package(Lzma)
endif()

if(WITH_XML2)
  find_package(LibXml2)
endif()

if(WITH_CMOCKA)
  find_package(CMocka)
  if (TARGET cmocka::cmocka)
    set(CMOCKA_LIBRARY cmocka::cmocka)
  endif()
endif()

if(WITH_PCRE)
  find_package(PCRE)
endif()

if(WITH_BZ2)
  find_package(BZip2)
endif()

if(WITH_CURL)
  find_package(CURL)
endif()

if(WITH_ZLIB)
  find_package(ZLIB)
endif()

if(WITH_LZ4)
  find_package(LZ4 1.8.0)
endif()

if(WITH_PROJ)
  find_package(Proj)
endif()

if(WITH_SNAPPY)
  find_package(Snappy)
endif()

if(WITH_VALGRIND)
  find_package(Valgrind)
endif()

if(WITH_READLINE)
  find_package(Readline)
endif()

if(FITS)
  find_package(CFitsIO)
endif()

if(NETCDF)
  find_package(NetCDF)
endif()

find_package(KVM)

if(GEOM)
  find_package(Geos)
endif()

if(SHP)
  if(NOT GEOS_FOUND)
    message(STATUS "Disable SHP, geom module required for ESRI Shapefile vault")
  else()
    find_package(GDAL)
  endif()
endif()

if(ODBC)
  find_package(ODBC)
  find_package(ODBCinst)
endif()

if(RINTEGRATION)
  find_package(LibR)
endif()

if(WITH_MALLOC)
  find_package(PkgConfig QUIET)
  if(${WITH_MALLOC} STREQUAL "mimalloc")
    find_package(mimalloc REQUIRED)
    add_library(Malloc::Malloc ALIAS mimalloc)
    set(MALLOC_FOUND 1)
  else()
    pkg_search_module(PC_MALLOC ${WITH_MALLOC} lib${WITH_MALLOC})

    if(PC_MALLOC_FOUND)
      set(MALLOC_FOUND 1)
      find_library(MALLOC_LIBRARY ${PC_MALLOC_LIBRARIES} HINTS ${PC_MALLOC_LIBDIR} ${PC_MALLOC_LIBRARY_DIRS})
      add_library(Malloc::Malloc UNKNOWN IMPORTED)
      set_target_properties(Malloc::Malloc
        PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${PC_MALLOC_INCLUDE_DIR}"
        IMPORTED_LINK_INTERFACE_LANGUAGES "C" IMPORTED_LOCATION "${MALLOC_LIBRARY}")
    endif()
  endif()
endif()

find_package(Sphinx)
find_package(Createrepo)
find_package(Rpmbuild)
find_package(DpkgBuildpackage)
find_package(Reprepro)
find_package(Semodule)
find_package(Awk)
find_package(Candle)
