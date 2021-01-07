#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
endif()

if(WITH_PCRE)
  find_package(PCRE)
endif()

if(WITH_CRYPTO)
  if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" AND ${CMAKE_HOST_SYSTEM_VERSION} VERSION_LESS "19.0.0")
    find_package(CommonCrypto)
  else()
    find_package(OpenSSL)
  endif()
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

if(WITH_UUID)
  find_package(UUID)
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

find_package(Sphinx)
find_package(Createrepo)
find_package(Rpmbuild)
find_package(DpkgBuildpackage)
find_package(Reprepro)
find_package(Semodule)
find_package(Awk)
find_package(Candle)

# vim: set ts=2:sw=2:et
