#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" AND ${CMAKE_SYSTEM_VERSION} VERSION_LESS "19.0.0")
  find_package(CommonCrypto)
else()
  find_package(OpenSSL)
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
  find_package(LZ4)
	if (LZ4_FOUND AND LZ4_VERSION VERSION_LESS "1.8.0")
					unset(LZ4_FOUND)
	endif()
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
  set(HAVE_ODBC "${ODBC_FOUND}")
  if(ODBC_FOUND)
    cmake_push_check_state()
    set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES};${ODBC_INCLUDE_DIR}")
    if(WIN32)
      set(CMAKE_EXTRA_INCLUDE_FILES "${CMAKE_EXTRA_INCLUDE_FILES};Windows.h;sqlext.h;sqltypes.h")
      check_include_file("afxres.h" HAVE_AFXRES_H)
    else()
      set(CMAKE_EXTRA_INCLUDE_FILES "${CMAKE_EXTRA_INCLUDE_FILES};sql.h;sqltypes.h")
    endif()
    check_type_size(SQLLEN _SQLLEN LANGUAGE C)
    if(HAVE__SQLLEN)
      set(LENP_OR_POINTER_T "SQLLEN *")
    else()
      set(LENP_OR_POINTER_T "SQLPOINTER")
    endif()
    check_type_size(SQLWCHAR SIZEOF_SQLWCHAR LANGUAGE C)
    cmake_pop_check_state()
  endif()
endif()

if(RINTEGRATION)
  find_package(LibR)
endif()

# vim: set ts=2:sw=2:et
