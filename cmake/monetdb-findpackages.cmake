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
  if(Python3_Interpreter_FOUND)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
  endif(Python3_Interpreter_FOUND)
  if(NumPy_FOUND)
    set(Python3_NumPy_FOUND 1)
  endif(NumPy_FOUND)
else()
  find_package(Python3 COMPONENTS Interpreter Development NumPy)
  if(Python3_Interpreter_FOUND)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
  endif(Python3_Interpreter_FOUND)
endif()
if(PY3INTEGRATION)
  set(HAVE_LIBPY3 "${Python3_NumPy_FOUND}")
else()
  message(STATUS "Disable Py3integration, because required NumPy is missing")
endif(PY3INTEGRATION)

if(WIN32)
  find_library(GETOPT_LIB "getopt.lib")
endif()

if(WITH_LZMA)
  find_package(Lzma)
endif()

if(WITH_XML2)
  find_package(LibXml2)
endif()

if(WITH_PCRE)
  find_package(PCRE)
else()
  check_symbol_exists("regcomp" "regex.h" HAVE_POSIX_REGEX)
endif()
if(NOT PCRE_FOUND AND NOT HAVE_POSIX_REGEX)
  message(FATAL_ERROR "PCRE library or GNU regex library not found but required for MonetDB5")
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
  find_package(CURL CONFIG)
endif()

if(WITH_ZLIB)
  find_package(ZLIB)
endif()

if(WITH_LZ4)
  find_package(LZ4)
endif()

if(WITH_PROJ)
  find_package(Proj)
endif()

if(WITH_SNAPPY)
  find_package(Snappy CONFIG)
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

if(CINTEGRATION)
  set(HAVE_CUDF ON CACHE INTERNAL "C udfs extension is available")
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

if(LIDAR)
  find_package(Lidar)
endif()

if(ODBC)
  find_package(ODBC)
  set(HAVE_ODBC "${ODBC_FOUND}")
  if(ODBC_FOUND)
    cmake_push_check_state()
    set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES};${ODBC_INCLUDE_DIR}")
    if(WIN32)
      set(CMAKE_EXTRA_INCLUDE_FILES "${CMAKE_EXTRA_INCLUDE_FILES};Windows.h;sqlext.h;sqltypes.h")
      find_path(HAVE_AFXRES_H "afxres.h")
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
  endif(ODBC_FOUND)
endif(ODBC)

if(RINTEGRATION)
  find_package(LibR)
  set(HAVE_LIBR "${LIBR_FOUND}")
  set(RHOME "${LIBR_HOME}")
endif()

# vim: set ts=2:sw=2:et
