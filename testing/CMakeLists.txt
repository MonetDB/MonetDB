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

set(PYTHON "${Python_EXECUTABLE}")
set(VERSION "${MONETDB_VERSION}")

set(QXbindir "${CMAKE_INSTALL_FULL_BINDIR}")
set(QXBUILD "${CMAKE_BINARY_DIR}")
set(QXdatadir "${CMAKE_INSTALL_DATADIR}")
set(QXdatarootdir "${CMAKE_INSTALL_DATAROOTDIR}")
set(QXexec_prefix "${CMAKE_INSTALL_PREFIX}")
set(QXincludedir "${CMAKE_INSTALL_INCLUDEDIR}")
set(QXinfodir "${CMAKE_INSTALL_INFODIR}")
set(QXlibdir "${CMAKE_INSTALL_FULL_LIBDIR}")
set(QXlibexecdir "${CMAKE_INSTALL_LIBEXECDIR}")
if (WIN32)
  string(REGEX REPLACE "[/\\]" "\\\\\\\\" QXlocalstatedir "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}")
else()
  set(QXlocalstatedir "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}")
endif()
set(QXmandir "${CMAKE_INSTALL_MANDIR}")
set(QXprefix "${CMAKE_INSTALL_PREFIX}")
set(QXPYTHON "${Python3_EXECUTABLE}")
if (WIN32)
  string(REGEX REPLACE "[/\\]" "\\\\\\\\" QXPYTHON_LIBDIR "${PYTHON3_LIBDIR}")
else()
  set(QXPYTHON_LIBDIR "${PYTHON3_LIBDIR}")
endif()
set(QXSOURCE "${CMAKE_SOURCE_DIR}")
set(QXsysconfdir "${CMAKE_INSTALL_FULL_SYSCONFDIR}")
set(SOURCE "${CMAKE_SOURCE_DIR}")

if (HAVE_CUDF)
  set(HAVE_CUDF_FALSE "#")
endif(HAVE_CUDF)
if (HAVE_CURL)
  set(HAVE_CURL_FALSE "#")
endif(HAVE_CURL)
if (HAVE_FITS)
  set(HAVE_FITS_FALSE "#")
endif(HAVE_FITS)
if (HAVE_GEOM)
  set(HAVE_GEOM_FALSE "#")
endif(HAVE_GEOM)
if (HAVE_HGE)
  set(HAVE_HGE_FALSE "#")
endif(HAVE_HGE)
if (HAVE_LIBBZ2)
  set(HAVE_LIBBZ2_FALSE "#")
endif(HAVE_LIBBZ2)
if (HAVE_LIBLZ4)
  set(HAVE_LIBLZ4_FALSE "#")
endif(HAVE_LIBLZ4)
if (HAVE_LIBLZMA)
  set(HAVE_LIBLZMA_FALSE "#")
endif(HAVE_LIBLZMA)
if (HAVE_LIBPCRE)
  set(HAVE_LIBPCRE_FALSE "#")
endif(HAVE_LIBPCRE)
if (HAVE_LIBPY3)
  set(HAVE_LIBPY3_FALSE "#")
endif(HAVE_LIBPY3)
if (HAVE_LIBR)
  set(HAVE_LIBR_FALSE "#")
endif(HAVE_LIBR)
if (HAVE_LIBXML)
  set(HAVE_LIBXML_FALSE "#")
endif(HAVE_LIBXML)
if (HAVE_LIBZ)
  set(HAVE_LIBZ_FALSE "#")
endif(HAVE_LIBZ)
if (HAVE_NETCDF)
  set(HAVE_NETCDF_FALSE "#")
endif(HAVE_NETCDF)
if (ODBC_FOUND)
  set(HAVE_ODBC_FALSE "#")
endif(ODBC_FOUND)
if (HAVE_PROJ)
  set(HAVE_PROJ_FALSE "#")
endif(HAVE_PROJ)
if (HAVE_SHP)
  set(HAVE_SHP_FALSE "#")
endif(HAVE_SHP)
if (HAVE_GETENTROPY)
  set(HAVE_GETENTROPY_FALSE "#")
endif(HAVE_GETENTROPY)
if (HAVE_OPENSSL)
  set(HAVE_OPENSSL_FALSE "#")
endif(HAVE_OPENSSL)
if (NOT WIN32)
  set(NOT_WIN32_FALSE "#")
endif()
if (SANITIZER)
  set(SANITIZER_FALSE "#")
endif()

configure_file(Mtest.py.in
  ${CMAKE_CURRENT_BINARY_DIR}/Mtest.py
  @ONLY)

configure_file(Mz.py.in
  ${CMAKE_CURRENT_BINARY_DIR}/Mz.py
  @ONLY)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/Mtest.py
  PERMISSIONS ${PROGRAM_PERMISSIONS_DEFAULT}
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT pytesting)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/Mz.py
  PERMISSIONS ${PROGRAM_PERMISSIONS_DEFAULT}
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT pytesting)

if(PYTHON3_LIBDIR)
  install(FILES
    process.py
    __init__.py
    listexports.py
    exportutils.py
    melcheck.py
    sqllogictest.py
    tpymonetdb.py
    explain.py
    plan.py
    floatutil.py
    mapicursor.py
    malmapi.py
    helpers.py
    sqltest.py
    tlstester.py
    utils.py
    DESTINATION ${PYTHON3_LIBDIR}/MonetDBtesting
    COMPONENT pytesting)
endif()

if(WIN32)
  configure_file(monetdb_mtest.bat.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_mtest.bat
    @ONLY)

  install(FILES
    Mtest.py.bat
    Mz.py.bat
    DESTINATION ${CMAKE_INSTALL_BINDIR})

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_mtest.bat
    PERMISSIONS ${PROGRAM_PERMISSIONS_DEFAULT}
    DESTINATION ${CMAKE_INSTALL_BINDIR})
else()
  configure_file(monetdb_mtest.sh.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_mtest.sh
    @ONLY)

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_mtest.sh
    PERMISSIONS ${PROGRAM_PERMISSIONS_DEFAULT}
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT pytesting)

endif()

configure_file(mktest.py
  ${CMAKE_CURRENT_BINARY_DIR}/mktest.py
  COPYONLY)

configure_file(sqllogictest.py
  ${CMAKE_CURRENT_BINARY_DIR}/sqllogictest.py
  COPYONLY)

if (NOT WIN32)
  add_library(mtest_fallocate SHARED)
  target_sources(mtest_fallocate PRIVATE fallocate.c)
  target_link_libraries(mtest_fallocate PRIVATE monetdb_config_header)
  target_link_libraries(mtest_fallocate)
endif()

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/mktest.py
  ${CMAKE_CURRENT_BINARY_DIR}/sqllogictest.py
  PERMISSIONS ${PROGRAM_PERMISSIONS_DEFAULT}
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT pytesting)
