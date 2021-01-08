#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(MonetODBCs
  MODULE)

target_sources(MonetODBCs
  PRIVATE
  setup.c
  setup.rc
  setup.def
  resource.h)

target_include_directories(MonetODBCs
#  PRIVATE
#  $<$<PLATFORM_ID:Windows>:${HAVE_AFXRES_H}>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_compile_definitions(MonetODBCs
  PRIVATE
  LIBMONETODBCS)

target_link_libraries(MonetODBCs
  PRIVATE
  monetdb_config_header
  mutils
  $<$<PLATFORM_ID:Windows>:odbccp32>
  $<$<PLATFORM_ID:Windows>:user32>
  $<$<PLATFORM_ID:Windows>:legacy_stdio_definitions>)

add_executable(odbcinstall
  install.c)

target_link_libraries(odbcinstall
  PRIVATE
  monetdb_config_header
  mutils
  $<$<PLATFORM_ID:Windows>:odbc32>
  $<$<PLATFORM_ID:Windows>:odbccp32>
  $<$<PLATFORM_ID:Windows>:shlwapi>
  $<$<PLATFORM_ID:Windows>:user32>
  $<$<PLATFORM_ID:Windows>:legacy_stdio_definitions>)

target_include_directories(odbcinstall
  PRIVATE
  ../driver
  PUBLIC
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)
set(SOURCES install.c ../driver/ODBCGlobal.h)

install(TARGETS
  MonetODBCs
  odbcinstall
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT odbc
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT odbc
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT odbc
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT clientdev
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# vim: set ts=2:sw=2:et
