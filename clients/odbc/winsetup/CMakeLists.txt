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

add_library(MonetODBCs
  MODULE)

target_sources(MonetODBCs
  PRIVATE
  setup.c
  setup.rc
  setup.def
  resource.h)

target_include_directories(MonetODBCs
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>
  ${CMAKE_CURRENT_BINARY_DIR}/../../../common/utils)

target_compile_definitions(MonetODBCs
  PRIVATE
  LIBMONETODBCS)

target_link_libraries(MonetODBCs
  PRIVATE
  monetdb_config_header
  mutils
  $<$<PLATFORM_ID:Windows>:odbc32>
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

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:odbcinstall>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
  install(FILES
    $<TARGET_PDB_FILE:MonetODBCs>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    OPTIONAL)
endif()
