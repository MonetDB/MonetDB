#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

if(ODBC_FOUND)
  add_library(odbc_loader MODULE)

  target_sources(odbc_loader
    PRIVATE
    odbc_loader.c)

  target_include_directories(odbc_loader
    PRIVATE
    $<$<BOOL:${ODBC_FOUND}>:${ODBC_INCLUDE_DIRS}>
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(odbc_loader
    PRIVATE
    monetdb_config_header
    sqlinclude
    sql
    monetdb5
    bat
    mutils
    ODBC::ODBC)

  set_target_properties(odbc_loader
    PROPERTIES
    OUTPUT_NAME
    _odbc_loader)

  target_compile_definitions(odbc_loader
    PRIVATE
    LIBODBC)

  install(TARGETS
    odbc_loader
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    COMPONENT server)

  if(WIN32)
    install(FILES
      $<TARGET_PDB_FILE:odbc_loader>
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
      OPTIONAL)
  endif()
endif()
