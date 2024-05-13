#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

if(LIBR_FOUND)
  add_library(rapi MODULE)

  target_sources(rapi
    PRIVATE
    rapi.c)

  target_include_directories(rapi
    PRIVATE
    $<TARGET_PROPERTY:mapi,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(rapi
    PRIVATE
    monetdb_config_header
    mutils
    bat
    monetdb5
    sqlinclude
    LibR::LibR)

  target_compile_definitions(rapi
    PRIVATE
    LIBRAPI)

  set_target_properties(rapi
    PROPERTIES
    OUTPUT_NAME
    _rapi)

  install(TARGETS
    rapi
    DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    COMPONENT rapi)

  install(FILES
    rapi.R
    DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    COMPONENT rapi)

  if(WIN32)
    install(FILES
      $<TARGET_PDB_FILE:rapi>
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
      OPTIONAL)
  endif()
endif()
