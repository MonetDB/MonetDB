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

add_library(monetdb_loader MODULE)

target_sources(monetdb_loader
    PRIVATE
    monetdb.c)

target_include_directories(monetdb_loader
    PRIVATE
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(monetdb_loader
    PRIVATE
    monetdb_config_header
    sqlinclude
    sql
    monetdb5
    bat
    stream
  )

set_target_properties(monetdb_loader
    PROPERTIES
    OUTPUT_NAME
    _monetdb_loader)

install(TARGETS
    monetdb_loader
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    COMPONENT server)
