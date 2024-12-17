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

if(MONETDB_STATIC)
  add_library(csv STATIC)
else()
  add_library(csv MODULE)
endif()

target_sources(csv
  PRIVATE
  csv.c)

target_include_directories(csv
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

target_link_libraries(csv
  PRIVATE
  monetdb_config_header
  sqlinclude
  sql
  monetdb5
  bat
  stream
)

set_target_properties(csv
  PROPERTIES
  OUTPUT_NAME
  _csv)

install(TARGETS
  csv
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
  COMPONENT server)
