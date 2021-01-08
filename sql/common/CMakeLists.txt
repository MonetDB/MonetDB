#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(sqlcommon STATIC)

target_sources(sqlcommon
  PRIVATE
  sql_mem.c
  sql_list.c
  sql_hash.c
  sql_stack.c
  sql_backend.c
  sql_keyword.c
  sql_changeset.c
  sql_types.c
  sql_string.c
  sql_backend.h
  sql_string.h
  exception_buffer.c
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/sql_types.h)

target_include_directories(sqlcommon
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(sqlcommon
  PRIVATE
  monetdb_config_header
  matomic
  stream
  bat
  sqlinclude
  sqlstorage)

target_compile_definitions(sqlcommon
  PRIVATE
  LIBSQL)

set_target_properties(sqlcommon
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/sql_types.h)
