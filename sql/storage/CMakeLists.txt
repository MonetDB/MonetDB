#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(sqlstorage STATIC)

target_sources(sqlstorage
  PRIVATE
  store_dependency.c
  store_sequence.c
  store.c
  sql_catalog.c
  objectset.c
  objlist.c
  store_sequence.h
  store_dependency.h
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/sql_storage.h)

target_include_directories(sqlstorage
  PRIVATE
  $<TARGET_PROPERTY:mapi,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(sqlstorage
  PRIVATE
  monetdb_config_header
  mutils
  bat
  sqlcommon
  sqlinclude)

set_target_properties(sqlstorage
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/sql_storage.h)

target_compile_definitions(sqlstorage
  PRIVATE
  LIBSQL)

add_subdirectory(bat)
