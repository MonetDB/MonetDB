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

add_library(batstore STATIC)

target_sources(batstore
  PRIVATE
  bat_table.c
  bat_storage.c
  bat_logger.c
  bat_utils.c
  res_table.c
  bat_logger.h
  bat_storage.h
  bat_table.h
  bat_utils.h
  res_table.h)

target_include_directories(batstore
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:kernel,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(batstore
  PRIVATE
  monetdb_config_header
  bat
  sqlcommon
  sqlinclude
  sqlstorage)

if(NOT MONETDB_STATIC)
  set_target_properties(batstore
    PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
endif()

if(MONETDB_STATIC)
  install(TARGETS
    batstore
    ARCHIVE
    DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

target_compile_definitions(batstore
  PRIVATE
  LIBSQL)
