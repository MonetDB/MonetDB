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

if(MONETDB_STATIC)
  add_library(generator STATIC)
else()
  add_library(generator MODULE)
endif()

set(include_sql_files
  90_generator)

if(HAVE_HGE)
  list(APPEND include_sql_files
    90_generator_hge)
endif()

create_include_object(
  SQL_LANGUAGE
  name_module "generator"
  modules_list ${include_sql_files})

target_sources(generator
  PRIVATE
  generator.c
  ${MONETDB_CURRENT_SQL_SOURCES})

target_include_directories(generator
  PRIVATE
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlinclude,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(generator
  PRIVATE
  monetdb_config_header
  bat
  monetdb5
  sql)

target_compile_definitions(generator
  PRIVATE
  LIBGENERATOR)

set_target_properties(generator
  PROPERTIES
  OUTPUT_NAME
  _generator)

install(TARGETS
  generator
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
  COMPONENT server
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:generator>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    OPTIONAL)
endif()
