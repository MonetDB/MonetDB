#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

BISON_TARGET(sqlparser
  sql_parser.y
  ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.tab.c
  COMPILE_FLAGS "-d -p sql"
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.tab.h)

add_library(sqlserver STATIC)

set(sqlserver_public_headers
  ${CMAKE_CURRENT_SOURCE_DIR}/sql_parser.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sql_mvc.h)

target_sources(sqlserver
  PRIVATE
  sql_atom.c
  sql_datetime.c
  sql_decimal.c
  sql_semantic.c
  sql_qc.c
  sql_symbol.c
  sql_scan.c
  sql_partition.c
  sql_mvc.c
  sql_env.c
  sql_privileges.c
  sql_query.c
  sql_var.c
  rel_semantic.c
  rel_select.c
  rel_updates.c
  rel_trans.c
  rel_schema.c
  rel_sequence.c
  rel_prop.c
  rel_exp.c
  rel_rel.c
  rel_basetable.c
  rel_rewriter.c
  rel_unnest.c
  rel_optimizer.c
  rel_partition.c
  rel_planner.c rel_planner.h
  rel_distribute.c
  rel_remote.c rel_remote.h
  rel_propagate.c rel_propagate.h
  rel_psm.c
  rel_xml.c
  rel_dump.c
  rel_dump.h rel_exp.h rel_rel.h
  rel_basetable.h
  rel_unnest.h
  rel_optimizer.h
  rel_partition.h
  rel_distribute.h
  rel_prop.h
  rel_schema.h
  rel_select.h
  rel_semantic.h
  rel_sequence.h
  rel_trans.h
  rel_updates.h
  rel_psm.h
  rel_xml.h
  sql_atom.h
  sql_datetime.h
  sql_decimal.h
  sql_env.h
  sql_privileges.h
  sql_query.h
  sql_qc.h
  sql_scan.h
  sql_semantic.h
  sql_symbol.h
  sql_tokens.h
  sql_partition.h
  ${BISON_sqlparser_OUTPUT_HEADER}
  ${BISON_sqlparser_OUTPUT_SOURCE}
  PUBLIC
  ${sqlserver_public_headers})

target_include_directories(sqlserver
  PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(sqlserver
  PRIVATE
  monetdb_config_header
  mutils
  bat
  sqlcommon
  sqlinclude
  sqlstorage)

target_compile_definitions(sqlserver
  PRIVATE
  LIBSQL)

set_target_properties(sqlserver
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER "${sqlserver_public_headers}")
