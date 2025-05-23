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

BISON_TARGET(sqlparser
  sql_parser.y
  ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.tab.c
  #COMPILE_FLAGS "-t -rstates,itemsets,lookaheads -ffixit,caret -d -p sql -Wno-conflicts-sr -Wno-conflicts-rr -o sql_parser.output"
  COMPILE_FLAGS "-p sql -Werror"
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/sql_parser.tab.h)

add_library(sqlserver STATIC)

set(sqlserver_public_headers
  ${CMAKE_CURRENT_SOURCE_DIR}/sql_parser.h
  ${CMAKE_CURRENT_SOURCE_DIR}/rel_file_loader.h
  ${CMAKE_CURRENT_SOURCE_DIR}/rel_proto_loader.h
  ${CMAKE_CURRENT_SOURCE_DIR}/sql_mvc.h)

target_sources(sqlserver
  PRIVATE
  sql_semantic.c
  sql_qc.c
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
  rel_statistics.c rel_statistics_functions.c
  rel_optimize_exps.c
  rel_optimize_others.c
  rel_optimize_proj.c
  rel_optimize_sel.c
  rel_planner.c rel_planner.h
  rel_distribute.c
  rel_remote.c rel_remote.h
  rel_propagate.c rel_propagate.h
  rel_psm.c
  rel_xml.c
  rel_dump.c
  rel_file_loader.c
  rel_proto_loader.c
  rel_dump.h rel_exp.h rel_rel.h
  rel_basetable.h
  rel_rewriter.h
  rel_unnest.h
  rel_optimizer.h
  rel_statistics.h
  rel_optimizer_private.h
  rel_prop.h
  rel_schema.h
  rel_select.h
  rel_semantic.h
  rel_sequence.h
  rel_trans.h
  rel_updates.h
  rel_psm.h
  rel_xml.h
  sql_env.h
  sql_privileges.h
  sql_query.h
  sql_qc.h
  sql_semantic.h
  sql_tokens.h
  sql_partition.h
  rel_file_loader.h
  rel_proto_loader.h
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
  bat
  sqlparserlib
  sqlcommon
  sqlinclude
  sqlstorage)

target_compile_definitions(sqlserver
  PRIVATE
  LIBSQL)

if(NOT MONETDB_STATIC)
  set_target_properties(sqlserver
    PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    PUBLIC_HEADER "${sqlserver_public_headers}")
endif()

add_library(sqlparserlib STATIC)

target_sources(sqlparserlib
  PRIVATE
  sql_atom.c
  sql_atom.h
  sql_datetime.c
  sql_datetime.h
  sql_decimal.c
  sql_decimal.h
  sql_scan.c
  sql_scan.h
  sql_symbol.c
  sql_symbol.h
  ${BISON_sqlparser_OUTPUT_HEADER}
  ${BISON_sqlparser_OUTPUT_SOURCE}
  ${sqlserver_public_headers})

target_include_directories(sqlparserlib
  PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(sqlparserlib
  PRIVATE
  monetdb_config_header
  bat
  sqlcommon
  sqlinclude
  sqlstorage)

target_compile_definitions(sqlparserlib
  PRIVATE
  LIBSQL)

if(NOT MONETDB_STATIC)
  set_target_properties(sqlparserlib
    PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    PUBLIC_HEADER "${sqlserver_public_headers}")
endif()

if(WITH_SQLPARSE)
add_executable(sqlparse)

target_sources(sqlparse
  PRIVATE
  sqlparse.c)

target_include_directories(sqlparse
  PRIVATE
  $<$<PLATFORM_ID:Windows>:${HAVE_GETOPT_H}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(sqlparse
  PRIVATE
  monetdb_config_header
  sqlparserlib
  stream
  bat
  sqlinclude
  sqlcommon
  mutils
  $<$<NOT:$<PLATFORM_ID:Windows>>:m>
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>
  $<$<BOOL:${MALLOC_FOUND}>:Malloc::Malloc>
  Threads::Threads)

set_target_properties(sqlparse
  PROPERTIES
  VERSION ${MONETDB_VERSION})
install(TARGETS
  sqlparse
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT server)
endif()

if(MONETDB_STATIC)
install(TARGETS
  sqlserver
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT server
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()
