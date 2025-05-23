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

set(sql_public_headers
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/sql_import.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/sql_mem.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/sql_list.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/sql_hash.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/sql_stack.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/sql_catalog.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/sql_relation.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/sql_keyword.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../common/sql_types.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../common/sql_string.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../common/sql_backend.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../storage/sql_storage.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../storage/store_sequence.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_tokens.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_scan.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_atom.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_symbol.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_semantic.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_query.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_qc.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_mvc.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_parser.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/sql_privileges.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_file_loader.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_optimizer.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_rewriter.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_prop.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_exp.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_rel.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_basetable.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_semantic.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../server/rel_dump.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/opt_backend.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/mal_backend.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/sql_statement.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_import.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_mem.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_list.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_hash.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_stack.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_catalog.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_relation.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_keyword.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_types.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_string.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_backend.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_storage.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/store_sequence.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_tokens.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_scan.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_atom.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_symbol.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_semantic.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_query.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_qc.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_mvc.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_parser.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_privileges.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/rel_optimizer.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/rel_prop.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/rel_exp.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/rel_rel.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/rel_basetable.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/rel_semantic.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/rel_dump.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/opt_backend.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/mal_backend.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_statement.h>
)

#add_library(sql MODULE)
if(MONETDB_STATIC)
  add_library(sql STATIC)
else()
  add_library(sql SHARED)
endif()

set(include_sql_files
  10_sys_schema_extension
  12_url
  13_date
  14_inet
  15_querylog
  16_tracelog
  17_temporal
  18_index
  21_dependency_views
  22_clients
  23_skyserver
  25_debug
  26_sysmon
  27_rejects
  39_analytics
  40_json
  42_md5sum
  45_uuid
  46_profiler
  47_regexp
  48_txtsim
  49_strings
  52_describe
  58_hot_snapshot
  75_storagemodel
  76_dump
  77_storage
  80_statistics
  81_tracer
  91_information_schema)

if(HAVE_HGE)
  list(APPEND include_sql_files
    39_analytics_hge
    41_json_hge)
endif()

create_include_object(
  SQL_LANGUAGE
  name_module "sql"
  path "${CMAKE_SOURCE_DIR}/sql/scripts/"
  modules_list ${include_sql_files})

target_sources(sql
  PRIVATE
  sql.c sql.h
  mal_backend.c
  sql_user.c sql_user.h
  sql_scenario.c sql_scenario.h
  sql_execute.c sql_execute.h
  sql_assert.c sql_assert.h
  sql_upgrades.c sql_upgrades.h
  rel_bin.c rel_bin.h
  rel_physical.c rel_physical.h
  rel_predicates.c rel_predicates.h
  sql_cat.c sql_cat.h
  sql_transaction.c sql_transaction.h
  sql_statement.c
  sql_statistics.c sql_statistics.h
  sql_gencode.c sql_gencode.h
  sql_optimizer.c sql_optimizer.h
  sql_result.c sql_result.h
  sql_cast.c sql_cast.h
  sql_cast_impl_int.h
  sql_round.c
  sql_round_impl.h
  sql_fround.c sql_fround_impl.h
  sql_orderidx.c sql_orderidx.h
  sql_strimps.c sql_strimps.h
  sql_time.c
  sql_bincopy.c sql_bincopyconvert.c sql_bincopyconvert.h
  sql_datetrunc.c
  sql_rank.c sql_rank.h
  sql_subquery.c sql_subquery.h
  opt_backend.h
  for.c for.h
  dict.c dict.h
  ${MONETDB_CURRENT_SQL_SOURCES}
  PUBLIC
  ${sql_public_headers})

target_include_directories(sql
  PRIVATE
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:kernel,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(sql
  PRIVATE
  monetdb_config_header
  sqlstorage
  sqlcommon
  batstore
  sqlserver
  sqlparserlib
  sqlinclude
  copybinary
  monetdb5
  mapi
  bat
  mutils)

set_target_properties(sql
  PROPERTIES
  OUTPUT_NAME monetdbsql-${MONETDB_VERSION}
  VERSION ${SQL_VERSION}
  SOVERSION ${SQL_VERSION_MAJOR}
  PUBLIC_HEADER "${sql_public_headers}")

target_compile_definitions(sql
  PRIVATE
  LIBMCRYPT
  LIBSQL)

install(TARGETS
  sql
  EXPORT sqlTargets
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT sql
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT sql
  NAMELINK_COMPONENT monetdbdev
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR})

add_subdirectory(generator)
if(WITH_UDF)
  add_subdirectory(UDF)
endif()
if(WITH_VAULTS)
  add_subdirectory(vaults)
endif()

add_library(sqlembedded INTERFACE)

target_include_directories(sqlembedded
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

if(NOT MONETDB_STATIC)
  install(EXPORT sqlTargets
    FILE sqlTargets.cmake
    NAMESPACE MonetDB::
    DESTINATION ${EXPORT_TARGET_PATH}
    COMPONENT monetdbdev)
endif()

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:sql>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    OPTIONAL)
endif()
