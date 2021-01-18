#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

set(sql_public_headers
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/sql_import.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/sql_import.h>)

#add_library(sql MODULE)
add_library(sql SHARED)

set(include_sql_files
  12_url
  13_date
  14_inet
  15_querylog
  16_tracelog
  17_temporal
  18_index
  20_vacuum
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
  51_sys_schema_extension
  52_describe
  58_hot_snapshot
  60_wlcr
  61_wlcr
  75_storagemodel
  76_dump
  80_statistics
  81_tracer)

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
  mal_backend.c mal_backend.h
  sql_user.c sql_user.h
  sql_scenario.c sql_scenario.h
  sql_execute.c sql_execute.h
  sql_assert.c sql_assert.h
  sql_upgrades.c sql_upgrades.h
  rel_bin.c rel_bin.h
  sql_cat.c sql_cat.h
  sql_transaction.c sql_transaction.h
  sql_statement.c sql_statement.h
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
  sql_time.c
  sql_bincopyfrom.c
  wlr.c wlr.h
  sql_datetrunc.c
  sql_rank.c sql_rank.h
  sql_subquery.c sql_subquery.h
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
  mcrypt
  bat
  monetdb5
  sqlstorage
  sqlcommon
  batstore
  sqlserver
  sqlinclude
  copybinary)

set_target_properties(sql
  PROPERTIES
  OUTPUT_NAME monetdbsql
  VERSION ${MONETDB_VERSION}
  SOVERSION ${MONETDB_VERSION_MAJOR}
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
add_subdirectory(UDF)
add_subdirectory(vaults)

add_library(sqlembedded INTERFACE)

target_include_directories(sqlembedded
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

install(EXPORT sqlTargets
  FILE sqlTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)
