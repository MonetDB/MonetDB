#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(udf MODULE)

set(include_sql_files
  80_udf)

if(HAVE_HGE)
  list(APPEND include_sql_files
    80_udf_hge)
endif()

create_include_object(
  SQL_LANGUAGE
  name_module "udf"
  modules_list ${include_sql_files})

target_sources(udf
  PRIVATE
  udf.c udf.h
  udf_impl.h
  ${MONETDB_CURRENT_SQL_SOURCES})

target_include_directories(udf
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlinclude,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(udf
  PRIVATE
  mutils
  bat
  monetdb5
  sql
  monetdb_config_header)

target_compile_definitions(udf
  PRIVATE 
  LIBUDF)

set_target_properties(udf
  PROPERTIES 
  OUTPUT_NAME 
  _udf)

install(TARGETS
  udf
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT server
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
