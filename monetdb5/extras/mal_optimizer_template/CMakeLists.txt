#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(opt_sql_append MODULE)

target_sources(opt_sql_append
  PRIVATE
  opt_sql_append.c)

target_include_directories(opt_sql_append
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(opt_sql_append 
  PRIVATE 
  monetdb_config_header
  mutils
  bat
  monetdb5)

target_compile_definitions(opt_sql_append 
  PRIVATE 
  LIBOPT_SQL_APPEND)

set_target_properties(opt_sql_append 
  PROPERTIES 
  NO_SONAME ON
  OUTPUT_NAME 
  _opt_sql_append)

install(TARGETS
  opt_sql_append
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT server)
