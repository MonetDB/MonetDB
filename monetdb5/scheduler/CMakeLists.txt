#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(run_adder MODULE)
target_sources(run_adder
  PRIVATE
  run_adder.c)
target_include_directories(run_adder
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)
target_link_libraries(run_adder
  PRIVATE
  monetdb_config_header
  bat
  monetdb5)
set_target_properties(run_adder
  PROPERTIES
  OUTPUT_NAME _run_adder)
install(TARGETS run_adder
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT run_adder)
install(FILES
  run_adder.mal
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT run_adder)

add_library(run_isolate MODULE)
target_sources(run_isolate
  PRIVATE
  run_isolate.c)
target_include_directories(run_isolate
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)
target_link_libraries(run_isolate
  PRIVATE
  monetdb_config_header
  monetdb5)
set_target_properties(run_isolate
  PROPERTIES
  OUTPUT_NAME _run_isolate)
install(TARGETS run_isolate
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT run_isolate)
install(FILES
  run_isolate.mal
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT run_isolate)

add_library(run_memo MODULE)
target_sources(run_memo
  PRIVATE
  run_memo.c)
target_include_directories(run_memo
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)
target_link_libraries(run_memo
  PRIVATE
  monetdb_config_header
  bat
  monetdb5)
set_target_properties(run_memo
  PROPERTIES
  OUTPUT_NAME _run_memo)
install(TARGETS run_memo
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT run_memo)
install(FILES
  run_memo.mal
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT run_memo)
