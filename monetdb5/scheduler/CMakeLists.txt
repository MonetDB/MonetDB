#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

add_library(scheduler OBJECT)

target_sources(scheduler
  PRIVATE
  run_adder.c run_adder.h
  run_isolate.c run_isolate.h
  run_memo.c run_memo.h
  run_pipeline.c run_pipeline.h)

target_include_directories(scheduler
  PRIVATE
  $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

if(WIN32)
  target_compile_definitions(scheduler
    PRIVATE
    LIBMAL
    LIBATOMS
    LIBKERNEL
    LIBOPTIMIZER
    LIBSCHEDULER
    LIBMONETDB5)
endif()

target_link_libraries(scheduler
  PRIVATE
  monetdb_config_header)

set_target_properties(scheduler
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON)

install(FILES
  run_adder.mal
  run_isolate.mal
  run_memo.mal
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5)
