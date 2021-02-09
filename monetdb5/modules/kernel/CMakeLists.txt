#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(kernel OBJECT)
add_library(microbenchmark MODULE)

target_sources(kernel
  PRIVATE
  alarm.c
  mmath.c mmath_private.h
  bat5.c
  algebra.c algebra.h
  status.c
  group.c group.h
  aggr.c
  batmmath.c
  batstr.c
  batcolor.c
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/bat5.h)
target_sources(microbenchmark
  PRIVATE
  microbenchmark.c
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/bat5.h)

target_include_directories(kernel
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)
target_include_directories(microbenchmark
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_compile_definitions(kernel
  PRIVATE
  LIBMONETDB5)

target_link_libraries(kernel
  PRIVATE
  monetdb_config_header
  mutils
  bat
  mal
  atoms)
target_link_libraries(microbenchmark
  PRIVATE
  monetdb_config_header
  bat
  monetdb5)

set_target_properties(kernel
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/bat5.h)
set_target_properties(microbenchmark
  PROPERTIES
  OUTPUT_NAME _microbenchmark)

install(TARGETS
  microbenchmark
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT microbenchmark)
install(FILES
  microbenchmark.mal
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
  COMPONENT microbenchmark)
