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

add_library(kernel OBJECT)
if(MONETDB_STATIC)
  add_library(microbenchmark STATIC)
else()
  add_library(microbenchmark MODULE)
endif()

target_sources(kernel
  PRIVATE
  alarm.c
  mmath.c mmath_private.h
  bat5.c
  algebra.c algebra.h
  group.c group.h
  aggr.c aggr.h
  batmmath.c
  batstr.c
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
  $<$<BOOL:${Iconv_FOUND}>:Iconv::Iconv>
  monetdb_config_header
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
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
  COMPONENT microbenchmark)

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:microbenchmark>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    OPTIONAL)
endif()
