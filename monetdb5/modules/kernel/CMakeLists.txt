#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

add_library(kernel OBJECT)

target_sources(kernel
  PRIVATE
  alarm.c
  mmath.c mmath_private.h
  bat5.c
  algebra.c algebra.h
  status.c status.h
  group.c group.h
  aggr.c
  batmmath.c
  batstr.c
  batcolor.c
  microbenchmark.c microbenchmark.h
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/bat5.h)

target_include_directories(kernel
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

if(WIN32)
  target_compile_definitions(kernel
    PRIVATE
    LIBMAL
    LIBATOMS
    LIBKERNEL
    LIBOPTIMIZER
    LIBSCHEDULER
    LIBMONETDB5)
endif()

target_link_libraries(kernel
  PRIVATE
  monetdb_config_header
  mutils
  gdk
  mal
  atoms)

set_target_properties(kernel
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/bat5.h)

install(FILES
  microbenchmark.mal
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5)
