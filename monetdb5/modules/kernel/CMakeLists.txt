#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
#]]

add_library(kernel OBJECT)

set(include_mal_files
  alarm
  mmath
  bat5
  algebra
  status
  group
  aggr
  batmmath
  batstr
  batcolor
  microbenchmark)

if(HAVE_HGE)
  list(APPEND include_mal_files
    00_aggr_hge)
endif()

create_include_object(
  MAL_LANGUAGE
  name_module "kernel"
  modules_list ${include_mal_files})

target_sources(kernel
  PRIVATE
  aggr.c
  alarm.c
  algebra.c algebra.h
  bat5.c
  batcolor.c batcolor.h
  batmmath.c
  batstr.c
  group.c group.h
  microbenchmark.c microbenchmark.h
  mmath.c mmath.h mmath_private.h
  status.c status.h
  ${MONETDB_CURRENT_MAL_SOURCES}
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

install(TARGETS
  kernel
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
