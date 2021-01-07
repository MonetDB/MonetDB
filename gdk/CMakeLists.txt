#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

set(gdk_public_headers
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_system.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_posix.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_atoms.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_tracer.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_delta.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_hash.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_bbp.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_utils.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_cand.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/gdk_calc.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_system.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_posix.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_atoms.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_tracer.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_delta.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_hash.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_bbp.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_utils.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_calc.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/gdk_cand.h>)

add_library(bat SHARED)

target_sources(bat
  PRIVATE
  gdk_select.c
  gdk_calc.c gdk_calc.h
  gdk_calc_compare.h gdk_calc_private.h
  gdk_ssort.c gdk_ssort_impl.h
  gdk_aggr.c
  gdk_batop.c
  gdk_cand.h gdk_cand.c
  gdk_search.c
  gdk_hash.c gdk_hash.h
  gdk_tm.c gdk_tm.h
  gdk_orderidx.c
  gdk_align.c
  gdk_bbp.c gdk_bbp.h
  gdk_heap.c
  gdk_utils.c gdk_utils.h
  gdk_atoms.c
  gdk_string.c
  gdk_qsort.c
  gdk_qsort_impl.h
  gdk_storage.c gdk_storage.h
  gdk_bat.c
  gdk_delta.c gdk_delta.h
  gdk_cross.c
  gdk_system.c
  gdk_value.c
  gdk_posix.c
  gdk_logger.c gdk_logger.h
  gdk_logger_old.c
  gdk_sample.c
  xoshiro256starstar.h
  gdk_private.h
  gdk_system_private.h
  gdk_group.c
  gdk_imprints.c gdk_imprints.h
  gdk_join.c
  gdk_project.c
  gdk_time.c gdk_time.h
  gdk_unique.c
  gdk_interprocess.c gdk_interprocess.h
  gdk_firstn.c
  gdk_subquery.c gdk_subquery.h
  gdk_analytic_bounds.c
  gdk_analytic_func.c
  gdk_analytic.h
  gdk_tracer.c gdk_tracer.h
  PUBLIC
  ${gdk_public_headers})

target_include_directories(bat
  PUBLIC
  $<TARGET_PROPERTY:stream,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:matomic,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:mstring,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(bat
  PRIVATE
  stream
  matomic
  mstring
  monetdb_config_header
  moptions
  mutils
  $<$<NOT:$<PLATFORM_ID:Windows>>:m>
  $<$<PLATFORM_ID:Windows>:ws2_32>
  $<$<BOOL:${KVM_FOUND}>:KVM::KVM>
  ${CMAKE_DL_LIBS}
  Threads::Threads)

# This definition is also used in Linux
target_compile_definitions(bat
  PRIVATE
  LIBGDK)

set_target_properties(bat
  PROPERTIES
  VERSION ${GDK_VERSION}
  SOVERSION ${GDK_VERSION_MAJOR}
  PUBLIC_HEADER "${gdk_public_headers}")

if (NOT WIN32)
  configure_file(monetdb-gdk.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-gdk.pc @ONLY)

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-gdk.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    COMPONENT monetdbdev)
endif()

install(TARGETS
  bat
  EXPORT gdkTargets
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT monetdb
  NAMELINK_COMPONENT monetdbdev
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT monetdb
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT monetdbdev
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  COMPONENT monetdbdev)

install(EXPORT gdkTargets
  FILE gdkTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)
