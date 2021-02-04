#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(mapi
  SHARED)

# We need to add the public library twice to the target sources, once for
# the build interface and once for the install interface. This is because
# of the fact that cmake < 3.13 does not handle relative paths well. If we
# do not handle this in the target sources, cmake cannot define the export
# information correctly.
target_sources(mapi
  PRIVATE
  mapi.c
  mapi_prompt.h
  PUBLIC
  $<BUILD_INTERFACE:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/mapi.h>
  $<BUILD_INTERFACE:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/mapi_querytype.h>
  $<INSTALL_INTERFACE:$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>/mapi.h>
  $<INSTALL_INTERFACE:$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>/mapi_querytype.h>)

target_include_directories(mapi
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(mapi
  PRIVATE
  monetdb_config_header
  moptions
  mcrypt
  stream
  $<$<PLATFORM_ID:Windows>:ws2_32>)

target_compile_definitions(mapi
  PRIVATE
  LIBMAPI)

set_target_properties(mapi
  PROPERTIES
  VERSION ${MAPI_VERSION}
  SOVERSION ${MAPI_VERSION_MAJOR}
  PUBLIC_HEADER "mapi.h;mapi_querytype.h")

if (NOT WIN32)
  configure_file(monetdb-mapi.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-mapi.pc @ONLY)

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-mapi.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    COMPONENT clientdev)
endif()

install(TARGETS
  mapi
  EXPORT mapiTargets
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT client
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT client
  NAMELINK_COMPONENT clientdev
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT client
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT clientdev
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT mapiTargets
  FILE mapiTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT clientdev)

# vim: set ts=2:sw=2:et
