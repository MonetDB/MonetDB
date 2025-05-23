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

if(MONETDB_STATIC)
  add_library(mapi STATIC)
else()
  add_library(mapi SHARED)
endif()

# We need to add the public library twice to the target sources, once for
# the build interface and once for the install interface. This is because
# of the fact that cmake < 3.13 does not handle relative paths well. If we
# do not handle this in the target sources, cmake cannot define the export
# information correctly.
target_sources(mapi
  PRIVATE
  mapi.c
  connect.c
  msettings.c
  msettings_internal.h
  parseurl.c
  writeurl.c
  $<$<BOOL:${HAVE_SYS_UN_H}>:connect_unix.c>
  $<$<BOOL:${OPENSSL_FOUND}>:connect_openssl.c>
  $<$<BOOL:${OPENSSL_FOUND}>:$<$<BOOL:${WIN32}>:openssl_windows.c>>
  mapi_intern.h
  PUBLIC
  $<BUILD_INTERFACE:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/mapi.h>
  $<BUILD_INTERFACE:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/mapi_querytype.h>
  $<BUILD_INTERFACE:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/msettings.h>
  $<INSTALL_INTERFACE:$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>/mapi.h>
  $<INSTALL_INTERFACE:$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>/mapi_querytype.h>
  $<INSTALL_INTERFACE:$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>/msettings.h>)

target_include_directories(mapi
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(mapi
  PRIVATE
  monetdb_config_header
  moptions
  stream
  mutils
  $<$<BOOL:${OPENSSL_FOUND}>:OpenSSL::SSL>
  $<$<BOOL:${OPENSSL_FOUND}>:$<$<BOOL:${WIN32}>:crypt32>>
  $<$<PLATFORM_ID:Windows>:ws2_32>)

target_compile_definitions(mapi
  PRIVATE
  LIBMAPI)

set_target_properties(mapi
  PROPERTIES
  OUTPUT_NAME mapi-${MONETDB_VERSION}
  VERSION ${MAPI_VERSION}
  SOVERSION ${MAPI_VERSION_MAJOR}
  PUBLIC_HEADER "mapi.h;mapi_querytype.h;msettings.h")

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

if(NOT MONETDB_STATIC)
  install(EXPORT mapiTargets
    FILE mapiTargets.cmake
    NAMESPACE MonetDB::
    DESTINATION ${EXPORT_TARGET_PATH}
    COMPONENT clientdev)
endif()

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:mapi>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    OPTIONAL)
endif()
