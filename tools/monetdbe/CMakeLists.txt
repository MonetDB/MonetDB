#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]
set(CMAKE_VERBOSE_MAKEFILE ON)

set(monetdbe_public_headers
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/monetdbe.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/monetdbe.h>)

add_library(monetdbe SHARED)

add_compile_definitions(COMPILING_MONETDBE)

target_sources(monetdbe
  PRIVATE
  monetdbe.c
  monetdbe_mapi.c monetdbe_mapi.h
  ${CMAKE_SOURCE_DIR}/clients/mapiclient/dump.c
  PUBLIC
  ${monetdbe_public_headers})

target_include_directories(monetdbe
  PRIVATE
  $<TARGET_PROPERTY:mapi,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlinclude,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>  
  ${CMAKE_SOURCE_DIR}/clients/mapiclient
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_compile_definitions(monetdbe
  PRIVATE
  LIBMAPI
  LIBMONETDBE)

set_target_properties(monetdbe
  PROPERTIES
  VERSION ${MONETDBE_VERSION}
  SOVERSION ${MONETDBE_VERSION_MAJOR}
  PUBLIC_HEADER "${monetdbe_public_headers}")

target_link_libraries(monetdbe
  PRIVATE
  monetdb_config_header
  matomic
  moptions
  mutils
  mapi
  stream
  bat
  monetdb5
  sql
  $<$<NOT:$<PLATFORM_ID:Windows>>:m>
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>
  Threads::Threads)

if (NOT WIN32)
  configure_file(monetdbe.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdbe.pc @ONLY)

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdbe.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    COMPONENT serverdev)
endif()

install(TARGETS
  monetdbe
  EXPORT monetdbeTargets
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT server
  NAMELINK_COMPONENT serverdev
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT server
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT monetdbeTargets
  FILE monetdbeTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT serverdev)
