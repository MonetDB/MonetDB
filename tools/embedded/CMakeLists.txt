#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

set(embedded_public_headers
  $<BUILD_INTERFACE:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>/monetdb_embedded.h>
  $<INSTALL_INTERFACE:$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>/monetdb_embedded.h>)

add_library(embedded SHARED)

target_sources(embedded
  PRIVATE
  monetdb_embedded.c
  PUBLIC
  ${embedded_public_headers})

target_include_directories(embedded
  PRIVATE
  $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_compile_definitions(embedded
  PRIVATE
  LIBEMBEDDED)

set_target_properties(embedded
  PROPERTIES
  VERSION ${MONETDBE_VERSION}
  SOVERSION ${MONETDBE_VERSION_MAJOR}
  PUBLIC_HEADER "${embedded_public_headers}")

target_link_libraries(embedded
  PRIVATE
  monetdb_config_header
  matomic
  moptions
  mutils
  mcrypt
  gdk
  mapi
  monetdb5
  sql
  $<$<NOT:$<PLATFORM_ID:Windows>>:m>
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>
  ${CMAKE_DL_LIBS}
  $<$<BOOL:${PCRE_FOUND}>:PCRE::PCRE>
  $<$<NOT:$<PLATFORM_ID:Windows>>:$<$<BOOL:${LibXml2_FOUND}>:LibXml2::LibXml2>>
  Threads::Threads)

install(TARGETS
  embedded
  EXPORT embeddedTargets
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT server
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT embeddedTargets
  FILE embeddedTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cmake
  COMPONENT serverdev)
