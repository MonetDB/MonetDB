#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

if(MONETDB_STATIC)
  add_library(mutils STATIC)
else()
  add_library(mutils SHARED)
endif()

target_sources(mutils
  PRIVATE
  copybinary_support.h
  mcrypt.c mcrypt.h
  md5.c md5.h
  mprompt.h
  msabaoth.c msabaoth.h
  mutf8.c mutf8.h
  mutils.c mutils.h
  muuid.c muuid.h
  mwidth.c
  prompt.c
  revision.c
  ripemd160.c ripemd160.h
  rmd160.c rmd160.h
  sha224-256.c
  sha384-512.c
  sha.c sha.h sha-private.h)

target_include_directories(mutils
  PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>
  $<$<BOOL:${OPENSSL_FOUND}>:${OPENSSL_INCLUDE_DIR}>)

target_compile_definitions(mutils
  PRIVATE
  LIBMUTILS)

target_link_libraries(mutils
  PRIVATE
  monetdb_config_header)

set_target_properties(mutils
  PROPERTIES
  OUTPUT_NAME mutils-${MONETDB_VERSION}
  VERSION ${MUTILS_VERSION}
  SOVERSION ${MUTILS_VERSION_MAJOR}
  PUBLIC_HEADER "${mutils_public_headers}")

if (NOT WIN32)
  configure_file(monetdb-mutils.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-mutils.pc @ONLY)

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-mutils.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    COMPONENT mutilsdev)
endif()

install(TARGETS
  mutils
  COMPONENT mutils
  EXPORT mutilsTargets
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT mutils
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT mutils
  NAMELINK_COMPONENT mutilsdev
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT mutils
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT mutilsdev
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

if(NOT MONETDB_STATIC)
  install(EXPORT mutilsTargets
    FILE mutilsTargets.cmake
    NAMESPACE MonetDB::
    DESTINATION ${EXPORT_TARGET_PATH}
    COMPONENT mutilsdev)
endif()

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:mutils>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    OPTIONAL)
endif()

add_custom_target(version
  ${CMAKE_COMMAND} -D SRC=${CMAKE_CURRENT_SOURCE_DIR}/monetdb_hgversion.h.in
  -D DST=${CMAKE_CURRENT_BINARY_DIR}/monetdb_hgversion.h
  -D DIR=${CMAKE_SOURCE_DIR}
  -P ${CMAKE_CURRENT_SOURCE_DIR}/GenerateVersionHeader.cmake
)
add_dependencies(mutils version)

add_library(mstring INTERFACE)

target_include_directories(mstring
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

install(TARGETS
  mstring
  EXPORT mstringTargets
  COMPONENT monetdbdev)

install(EXPORT mstringTargets
  FILE mstringTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)

add_library(matomic INTERFACE)

target_include_directories(matomic
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

install(TARGETS
  matomic
  EXPORT matomicTargets
  COMPONENT monetdbdev)

install(EXPORT matomicTargets
  FILE matomicTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)

add_library(copybinary INTERFACE)

target_include_directories(copybinary
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

install(FILES
  matomic.h
  mstring.h
  copybinary.h
  DESTINATION
  ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT
  monetdbdev)
