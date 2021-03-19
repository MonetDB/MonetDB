#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

configure_file(monetdb_hgversion.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/monetdb_hgversion.h)

add_library(mutils STATIC)

target_sources(mutils
  PRIVATE
  mutils.c
  revision.c
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mutils.h)

target_include_directories(mutils
  PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(mutils
  PRIVATE
  monetdb_config_header)

target_compile_definitions(mutils PRIVATE
  LIBMUTILS
  LIBGDK
  LIBMEROUTIL)

set_target_properties(mutils
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER mutils.h)

add_library(mcrypt STATIC)

target_sources(mcrypt
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mcrypt.h
  PRIVATE
  mcrypt.c)

target_include_directories(mcrypt
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(mcrypt
  PRIVATE
  $<$<BOOL:${OPENSSL_FOUND}>:OpenSSL::Crypto>
  monetdb_config_header)

target_compile_definitions(mcrypt
  PRIVATE
  LIBMONETDB5
  LIBMAPI
  LIBMCRYPT)

set_target_properties(mcrypt
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER mcrypt.h)

set(msabaoth_public_headers
  ${CMAKE_CURRENT_SOURCE_DIR}/msabaoth.h
  ${CMAKE_CURRENT_SOURCE_DIR}/muuid.h)

add_library(msabaoth STATIC)

target_sources(msabaoth
  PRIVATE
  msabaoth.c
  muuid.c
  PUBLIC
  ${msabaoth_public_headers})

target_include_directories(msabaoth
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(msabaoth
  PRIVATE
  $<$<BOOL:${UUID_FOUND}>:UUID::UUID>
  $<$<BOOL:${OPENSSL_FOUND}>:OpenSSL::Crypto>
  mutils
  monetdb_config_header)

target_compile_definitions(msabaoth
  PRIVATE
  LIBMSABAOTH
  LIBMONETDB5)

set_target_properties(msabaoth
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER "${msabaoth_public_headers}")

add_library(matomic INTERFACE)

target_include_directories(matomic
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

add_library(mstring INTERFACE)

target_include_directories(mstring
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

add_library(mprompt STATIC)

target_sources(mprompt
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mprompt.h
  PRIVATE
  prompt.c)

target_include_directories(mprompt
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(mprompt
  PRIVATE
  monetdb_config_header)

set_target_properties(mprompt
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER mprompt.h)

add_library(copybinary INTERFACE)

target_include_directories(copybinary
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

# We need to define the matomic target because it is a public
# dependency of gdk.
install(TARGETS
  matomic
  EXPORT matomicTargets
  COMPONENT monetdbdev)

install(EXPORT matomicTargets
  FILE matomicTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)

install(TARGETS
  mstring
  EXPORT mstringTargets
  COMPONENT monetdbdev)

install(EXPORT mstringTargets
  FILE mstringTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT monetdbdev)

install(FILES
  matomic.h
  mstring.h
  copybinary.h
  DESTINATION
  ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT
  monetdbdev)
