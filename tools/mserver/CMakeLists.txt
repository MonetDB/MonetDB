#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_executable(mserver5)

target_sources(mserver5
  PRIVATE
  mserver5.c
  ${CMAKE_CURRENT_BINARY_DIR}/monet_version.c
  monet_version.h)

set(builtby $ENV{USER}) # $USER should be there in POSIX compliant systems
cmake_host_system_information(RESULT host QUERY HOSTNAME)
string(APPEND builtby "@" ${host})

set(compilercall ${CMAKE_C_COMPILER})
set(linkercall ${CMAKE_LINKER})

if(${CMAKE_BUILD_TYPE})
  string(TOUPPER ${CMAKE_BUILD_TYPE} build_type)
  string(APPEND compilercall " " ${CMAKE_C_FLAGS_${build_type}} ${CMAKE_C_FLAGS})
else()
  string(APPEND compilercall " " ${CMAKE_C_FLAGS})
endif()

configure_file(monet_version.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/monet_version.c
  @ONLY)

target_include_directories(mserver5
  PRIVATE
  $<$<BOOL:${LibXml2_FOUND}>:$<TARGET_PROPERTY:LibXml2::LibXml2,INTERFACE_INCLUDE_DIRECTORIES>>
  $<$<BOOL:${COMMONCRYPTO_FOUND}>:${COMMONCRYPTO_INCLUDE_DIR}>
  $<$<BOOL:${OPENSSL_FOUND}>:$<TARGET_PROPERTY:OpenSSL::Crypto,INTERFACE_INCLUDE_DIRECTORIES>>
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

target_link_libraries(mserver5
  PRIVATE
  monetdb_config_header
  matomic
  moptions
  mutils
  mcrypt
  bat
  monetdb5
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>
  $<$<BOOL:${PCRE_FOUND}>:PCRE::PCRE>
  Threads::Threads)

add_executable(shutdowntest)
target_sources(shutdowntest
  PRIVATE
  shutdowntest.c)

target_include_directories(shutdowntest
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlinclude,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

target_link_libraries(shutdowntest
  PRIVATE
  monetdb_config_header
  matomic
  moptions
  sql
  mutils
  bat
  stream
  monetdb5
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>
  Threads::Threads)

install(TARGETS
  mserver5
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT server
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS
  shutdowntest
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT clienttest)

configure_file(mserver5.1.in
  ${CMAKE_CURRENT_BINARY_DIR}/mserver5.1 @ONLY)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/mserver5.1
  DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
  COMPONENT server)

# These definitions should not be necessary. CMake should take care of
# creating these automatically. But maybe this is not the case when
# running mtest.
install(DIRECTORY DESTINATION
  ${CMAKE_INSTALL_DATADIR}
  COMPONENT server)
install(DIRECTORY DESTINATION
  ${CMAKE_INSTALL_LOCALSTATEDIR}/monetdb5/dbfarm
  COMPONENT server)

install(FILES
  monetdblogo.png
  DESTINATION
  ${CMAKE_INSTALL_DOCDIR}
  COMPONENT server)

# vim: set ts=2:sw=2:et
