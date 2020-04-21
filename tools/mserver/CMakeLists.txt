#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

add_executable(mserver5)

target_sources(mserver5
  PRIVATE
  mserver5.c
  ${CMAKE_CURRENT_BINARY_DIR}/monet_version.c
  monet_version.h)

configure_file(monet_version.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/monet_version.c
  @ONLY)

target_include_directories(mserver5
  PRIVATE
  $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

target_link_libraries(mserver5
  PRIVATE
  monetdb_config_header
  matomic
  moptions
  mutils
  mcrypt
  gdk
  mapi
  monetdb5
  $<$<NOT:$<PLATFORM_ID:Windows>>:m>
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>
  ${CMAKE_DL_LIBS}
  $<$<BOOL:${PCRE_FOUND}>:PCRE::PCRE>
  $<$<NOT:$<PLATFORM_ID:Windows>>:$<$<BOOL:${LibXml2_FOUND}>:LibXml2::LibXml2>>
  Threads::Threads)

add_executable(shutdowntest)
target_sources(shutdowntest
  PRIVATE
  shutdowntest.c)

target_include_directories(shutdowntest
  PRIVATE
  $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

target_link_libraries(shutdowntest
  PRIVATE
  monetdb_config_header
  matomic
  moptions
  mutils
  mcrypt
  gdk
  mapi
  monetdb5
  $<$<NOT:$<PLATFORM_ID:Windows>>:m>
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>
  ${CMAKE_DL_LIBS}
  $<$<BOOL:${PCRE_FOUND}>:PCRE::PCRE>
  $<$<NOT:$<PLATFORM_ID:Windows>>:$<$<BOOL:${LibXml2_FOUND}>:LibXml2::LibXml2>>
  Threads::Threads)

install(TARGETS
  mserver5 shutdowntest
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

configure_file(mserver5.1.in
  ${CMAKE_CURRENT_BINARY_DIR}/mserver5.1 @ONLY)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/mserver5.1
  DESTINATION ${CMAKE_INSTALL_FULL_MANDIR}/man1)

install(DIRECTORY DESTINATION ${CMAKE_INSTALL_DATADIR})
install(DIRECTORY DESTINATION ${CMAKE_INSTALL_LOCALSTATEDIR}/monetdb5/dbfarm)
