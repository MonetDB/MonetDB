#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_executable(monetdbd)

target_sources(monetdbd
  PRIVATE
  merovingian.c merovingian.h
  argvcmds.c argvcmds.h
  client.c client.h
  connections.c connections.h
  controlrunner.c controlrunner.h
  discoveryrunner.c discoveryrunner.h
  forkmserver.c forkmserver.h
  handlers.c handlers.h
  multiplex-funnel.c multiplex-funnel.h
  snapshot.c snapshot.h
  proxy.c proxy.h)

target_include_directories(monetdbd
  PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

target_link_libraries(monetdbd
  PRIVATE
  monetdb_config_header
  meroutil
  meroutil_headers
  mutils
  moptions
  mcrypt
  stream
  mapi
  msabaoth
  Threads::Threads)

install(TARGETS
  monetdbd
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT sql
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

configure_file(monetdbd.1.in
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd.1
  @ONLY)

configure_file(${CMAKE_SOURCE_DIR}/buildtools/conf/monetdbd.conf.in
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd.conf
  @ONLY)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd.1
  DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
  COMPONENT sql)

install(DIRECTORY
  DESTINATION
	${CMAKE_INSTALL_SYSCONFDIR}/tmpfiles.d
  COMPONENT sql)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/monetdbd.conf
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/tmpfiles.d
  COMPONENT sql)

add_subdirectory(config)

# vim: set ts=2:sw=2:et
