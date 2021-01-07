#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_executable(monetdb)

target_sources(monetdb
  PRIVATE
  monetdb.c)

target_include_directories(monetdb
  PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

target_link_libraries(monetdb
  PRIVATE
  monetdb_config_header
  meroutil
  mcrypt
  msabaoth
  mutils
  #$<$<BOOL:${Iconv_FOUND}>:Iconv::Iconv>)
  )

install(TARGETS
  monetdb
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

install(FILES
  monetdb.1
  DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
  COMPONENT sql)

# vim: set ts=2:sw=2:et
