#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

if(${HAVE_READLINE})
  add_compile_definitions(HAVE_LIBREADLINE)
endif()

add_library(mcutil
  STATIC)

target_sources(mcutil
  PRIVATE
  dump.c
  dotmonetdb.c
  dotmonetdb.h)

target_link_libraries(mcutil
  PRIVATE
  monetdb_config_header
  mstring
  mapi
  stream)

add_executable(mclient)

target_sources(mclient
  PRIVATE
  mclient.c
  ReadlineTools.c
  ReadlineTools.h
  mhelp.c
  mhelp.h
  iconv-stream.h
  curl-stream.h)

target_include_directories(mclient
  PRIVATE
  $<$<BOOL:${Iconv_IS_BUILT_IN}>:${CMAKE_REQUIRED_INCLUDES}>
  $<$<PLATFORM_ID:Windows>:${HAVE_GETOPT_H}>)

target_link_libraries(mclient
  PRIVATE
  monetdb_config_header
  mcutil
  mprompt
  moptions
  mutils
  mapi
  stream
  $<$<BOOL:${READLINE_FOUND}>:Readline::Readline>
  $<$<BOOL:${CURL_FOUND}>:CURL::libcurl>
  $<$<BOOL:${Iconv_FOUND}>:Iconv::Iconv>
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>)

add_executable(msqldump)

target_sources(msqldump
  PRIVATE
  msqldump.c
  msqldump.h)

target_include_directories(msqldump
  PRIVATE
  $<$<PLATFORM_ID:Windows>:${HAVE_GETOPT_H}>)

target_link_libraries(msqldump
  PRIVATE
  monetdb_config_header
  mcutil
  moptions
  mutils
  mapi
  stream
  mprompt
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>)

set_target_properties(mclient msqldump
  PROPERTIES
  VERSION ${MONETDB_VERSION})

install(TARGETS
  mclient
  msqldump
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT client
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(FILES
  mclient.1
  msqldump.1
  DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
  COMPONENT client)

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:mclient>
    $<TARGET_PDB_FILE:msqldump>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
endif()
