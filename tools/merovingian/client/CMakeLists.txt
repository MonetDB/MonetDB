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
  mutils
  stream
  #$<$<BOOL:${Iconv_FOUND}>:Iconv::Iconv>)
  )

set_target_properties(monetdb
  PROPERTIES
  VERSION ${MONETDB_VERSION})

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

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:monetdb>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
endif()
