#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

if(HAVE_HTSLIB)
  add_library(htslib MODULE)

  target_sources(htslib
    PRIVATE
    sambam.c)

  target_include_directories(htslib
    PRIVATE
    $<$<BOOL:${HTSLIB_FOUND}>:${HTSLIB_INCLUDE_DIRS}>
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  set_target_properties(htslib
    PROPERTIES
    NO_SONAME ON
    OUTPUT_NAME
    _htslib)

  target_compile_definitions(htslib
    PRIVATE
    LIBHTSLIB)

  target_link_libraries(htslib
    PRIVATE
    monetdb_config_header
    bat
    monetdb5
    sqlinclude
    HTSLIB::HTSLIB)

  install(TARGETS htslib
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
    COMPONENT htslib)

  if(WIN32)
    install(FILES
      $<TARGET_PDB_FILE:htslib>
      DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5-${MONETDB_VERSION}
      OPTIONAL)
  endif()
endif()

