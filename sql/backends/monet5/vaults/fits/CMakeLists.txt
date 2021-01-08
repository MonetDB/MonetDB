#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(HAVE_FITS)
  add_library(fits MODULE)

  set(include_sql_files
    72_fits)

  create_include_object(
    SQL_LANGUAGE
    name_module "fits"
    modules_list ${include_sql_files})

  target_include_directories(fits
    PRIVATE
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_sources(fits
    PRIVATE
    fits.c
    fits.h
    ${MONETDB_CURRENT_SQL_SOURCES})

  set_target_properties(fits 
    PROPERTIES 
    NO_SONAME ON
    OUTPUT_NAME 
    _fits)

  target_compile_definitions(fits 
    PRIVATE 
    LIBFITS)

  target_link_libraries(fits 
    PRIVATE 
    monetdb_config_header
    mutils
    bat
    monetdb5
    sqlinclude
    CFitsIO::CFitsIO)

  install(TARGETS fits 
    DESTINATION 
    ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT fits)
endif()
