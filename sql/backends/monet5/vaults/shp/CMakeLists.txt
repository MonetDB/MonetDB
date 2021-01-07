#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(GDAL_FOUND)
  add_library(shp MODULE)

  set(include_sql_files
    75_shp)

  create_include_object(
    SQL_LANGUAGE
    name_module "shp"
    modules_list ${include_sql_files})

  target_sources(shp
    PRIVATE
    shp.c shp.h
    ${MONETDB_CURRENT_SQL_SOURCES})

  target_include_directories(shp
    PRIVATE
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:geomlib,INTERFACE_INCLUDE_DIRECTORIES>
    $<$<VERSION_LESS:${CMAKE_VERSION},3.14.0>:${GDAL_INCLUDE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(shp 
    PRIVATE
    monetdb_config_header
    mutils
    bat
    monetdb5
    sqlinclude
    $<$<VERSION_LESS:${CMAKE_VERSION},3.14.0>:${GDAL_LIBRARY}>
    $<$<VERSION_GREATER_EQUAL:${CMAKE_VERSION},3.14.0>:GDAL::GDAL>)

  set_target_properties(shp 
    PROPERTIES 
    OUTPUT_NAME 
    _shp)

  target_compile_definitions(shp 
    PRIVATE 
    LIBGTIFF)

  install(TARGETS 
    shp 
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server)
endif()
