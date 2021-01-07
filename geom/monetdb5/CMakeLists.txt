#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(GEOS_FOUND)
  add_library(geomodule MODULE)

  set(include_sql_files
    40_geom)

  create_include_object(
    SQL_LANGUAGE
    name_module "geomodule"
    path "${CMAKE_SOURCE_DIR}/geom/sql"
    modules_list ${include_sql_files})

  target_sources(geomodule
    PRIVATE
    geom.c
    geomBulk.c
    geom_upgrade.c
    ${MONETDB_CURRENT_SQL_SOURCES}
    PUBLIC
    ${geomodule_public_headers})

  target_include_directories(geomodule
    PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlinclude,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(geomodule
    PRIVATE
    mutils
    stream
    bat
    geomlib
    monetdb5
    sql
    monetdb_config_header)

  target_compile_definitions(geomodule
    PRIVATE
    LIBGEOM)

  set_target_properties(geomodule
    PROPERTIES
    OUTPUT_NAME _geom
    PUBLIC_HEADER "${geomodule_public_headers}")

  install(TARGETS
    geomodule
    LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT geom
    ARCHIVE
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    PUBLIC_HEADER
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

# vim: set ts=2:sw=2:et
