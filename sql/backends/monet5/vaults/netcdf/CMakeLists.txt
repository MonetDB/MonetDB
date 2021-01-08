#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(NETCDF_FOUND)
  # the netcdf library is also called netcdf, so cmake gets confused.
  # Give it another name
  add_library(netcdff MODULE)

  set(include_sql_files
    74_netcdf)

  create_include_object(
    SQL_LANGUAGE
    name_module "netcdff"
    modules_list ${include_sql_files})

  target_sources(netcdff
    PRIVATE
    netcdf.c 
    netcdf_vault.h
    ${MONETDB_CURRENT_SQL_SOURCES})

  target_include_directories(netcdff
    PRIVATE
    $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlcommon,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlstorage,INTERFACE_INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(netcdff 
    PRIVATE 
    monetdb_config_header
    mutils
    bat
    monetdb5
    sqlinclude
    NetCDF::NetCDF)

  set_target_properties(netcdff 
    PROPERTIES 
    OUTPUT_NAME 
    _netcdf)

  target_compile_definitions(netcdff 
    PRIVATE 
    LIBNCDF)

  install(TARGETS 
    netcdff 
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT server)
endif()
