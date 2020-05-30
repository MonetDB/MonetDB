#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(HAVE_LIDAR)
  add_library(lidar 
    MODULE 
    lidar.c lidar.h)

  target_include_directories(lidar
    PRIVATE
    $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sqlserver,INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(lidar 
    PRIVATE 
    monetdb_config_header
    mutils
    stream
    gdk
    monetdb5
    sqlinclude
    Liblas::Liblas)

  set_target_properties(lidar 
    PROPERTIES 
    OUTPUT_NAME 
    _lidar)

  target_compile_definitions(lidar 
    PRIVATE 
    LIBLIDAR)

  install(TARGETS lidar 
    DESTINATION 
    ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT lidar)

  install(FILES 
   lidar.mal 
   DESTINATION 
   ${CMAKE_INSTALL_LIBDIR}/monetdb5
   COMPONENT lidar)

  install(FILES
    75_lidar.mal
    DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5/autoload
    COMPONENT lidar)

  install(FILES
    75_lidar.sql
    DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5/createdb
    COMPONENT lidar)
endif()
