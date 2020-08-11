#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(LIBR_FOUND)
  add_library(rapi MODULE)

  target_sources(rapi
    PRIVATE
    rapi.c)

  target_include_directories(rapi
    PRIVATE
    $<TARGET_PROPERTY:mapi,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(rapi 
    PRIVATE
    monetdb_config_header
    mutils 
    stream
    bat
    monetdb5
    sqlinclude
    LibR::LibR)

  target_compile_definitions(rapi 
    PRIVATE 
    LIBRAPI)

  set_target_properties(rapi 
    PROPERTIES 
    OUTPUT_NAME 
    _rapi)

  install(TARGETS 
    rapi 
    DESTINATION 
    ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT rapi)

  install(FILES 
    rapi.R 
    DESTINATION 
    ${CMAKE_INSTALL_LIBDIR}/monetdb5
    COMPONENT rapi)
endif()
