#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
#]]

if(HAVE_SAMTOOLS)
  configure_file(bam_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/bam_config.h @ONLY)

  # the bam library is also called bam, so cmake gets confused. Give it another name
  add_library(bamm MODULE bam_loader.c bam_loader.h bam_wrapper.c bam_wrapper.h bam_db_interface.c bam_db_interface.h
			bam_globals.c bam_globals.h bam_lib.c bam_lib.h bam_export.c bam_export.h mykstring.h)

  target_include_directories(bamm
    PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:sql,INCLUDE_DIRECTORIES>
    $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

  target_link_libraries(bamm 
    PRIVATE 
    monetdb_config_header
    mutils
    stream
    gdk
    monetdb5
    sqlinclude
    NetCDF::NetCDF)

  set_target_properties(bamm 
    PROPERTIES 
    OUTPUT_NAME 
    _bam)

  target_compile_definitions(bamm 
    PRIVATE 
    LIBBAM)

  install(TARGETS bamm 
    DESTINATION 
    ${CMAKE_INSTALL_LIBDIR}/monetdb5)

  install(FILES 
   bam.mal 
   DESTINATION 
   ${CMAKE_INSTALL_LIBDIR}/monetdb5)

  install(FILES
    85_bam.mal
    DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5/autoload
    COMPONENT server)

  install(FILES
    85_bam.sql
    DESTINATION
    ${CMAKE_INSTALL_LIBDIR}/monetdb5/createdb
    COMPONENT server)
endif()
