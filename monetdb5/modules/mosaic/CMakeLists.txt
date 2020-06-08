#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

add_library(mosaic OBJECT)

target_sources(mosaic
  PRIVATE
    mosaic.c mosaic_template.h
    mosaic_utility.h
    mosaic_hdr.c mosaic_hdr.h
    mosaic_select_template.h
    mosaic_projection_template.h
    mosaic_join.h
    mosaic_raw.c mosaic_raw.h mosaic_raw_templates.h
    mosaic_runlength.c mosaic_runlength.h mosaic_runlength_templates.h
    mosaic_dict.c mosaic_dict.h
    mosaic_dict256.c mosaic_dict256.h
    mosaic_dictionary.h mosaic_dictionary_templates.h
    mosaic_delta.c mosaic_delta.h mosaic_delta_templates.h
    mosaic_linear.c mosaic_linear.h mosaic_linear_templates.h
    mosaic_frame.c mosaic_frame.h mosaic_frame_templates.h
    mosaic_prefix.c mosaic_prefix.h mosaic_prefix_templates.h
    mosaic_private.h
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mosaic.h)

target_include_directories(mosaic
  PRIVATE
  $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:atoms,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_compile_definitions(mosaic
  PRIVATE
  LIBMAL
  LIBATOMS
  LIBKERNEL
  LIBMOSAIC
  LIBOPTIMIZER
  LIBSCHEDULER
  LIBMONETDB5)

target_link_libraries(mosaic
  PUBLIC
  stream
  PRIVATE
  monetdb_config_header
  mutils
  gdk)

set_target_properties(mosaic
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/mosaic.h)

install(TARGETS
  mosaic
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
