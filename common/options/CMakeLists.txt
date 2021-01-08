#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

set(moptions_public_headers
  ${CMAKE_CURRENT_SOURCE_DIR}/monet_options.h
  ${CMAKE_CURRENT_SOURCE_DIR}/monet_getopt.h)

add_library(moptions
  STATIC)

target_sources(moptions
  PRIVATE
  monet_options.c
  PUBLIC
  ${moptions_public_headers})

target_include_directories(moptions
  PRIVATE
  $<$<PLATFORM_ID:Windows>:${HAVE_GETOPT_H}>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(moptions
  PRIVATE
  monetdb_config_header
  $<$<PLATFORM_ID:Windows>:${GETOPT_LIB}>)

set_target_properties(moptions
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER "${moptions_public_headers}")

install(FILES
  monet_options.h
  monet_getopt.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT
  monetdbdev)
