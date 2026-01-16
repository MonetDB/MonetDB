#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

set(moptions_public_headers
  ${CMAKE_CURRENT_SOURCE_DIR}/monet_options.h)

add_library(moptions
  STATIC)

target_sources(moptions
  PRIVATE
  monet_options.c
  PUBLIC
  ${moptions_public_headers})

target_include_directories(moptions
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_link_libraries(moptions
  PRIVATE
  monetdb_config_header)

if(NOT MONETDB_STATIC)
  set_target_properties(moptions
    PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    PUBLIC_HEADER "${moptions_public_headers}")
endif()

install(FILES
  monet_options.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT
  monetdbdev)

if(MONETDB_STATIC)
  install(TARGETS
    moptions
    COMPONENT monetdbdev
    RUNTIME
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PUBLIC_HEADER
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()
