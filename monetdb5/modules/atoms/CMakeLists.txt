#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

add_library(atoms OBJECT)

target_sources(atoms
  PRIVATE
  streams.c streams.h
  blob.c blob.h
  color.c color.h
  str.c str.h
  strptime.c
  url.c
  uuid.c uuid.h
  json.c
  mtime.c
  mtime_analytic.c
  inet.c
  identifier.c
  xml.c xml.h
  batxml.c
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mtime.h)

target_include_directories(atoms
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(atoms
  PUBLIC
  $<$<BOOL:${UUID_FOUND}>:UUID::UUID>
  stream
  PRIVATE
  monetdb_config_header
  mutils
  gdk
  mal)

if(WIN32)
  target_include_directories(atoms
    PRIVATE
    $<$<BOOL:${LibXml2_FOUND}>:${LIBXML2_INCLUDE_DIR}>)

  target_compile_definitions(atoms
    PRIVATE
    LIBMAL
    LIBATOMS
    LIBKERNEL
    LIBOPTIMIZER
    LIBSCHEDULER
    LIBMONETDB5)

  target_link_libraries(atoms
    PUBLIC
    $<$<BOOL:${LibXml2_FOUND}>:${LIBXML2_LIBRARIES}>)
else()
  target_link_libraries(atoms
    PUBLIC
    $<$<BOOL:${LibXml2_FOUND}>:LibXml2::LibXml2>)
endif()

set_target_properties(atoms
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/mtime.h)
