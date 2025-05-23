#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

add_library(atoms OBJECT)

target_sources(atoms
  PRIVATE
  streams.c streams.h
  blob.c
  str.c str.h bigram.h
  strptime.c
  url.c
  uuid.c
  json.c
  mtime.c mtime.h
  inet.c
  xml.c xml.h
  batxml.c)

target_include_directories(atoms
  PRIVATE
  $<$<BOOL:${LibXml2_FOUND}>:$<TARGET_PROPERTY:LibXml2::LibXml2,INTERFACE_INCLUDE_DIRECTORIES>>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(atoms
  PRIVATE
  $<$<BOOL:${LibXml2_FOUND}>:LibXml2::LibXml2>
  $<$<BOOL:${Iconv_FOUND}>:Iconv::Iconv>
  stream
  monetdb_config_header
  mutils
  bat
  mal)

target_compile_definitions(atoms
  PRIVATE
  LIBMONETDB5)

set_target_properties(atoms
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON)
