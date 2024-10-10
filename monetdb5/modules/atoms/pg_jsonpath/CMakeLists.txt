#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

add_library(pg_jsonpath OBJECT)

FLEX_TARGET(jsonpath_scanner
    jsonpath_scan.l 
    ${CMAKE_CURRENT_BINARY_DIR}/jsonpath_scan.c
    DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/jsonpath_scan.h)

BISON_TARGET(jsonpath_parser
  jsonpath_gram.y
  ${CMAKE_CURRENT_BINARY_DIR}/jsonpath_gram.c
  COMPILE_FLAGS "-d -p json -Wno-conflicts-sr -Wno-conflicts-rr"
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/jsonpath_gram.h)

  target_sources(pg_jsonpath
  PRIVATE
  ${FLEX_jsonpath_scanner_OUTPUTS}
  ${BISON_jsonpath_parser_OUTPUT_SOURCE})

target_link_libraries(pg_jsonpath
  PRIVATE
  monetdb_config_header
  bat
  sqlinclude
  sqlcommon)

target_include_directories(pg_jsonpath
  PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)


target_compile_definitions(pg_jsonpath
  PRIVATE
  LIBMONETDB5)

set_target_properties(pg_jsonpath
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON)
