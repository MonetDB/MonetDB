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

add_library(malmodules OBJECT)

target_sources(malmodules
  PRIVATE
  language.c
  bbp.c
  profiler.c
  batExtensions.c
  batMask.c
  orderidx.c orderidx.h
  inspect.c
  manual.c
  mal_io.c
  mkey.c
  manifold.c manifold.h
  iterator.c
  clients.c clients.h
  groupby.c
  mdb.c
  pcre.c
  mat.c
  mal_mapi.c
  remote.c remote.h
  txtsim.c
  sample.c
  querylog.c querylog.h
  sysmon.c
  tracer.c
  projectionpath.c
  tablet.c tablet.h
  batcalc.c calc.c)

target_include_directories(malmodules
  PRIVATE
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:kernel,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:mapi,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:atoms,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:optimizer,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_compile_definitions(malmodules
  PRIVATE
  LIBMONETDB5)

target_link_libraries(malmodules
  PRIVATE
  mutils
  $<$<BOOL:${PCRE_FOUND}>:PCRE::PCRE>
  mapi
  monetdb_config_header)

set_target_properties(malmodules
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON)
