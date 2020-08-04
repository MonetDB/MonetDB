#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

add_library(malmodules OBJECT)

target_sources(malmodules
  PRIVATE
  language.c
  bbp.c bbp.h
  profiler.c profiler.h
  batExtensions.c
  batMask.c
  orderidx.c orderidx.h
  inspect.c inspect.h
  manual.c manual.h
  mal_io.c mal_io.h
  mkey.c mkey.h
  manifold.c manifold.h
  iterator.c  iterator.h
  clients.c clients.h
  factories.c factories.h
  groupby.c groupby.h
  mdb.c
  pcre.c
  mat.c mat.h
  transaction.c
  oltp.c oltp.h
  wlc.c
  mal_mapi.c mal_mapi.h
  remote.c remote.h
  txtsim.c txtsim.h
  tokenizer.c tokenizer.h
  sample.c sample.h
  json_util.c
  querylog.c querylog.h
  sysmon.c sysmon.h
  tracer.c tracer.h
  projectionpath.c  projectionpath.h
  tablet.c tablet.h
  batcalc.c calc.c 
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/wlc.h)

target_include_directories(malmodules
  PRIVATE
  $<TARGET_PROPERTY:mal,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:kernel,INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:mapi,INCLUDE_DIRECTORIES>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_compile_definitions(malmodules
  PRIVATE
  LIBMAL
  LIBATOMS
  LIBKERNEL
  LIBOPTIMIZER
  LIBSCHEDULER
  LIBMONETDB5)

target_link_libraries(malmodules
  PUBLIC
  mutils
  $<$<BOOL:${PCRE_FOUND}>:PCRE::PCRE>
  mapi
  PRIVATE
  monetdb_config_header)

set_target_properties(malmodules
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/wlc.h)
