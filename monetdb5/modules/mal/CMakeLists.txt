#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
#]]

add_library(malmodules OBJECT)

set(include_mal_files
  language
  bbp
  profiler
  batExtensions
  orderidx
  inspect
  manual
  mal_io
  mkey
  manifold
  iterator
  clients
  factories
  groupby
  mdb
  pcre
  mat
  transaction
  oltp
  wlc
  mal_mapi
  remote
  txtsim
  tokenizer
  sample
  json_util
  batmtime
  querylog
  sysmon
  tracer)

if(HAVE_HGE)
  list(APPEND include_mal_files
    00_batExtensions_hge
    00_batcalc_hge
    00_calc_hge
    00_iterator_hge
    00_language_hge
    00_mal_mapi_hge
    00_mkey_hge)
endif()

list(APPEND include_mal_files
  01_batcalc
  01_calc)

create_include_object(
  MAL_LANGUAGE
  name_module "malmodules"
  modules_list ${include_mal_files})

target_sources(malmodules
  PRIVATE
  batExtensions.c batExtensions.h
  bbp.c bbp.h
  clients.c clients.h
  factories.c factories.h
  groupby.c groupby.h
  inspect.c inspect.h
  orderidx.c orderidx.h
  iterator.c  iterator.h
  projectionpath.c  projectionpath.h
  language.c language.h
  mal_io.c mal_io.h
  mal_mapi.c mal_mapi.h
  manual.c manual.h
  mat.c mat.h
  mdb.c mdb.h
  mkey.c mkey.h
  manifold.c manifold.h
  oltp.c oltp.h
  wlc.c
  pcre.c
  profiler.c profiler.h
  querylog.c querylog.h
  remote.c remote.h
  sysmon.c sysmon.h
  tablet.c tablet.h
  tokenizer.c tokenizer.h
  transaction.c
  txtsim.c txtsim.h
  sample.c sample.h
  json_util.c json_util.h
  calc.c batcalc.c
  tracer.c tracer.h
  ${MONETDB_CURRENT_MAL_SOURCES}
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

install(TARGETS
  malmodules
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
