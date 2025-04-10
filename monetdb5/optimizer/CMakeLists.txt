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

add_library(optimizer OBJECT)

target_sources(optimizer
  PRIVATE
  optimizer.c optimizer.h optimizer_private.h
  opt_aliases.c opt_aliases.h
  opt_coercion.c opt_coercion.h
  opt_commonTerms.c opt_commonTerms.h
  opt_candidates.c opt_candidates.h
  opt_constants.c opt_constants.h
  opt_costModel.c opt_costModel.h
  opt_dataflow.c opt_dataflow.h
  opt_dict.c opt_dict.h
  opt_for.c opt_for.h
  opt_deadcode.c opt_deadcode.h
  opt_emptybind.c opt_emptybind.h
  opt_evaluate.c opt_evaluate.h
  opt_garbageCollector.c opt_garbageCollector.h
  opt_generator.c opt_generator.h
  opt_querylog.c opt_querylog.h
  opt_inline.c opt_inline.h
  opt_projectionpath.c opt_projectionpath.h
  opt_matpack.c opt_matpack.h
  opt_mergetable.c opt_mergetable.h
  opt_mitosis.c opt_mitosis.h
  opt_multiplex.c opt_multiplex.h
  opt_pipes.c
  opt_reduce.c opt_reduce.h
  opt_remap.c opt_remap.h
  opt_remoteQueries.c opt_remoteQueries.h
  opt_reorder.c opt_reorder.h
  opt_support.c opt_support.h
  opt_pushselect.c opt_pushselect.h
  opt_profiler.c opt_profiler.h
  opt_postfix.c opt_postfix.h
  opt_fastpath.c opt_fastpath.h
  opt_wrapper.c
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/opt_pipes.h)

target_include_directories(optimizer
  PUBLIC
  $<TARGET_PROPERTY:mal,INTERFACE_INCLUDE_DIRECTORIES>
  $<TARGET_PROPERTY:malmodules,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_compile_definitions(optimizer
  PRIVATE
  LIBOPTIMIZER
  LIBMONETDB5)

target_link_libraries(optimizer
  PRIVATE
  monetdb_config_header)

set_target_properties(optimizer
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/opt_pipes.h)
