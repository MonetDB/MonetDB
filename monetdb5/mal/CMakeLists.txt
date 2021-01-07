#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

set(mal_public_headers
  ${CMAKE_CURRENT_SOURCE_DIR}/mal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_authorize.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_client.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_errors.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_exception.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_import.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_instruction.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_linker.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_namespace.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_stack.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_type.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mal_prelude.h
  ${CMAKE_CURRENT_SOURCE_DIR}/mel.h)

add_library(mal OBJECT)

target_sources(mal
  PRIVATE
  mal.c
  mal_atom.c mal_atom.h
  mal_authorize.c
  mal_builder.c mal_builder.h
  mal_client.c
  mal_debugger.c mal_debugger.h
  mal_exception.c
  mal_factory.c mal_factory.h
  mal_function.c mal_function.h
  mal_import.c
  mal_runtime.c mal_runtime.h
  mal_instruction.c
  mal_resource.c mal_resource.h
  mal_interpreter.c mal_interpreter.h
  mal_dataflow.c mal_dataflow.h
  mal_linker.c  mal_linker.h
  mal_listing.c mal_listing.h
  mal_module.c mal_module.h
  mal_namespace.c
  mal_parser.c mal_parser.h
  mal_profiler.c mal_profiler.h
  mal_resolve.c mal_resolve.h
  mal_scenario.c mal_scenario.h
  mal_session.c mal_session.h
  mal_stack.c mal_stack.h
  mal_type.c
  mal_utils.c mal_utils.h
  mal_embedded.c mal_embedded.h
  mal_private.h mal_internal.h
  mal_prelude.c
  PUBLIC
  ${mal_public_headers})

target_include_directories(mal
  PRIVATE
  $<$<BOOL:${COMMONCRYPTO_FOUND}>:${COMMONCRYPTO_INCLUDE_DIR}>
  $<$<BOOL:${OPENSSL_FOUND}>:$<TARGET_PROPERTY:OpenSSL::Crypto,INTERFACE_INCLUDE_DIRECTORIES>>
  PUBLIC
  $<TARGET_PROPERTY:bat,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_compile_definitions(mal
  PRIVATE
  LIBMAL
  LIBMONETDB5)

target_link_libraries(mal
  PRIVATE
  monetdb_config_header
  mcrypt
  msabaoth
  mutils
  stream
  mapi
  bat)

set_target_properties(mal
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  PUBLIC_HEADER "${mal_public_headers}")

install(TARGETS
  mal
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT serverdev
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
