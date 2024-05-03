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

set(stream_public_headers
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/stream.h>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/stream_socket.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/stream.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/stream_socket.h>)

add_library(stream
  SHARED)

target_sources(stream
  PRIVATE
  stream.c
  rw.c
  bstream.c
  bs.c
  stdio_stream.c
  winio.c
  compressed.c
  gz_stream.c
  bz2_stream.c
  xz_stream.c
  lz4_stream.c
  socket_stream.c
  mapi_stream.c
  memio.c
  callback.c
  blackhole.c
  fwf.c
  text_stream.c
  $<$<BOOL:${OPENSSL_FOUND}>:openssl_stream.c>
  pump.c
  stream.h
  stream_internal.h
  stream_socket.h
  mapi_prompt.h
  pump.h
  PUBLIC
  ${stream_public_headers})

target_include_directories(stream
  PRIVATE
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>
  $<$<BOOL:${OPENSSL_FOUND}>:${OPENSSL_INCLUDE_DIR}>)

target_compile_definitions(stream
  PRIVATE
  LIBSTREAM)

target_link_libraries(stream
  PRIVATE
  $<$<BOOL:${ZLIB_FOUND}>:ZLIB::ZLIB>
  $<$<BOOL:${BZIP2_FOUND}>:BZip2::BZip2>
  $<$<BOOL:${LIBLZMA_FOUND}>:LibLZMA::LibLZMA>
  $<$<BOOL:${LZ4_FOUND}>:LZ4::LZ4>
  $<$<BOOL:${OPENSSL_FOUND}>:OpenSSL::SSL>
  matomic
  monetdb_config_header
  $<$<PLATFORM_ID:Windows>:ws2_32>
  Threads::Threads)

set_target_properties(stream
  PROPERTIES
  VERSION ${STREAM_VERSION}
  SOVERSION ${STREAM_VERSION_MAJOR}
  PUBLIC_HEADER "${stream_public_headers}")

if (NOT WIN32)
  if(ZLIB_FOUND)
    set(PKG_ZLIB "zlib")
  endif()

  if(BZIP2_FOUND)
    set(PKG_BZIP2 "bzip2")
  endif()

  if(LZ4_FOUND)
    set(PKG_LZ4 "liblz4")
  endif()

  if(LIBLZMA_FOUND)
    set(PKG_LZMA "liblzma")
  endif()

  configure_file(monetdb-stream.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-stream.pc @ONLY)

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb-stream.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    COMPONENT streamdev)
endif()

install(TARGETS
  stream
  COMPONENT stream
  EXPORT streamTargets
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT stream
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT stream
  NAMELINK_COMPONENT streamdev
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT stream
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT streamdev
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT streamTargets
  FILE streamTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${EXPORT_TARGET_PATH}
  COMPONENT streamdev)

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:stream>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    OPTIONAL)
endif()
