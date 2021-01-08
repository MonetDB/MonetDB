#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_library(meroutil STATIC)

target_sources(meroutil
  PRIVATE
  control.c control.h
  database.c database.h
  glob.c glob.h
  properties.c properties.h
  utils.c utils.h)

target_include_directories(meroutil
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)

target_link_libraries(meroutil
  PRIVATE
  monetdb_config_header
  mutils
  stream
  $<$<BOOL:${OPENSSL_FOUND}>:OpenSSL::Crypto>)
