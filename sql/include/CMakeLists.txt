#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

set(sqlinclude_public_headers
  sql_catalog.h
  sql_hash.h
  sql_keyword.h
  sql_list.h
  sql_mem.h
  sql_relation.h
  sql_stack.h
  exception_buffer.h)

add_library(sqlinclude INTERFACE)

target_include_directories(sqlinclude
  INTERFACE
  $<TARGET_PROPERTY:mapi,INTERFACE_INCLUDE_DIRECTORIES>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDEDIR}/monetdb>)
