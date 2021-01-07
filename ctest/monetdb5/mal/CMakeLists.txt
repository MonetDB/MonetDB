#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

add_executable(test_malEmbeddedBoot test_malEmbeddedBoot.c)
target_link_libraries(test_malEmbeddedBoot
  PRIVATE
  monetdb_config_header
  mal
  mcrypt
  msabaoth
  mutils
  stream
  bat
  ${CMAKE_DL_LIBS}
  Threads::Threads)
target_compile_definitions(test_malEmbeddedBoot PRIVATE LIBMONETDB5)
add_test(malEmbeddedBoot test_malEmbeddedBoot)

add_executable(test_malInclude test_malInclude.c)
target_link_libraries(test_malInclude
  PRIVATE
  monetdb_config_header
  mal
  mcrypt
  msabaoth
  mutils
  stream
  bat
  ${CMAKE_DL_LIBS}
  Threads::Threads)
target_compile_definitions(test_malInclude PRIVATE LIBMONETDB5)
add_test(malInclude test_malInclude)
