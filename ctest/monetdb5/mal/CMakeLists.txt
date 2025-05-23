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

add_executable(test_malEmbeddedBoot test_malEmbeddedBoot.c)
target_link_libraries(test_malEmbeddedBoot
  PRIVATE
  monetdb_config_header
  mal
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
  mutils
  stream
  bat
  ${CMAKE_DL_LIBS}
  Threads::Threads)
target_compile_definitions(test_malInclude PRIVATE LIBMONETDB5)
add_test(malInclude test_malInclude)
