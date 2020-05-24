#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

# Test for the existance of these cmake variables, the source code assumes
# that they are set by the buildsystem.

if(NOT DEFINED HAVE_CUDF)
  message(FATAL_ERROR "variable HAVE_CUDF not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_have_cudf_var.c
  @ONLY)

add_executable(test_have_cudf_var)
target_sources(test_have_cudf_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_have_cudf_var.c)
add_test(testDetectHave_cudf test_have_cudf_var)

if(DEFINED HAVE_LIBR)
  if(NOT DEFINED RHOME)
    message(FATAL_ERROR "variable RHOME not defined")
    set(DETECT "1")
  else()
    set(DETECT "0")
  endif()

  configure_file(test_cmake_var.c.in
    ${CMAKE_CURRENT_BINARY_DIR}/test_rhome_var.c
    @ONLY)

  add_executable(test_rhome_var)
  target_sources(test_rhome_var
    PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/test_rhome_var.c)
  add_test(testDetectRhome test_rhome_var)
endif()
