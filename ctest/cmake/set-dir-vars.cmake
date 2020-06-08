#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

# Test for the existance of these cmake variables, the source code assumes
# that they are set by the buildsystem.
#if(NOT DEFINED BIN_DIR)
#  message(FATAL_ERROR "variable BIN_DIR not defined")
#  set(DETECT "1")
#else()
#  set(DETECT "0")
#endif()

#configure_file(test_cmake_var.c.in
#  ${CMAKE_CURRENT_BINARY_DIR}/test_bin_dir_var.c
#  @ONLY)

#add_executable(test_bin_dir_var)
#target_sources(test_bin_dir_var
#  PRIVATE
#  ${CMAKE_CURRENT_BINARY_DIR}/test_bin_dir_var.c)
#add_test(testDetectBin_dir test_bin_dir_var)

if(NOT DEFINED DIR_SEP)
  message(FATAL_ERROR "variable DIR_SEP not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_dir_sep_var.c
  @ONLY)

add_executable(test_dir_sep_var)
target_sources(test_dir_sep_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_dir_sep_var.c)
add_test(testDetectDir_sep test_dir_sep_var)

if(NOT DEFINED PKGCONFIGDIR)
  message(FATAL_ERROR "variable PKGCONFIGDIR not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_pkgconfigdir_var.c
  @ONLY)

add_executable(test_pkgconfigdir_var)
target_sources(test_pkgconfigdir_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_pkgconfigdir_var.c)
add_test(testDetectPkgconfigdir test_pkgconfigdir_var)

if(NOT DEFINED BINDIR)
  message(FATAL_ERROR "variable BINDIR not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_bindir_var.c
  @ONLY)

add_executable(test_bindir_var)
target_sources(test_bindir_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_bindir_var.c)
add_test(testDetectBindir test_bindir_var)

if(NOT DEFINED LOCALSTATEDIR)
  message(FATAL_ERROR "variable LOCALSTATEDIR not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_localstatedir_var.c
  @ONLY)

add_executable(test_localstatedir_var)
target_sources(test_localstatedir_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_localstatedir_var.c)
add_test(testDetectLocalstatedir test_localstatedir_var)

if(NOT DEFINED DIR_SEP_STR)
  message(FATAL_ERROR "variable DIR_SEP_STR not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_dir_sep_str_var.c
  @ONLY)

add_executable(test_dir_sep_str_var)
target_sources(test_dir_sep_str_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_dir_sep_str_var.c)
add_test(testDetectDir_sep_str test_dir_sep_str_var)

if(NOT DEFINED PATH_SEP)
  message(FATAL_ERROR "variable PATH_SEP not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_path_sep_var.c
  @ONLY)

add_executable(test_path_sep_var)
target_sources(test_path_sep_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_path_sep_var.c)
add_test(testDetectPath_sep test_path_sep_var)

if(NOT DEFINED SO_PREFIX)
  message(FATAL_ERROR "variable SO_PREFIX not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_so_prefix_var.c
  @ONLY)

add_executable(test_so_prefix_var)
target_sources(test_so_prefix_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_so_prefix_var.c)
add_test(testDetectSo_prefix test_so_prefix_var)

if(NOT DEFINED SO_EXT)
  message(FATAL_ERROR "variable SO_EXT not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_so_ext_var.c
  @ONLY)

add_executable(test_so_ext_var)
target_sources(test_so_ext_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_so_ext_var.c)
add_test(testDetectSo_ext test_so_ext_var)

if(NOT DEFINED LIBDIR)
  message(FATAL_ERROR "variable LIBDIR not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_libdir_var.c
  @ONLY)

add_executable(test_libdir_var)
target_sources(test_libdir_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_libdir_var.c)
add_test(testDetectLibdir test_libdir_var)

if(DEFINED HAVE_GETOPT_H)
  if(NOT DEFINED HAVE_GETOPT)
    message(FATAL_ERROR "variable HAVE_GETOPT not defined")
    set(DETECT "1")
  else()
    set(DETECT "0")
  endif()

  configure_file(test_cmake_var.c.in
    ${CMAKE_CURRENT_BINARY_DIR}/test_have_getopt_var.c
    @ONLY)

  add_executable(test_have_getopt_var)
  target_sources(test_have_getopt_var
    PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/test_have_getopt_var.c)
  add_test(testDetectHave_getopt test_have_getopt_var)
endif()

if(NOT DEFINED ENABLE_STATIC_ANALYSIS)
  message(FATAL_ERROR "variable ENABLE_STATIC_ANALYSIS not defined")
  set(DETECT "1")
else()
  set(DETECT "0")
endif()

configure_file(test_cmake_var.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_enable_static_var.c
  @ONLY)

add_executable(test_enable_static_var)
target_sources(test_enable_static_var
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_enable_static_var.c)
add_test(testDetectEnable_static test_enable_static_var)


