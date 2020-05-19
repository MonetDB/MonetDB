#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

# For now these tests only work on special docker containers.

project(MonetDB-unittest LANGUAGES C)

include(os_release_info)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  get_os_release_info(LINUX_DISTRO LINUX_DISTRO_VERSION)
endif()

add_executable(test_detect test_detect.c)
target_link_libraries(test_detect
  PRIVATE
  monetdb_config_header)
add_test(testDetect test_detect)

add_executable(test_detect_pcre test_detect_pcre.c)
target_link_libraries(test_detect_pcre
  PRIVATE
  monetdb_config_header)
add_test(testDetectPcre test_detect_pcre)

add_executable(test_detect_bz2 test_detect_bz2.c)
target_link_libraries(test_detect_bz2
  PRIVATE
  monetdb_config_header)
add_test(testDetectBz2 test_detect_bz2)

add_executable(test_detect_curl test_detect_curl.c)
target_link_libraries(test_detect_curl
  PRIVATE
  monetdb_config_header)
add_test(testDetectCurl test_detect_curl)

add_executable(test_detect_libz test_detect_libz.c)
target_link_libraries(test_detect_libz
  PRIVATE
  monetdb_config_header)
add_test(testDetectLibz test_detect_libz)

add_executable(test_detect_lz4 test_detect_lz4.c)
target_link_libraries(test_detect_lz4
  PRIVATE
  monetdb_config_header)
add_test(testDetectLz4 test_detect_lz4)

add_executable(test_detect_proj test_detect_proj.c)
target_link_libraries(test_detect_proj
  PRIVATE
  monetdb_config_header)
add_test(testDetectProj test_detect_proj)

include(${CMAKE_CURRENT_SOURCE_DIR}/detect-netcdf.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-lzma.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-readline.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-unixgetaddrinfo.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-wingetaddrinfo.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-getaddrinfo.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadh.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadinit.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadkill.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadsig.cmake)

include(${CMAKE_CURRENT_SOURCE_DIR}/set-dir-vars.cmake)
