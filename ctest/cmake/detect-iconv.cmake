#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if (${LINUX_DISTRO} STREQUAL "debian")
  if(${LINUX_DISTRO_VERSION} STREQUAL "9")
    set(DETECT "0")
    set(UNDETECT "1")
  endif()
  if(${LINUX_DISTRO_VERSION} STREQUAL "10")
    set(DETECT "0")
    set(UNDETECT "1")
  endif()
elseif (${LINUX_DISTRO} STREQUAL "ubuntu")
  if(${LINUX_DISTRO_VERSION} STREQUAL "18")
    set(DETECT "1")
    set(UNDETECT "0")
  endif()
  if(${LINUX_DISTRO_VERSION} STREQUAL "19")
    set(DETECT "1")
    set(UNDETECT "0")
  endif()
  if(${LINUX_DISTRO_VERSION} STREQUAL "20")
    set(DETECT "1")
    set(UNDETECT "0")
  endif()
elseif(${LINUX_DISTRO} STREQUAL "fedora")
  if(${LINUX_DISTRO_VERSION} STREQUAL "30")
    set(DETECT "0")
    set(UNDETECT "1")
  endif()
  if(${LINUX_DISTRO_VERSION} STREQUAL "31")
    set(DETECT "0")
    set(UNDETECT "1")
  endif()
  if(${LINUX_DISTRO_VERSION} STREQUAL "32")
    set(DETECT "0")
    set(UNDETECT "1")
  endif()
else()
  message(ERROR "Linux distro: ${LINUX_DISTRO} not known")
  message(ERROR "Linux distro version: ${LINUX_DISTRO_VERSION} not known")
endif()

configure_file(test_detect_iconv.c.in
  ${CMAKE_CURRENT_BINARY_DIR}/test_detect_iconv.c
  @ONLY)

add_executable(test_detect_iconv)
target_sources(test_detect_iconv
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/test_detect_iconv.c)
target_link_libraries(test_detect_iconv
  PRIVATE
  monetdb_config_header)
add_test(testDetectIconv test_detect_iconv)
