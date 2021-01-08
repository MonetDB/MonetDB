#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

# For now these tests only work on special docker containers.

include(os_release_info)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  get_os_release_info(LINUX_DISTRO LINUX_DISTRO_VERSION)
endif()

include(${CMAKE_CURRENT_SOURCE_DIR}/detect-iconv.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadh.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pcre.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-openssl.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-commoncrypto.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-bz2.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-curl.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-lzma.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-xml.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-libz.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-lz4.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-proj.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-netcdf.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-readline.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-libr.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-snappy.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-fits.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-uuid.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-valgrind.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-geos.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-gdal.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-libpy3.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-unixgetaddrinfo.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-wingetaddrinfo.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-getaddrinfo.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadinit.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadkill.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/detect-pthreadsig.cmake)

include(${CMAKE_CURRENT_SOURCE_DIR}/set-dir-vars.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/set-cmake-vars.cmake)

assert_legacy_variable_exists(
  legacy_variablename HAVE_FITS
  variablename CFITSIO_FOUND)
assert_legacy_variable_exists(
  legacy_variablename HAVE_CURL
  variablename CURL_FOUND)
