#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

include(os_release_info)

set(CPACK_SOURCE_GENERATOR "TGZ;ZIP")
set(CPACK_GENERATOR "TGZ;ZIP")

list (APPEND CPACK_SOURCE_IGNORE_FILES "Tests")
list (APPEND CPACK_SOURCE_IGNORE_FILES "\\\\.hg")
list (APPEND CPACK_SOURCE_IGNORE_FILES "/test/")
list (APPEND CPACK_SOURCE_IGNORE_FILES "benchmarks")

include(monetdb-deb-packages)
include(monetdb-rpm-packages)
include(monetdb-wix-packages)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  get_os_release_info(LINUX_DISTRO LINUX_DISTRO_VERSION)

  if (${LINUX_DISTRO} STREQUAL "debian")
    monetdb_debian_extra_files()
  endif()
endif()

include(CPack)
