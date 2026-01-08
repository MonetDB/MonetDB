#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

include(os_release_info)

set(CPACK_SOURCE_GENERATOR "TGZ;ZIP")
set(CPACK_GENERATOR "TGZ;ZIP")

list (APPEND CPACK_SOURCE_IGNORE_FILES "Tests")
list (APPEND CPACK_SOURCE_IGNORE_FILES "\\\\.hg")
list (APPEND CPACK_SOURCE_IGNORE_FILES "/test/")
list (APPEND CPACK_SOURCE_IGNORE_FILES "benchmarks")

include(CPack)
