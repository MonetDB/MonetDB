#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.
#]]

add_subdirectory(mapilib)
if(WITH_MAPI_CLIENT)
  add_subdirectory(mapiclient)
endif()
add_subdirectory(odbc)
if(TESTING)
  add_subdirectory(examples)
endif()
if(WIN32)
  add_subdirectory(NT)
endif()
