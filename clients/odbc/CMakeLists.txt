#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
#]]

if(ODBC_FOUND)
  if(WIN32)
    add_subdirectory(winsetup)
  else()
    add_subdirectory(setup)
  endif()
  add_subdirectory(driver)
  if(TESTING)
    add_subdirectory(samples)
    add_subdirectory(tests)
  endif()
endif()
