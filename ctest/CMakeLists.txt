#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

if(BUILD_TESTING)
  add_test(NAME TestEcho
    COMMAND ${CMAKE_COMMAND} -E echo 'MonetDB Testing')

  if(CMAKE_UNITTESTS)
    add_subdirectory(cmake)
  endif()
  add_subdirectory(monetdb5)
  add_subdirectory(tools)
endif()
