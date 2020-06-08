#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(WIN32)
  add_custom_target(mtest
    COMMAND
    ${CMAKE_INSTALL_FULL_BINDIR}/monetdb_mtest.bat
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
else()
  add_custom_target(mtest
    COMMAND
    ${CMAKE_INSTALL_FULL_BINDIR}/monetdb_mtest.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()
