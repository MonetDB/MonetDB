#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

if(WIN32)
  set (MONETDB_MTEST_SCRIPT "monetdb_mtest.bat")
else()
  set (MONETDB_MTEST_SCRIPT "monetdb_mtest.sh")
endif()

if (TESTING)
  add_custom_target(mtest
      COMMAND
      ${CMAKE_INSTALL_FULL_BINDIR}/${MONETDB_MTEST_SCRIPT}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()
