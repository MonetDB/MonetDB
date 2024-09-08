#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
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

if(CTAGS_PATH)
add_custom_target(tags
  COMMAND ${CTAGS_PATH} -R --kinds-C=+pLl --fields=+iaS --exclude=*.js --exclude=build --exclude=install
        ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/tags ${CMAKE_CURRENT_SOURCE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()

if(CSCOPE_PATH)
add_custom_target(cscope
  COMMAND ${CSCOPE_PATH} -bcqR -s${CMAKE_CURRENT_SOURCE_DIR}
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cscope.out ${CMAKE_CURRENT_SOURCE_DIR}/
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cscope.in.out ${CMAKE_CURRENT_SOURCE_DIR}/
        COMMAND ln -sf ${CMAKE_CURRENT_BINARY_DIR}/cscope.po.out ${CMAKE_CURRENT_SOURCE_DIR}/
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()
