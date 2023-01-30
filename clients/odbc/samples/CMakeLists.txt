#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
#]]

include_directories($<$<BOOL:${ODBC_FOUND}>:${ODBC_INCLUDE_DIRS}>)

add_executable(odbcsample1
  odbcsample1.c)

target_link_libraries(odbcsample1
  PRIVATE
  ODBC::ODBC)

add_executable(arraytest
  arraytest.c)

target_link_libraries(arraytest
  PRIVATE
  ODBC::ODBC)

install(TARGETS
  odbcsample1
  arraytest
  RUNTIME
  DESTINATION
  ${CMAKE_INSTALL_BINDIR}
  COMPONENT clienttest)

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:odbcsample1>
    $<TARGET_PDB_FILE:arraytest>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
endif()
