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

add_executable(ODBCgetInfo
  ODBCgetInfo.c)
target_link_libraries(ODBCgetInfo
  PRIVATE
  ODBC::ODBC)

add_executable(ODBCStmtAttr
  ODBCStmtAttr.c)

target_link_libraries(ODBCStmtAttr
  PRIVATE
  ODBC::ODBC)

add_executable(ODBCmetadata
  ODBCmetadata.c)

target_link_libraries(ODBCmetadata
  PRIVATE
  ODBC::ODBC)

install(TARGETS
  ODBCgetInfo
  ODBCStmtAttr
  ODBCmetadata
  RUNTIME
  DESTINATION
  ${CMAKE_INSTALL_BINDIR}
  COMPONENT clienttest)

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:ODBCgetInfo>
    $<TARGET_PDB_FILE:ODBCStmtAttr>
    $<TARGET_PDB_FILE:ODBCmetadata>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
endif()
