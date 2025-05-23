#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
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

add_executable(ODBCtester
  ODBCtester.c)

target_link_libraries(ODBCtester
  PRIVATE
  ODBC::ODBC)

add_executable(odbcconnect
  odbcconnect.c)

target_link_libraries(odbcconnect
  PRIVATE
  mutils
  ODBC::ODBC)

install(TARGETS
  ODBCgetInfo
  ODBCStmtAttr
  ODBCmetadata
  ODBCtester
  odbcconnect
  RUNTIME
  DESTINATION
  ${CMAKE_INSTALL_BINDIR}
  COMPONENT clienttest)

if(WIN32)
  install(FILES
    $<TARGET_PDB_FILE:ODBCgetInfo>
    $<TARGET_PDB_FILE:ODBCStmtAttr>
    $<TARGET_PDB_FILE:ODBCmetadata>
    $<TARGET_PDB_FILE:ODBCtester>
    $<TARGET_PDB_FILE:odbcconnect>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL)
endif()
