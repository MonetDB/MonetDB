#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
#]]

include_directories($<$<BOOL:${ODBC_FOUND}>:${ODBC_INCLUDE_DIRS}>)

if(TESTING)
  add_executable(ODBCStmtAttr
    ODBCStmtAttr.c)

  target_link_libraries(ODBCStmtAttr
    PRIVATE
    ODBC::ODBC)

  install(TARGETS
    ODBCStmtAttr
    RUNTIME
    DESTINATION
    ${CMAKE_INSTALL_BINDIR}
    COMPONENT clienttest)

  if(WIN32)
    install(FILES
      $<TARGET_PDB_FILE:ODBCStmtAttr>
      DESTINATION ${CMAKE_INSTALL_BINDIR}
      OPTIONAL)
  endif()
endif()
