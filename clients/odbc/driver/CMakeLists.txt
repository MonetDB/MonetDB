#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

add_library(MonetODBC
  SHARED)

target_sources(MonetODBC
  PRIVATE
  ODBCConvert.c
  ODBCDbc.c
  ODBCDbc.h
  ODBCDesc.c
  ODBCDesc.h
  ODBCEnv.c
  ODBCEnv.h
  ODBCError.c
  ODBCError.h
  ODBCGlobal.h
  ODBCQueries.h
  ODBCStmt.c
  ODBCStmt.h
  ODBCUtil.c
  ODBCUtil.h
  SQLAllocConnect.c
  SQLAllocEnv.c
  SQLAllocHandle.c
  SQLAllocStmt.c
  SQLBindCol.c
  SQLBindParameter.c
  SQLBrowseConnect.c
  SQLBulkOperations.c
  SQLCancel.c
  SQLCloseCursor.c
  SQLColAttribute.c
  SQLColAttributes.c
  SQLColumnPrivileges.c
  SQLColumns.c
  SQLConnect.c
  SQLCopyDesc.c
  SQLDataSources.c
  SQLDescribeCol.c
  SQLDescribeParam.c
  SQLDisconnect.c
  SQLDriverConnect.c
  SQLDrivers.c
  SQLEndTran.c
  SQLError.c
  SQLExecDirect.c
  SQLExecute.c
  SQLExtendedFetch.c
  SQLFetch.c
  SQLFetchScroll.c
  SQLForeignKeys.c
  SQLFreeConnect.c
  SQLFreeEnv.c
  SQLFreeHandle.c
  SQLFreeStmt.c
  SQLGetConnectAttr.c
  SQLGetConnectOption.c
  SQLGetCursorName.c
  SQLGetData.c
  SQLGetDescField.c
  SQLGetDescRec.c
  SQLGetDiagField.c
  SQLGetDiagRec.c
  SQLGetEnvAttr.c
  SQLGetFunctions.c
  SQLGetInfo.c
  SQLGetStmtAttr.c
  SQLGetStmtOption.c
  SQLGetTypeInfo.c
  SQLMoreResults.c
  SQLNativeSql.c
  SQLNumParams.c
  SQLNumResultCols.c
  SQLParamData.c
  SQLParamOptions.c
  SQLPrepare.c
  SQLPrimaryKeys.c
  SQLProcedureColumns.c
  SQLProcedures.c
  SQLPutData.c
  SQLRowCount.c
  SQLSetConnectAttr.c
  SQLSetConnectOption.c
  SQLSetCursorName.c
  SQLSetDescField.c
  SQLSetDescRec.c
  SQLSetEnvAttr.c
  SQLSetParam.c
  SQLSetPos.c
  SQLSetScrollOptions.c
  SQLSetStmtAttr.c
  SQLSetStmtOption.c
  SQLSpecialColumns.c
  SQLStatistics.c
  SQLTablePrivileges.c
  SQLTables.c
  SQLTransact.c
  driver.rc
  ODBC.syms)

target_include_directories(MonetODBC
  PRIVATE
  $<$<BOOL:${ODBC_FOUND}>:${ODBC_INCLUDE_DIR}>
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

target_compile_definitions(MonetODBC
  PRIVATE 
  LIBMONETODBCS)

target_link_libraries(MonetODBC
  PRIVATE
  monetdb_config_header
  mutils
  mapi
	ODBC::ODBC)

install(TARGETS
  MonetODBC
  RUNTIME
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT odbc
  LIBRARY
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT odbc
  ARCHIVE
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
  COMPONENT odbc
  PUBLIC_HEADER
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT clientdev
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# vim: set ts=2:sw=2:et
