/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 * 
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

/**********************************************************************
 * SQLTablePrivileges()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not implemented (it only sets an error),
 * because monetDB SQL frontend does not support table based authorization.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLTablePrivileges(SQLHSTMT hStmt, SQLCHAR *szCatalogName,
		   SQLSMALLINT nCatalogNameLength, SQLCHAR *szSchemaName,
		   SQLSMALLINT nSchemaNameLength, SQLCHAR *szTableName,
		   SQLSMALLINT nTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

	(void) szCatalogName;	/* Stefan: unused!? */
	(void) nCatalogNameLength;	/* Stefan: unused!? */
	(void) szSchemaName;	/* Stefan: unused!? */
	(void) nSchemaNameLength;	/* Stefan: unused!? */
	(void) szTableName;	/* Stefan: unused!? */
	(void) nTableNameLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* IM001 = Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}
