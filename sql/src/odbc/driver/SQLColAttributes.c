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
 * SQLColAttributes()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLColAttribute())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN
SQLColAttributes(SQLHSTMT hStmt, SQLUSMALLINT nCol, SQLUSMALLINT nDescType,
		 SQLPOINTER pszDesc, SQLSMALLINT nDescMax,
		 SQLSMALLINT *pcbDesc, SQLINTEGER *pfDesc)
{
	/* use mapping as described in ODBC 3 SDK Help file */
	return SQLColAttribute_(hStmt, nCol, nDescType, pszDesc, nDescMax,
				pcbDesc, pfDesc);

	/* TODO: implement specials semantics for nDescTypes: SQL_COLUMN_TYPE,
	   SQL_COLUMN_NAME, SQL_COLUMN_NULLABLE and SQL_COLUMN_COUNT.
	   See ODBC 3 SDK Help file, SQLColAttributes Mapping.
	 */
}
