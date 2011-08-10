/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

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
 * SQLSetStmtOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetStmtAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLSetStmtOption(SQLHSTMT hStmt,
		 SQLUSMALLINT fOption,
		 SQLULEN vParam)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetStmtOption " PTRFMT " %u %lx\n",
		PTRFMTCAST hStmt, (unsigned int) fOption,
		(unsigned long) vParam);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	switch (fOption) {
		/* only the ODBC 1.0 and ODBC 2.0 options */
	case SQL_ROWSET_SIZE:
		fOption = SQL_ATTR_ROW_ARRAY_SIZE;
		/* fall through */
	case SQL_QUERY_TIMEOUT:
	case SQL_MAX_ROWS:
	case SQL_NOSCAN:
	case SQL_MAX_LENGTH:
	case SQL_ASYNC_ENABLE:
	case SQL_BIND_TYPE:
	case SQL_CURSOR_TYPE:
	case SQL_CONCURRENCY:
	case SQL_KEYSET_SIZE:
	case SQL_SIMULATE_CURSOR:
	case SQL_RETRIEVE_DATA:
	case SQL_USE_BOOKMARKS:
		/* use mapping as described in ODBC 3.0 SDK Help */
		return SQLSetStmtAttr_(stmt, fOption, (SQLPOINTER) (size_t) vParam, SQL_NTS);
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		break;
	}

	return SQL_ERROR;
}
