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
 * SQLExtendedFetch
 * CLI Compliance: Deprecated
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLExtendedFetch(SQLHSTMT StatementHandle,
		 SQLUSMALLINT FetchOrientation,
		 SQLLEN FetchOffset,
#ifdef BUILD_REAL_64_BIT_MODE	/* note: only defined on Debian Lenny */
		 SQLUINTEGER  *RowCountPtr,
#else
		 SQLULEN *RowCountPtr,
#endif
		 SQLUSMALLINT *RowStatusArray)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLUSMALLINT *array_status_ptr;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLExtendedFetch " PTRFMT " %d " LENFMT "\n",
		PTRFMTCAST StatementHandle, FetchOrientation,
		LENCAST FetchOffset);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State < EXECUTED0 || stmt->State == FETCHED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	array_status_ptr = stmt->ApplRowDescr->sql_desc_array_status_ptr;
	stmt->ApplRowDescr->sql_desc_array_status_ptr = RowStatusArray;

	rc = SQLFetchScroll_(stmt, FetchOrientation, FetchOffset);

	stmt->ApplRowDescr->sql_desc_array_status_ptr = array_status_ptr;

	if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
		stmt->State = EXTENDEDFETCHED;

	if (SQL_SUCCEEDED(rc) && RowCountPtr) {
#ifdef BUILD_REAL_64_BIT_MODE	/* note: only defined on Debian Lenny */
		*RowCountPtr = (SQLUINTEGER) stmt->rowSetSize;
#else
		*RowCountPtr = (SQLULEN) stmt->rowSetSize;
#endif
	}

	return rc;
}
