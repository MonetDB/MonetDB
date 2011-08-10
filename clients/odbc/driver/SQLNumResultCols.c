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
 * SQLNumResultCols()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQL_API
SQLNumResultCols(SQLHSTMT hStmt,
		 SQLSMALLINT *pnColumnCount)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNumResultCols " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be prepared or executed */
	if (stmt->State == INITED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* check output parameter */
	if (pnColumnCount == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	/* When the query is only prepared (via SQLPrepare) we do not have
	 * the correct nrCols value yet (this is a limitation of the current
	 * MonetDB SQL frontend implementation). */
	/* we only have a correct nrCols value when the query is executed */
	if (stmt->State < EXECUTED0) {
		/* General error */
		addStmtError(stmt, "HY000", "Cannot return the number of output columns. Query must be executed first", 0);
		return SQL_ERROR;
	}

	/* We can now set the "number of output columns" value */
	/* Note: row count can be 0 (for non SELECT queries) */
	*pnColumnCount = stmt->ImplRowDescr->sql_desc_count;

	return SQL_SUCCESS;
}
