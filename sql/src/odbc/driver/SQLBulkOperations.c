/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************************************
 * SQLBulkOperations()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLBulkOperations(
	SQLHSTMT	hStmt,
	SQLSMALLINT	nOperation )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;

	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check nOperation code */
	switch (nOperation)
	{
		case SQL_ADD:
		case SQL_UPDATE_BY_BOOKMARK:
		case SQL_DELETE_BY_BOOKMARK:
		case SQL_FETCH_BY_BOOKMARK:
			break;
		default:
			/* HY092 = Invalid attribute identifier */
			addStmtError(stmt, "HY092", NULL, 0);
			return SQL_ERROR;
	}

	/* TODO: finish implementation */

	/* IM001 = Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);
	return SQL_ERROR;
}
