/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
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
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************************************
 * SQLSetStmtOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetStmtAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQLSetStmtOption(
	SQLHSTMT	hStmt,
	UWORD		fOption,
	UDWORD		vParam )
{
	switch (fOption) {
		/* only the ODBC 1.0 and ODBC 2.0 options */
		case SQL_QUERY_TIMEOUT:
		case SQL_MAX_ROWS:
		case SQL_NOSCAN:
		case SQL_MAX_LENGTH:
		case SQL_ASYNC_ENABLE:
		case SQL_BIND_TYPE:
		case SQL_CURSOR_TYPE:
		case SQL_CONCURRENCY:
		case SQL_KEYSET_SIZE:
		case SQL_ROWSET_SIZE:
		case SQL_SIMULATE_CURSOR:
		case SQL_RETRIEVE_DATA:
		case SQL_USE_BOOKMARKS:
			/* use mapping as described in ODBC 3.0 SDK Help */
			return SQLSetStmtAttr(hStmt, fOption, &vParam, 0);
		default:
		{
			ODBCStmt * stmt = (ODBCStmt *)hStmt;

			if (! isValidStmt(stmt))
				return SQL_INVALID_HANDLE;

			clearStmtErrors(stmt);

			/* return error: Invalid option/attribute identifier */
			addStmtError(stmt, "HY092", NULL, 0);
			return SQL_ERROR;
		}
	}

	return SQL_ERROR;
}
