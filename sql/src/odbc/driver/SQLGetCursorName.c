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
 * SQLGetCursorName()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLGetCursorName(
	SQLHSTMT	hStmt,
	SQLCHAR *	szCursor,
	SQLSMALLINT	nCursorMaxLength,
	SQLSMALLINT *	pnCursorLength )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;

	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* TODO: implement the requested behavior when SQLSetCursorName() is implemented */

	/* for now always return error: No cursor name available */
	addStmtError(stmt, "HY015", NULL, 0);
	return SQL_ERROR;
}
