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
 * SQLNumResultCols()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLNumResultCols(
	SQLHSTMT	hStmt,
	SQLSMALLINT *	pnColumnCount )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;

	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be prepared or executed */
	if (stmt->State == INITED) {
		/* caller should have called SQLPrepare or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* When the query is only prepared (via SQLPrepare) we do not have
	 * the correct nrCols value yet (this is a limitation of the current
	 * MonetDB SQL frontend implementation). */
	/* we only have a correct nrCols value when the query is executed */
	if (stmt->State != EXECUTED) {
		/* HY000 = General Error */
		addStmtError(stmt, "HY000", "Cannot return the number of output columns. Query must be executed first", 0);
		return SQL_ERROR;
	}

	/* check output parameter */
	if (pnColumnCount == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	/* We can now set the "number of output columns" value */
	/* Note: nrCols can be 0 (for non SELECT queries) */
	*pnColumnCount = stmt->nrCols;

	return SQL_SUCCESS;
}
