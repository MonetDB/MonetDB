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

/*****************************************************************************
 * SQLFetchScroll()
 * CLI Compliance: ISO 92
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 *****************************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLFetchScroll(
	SQLHSTMT	hStmt,
	SQLSMALLINT	nOrientation,
	SQLINTEGER	nOffset )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;

	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	if (stmt->ResultRows == NULL) {
		return SQL_NO_DATA;
	}
	if (stmt->nrRows <= 0) {
		return SQL_NO_DATA;
	}

	switch(nOrientation){
	case SQL_FETCH_NEXT: 
		break;
	case SQL_FETCH_FIRST:
		stmt->currentRow = 0;
		break;
	case SQL_FETCH_LAST:
		stmt->currentRow = stmt->nrRows-1;
		break;
	case SQL_FETCH_PRIOR:
		stmt->currentRow -= 2;
		break;
	case SQL_FETCH_ABSOLUTE:
		stmt->currentRow = nOffset-1;
		break;
	case SQL_FETCH_RELATIVE:
		stmt->currentRow += nOffset;
		break;
	default:
		/* TODO change to unkown Orientation */
		/* for now return error IM001: driver not capable */
		addStmtError(stmt, "IM001", NULL, 0);
		return SQL_ERROR;
	}
	return SQLFetch(stmt);
}
