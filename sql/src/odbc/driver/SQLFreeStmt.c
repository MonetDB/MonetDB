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
 * SQLFreeStmt()
 * CLI Compliance: ISO 92
 *
 * Note: the option SQL_DROP is deprecated in ODBC 3.0 and replaced by
 * SQLFreeHanlde(). It is provided here for old (pre ODBC 3.0) applications.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQLFreeStmt(
	SQLHSTMT	handle,
	SQLUSMALLINT	option )
{
	ODBCStmt * stmt = (ODBCStmt *)handle;

        /* Check parameter handle */
	if (! isValidStmt(stmt)) {
		return SQL_INVALID_HANDLE;
	}

	clearStmtErrors(stmt);

	switch (option) {
		case SQL_CLOSE:
		/* Note: this option is also called from SQLCancel() and
		   SQLCloseCursor(), so be careful when changing the code */
		{
			/* close cursor, discard result set, set to prepared */
			if (stmt->Result) {
				GDKfree(stmt->Result);
				stmt->Result = NULL;
			}
			stmt->nrCols = 0;
			stmt->nrRows = 0;
			stmt->currentRow = 0;
			if (stmt->State == EXECUTED) {
				stmt->State = PREPARED;
			}
			/* Important: do not destroy the bind parameters and columns! */
			break;
		}
		case SQL_DROP:
			return SQLFreeHandle(SQL_HANDLE_STMT, (SQLHANDLE)stmt);

		case SQL_UNBIND:
		{
			destroyOdbcOutArray(&(stmt->bindCols));
			break;
		}
		case SQL_RESET_PARAMS:
		{
			destroyOdbcInArray(&(stmt->bindParams));
			break;
		}
		default:
		{
			addStmtError(stmt, "HY092", NULL, 0);
			return SQL_ERROR;
		}
	}

	return SQL_SUCCESS;
}
