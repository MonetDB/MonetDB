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
 * SQLExecDirect()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQLExecDirect(
	SQLHSTMT	hStmt,
	SQLCHAR *	szSqlStr,
	SQLINTEGER	nSqlStr )
{
	RETCODE rc;

	if (! isValidStmt((ODBCStmt *)hStmt)) {
		return SQL_INVALID_HANDLE;
	}

	/* prepare SQL command */
	rc = SQLPrepare(hStmt, szSqlStr, nSqlStr);
	if (rc == SQL_SUCCESS)
	{
		/* execute prepared statement */
		rc = SQLExecute(hStmt);
	}

	/* Do not set errors here, they are set in SQLPrepare() and/or SQLExecute() */

	return rc;
}
