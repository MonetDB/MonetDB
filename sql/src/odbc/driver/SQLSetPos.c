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

/********************************************************************
 * SQLSetPos()
 * CLI Compliance: ODBC
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"	/* for isValidStmt() & addStmtError() */

SQLRETURN SQLSetPos(
	SQLHSTMT	hStmt,
	SQLUSMALLINT	nRow,
	SQLUSMALLINT	nOperation,
	SQLUSMALLINT	nLockType )
{
	(void) nRow;	/* Stefan: unused!? */

	if (! isValidStmt(hStmt))
		return SQL_INVALID_HANDLE;

	/* check the parameter values */
	switch (nOperation)
	{
		case SQL_POSITION:
		case SQL_REFRESH:
		case SQL_UPDATE:
		case SQL_DELETE:
		default:
			/* return error: "Optional feature not implemented" */
			addStmtError(hStmt, "HYC00", NULL, 0);
			return SQL_ERROR;
	}

	switch (nLockType)
	{
		case SQL_LOCK_NO_CHANGE:
		case SQL_LOCK_EXCLUSIVE:
		case SQL_LOCK_UNLOCK:
		default:
			/* return error: "Optional feature not implemented" */
			addStmtError(hStmt, "HYC00", NULL, 0);
			return SQL_ERROR;
	}

	/* TODO: implement the requested behavior */

	/* for now always return error */
	addStmtError(hStmt, "IM001", NULL, 0);
	return SQL_ERROR;
}
