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
 * SQLPrepare
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


SQLRETURN SQLPrepare(
	SQLHSTMT	hStmt,
	SQLCHAR *	szSqlStr,
	SQLINTEGER	nSqlStrLength )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	RETCODE rc = SQL_ERROR;
	int params = 0;
	char *query = 0;


	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should NOT be executed */
	if (stmt->State == EXECUTED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}
	assert(stmt->ResultRows == NULL);

	/* check input parameter */
	if (szSqlStr == NULL)
	{
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	if (stmt->Query != NULL) {
		/* there was already a prepared statement, free it */
		free(stmt->Query);
		stmt->Query = NULL;
	}

	/* make a duplicate of the SQL command string */
	stmt->Query = copyODBCstr2Cstr(szSqlStr, nSqlStrLength);
	if (stmt->Query == NULL)
	{
		/* the value for nSqlStrLength was invalid */
		/* HY090 = Invalid string or buffer length */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: check (parse) the Query on correctness */
	/* TODO: convert ODBC escape sequences ( {d 'value'} or {t 'value'} or
	   {ts 'value'} or {escape 'e-char'} or {oj outer-join} or
	   {fn scalar-function} etc. ) to MonetDB SQL syntax */
	/* count the number of parameter markers (question mark: ?) */

	/* should move to the parser (or a parser should be moved in here) */
	if (stmt->bindParams.size){
		query = stmt->Query;
		while(query){
			/* problem with strings with ?s */
			if ((query = strchr(query, '?')) != NULL)
				params++;
		}
	       	if (stmt->bindParams.size != params){
			addStmtError(stmt, "HY000", NULL, 0);
			return SQL_ERROR;
		}
	}

	/* TODO: count the number of output columns and their description */

	/* update the internal state */
	stmt->State = PREPARED;

	return SQL_SUCCESS;
}
