/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
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
 * SQLProcedures()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not implemented (it only sets an error),
 * because MonetDB SQL frontend does not support stored procedures.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLProcedures_(ODBCStmt *stmt,
	       SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	       SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	       SQLCHAR *szProcName, SQLSMALLINT nProcNameLength)
{
	fixODBCstring(szCatalogName, nCatalogNameLength, SQLSMALLINT, addStmtError, stmt);
	fixODBCstring(szSchemaName, nSchemaNameLength, SQLSMALLINT, addStmtError, stmt);
	fixODBCstring(szProcName, nProcNameLength, SQLSMALLINT, addStmtError, stmt);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\"\n",
		nCatalogNameLength, (char*)szCatalogName,
		nSchemaNameLength, (char*)szSchemaName,
		nProcNameLength, (char*)szProcName);
#endif

	/* SQLProcedures returns a table with the following columns:
	   VARCHAR      procedure_cat
	   VARCHAR      procedure_schem
	   VARCHAR      procedure_name NOT NULL
	   n/a          num_input_params (reserved for future use)
	   n/a          num_output_params (reserved for future use)
	   n/a          num_result_sets (reserved for future use)
	   VARCHAR      remarks
	   SMALLINT     procedure_type
	 */

	/* for now return dummy result set */
	return SQLExecDirect_(stmt, (SQLCHAR *)
			      "select "
			      "cast('' as varchar(1)) as procedure_cat, "
			      "cast('' as varchar(1)) as procedure_schem, "
			      "cast('' as varchar(1)) as procedure_name, "
			      "0 as num_input_params, "
			      "0 as num_output_params, "
			      "0 as num_result_sets, "
			      "cast('' as varchar(1)) as remarks, "
			      "cast(0 as smallint) as procedure_type "
			      "where 0 = 1", SQL_NTS);
}

SQLRETURN SQL_API
SQLProcedures(SQLHSTMT hStmt,
	      SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	      SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	      SQLCHAR *szProcName, SQLSMALLINT nProcNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedures " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLProcedures_(stmt,
			      szCatalogName, nCatalogNameLength,
			      szSchemaName, nSchemaNameLength,
			      szProcName, nProcNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLProceduresA(SQLHSTMT hStmt,
	       SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	       SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	       SQLCHAR *szProcName, SQLSMALLINT nProcNameLength)
{
	return SQLProcedures(hStmt,
			     szCatalogName, nCatalogNameLength,
			     szSchemaName, nSchemaNameLength,
			     szProcName, nProcNameLength);
}

SQLRETURN SQL_API
SQLProceduresW(SQLHSTMT hStmt,
	       SQLWCHAR * szCatalogName, SQLSMALLINT nCatalogNameLength,
	       SQLWCHAR * szSchemaName, SQLSMALLINT nSchemaNameLength,
	       SQLWCHAR * szProcName, SQLSMALLINT nProcNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *proc = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProceduresW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, SQLCHAR, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, SQLCHAR, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szProcName, nProcNameLength, SQLCHAR, proc, addStmtError, stmt, goto exit);

	rc = SQLProcedures_(stmt, catalog, SQL_NTS, schema, SQL_NTS, proc, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (proc)
		free(proc);

	return rc;
}
#endif /* WITH_WCHAR */
