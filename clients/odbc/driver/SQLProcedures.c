/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
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
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end;

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(szCatalogName, nCatalogNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szSchemaName, nSchemaNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szProcName, nProcNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) nCatalogNameLength, (char *) szCatalogName,
		(int) nSchemaNameLength, (char *) szSchemaName,
		(int) nProcNameLength, (char *) szProcName);
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

	query = (char *) malloc(1000 + nSchemaNameLength + nProcNameLength);
	assert(query);
	query_end = query;

	snprintf(query_end, 1000,
		 "select "
		 "cast(null as varchar(1)) as \"procedure_cat\", "
		 "\"s\".\"name\" as \"procedure_schem\", "
		 "\"p\".\"name\" as \"procedure_name\", "
		 "0 as \"num_input_params\", "
		 "0 as \"num_output_params\", "
		 "0 as \"num_result_sets\", "
		 "cast('' as varchar(1)) as \"remarks\", "
		 "cast(%d as smallint) as \"procedure_type\" "
		 "from sys.\"functions\" as \"p\", sys.\"schemas\" as \"s\" "
		 "where \"p\".\"schema_id\" = \"s\".\"id\" "
		 "and \"p\".\"sql\" = true ",
		 SQL_PT_UNKNOWN);
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	if (nCatalogNameLength > 0) {
		/* filtering requested on catalog name */
		/* we do not support catalog names, so ignore it */
	}

	if (nSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped. Check it
		   and maybe convert it. */
		sprintf(query_end, "and \"s\".\"name\" %s '%.*s' ",
			memchr(szSchemaName, '%', nSchemaNameLength) || memchr(szSchemaName, '_', nSchemaNameLength) ? "like" : "=",
			nSchemaNameLength, (char*)szSchemaName);
		query_end += strlen(query_end);
	}

	if (nProcNameLength > 0) {
		/* filtering requested on procedure name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check
		   it and may be convert it. */
		sprintf(query_end, "and \"p\".\"name\" %s '%.*s' ",
			memchr(szProcName, '%', nProcNameLength) || memchr(szProcName, '_', nProcNameLength) ? "like" : "=",
			nProcNameLength, (char*)szProcName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end,
	       "order by \"procedure_cat\", \"procedure_schem\", \"procedure_name\"");
	query_end += strlen(query_end);

	/* query the MonetDB data dictionary tables */

	rc = SQLExecDirect_(stmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;
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
