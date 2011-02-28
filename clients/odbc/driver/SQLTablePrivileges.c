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
 * SQLTablePrivileges()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not implemented (it only sets an error),
 * because MonetDB SQL frontend does not support table based authorization.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLTablePrivileges_(ODBCStmt *stmt,
		    SQLCHAR *szCatalogName,
		    SQLSMALLINT nCatalogNameLength,
		    SQLCHAR *szSchemaName,
		    SQLSMALLINT nSchemaNameLength,
		    SQLCHAR *szTableName,
		    SQLSMALLINT nTableNameLength)
{
	fixODBCstring(szCatalogName, nCatalogNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szSchemaName, nSchemaNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szTableName, nTableNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) nCatalogNameLength, (char *) szCatalogName,
		(int) nSchemaNameLength, (char *) szSchemaName,
		(int) nTableNameLength, (char *) szTableName);
#endif

	/* SQLTablePrivileges returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name NOT NULL
	   VARCHAR      grantor
	   VARCHAR      grantee NOT NULL
	   VARCHAR      privilege NOT NULL
	   VARCHAR      is_grantable
	 */

	/* Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLTablePrivileges(SQLHSTMT hStmt,
		   SQLCHAR *szCatalogName,
		   SQLSMALLINT nCatalogNameLength,
		   SQLCHAR *szSchemaName,
		   SQLSMALLINT nSchemaNameLength,
		   SQLCHAR *szTableName,
		   SQLSMALLINT nTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLTablePrivileges " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLTablePrivileges_(stmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szTableName, nTableNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLTablePrivilegesA(SQLHSTMT hStmt,
		    SQLCHAR *szCatalogName,
		    SQLSMALLINT nCatalogNameLength,
		    SQLCHAR *szSchemaName,
		    SQLSMALLINT nSchemaNameLength,
		    SQLCHAR *szTableName,
		    SQLSMALLINT nTableNameLength)
{
	return SQLTablePrivileges(hStmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szTableName, nTableNameLength);
}

SQLRETURN SQL_API
SQLTablePrivilegesW(SQLHSTMT hStmt,
		    SQLWCHAR * szCatalogName,
		    SQLSMALLINT nCatalogNameLength,
		    SQLWCHAR * szSchemaName,
		    SQLSMALLINT nSchemaNameLength,
		    SQLWCHAR * szTableName,
		    SQLSMALLINT nTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLTablePrivilegesW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, SQLCHAR, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, SQLCHAR, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szTableName, nTableNameLength, SQLCHAR, table, addStmtError, stmt, goto exit);

	rc = SQLTablePrivileges_(stmt, catalog, SQL_NTS, schema, SQL_NTS, table, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);

	return rc;
}
#endif /* WITH_WCHAR */
