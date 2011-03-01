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
 * SQLColumnPrivileges()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not implemented (it only sets an error),
 * because monetDB SQL frontend does not support column based authorization.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCUtil.h"
#include "ODBCStmt.h"


static SQLRETURN
SQLColumnPrivileges_(ODBCStmt *stmt,
		     SQLCHAR *szCatalogName,
		     SQLSMALLINT nCatalogNameLength,
		     SQLCHAR *szSchemaName,
		     SQLSMALLINT nSchemaNameLength,
		     SQLCHAR *szTableName,
		     SQLSMALLINT nTableNameLength,
		     SQLCHAR *szColumnName,
		     SQLSMALLINT nColumnNameLength)
{
	fixODBCstring(szCatalogName, nCatalogNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szSchemaName, nSchemaNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szTableName, nTableNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szColumnName, nColumnNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) nCatalogNameLength, (char *) szCatalogName,
		(int) nSchemaNameLength, (char *) szSchemaName,
		(int) nTableNameLength, (char *) szTableName,
		(int) nColumnNameLength, (char *) szColumnName);
#endif

	/* SQLColumnPrivileges returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name NOT NULL
	   VARCHAR      column_name NOT NULL
	   VARCHAR      grantor
	   VARCHAR      grantee NOT NULL
	   VARCHAR      privilege NOT NULL
	   VARCHAR      is_grantable
	 */

	/* for now return dummy result set */
	return SQLExecDirect_(stmt, (SQLCHAR *)
			      "select "
			      "cast('' as varchar(1)) as table_cat, "
			      "cast('' as varchar(1)) as table_schem, "
			      "cast('' as varchar(1)) as table_name, "
			      "cast('' as varchar(1)) as column_name, "
			      "cast('' as varchar(1)) as grantor, "
			      "cast('' as varchar(1)) as grantee, "
			      "cast('' as varchar(1)) as privilege, "
			      "cast('' as varchar(1)) as is_grantable "
			      "where 0 = 1", SQL_NTS);
}

SQLRETURN SQL_API
SQLColumnPrivileges(SQLHSTMT hStmt,
		    SQLCHAR *szCatalogName,
		    SQLSMALLINT nCatalogNameLength,
		    SQLCHAR *szSchemaName,
		    SQLSMALLINT nSchemaNameLength,
		    SQLCHAR *szTableName,
		    SQLSMALLINT nTableNameLength,
		    SQLCHAR *szColumnName,
		    SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnPrivileges " PTRFMT, PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLColumnPrivileges_(stmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szTableName, nTableNameLength, szColumnName, nColumnNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLColumnPrivilegesA(SQLHSTMT hStmt,
		     SQLCHAR *szCatalogName,
		     SQLSMALLINT nCatalogNameLength,
		     SQLCHAR *szSchemaName,
		     SQLSMALLINT nSchemaNameLength,
		     SQLCHAR *szTableName,
		     SQLSMALLINT nTableNameLength,
		     SQLCHAR *szColumnName,
		     SQLSMALLINT nColumnNameLength)
{
	return SQLColumnPrivileges(hStmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szTableName, nTableNameLength, szColumnName, nColumnNameLength);
}

SQLRETURN SQL_API
SQLColumnPrivilegesW(SQLHSTMT hStmt,
		     SQLWCHAR * szCatalogName,
		     SQLSMALLINT nCatalogNameLength,
		     SQLWCHAR * szSchemaName,
		     SQLSMALLINT nSchemaNameLength,
		     SQLWCHAR * szTableName,
		     SQLSMALLINT nTableNameLength,
		     SQLWCHAR * szColumnName,
		     SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL, *column = NULL;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnPrivilegesW " PTRFMT, PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, SQLCHAR, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, SQLCHAR, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szTableName, nTableNameLength, SQLCHAR, table, addStmtError, stmt, goto exit);
	fixWcharIn(szColumnName, nColumnNameLength, SQLCHAR, column, addStmtError, stmt, goto exit);

	rc = SQLColumnPrivileges_(stmt, catalog, SQL_NTS, schema, SQL_NTS, table, SQL_NTS, column, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);
	if (column)
		free(column);
	return rc;
}
#endif /* WITH_WCHAR */
