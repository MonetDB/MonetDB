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
		     SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
		     SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
		     SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
		     SQLCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	fixODBCstring(szCatalogName, nCatalogNameLength, addStmtError, stmt);
	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);
	fixODBCstring(szColumnName, nColumnNameLength, addStmtError, stmt);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		nCatalogNameLength, szCatalogName,
		nSchemaNameLength, szSchemaName,
		nTableNameLength, szTableName,
		nColumnNameLength, szColumnName);
#endif

	/* SQLColumnPrivileges returns a table with the following columns:
	   VARCHAR	table_cat
	   VARCHAR	table_schem
	   VARCHAR	table_name NOT NULL
	   VARCHAR	column_name NOT NULL
	   VARCHAR	grantor
	   VARCHAR	grantee NOT NULL
	   VARCHAR	privilege NOT NULL
	   VARCHAR	is_grantable
	*/

	/* for now return dummy result set */
	return SQLExecDirect_(stmt,
			      (SQLCHAR *) "select "
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
		    SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
		    SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
		    SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
		    SQLCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnPrivileges " PTRFMT, PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLColumnPrivileges_(stmt, szCatalogName, nCatalogNameLength,
				    szSchemaName, nSchemaNameLength,
				    szTableName, nTableNameLength,
				    szColumnName, nColumnNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLColumnPrivilegesA(SQLHSTMT hStmt,
		     SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
		     SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
		     SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
		     SQLCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	return SQLColumnPrivileges(hStmt, szCatalogName, nCatalogNameLength,
				   szSchemaName, nSchemaNameLength,
				   szTableName, nTableNameLength,
				   szColumnName, nColumnNameLength);
}

SQLRETURN SQL_API
SQLColumnPrivilegesW(SQLHSTMT hStmt,
		     SQLWCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
		     SQLWCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
		     SQLWCHAR *szTableName, SQLSMALLINT nTableNameLength,
		     SQLWCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
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

	fixWcharIn(szCatalogName, nCatalogNameLength, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szTableName, nTableNameLength, table, addStmtError, stmt, goto exit);
	fixWcharIn(szColumnName, nColumnNameLength, column, addStmtError, stmt, goto exit);

	rc = SQLColumnPrivileges_(stmt, catalog, SQL_NTS, schema, SQL_NTS,
				  table, SQL_NTS, column, SQL_NTS);
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
#endif	/* WITH_WCHAR */
