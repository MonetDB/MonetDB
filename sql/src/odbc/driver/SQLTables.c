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
 * SQLTables()
 * CLI Compliance: X/Open
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLTables_(ODBCStmt *stmt,
	   SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	   SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	   SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	   SQLCHAR *szTableType, SQLSMALLINT nTableTypeLength)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(szCatalogName, nCatalogNameLength, addStmtError, stmt);
	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);
	fixODBCstring(szTableType, nTableTypeLength, addStmtError, stmt);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		nCatalogNameLength, szCatalogName ? (char *)szCatalogName : "",
		nSchemaNameLength, szSchemaName ? (char *) szSchemaName : "",
		nTableNameLength, szTableName ? (char *) szTableName : "",
		nTableTypeLength, szTableType ? (char *) szTableType : "");
#endif

	/* SQLTables returns a table with the following columns:
	   VARCHAR	table_cat
	   VARCHAR	table_schem
	   VARCHAR	table_name
	   VARCHAR	table_type
	   VARCHAR	remarks
	*/

	/* Check first on the special cases */
	if (nSchemaNameLength == 0 && nTableNameLength == 0 && szCatalogName &&
	    strcmp((char*)szCatalogName, SQL_ALL_CATALOGS) == 0) {
		/* Special case query to fetch all Catalog names. */
		/* Note: Catalogs are not supported so the result set
		   will be empty. */
		query = strdup("select "
			       "cast('' as varchar) as table_cat, "
			       "cast('' as varchar) as table_schem, "
			       "cast('' as varchar) as table_name, "
			       "cast('' as varchar) as table_type, "
			       "cast('' as varchar) as remarks "
			       "from sys.\"schemas\" where 0 = 1");
	} else if (nCatalogNameLength == 0 && nTableNameLength == 0 &&
		   szSchemaName && 
		   strcmp((char*)szSchemaName, SQL_ALL_SCHEMAS) == 0) {
		/* Special case query to fetch all Schema names. */
		query = strdup("select cast(null as varchar) as table_cat, "
			       "name as table_schem, "
			       "cast('' as varchar) as table_name, "
			       "cast('' as varchar) as table_type, "
			       "cast('' as varchar) as remarks "
			       "from sys.\"schemas\" order by table_schem");
	} else if (nCatalogNameLength == 0 && nSchemaNameLength == 0 &&
		   nTableNameLength == 0 && szTableType && 
		   strcmp((char*)szTableType, SQL_ALL_TABLE_TYPES) == 0) {
		/* Special case query to fetch all Table type names. */
		query = strdup("select distinct "
			       "cast(null as varchar) as table_cat, "
			       "cast('' as varchar) as table_schem, "
			       "cast('' as varchar) as table_name, "
			       "case when t.\"istable\" = true and t.\"system\" = false and t.\"temporary\" = 0 then cast('TABLE' as varchar) "
			       "when t.\"istable\" = true and t.\"system\" = true and t.\"temporary\" = 0 then cast('SYSTEM TABLE' as varchar) "
			       "when t.\"istable\" = false then cast('VIEW' as varchar) "
			       "when t.\"istable\" = true and t.\"system\" = false and t.\"temporary\" = 1 then cast('LOCAL TEMPORARY' as varchar) "
			       "else cast('INTERNAL TABLE TYPE' as varchar) end as table_type, "
			       "cast('' as varchar) as remarks "
			       "from sys.\"tables\" t order by table_type");
		/* TODO: UNION it with all supported table types */
	} else {
		/* no special case argument values */
		char *query_end;

		/* construct the query now */
		query = (char *) malloc(1000 + nSchemaNameLength +
					nTableNameLength + nTableTypeLength);
		assert(query);
		query_end = query;

		strcpy(query_end,
		       "select "
		       "cast(null as varchar) as table_cat, "
		       "cast(s.\"name\" as varchar) as table_schem, "
		       "cast(t.\"name\" as varchar) as table_name, "
		       "case when t.\"istable\" = true and t.\"system\" = false and t.\"temporary\" = 0 then cast('TABLE' as varchar) "
		       "when t.\"istable\" = true and t.\"system\" = true and t.\"temporary\" = 0 then cast('SYSTEM TABLE' as varchar) "
		       "when t.\"istable\" = false then cast('VIEW' as varchar) "
		       "when t.\"istable\" = true and t.\"system\" = false and t.\"temporary\" = 1 then cast('LOCAL TEMPORARY' as varchar) "
		       "else cast('INTERNAL TABLE TYPE' as varchar) end as table_type, "
		       "cast('' as varchar) as remarks "
		       "from sys.\"schemas\" s, sys.\"tables\" t "
		       "where s.\"id\" = t.\"schema_id\"");
		query_end += strlen(query_end);

		/* dependent on the input parameter values we must add a
		   variable selection condition dynamically */

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
			sprintf(query_end, " and s.\"name\" %s '%.*s'",
				memchr(szSchemaName, '%', nSchemaNameLength) ||
				memchr(szSchemaName, '_', nSchemaNameLength) ?
				"like" : "=",
				nSchemaNameLength, szSchemaName);
			query_end += strlen(query_end);
		}

		if (nTableNameLength > 0) {
			/* filtering requested on table name */
			/* use LIKE when it contains a wildcard '%' or a '_' */
			/* TODO: the wildcard may be escaped.  Check
			   it and may be convert it. */
			sprintf(query_end, " and t.\"name\" %s '%.*s'",
				memchr(szTableName, '%', nTableNameLength) ||
				memchr(szTableName, '_', nTableNameLength) ?
				"like" : "=",
				nTableNameLength, szTableName);
			query_end += strlen(query_end);
		}

		if (nTableTypeLength > 0) {
			/* filtering requested on table type */
			/* TODO: decompose the string szTableType into
			   separate string values (separated by
			   comma).
			   Mapped these string values to type numbers
			   (see enum table_type in catalog.h).  The
			   type numbers need to be inserted in the
			   SQL: " AND T.TYPE IN (<insert the list of
			   numbers here>)" where the comma separated
			   numbers are placed in the <insert ...here>
			   part.  Note: when there is no single type
			   number mapped use -1 so the SQL becomes: "
			   AND T.TYPE IN (-1)" This way no records
			   will be returned and the result set will be
			   empty.
			 */
		}

		/* add the ordering */
		strcpy(query_end,
		       " order by table_type, "
		       "table_schem, table_name");
		query_end += strlen(query_end);
		assert(query_end - query < 1000 + nSchemaNameLength + nTableNameLength + nTableTypeLength);
	}

	/* query the MonetDB data dictionary tables */

	rc = SQLExecDirect_(stmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLTables(SQLHSTMT hStmt,
	  SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	  SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	  SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	  SQLCHAR *szTableType, SQLSMALLINT nTableTypeLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLTables " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLTables_(stmt,
			  szCatalogName, nCatalogNameLength,
			  szSchemaName, nSchemaNameLength,
			  szTableName, nTableNameLength,
			  szTableType, nTableTypeLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLTablesA(SQLHSTMT hStmt,
	   SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	   SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	   SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	   SQLCHAR *szTableType, SQLSMALLINT nTableTypeLength)
{
	return SQLTables(hStmt,
			 szCatalogName, nCatalogNameLength,
			 szSchemaName, nSchemaNameLength,
			 szTableName, nTableNameLength,
			 szTableType, nTableTypeLength);
}

SQLRETURN SQL_API
SQLTablesW(SQLHSTMT hStmt,
	   SQLWCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	   SQLWCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	   SQLWCHAR *szTableName, SQLSMALLINT nTableNameLength,
	   SQLWCHAR *szTableType, SQLSMALLINT nTableTypeLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL, *type = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLTablesW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szTableName, nTableNameLength, table, addStmtError, stmt, goto exit);
	fixWcharIn(szTableType, nTableTypeLength, type, addStmtError, stmt, goto exit);

	rc = SQLTables_(stmt, catalog, SQL_NTS, schema, SQL_NTS,
			table, SQL_NTS, type, SQL_NTS);

  exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);
	if (type)
		free(type);

	return rc;
}
#endif	/* WITH_WCHAR */
