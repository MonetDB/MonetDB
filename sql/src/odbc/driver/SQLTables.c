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


SQLRETURN
SQLTables(SQLHSTMT hStmt,
	  SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	  SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	  SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	  SQLCHAR *szTableType, SQLSMALLINT nTableTypeLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLTables ");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

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
	/* Check first on the special cases */
	if (nSchemaNameLength == 0 && nTableNameLength == 0 && szCatalogName &&
	    strcmp((char*)szCatalogName, SQL_ALL_CATALOGS) == 0) {
		/* Special case query to fetch all Catalog names. */
		/* Note: Catalogs are not supported so the result set will be empty. */
		query = strdup("select '' as table_cat, '' as table_schem, "
			       "'' as table_name, '' as table_type, "
			       "'' as remarks from schemas where 0 = 1");
	} else if (nCatalogNameLength == 0 && nTableNameLength == 0 &&
		   szSchemaName && 
		   strcmp((char*)szSchemaName, SQL_ALL_SCHEMAS) == 0) {
		/* Special case query to fetch all Schema names. */
		query = strdup("select '' as table_cat, name as table_schem, "
			       "'' as table_name, '' as table_type, "
			       "'' as remarks from schemas order by name");
	} else if (nCatalogNameLength == 0 && nSchemaNameLength == 0 &&
		   nTableNameLength == 0 && szTableType && 
		   strcmp((char*)szTableType, SQL_ALL_TABLE_TYPES) == 0) {
		/* Special case query to fetch all Table type names. */
		query = strdup("select '' as table_cat, '' as table_schem, "
			       "'' as table_name, "
			       "distinct case t.type when 1 then 'TABLE' "
			       "when 0 then 'SYSTEM_TABLE' when 2 then 'VIEW' "
			       "when 3 then 'LOCAL TEMPORARY TABLE' "
			       "else 'INTERNAL TYPE' end as table_type, "
			       "'' as remarks from tables order by type");
		/* TODO: UNION it with all supported table types */
	} else {
		/* no special case argument values */
		char *query_end;

		/* construct the query now */
		query = malloc(1000 + nCatalogNameLength + nSchemaNameLength +
			       nTableNameLength + nTableTypeLength);
		assert(query);
		query_end = query;

		sprintf(query_end,
			"select '%.*s' as table_cat, s.name as table_schem, "
			"t.name as table_name, "
			"case t.type when 0 then 'TABLE' "
			"when 1 then 'SYSTEM_TABLE' when 2 then 'VIEW' "
			"when 3 then 'LOCAL TEMPORARY TABLE' "
			"else 'INTERNAL TABLE TYPE' end as table_type, "
			"'' as remarks from schemas s, tables t "
			"where s.id = t.schema_id",
			nCatalogNameLength, szCatalogName);
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
			sprintf(query_end, " and s.name %s '%.*s'",
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
			sprintf(query_end, " and t.name %s '%.*s'",
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
		strcpy(query_end, " order by s.name, t.name, t.type");
	}

	/* query the MonetDb data dictionary tables */

	rc = SQLExecDirect_(hStmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;
}
