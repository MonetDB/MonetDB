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
 * SQLStatistics()
 * CLI Compliance: ISO 92
 *
 * Note: catalogs are not supported, we ignore any value set for
 * szCatalogName.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLStatistics_(ODBCStmt *stmt,
	       SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	       SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	       SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	       SQLUSMALLINT nUnique, SQLUSMALLINT nReserved)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);

		return SQL_ERROR;
	}

	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);
	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szCatalogName, nCatalogNameLength, addStmtError, stmt);

	/* check for valid Unique argument */
	switch (nUnique) {
	case SQL_INDEX_ALL:
	case SQL_INDEX_UNIQUE:
		break;
	default:
		/* HY100 = Invalid Unique value */
		addStmtError(stmt, "HY100", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Reserved argument */
	switch (nReserved) {
	case SQL_ENSURE:
	case SQL_QUICK:
		break;
	default:
		/* HY101 = Invalid Reserved value */
		addStmtError(stmt, "HY101", NULL, 0);
		return SQL_ERROR;
	}


	/* check if a valid (non null, not empty) table name is supplied */
	if (szTableName == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	if (nTableNameLength == 0) {
		/* HY090 = Invalid string */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	/* construct the query now */
	query = malloc(1000 + nTableNameLength + nSchemaNameLength);
	assert(query);
	query_end = query;

	/* SQLStatistics returns a table with the following columns:
	   VARCHAR	table_cat
	   VARCHAR	table_schem
	   VARCHAR	table_name NOT NULL
	   SMALLINT	non_unique
	   VARCHAR	index_qualifier
	   VARCHAR	index_name
	   SMALLINT	type NOT NULL
	   SMALLINT	ordinal_position
	   VARCHAR	column_name
	   CHAR(1)	asc_or_desc
	   INTEGER	cardinality
	   INTEGER	pages
	   VARCHAR	filter_condition
	*/
	/* TODO: finish the SQL query */
	strcpy(query_end,
	       "select "
	       "'' as table_cat, "
	       "s.name as table_schem, "
	       "t.name as table_name, "
	       "cast(1 as smallint) as non_unique, "
	       "cast(null as varchar) as index_qualifier, "
	       "cast(null as varchar) as index_name, "
	       "cast(0 as smallint) as type, "
	       "cast(null as smallint) as ordinal_position, "
	       "c.name as column_name, "
	       "'a' as asc_or_desc, "
	       "cast(null as integer) as cardinality, "
	       "cast(null as integer) as pages, "
	       "cast(null as varchar) as filter_condition "
	       "from sys.schemas s, sys.tables t, columns c "
	       "where s.id = t.schema_id and t.id = c.table_id and "
	       "t.id = k.table_id");
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	/* search pattern is not allowed for table name so use = and not LIKE */
	sprintf(query_end, " and t.name = '%.*s'",
		nTableNameLength, szTableName);
	query_end += strlen(query_end);

	if (szSchemaName != NULL) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and s.name = '%.*s'",
			nSchemaNameLength, szSchemaName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end,
	       " order by non_unique, type, index_quallifier, index_name, "
	       "ordinal_position");
	query_end += strlen(query_end);

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLStatistics(SQLHSTMT hStmt,
	      SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	      SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	      SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	      SQLUSMALLINT nUnique, SQLUSMALLINT nReserved)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLStatistics\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLStatistics_(stmt, szCatalogName, nCatalogNameLength,
			      szSchemaName, nSchemaNameLength,
			      szTableName, nTableNameLength,
			      nUnique, nReserved);
}

SQLRETURN SQL_API
SQLStatisticsW(SQLHSTMT hStmt,
	       SQLWCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	       SQLWCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	       SQLWCHAR *szTableName, SQLSMALLINT nTableNameLength,
	       SQLUSMALLINT nUnique, SQLUSMALLINT nReserved)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLStatisticsW\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szTableName, nTableNameLength, table, addStmtError, stmt, goto exit);

	rc = SQLStatistics_(stmt, catalog, SQL_NTS, schema, SQL_NTS,
			    table, SQL_NTS, nUnique, nReserved);

  exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);

	return rc;
}
