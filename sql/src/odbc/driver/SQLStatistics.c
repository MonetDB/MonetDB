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


SQLRETURN
SQLStatistics(SQLHSTMT hStmt, SQLCHAR *szCatalogName,
	      SQLSMALLINT nCatalogNameLength, SQLCHAR *szSchemaName,
	      SQLSMALLINT nSchemaNameLength, SQLCHAR *szTableName,
	      SQLSMALLINT nTableNameLength, SQLUSMALLINT nUnique,
	      SQLUSMALLINT nReserved)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLStatistics\n");
#endif

	(void) szCatalogName;	/* Stefan: unused!? */
	(void) nCatalogNameLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);

		return SQL_ERROR;
	}

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
	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);
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

	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);

	/* construct the query now */
	query = malloc(1000 + nTableNameLength + nSchemaNameLength);
	assert(query);
	query_end = query;

	/* TODO: finish the SQL query */
	strcpy(query_end,
	       "select '' as table_cat, s.name as table_schem, "
	       "t.name as table_name, 1 as non_unique, "
	       "null as index_qualifier, null as index_name, 0 as type, "
	       "null as ordinal_position, c.name as column_name, "
	       "'a' as asc_or_desc, null as cardinality, null as pages, "
	       "null as filter_condition from schemas s, tables t, columns c "
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
	rc = SQLExecDirect_(hStmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
