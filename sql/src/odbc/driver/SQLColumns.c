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
 * SQLColumns()
 * CLI Compliance: X/Open
 *
 * Note: catalogs are not supported, we ignore any value set for szCatalogName
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


SQLRETURN
SQLColumns(SQLHSTMT hStmt, SQLCHAR *szCatalogName,
	   SQLSMALLINT nCatalogNameLength, SQLCHAR *szSchemaName,
	   SQLSMALLINT nSchemaNameLength, SQLCHAR *szTableName,
	   SQLSMALLINT nTableNameLength, SQLCHAR *szColumnName,
	   SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumns\n");
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

	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);
	fixODBCstring(szColumnName, nColumnNameLength, addStmtError, stmt);

	/* construct the query now */
	query = malloc(1000 + nSchemaNameLength + nTableNameLength +
		       nColumnNameLength);
	assert(query);
	query_end = query;

	strcpy(query_end,
	       "select '' as table_cat, s.name as table_schem, "
	       "t.name as table_name, c.name as column_name, "
	       "c.type as data_type, c.type as type_name, "
	       "c.type_digits as column_size, c.type_digits as buffer_length, "
	       "c.type_scale as decimal_digits, '' as num_prec_radix, "
	       "c.null as nullable, '' as remarks, '' as column_def, "
	       "c.type as sql_data_type, '' as sql_datetime_sub, "
	       "'' as char_octet_length, c.number as ordinal_position, "
	       "c.null as is_nullable from schemas s, tables t, columns c "
	       "where s.id = t.schema_id and t.id = c.table_id");
	query_end += strlen(query_end);

	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	/* Construct the selection condition query part */
	if (nSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped. Check it and may
		   be convert it. */
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
		/* TODO: the wildcard may be escaped.  Check it and
		   may be convert it. */
		sprintf(query_end, " and t.name %s '%.*s'",
			memchr(szTableName, '%', nTableNameLength) ||
			memchr(szTableName, '_', nTableNameLength) ?
			"like" : "=",
			nTableNameLength, szTableName);
		query_end += strlen(query_end);
	}

	if (nColumnNameLength > 0) {
		/* filtering requested on column name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check it and
		   may be convert it. */
		sprintf(query_end, " and c.name %s '%.*s'",
			memchr(szColumnName, '%', nColumnNameLength) ||
			memchr(szColumnName, '_', nColumnNameLength) ?
			"like" : "=",
			nColumnNameLength, szColumnName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end, " order by s.name, t.name, c.number");
	query_end += strlen(query_end);

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
