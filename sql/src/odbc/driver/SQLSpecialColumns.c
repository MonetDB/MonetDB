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
 * SQLSpecialColumns()
 * CLI Compliance: X/Open
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
SQLSpecialColumns(SQLHSTMT hStmt, SQLUSMALLINT nIdentifierType,
		  SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
		  SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
		  SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
		  SQLUSMALLINT nScope, SQLUSMALLINT nNullable)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSpecialColumns %d ", nIdentifierType);
#endif

	(void) szCatalogName;	/* Stefan: unused!? */
	(void) nCatalogNameLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixODBCstring(szCatalogName, nCatalogNameLength, addStmtError, stmt);
	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" %d %d\n",
		nCatalogNameLength, szCatalogName,
		nSchemaNameLength, szSchemaName, nTableNameLength, szTableName,
		nScope, nNullable);
#endif

	/* check statement cursor state, no query should be prepared
	   or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid IdentifierType argument */
	switch (nIdentifierType) {
	case SQL_BEST_ROWID:
	case SQL_ROWVER:
		break;
	default:
		/* HY097 = Invalid identifier type */
		addStmtError(stmt, "HY097", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Scope argument */
	switch (nScope) {
	case SQL_SCOPE_CURROW:
	case SQL_SCOPE_TRANSACTION:
	case SQL_SCOPE_SESSION:
		break;
	default:
		/* HY098 = Invalid scope type */
		addStmtError(stmt, "HY098", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Nullable argument */
	switch (nNullable) {
	case SQL_NO_NULLS:
	case SQL_NULLABLE:
		break;
	default:
		/* HY099 = Invalid nullable type */
		addStmtError(stmt, "HY099", NULL, 0);
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

	/* first create a string buffer (1000 extra bytes is plenty */
	query = malloc(1000 + nTableNameLength + nSchemaNameLength);
	assert(query);
	query_end = query;

	if (nIdentifierType == SQL_BEST_ROWID) {
		/* Select from the key table the (smallest) primary/unique key */
		/* Note: SCOPE is SQL_SCOPE_TRANSACTION is 1 */
		/* Note: PSEUDO_COLUMN is SQL_PC_NOT_PSEUDO is 1 */
		strcpy(query_end,
		       "select 1 as scope, c.name as column_name, "
		       "c.type as data_type, c.type_name as type_name, "
		       "c.column_size as column_size, "
		       "c.buffer_length as buffer_length, "
		       "c.decimal_digits as decimal_digits, "
		       "1 as pseudo_column from schemas s, tables t, "
		       "columns c, keys k, keycolumns kc "
		       "where s.id = t.schema_id and t.id = c.table_id and "
		       "t.id = k.table_id and c.id = kc.'column' and "
		       "kc.id = k.id" /*" and k.is_primary = 1"*/);
		query_end += strlen(query_end);
		/* TODO: improve the SQL to get the correct result:
		   - only one set of columns should be returned, also when
		   multiple primary keys are available for this table.
		   - when the table has NO primary key it should return the
		   columns of a unique key (only from ONE unique key which
		   is also the best/smallest key)
		   TODO: optimize SQL:
		   - when no szSchemaName is set (see above) also no filtering on
		   SCHEMA NAME and join with table SCHEMAS is needed!
		 */

		/* add the selection condition */
		/* search pattern is not allowed for table name so use = and not LIKE */
		sprintf(query_end, " and t.name = '%.*s'",
			nTableNameLength, szTableName);
		query_end += strlen(query_end);

		if (szSchemaName != NULL && nSchemaNameLength > 0) {
			/* filtering requested on schema name */
			/* search pattern is not allowed so use = and not LIKE */
			sprintf(query_end, " and s.name = '%.*s'",
				nSchemaNameLength, szSchemaName);
			query_end += strlen(query_end);
		}

		/* add an extra selection when SQL_NO_NULLS is requested */
		if (nNullable == SQL_NO_NULLS) {
			strcpy(query_end, " and c.is_nullable = 0");
			query_end += strlen(query_end);
		}

		/* no ordering needed */
	} else {
		assert(nIdentifierType == SQL_ROWVER);
		/* The backend does not have such info available */
		/* create just a query which results in zero rows */
		/* Note: pseudo_column is sql_pc_unknown is 0 */
		strcpy(query_end,
		       "select null as scope, '' as column_name, "
		       "1 as data_type, 'char' as type_name, "
		       "1 as column_size, 1 as buffer_length, "
		       "0 as decimal_digits, 0 as pseudo_column "
		       "from schemas s where 0 = 1");
		query_end += strlen(query_end);
	}

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect_(hStmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
