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

	(void) szCatalogName;	/* Stefan: unused!? */
	(void) nCatalogNameLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

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
	fixODBCstring(szTableName, nTableNameLength);
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

	fixODBCstring(szSchemaName, nSchemaNameLength);

	/* first create a string buffer (1000 extra bytes is plenty */
	query = malloc(1000 + nTableNameLength + nSchemaNameLength);
	assert(query);
	query_end = query;

	if (nIdentifierType == SQL_BEST_ROWID) {
		/* Select from the key table the (smallest) primary/unique key */
		/* Note: SCOPE is SQL_SCOPE_TRANSACTION is 1 */
		/* Note: PSEUDO_COLUMN is SQL_PC_NOT_PSEUDO is 1 */
		strcpy(query_end,
		       "SELECT 1 AS SCOPE, C.COLUMN_NAME AS COLUMN_NAME, "
		       "C.DATA_TYPE AS DATA_TYPE, C.TYPE_NAME AS TYPE_NAME, "
		       "C.COLUMN_SIZE AS COLUMN_SIZE, "
		       "C.BUFFER_LENGTH AS BUFFER_LENGTH, "
		       "C.DECIMAL_DIGITS AS DECIMAL_DIGITS, "
		       "1 AS PSEUDO_COLUMN FROM SCHEMAS S, TABLES T, "
		       "COLUMNS C, KEYS K, KEYCOLUMNS KC "
		       "WHERE S.ID = T.SCHEMA_ID AND T.ID = C.TABLE_ID AND "
		       "T.ID = K.TABLE_ID AND C.ID = KC.COLUMN_ID AND "
		       "KC.KEY_ID = K.KEY_ID AND K.IS_PRIMARY = 1");
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
		sprintf(query_end, " AND T.TABLE_NAME = '%.*s'",
			nTableNameLength, szTableName);
		query_end += strlen(query_end);

		if (szSchemaName != NULL && nSchemaNameLength > 0) {
			/* filtering requested on schema name */
			/* search pattern is not allowed so use = and not LIKE */
			sprintf(query_end, " AND S.NAME = '%.*s'",
				nSchemaNameLength, szSchemaName);
			query_end += strlen(query_end);
		}

		/* add an extra selection when SQL_NO_NULLS is requested */
		if (nNullable == SQL_NO_NULLS) {
			strcpy(query_end, " AND C.IS_NULLABLE = 0");
			query_end += strlen(query_end);
		}

		/* no ordering needed */
	} else {
		assert(nIdentifierType == SQL_ROWVER);
		/* The backend does not have such info available */
		/* create just a query which results in zero rows */
		/* Note: PSEUDO_COLUMN is SQL_PC_UNKNOWN is 0 */
		strcpy(query_end,
		       "SELECT NULL AS SCOPE, '' AS COLUMN_NAME, "
		       "1 AS DATA_TYPE, 'CHAR' AS TYPE_NAME, "
		       "1 AS COLUMN_SIZE, 1 AS BUFFER_LENGTH, "
		       "0 AS DECIMAL_DIGITS, 0 AS PSEUDO_COLUMN "
		       "FROM SCHEMAS S WHERE 0 = 1");
		query_end += strlen(query_end);
	}

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect(hStmt, (SQLCHAR *) query,
			   (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
