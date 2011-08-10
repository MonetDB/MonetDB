/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * SQLSpecialColumns()
 * CLI Compliance: X/Open
 *
 * Note: catalogs are not supported, we ignore any value set for
 * CatalogName.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLSpecialColumns_(ODBCStmt *stmt,
		   SQLUSMALLINT IdentifierType,
		   SQLCHAR *CatalogName,
		   SQLSMALLINT NameLength1,
		   SQLCHAR *SchemaName,
		   SQLSMALLINT NameLength2,
		   SQLCHAR *TableName,
		   SQLSMALLINT NameLength3,
		   SQLUSMALLINT Scope,
		   SQLUSMALLINT Nullable)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" %u %u\n",
		(int) NameLength1, (char *) CatalogName,
		(int) NameLength2, (char *) SchemaName,
		(int) NameLength3, (char *) TableName,
		(unsigned int) Scope, (unsigned int) Nullable);
#endif

	/* check for valid IdentifierType argument */
	switch (IdentifierType) {
	case SQL_BEST_ROWID:
	case SQL_ROWVER:
		break;
	default:
		/* Column type out of range */
		addStmtError(stmt, "HY097", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Scope argument */
	switch (Scope) {
	case SQL_SCOPE_CURROW:
	case SQL_SCOPE_TRANSACTION:
	case SQL_SCOPE_SESSION:
		break;
	default:
		/* Scope type out of range */
		addStmtError(stmt, "HY098", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Nullable argument */
	switch (Nullable) {
	case SQL_NO_NULLS:
	case SQL_NULLABLE:
		break;
	default:
		/* Nullable type out of range */
		addStmtError(stmt, "HY099", NULL, 0);
		return SQL_ERROR;
	}

	/* check if a valid (non null, not empty) table name is supplied */
	if (TableName == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	if (NameLength3 == 0) {
		/* Invalid string or buffer length */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	/* first create a string buffer (1000 extra bytes is plenty */
	query = (char *) malloc(1000 + NameLength3 + NameLength2);
	assert(query);
	query_end = query;

	/* SQLSpecialColumns returns a table with the following columns:
	   SMALLINT     scope
	   VARCHAR      column_name NOT NULL
	   SMALLINT     data_type NOT NULL
	   VARCHAR      type_name NOT NULL
	   INTEGER      column_size
	   INTEGER      buffer_length
	   SMALLINT     decimal_digits
	   SMALLINT     pseudo_column
	 */
	if (IdentifierType == SQL_BEST_ROWID) {
		/* Select from the key table the (smallest) primary/unique key */
		/* Note: SCOPE is SQL_SCOPE_TRANSACTION is 1 */
		/* Note: PSEUDO_COLUMN is SQL_PC_NOT_PSEUDO is 1 */
		strcpy(query_end,
		       "select "
		       "cast(1 as smallint) as scope, "
		       "c.\"name\" as column_name, "
		       "cast(c.\"type\" as smallint) as data_type, "
		       "c.\"type\" as type_name, "
		       "cast(c.\"type_digits\" as integer) as column_size, "
		       "cast(0 as integer) as buffer_length, "
		       "cast(c.\"type_scale\" as smallint) as decimal_digits, "
		       "cast(1 as smallint) as pseudo_column "
		       "from sys.\"schemas\" s, sys.\"tables\" t, "
		       "sys.\"columns\" c, sys.\"keys\" k, "
		       "sys.\"objects\" kc "
		       "where s.\"id\" = t.\"schema_id\" and "
		       "t.\"id\" = c.\"table_id\" and "
		       "t.\"id\" = k.\"table_id\" and "
		       "c.\"name\" = kc.\"name\" and "
		       "kc.\"id\" = k.\"id\" and "
		       "k.\"type\" in (0, 1)");
		query_end += strlen(query_end);
		/* TODO: improve the SQL to get the correct result:
		   - only one set of columns should be returned, also
		     when multiple primary keys are available for this
		     table.
		   - when the table has NO primary key it should
		     return the columns of a unique key (only from ONE
		     unique key which is also the best/smallest key)
		   TODO: optimize SQL:
		   - when no SchemaName is set (see above) also no
		     filtering on SCHEMA NAME and join with table
		     SCHEMAS is needed!
		 */

		/* add the selection condition */
		/* search pattern is not allowed for table name so use = and not LIKE */
		sprintf(query_end, " and t.\"name\" = '%.*s'",
			NameLength3, (char*)TableName);
		query_end += strlen(query_end);

		if (SchemaName != NULL && NameLength2 > 0) {
			/* filtering requested on schema name */
			/* search pattern is not allowed so use = and not LIKE */
			sprintf(query_end, " and s.\"name\" = '%.*s'",
				NameLength2, (char*)SchemaName);
			query_end += strlen(query_end);
		}

		/* add an extra selection when SQL_NO_NULLS is requested */
		if (Nullable == SQL_NO_NULLS) {
			strcpy(query_end, " and c.\"null\" = false");
			query_end += strlen(query_end);
		}

		/* no ordering needed */
	} else {
		assert(IdentifierType == SQL_ROWVER);
		/* The backend does not have such info available */
		/* create just a query which results in zero rows */
		/* Note: pseudo_column is sql_pc_unknown is 0 */
		strcpy(query_end,
		       "select "
		       "cast(null as smallint) as scope, "
		       "cast('' as varchar(1)) as column_name, "
		       "cast(1 as smallint) as data_type, "
		       "cast('char' as varchar(4)) as type_name, "
		       "cast(1 as integer) as column_size, "
		       "cast(1 as integer) as buffer_length, "
		       "cast(0 as smallint) as decimal_digits, "
		       "cast(0 as smallint) as pseudo_column "
		       "where 0 = 1");
		query_end += strlen(query_end);
	}
	assert(query_end - query < 1000 + NameLength3 + NameLength2);

	/* query the MonetDB data dictionary tables */
	rc = SQLExecDirect_(stmt,
			    (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLSpecialColumns(SQLHSTMT StatementHandle,
		  SQLUSMALLINT IdentifierType,
		  SQLCHAR *CatalogName,
		  SQLSMALLINT NameLength1,
		  SQLCHAR *SchemaName,
		  SQLSMALLINT NameLength2,
		  SQLCHAR *TableName,
		  SQLSMALLINT NameLength3,
		  SQLUSMALLINT Scope,
		  SQLUSMALLINT Nullable)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSpecialColumns " PTRFMT " %u ",
		PTRFMTCAST StatementHandle, (unsigned int) IdentifierType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLSpecialColumns_(stmt,
				  IdentifierType,
				  CatalogName, NameLength1,
				  SchemaName, NameLength2,
				  TableName, NameLength3,
				  Scope,
				  Nullable);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLSpecialColumnsA(SQLHSTMT StatementHandle,
		   SQLUSMALLINT IdentifierType,
		   SQLCHAR *CatalogName,
		   SQLSMALLINT NameLength1,
		   SQLCHAR *SchemaName,
		   SQLSMALLINT NameLength2,
		   SQLCHAR *TableName,
		   SQLSMALLINT NameLength3,
		   SQLUSMALLINT Scope,
		   SQLUSMALLINT Nullable)
{
	return SQLSpecialColumns(StatementHandle,
				 IdentifierType,
				 CatalogName, NameLength1,
				 SchemaName, NameLength2,
				 TableName, NameLength3,
				 Scope,
				 Nullable);
}

SQLRETURN SQL_API
SQLSpecialColumnsW(SQLHSTMT StatementHandle,
		   SQLUSMALLINT IdentifierType,
		   SQLWCHAR *CatalogName,
		   SQLSMALLINT NameLength1,
		   SQLWCHAR *SchemaName,
		   SQLSMALLINT NameLength2,
		   SQLWCHAR *TableName,
		   SQLSMALLINT NameLength3,
		   SQLUSMALLINT Scope,
		   SQLUSMALLINT Nullable)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSpecialColumnsW " PTRFMT " %u ",
		PTRFMTCAST StatementHandle, (unsigned int) IdentifierType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto exit);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto exit);
	fixWcharIn(TableName, NameLength3, SQLCHAR, table,
		   addStmtError, stmt, goto exit);

	rc = SQLSpecialColumns_(stmt,
				IdentifierType,
				catalog, SQL_NTS,
				schema, SQL_NTS,
				table, SQL_NTS,
				Scope,
				Nullable);

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
