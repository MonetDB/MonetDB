/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


#ifdef ODBCDEBUG
static char *
translateIdentifierType(SQLUSMALLINT IdentifierType)
{
	switch (IdentifierType) {
	case SQL_BEST_ROWID:
		return "SQL_BEST_ROWID";
	case SQL_ROWVER:
		return "SQL_ROWVER";
	default:
		return "unknown";
	}
}

static char *
translateScope(SQLUSMALLINT Scope)
{
	/* check for valid Scope argument */
	switch (Scope) {
	case SQL_SCOPE_CURROW:
		return "SQL_SCOPE_CURROW";
	case SQL_SCOPE_TRANSACTION:
		return "SQL_SCOPE_TRANSACTION";
	case SQL_SCOPE_SESSION:
		return "SQL_SCOPE_SESSION";
	default:
		return "unknown";
	}
}

static char *
translateNullable(SQLUSMALLINT Nullable)
{
	/* check for valid Nullable argument */
	switch (Nullable) {
	case SQL_NO_NULLS:
		return "SQL_NO_NULLS";
	case SQL_NULLABLE:
		return "SQL_NULLABLE";
	default:
		return "unknown";
	}
}
#endif

static SQLRETURN
MNDBSpecialColumns(ODBCStmt *stmt,
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
	char *cat = NULL, *sch = NULL, *tab = NULL;

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" %s %s\n",
		(int) NameLength1, (char *) CatalogName,
		(int) NameLength2, (char *) SchemaName,
		(int) NameLength3, (char *) TableName,
		translateScope(Scope), translateNullable(Nullable));
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
		if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
			if (NameLength1 > 0) {
				cat = ODBCParseOA("e", "value",
						  (const char *) CatalogName,
						  (size_t) NameLength1);
				if (cat == NULL)
					goto nomem;
			}
			if (NameLength2 > 0) {
				sch = ODBCParseOA("s", "name",
						  (const char *) SchemaName,
						  (size_t) NameLength2);
				if (sch == NULL)
					goto nomem;
			}
			if (NameLength3 > 0) {
				tab = ODBCParseOA("t", "name",
						  (const char *) TableName,
						  (size_t) NameLength3);
				if (tab == NULL)
					goto nomem;
			}
		} else {
			if (NameLength1 > 0) {
				cat = ODBCParseID("e", "value",
						  (const char *) CatalogName,
						  (size_t) NameLength1);
				if (cat == NULL)
					goto nomem;
			}
			if (NameLength2 > 0) {
				sch = ODBCParseID("s", "name",
						  (const char *) SchemaName,
						  (size_t) NameLength2);
				if (sch == NULL)
					goto nomem;
			}
			if (NameLength3 > 0) {
				tab = ODBCParseID("t", "name",
						  (const char *) TableName,
						  (size_t) NameLength3);
				if (tab == NULL)
					goto nomem;
			}
		}

		/* first create a string buffer (1000 extra bytes is plenty */
		query = (char *) malloc(5000 + NameLength1 + NameLength2 + NameLength3);
		if (query == NULL)
			goto nomem;
		query_end = query;

		/* Note: SCOPE is SQL_SCOPE_TRANSACTION */
		/* Note: PSEUDO_COLUMN is SQL_PC_NOT_PSEUDO */
		sprintf(query_end,
			"with sc as ("
			"select t.id as table_id, k.type as type, "
			       "cast(%d as smallint) as scope, "
			       "c.name as column_name, "
			       "case c.type "
				    "when 'bigint' then %d "
				    "when 'blob' then %d "
				    "when 'boolean' then %d "
				    "when 'char' then %d "
				    "when 'clob' then %d "
				    "when 'date' then %d "
				    "when 'decimal' then %d "
				    "when 'double' then %d "
				    "when 'int' then %d "
				    "when 'month_interval' then "
					 "case c.type_digits "
					      "when 1 then %d "
					      "when 2 then %d "
					      "when 3 then %d "
					 "end "
				    "when 'real' then %d "
				    "when 'sec_interval' then "
					 "case c.type_digits "
					      "when 4 then %d "
					      "when 5 then %d "
					      "when 6 then %d "
					      "when 7 then %d "
					      "when 8 then %d "
					      "when 9 then %d "
					      "when 10 then %d "
					      "when 11 then %d "
					      "when 12 then %d "
					      "when 13 then %d "
					 "end "
				    "when 'smallint' then %d "
				    "when 'timestamp' then %d "
				    "when 'timestamptz' then %d "
				    "when 'time' then %d "
				    "when 'timetz' then %d "
				    "when 'tinyint' then %d "
				    "when 'varchar' then %d "
			       "end as data_type, "
			       "case c.type "
				    "when 'bigint' then 'BIGINT' "
				    "when 'blob' then 'BINARY LARGE OBJECT' "
				    "when 'boolean' then 'BOOLEAN' "
				    "when 'char' then 'CHARACTER' "
				    "when 'clob' then 'CHARACTER LARGE OBJECT' "
				    "when 'date' then 'DATE' "
				    "when 'decimal' then 'DECIMAL' "
				    "when 'double' then 'DOUBLE' "
				    "when 'int' then 'INTEGER' "
				    "when 'month_interval' then "
					 "case c.type_digits "
					      "when 1 then 'INTERVAL YEAR' "
					      "when 2 then 'INTERVAL YEAR TO MONTH' "
					      "when 3 then 'INTERVAL MONTH' "
					 "end "
				    "when 'real' then 'REAL' "
				    "when 'sec_interval' then "
					 "case c.type_digits "
					      "when 4 then 'INTERVAL DAY' "
					      "when 5 then 'INTERVAL DAY TO HOUR' "
					      "when 6 then 'INTERVAL DAY TO MINUTE' "
					      "when 7 then 'INTERVAL DAY TO SECOND' "
					      "when 8 then 'INTERVAL HOUR' "
					      "when 9 then 'INTERVAL HOUR TO MINUTE' "
					      "when 10 then 'INTERVAL HOUR TO SECOND' "
					      "when 11 then 'INTERVAL MINUTE' "
					      "when 12 then 'INTERVAL MINUTE TO SECOND' "
					      "when 13 then 'INTERVAL SECOND' "
					 "end "
				    "when 'smallint' then 'SMALLINT' "
				    "when 'timestamp' then 'TIMESTAMP' "
				    "when 'timestamptz' then 'TIMESTAMP' "
				    "when 'time' then 'TIME' "
				    "when 'timetz' then 'TIME' "
				    "when 'tinyint' then 'TINYINT' "
				    "when 'varchar' then 'VARCHAR' "
			       "end as type_name, "
			       "case c.type "
				    "when 'month_interval' then "
					 "case c.type_digits "
					      "when 1 then 26 "
					      "when 2 then 38 "
					      "when 3 then 27 "
					 "end "
				    "when 'sec_interval' then "
					 "case c.type_digits "
					      "when 4 then 25 "
					      "when 5 then 36 "
					      "when 6 then 41 "
					      "when 7 then 47 "
					      "when 8 then 26 "
					      "when 9 then 39 "
					      "when 10 then 45 "
					      "when 11 then 28 "
					      "when 12 then 44 "
					      "when 13 then 30 "
					 "end "
				    "when 'date' then 10 "
				    "when 'time' then 12 "
				    "when 'timetz' then 12 "
				    "when 'timestamp' then 23 "
				    "when 'timestamptz' then 23 "
				    "else c.type_digits "
			       "end as column_size, "
			       "case c.type "
				    "when 'month_interval' then "
					 "case c.type_digits "
					      "when 1 then 26 "
					      "when 2 then 38 "
					      "when 3 then 27 "
					 "end "
				    "when 'sec_interval' then "
					 "case c.type_digits "
					      "when 4 then 25 "
					      "when 5 then 36 "
					      "when 6 then 41 "
					      "when 7 then 47 "
					      "when 8 then 26 "
					      "when 9 then 39 "
					      "when 10 then 45 "
					      "when 11 then 28 "
					      "when 12 then 44 "
					      "when 13 then 30 "
					 "end "
				    "when 'date' then 10 "
				    "when 'time' then 12 "
				    "when 'timetz' then 12 "
				    "when 'timestamp' then 23 "
				    "when 'timestamptz' then 23 "
				    "when 'bigint' then 20 "
				    "when 'int' then 11 "
				    "when 'smallint' then 6 "
				    "when 'tinyint' then 4 "
				    "when 'char' then 6 * c.type_digits "
				    "when 'varchar' then 6 * c.type_digits "
				    "when 'double' then 24 "
				    "when 'real' then 14 "
				    "else c.type_digits "
			       "end as buffer_length, "
			       "case c.type "
				    "when 'time' then c.type_digits - 1 "
				    "when 'timetz' then c.type_digits - 1 "
				    "when 'timestamp' then c.type_digits - 1 "
				    "when 'timestamptz' then c.type_digits - 1 "
				    "when 'sec_interval' then 0 "
				    "when 'month_interval' then 0 "
				    "when 'real' then "
					 "case when c.type_digits = 24 and c.type_scale = 0 then 7 "
					 "else c.type_digits "
					 "end "
				    "when 'double' then "
					 "case when c.type_digits = 53 and c.type_scale = 0 then 15 "
					 "else c.type_digits "
					 "end "
				    "when 'decimal' then c.type_digits "
				    "when 'bigint' then 19 "
				    "when 'int' then 10 "
				    "when 'smallint' then 5 "
				    "when 'tinyint' then 3 "
			       "end as decimal_digits, "
			       "cast(%d as smallint) as pseudo_column "
			 "from sys.schemas s, "
			      "sys.tables t, "
			      "sys.columns c, "
			      "sys.keys k, "
			      "sys.objects kc, "
			      "sys.env() e  "
			 "where s.id = t.schema_id and "
			       "t.id = c.table_id and "
			       "t.id = k.table_id and "
			       "c.name = kc.name and "
			       "kc.id = k.id and "
			       "k.type = 0 and "
			       "e.name = 'gdk_dbname'",
			/* scope: */
			SQL_SCOPE_TRANSACTION,
			/* data_type: */
			SQL_BIGINT, SQL_LONGVARBINARY, SQL_BIT, SQL_WCHAR,
			SQL_WLONGVARCHAR, SQL_TYPE_DATE, SQL_DECIMAL,
			SQL_DOUBLE, SQL_INTEGER, SQL_INTERVAL_YEAR,
			SQL_INTERVAL_YEAR_TO_MONTH, SQL_INTERVAL_MONTH,
			SQL_REAL, SQL_INTERVAL_DAY, SQL_INTERVAL_DAY_TO_HOUR,
			SQL_INTERVAL_DAY_TO_MINUTE, SQL_INTERVAL_DAY_TO_SECOND,
			SQL_INTERVAL_HOUR, SQL_INTERVAL_HOUR_TO_MINUTE,
			SQL_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL_MINUTE,
			SQL_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL_SECOND,
			SQL_SMALLINT, SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP,
			SQL_TYPE_TIME, SQL_TYPE_TIME, SQL_TINYINT,
			SQL_WVARCHAR,
			/* pseudo_column: */
			SQL_PC_NOT_PSEUDO);
		assert(strlen(query) < 4300);
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
		if (cat) {
			/* filtering requested on catalog name */
			sprintf(query_end, " and %s", cat);
			query_end += strlen(query_end);
			free(cat);
		}
		if (sch) {
			/* filtering requested on schema name */
			sprintf(query_end, " and %s", sch);
			query_end += strlen(query_end);
			free(sch);
		}
		if (tab) {
			/* filtering requested on table name */
			sprintf(query_end, " and %s", tab);
			query_end += strlen(query_end);
			free(tab);
		}

		/* add an extra selection when SQL_NO_NULLS is requested */
		if (Nullable == SQL_NO_NULLS) {
			strcpy(query_end, " and c.\"null\" = false");
			query_end += strlen(query_end);
		}

		strcpy(query_end,
		       "), "
			"tid as ("
			   "select t.id as tid "
			    "from sys._tables t, sys.keys k "
			    "where t.id = k.table_id and k.type = 0"
		       ") "
			"select sc.scope, sc.column_name, sc.data_type, "
			       "sc.type_name, sc.column_size, "
			       "sc.buffer_length, sc.decimal_digits, "
			       "sc.pseudo_column "
			"from sc "
			"where (sc.type = 0 and "
			       "sc.table_id in (select tid from tid)) or "
			      "(sc.type = 1 and "
			       "sc.table_id not in (select tid from tid))");
		query_end += strlen(query_end);

		/* ordering on SCOPE not needed (since it is constant) */
	} else {
		assert(IdentifierType == SQL_ROWVER);
		/* The backend does not have such info available */
		/* create just a query which results in zero rows */
		/* Note: pseudo_column is sql_pc_unknown is 0 */
		query = strdup("select cast(null as smallint) as scope, "
				      "cast('' as varchar(1)) as column_name, "
				      "cast(1 as smallint) as data_type, "
				      "cast('char' as varchar(4)) as type_name, "
				      "cast(1 as integer) as column_size, "
				      "cast(1 as integer) as buffer_length, "
				      "cast(0 as smallint) as decimal_digits, "
				      "cast(0 as smallint) as pseudo_column "
			       "where 0 = 1");
		if (query == NULL)
			goto nomem;
		query_end = query + strlen(query);
	}

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt,
			    (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;

  nomem:
	/* note that query must be NULL when we get here */
	if (cat)
		free(cat);
	if (sch)
		free(sch);
	if (tab)
		free(tab);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
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
	ODBCLOG("SQLSpecialColumns " PTRFMT " %s ",
		PTRFMTCAST StatementHandle,
		translateIdentifierType(IdentifierType));
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBSpecialColumns(stmt,
				  IdentifierType,
				  CatalogName, NameLength1,
				  SchemaName, NameLength2,
				  TableName, NameLength3,
				  Scope,
				  Nullable);
}

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
	ODBCLOG("SQLSpecialColumnsW " PTRFMT " %s ",
		PTRFMTCAST StatementHandle,
		translateIdentifierType(IdentifierType));
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

	rc = MNDBSpecialColumns(stmt,
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
