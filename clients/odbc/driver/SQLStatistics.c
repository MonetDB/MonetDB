/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
 * SQLStatistics()
 * CLI Compliance: ISO 92
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
translateUnique(SQLUSMALLINT Unique)
{
	switch (Unique) {
	case SQL_INDEX_ALL:
		return "SQL_INDEX_ALL";
	case SQL_INDEX_UNIQUE:
		return "SQL_INDEX_UNIQUE";
	default:
		return "unknown";
	}
}

static char *
translateReserved(SQLUSMALLINT Reserved)
{
	switch (Reserved) {
	case SQL_ENSURE:
		return "SQL_ENSURE";
	case SQL_QUICK:
		return "SQL_QUICK";
	default:
		return "unknown";
	}
}
#endif

static SQLRETURN
MNDBStatistics(ODBCStmt *stmt,
	       const SQLCHAR *CatalogName,
	       SQLSMALLINT NameLength1,
	       const SQLCHAR *SchemaName,
	       SQLSMALLINT NameLength2,
	       const SQLCHAR *TableName,
	       SQLSMALLINT NameLength3,
	       SQLUSMALLINT Unique,
	       SQLUSMALLINT Reserved)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *sch = NULL, *tab = NULL;

	fixODBCstring(TableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" %s %s\n",
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, TableName ? (char *) TableName : "",
		translateUnique(Unique), translateReserved(Reserved));
#endif

	/* check for valid Unique argument */
	switch (Unique) {
	case SQL_INDEX_ALL:
	case SQL_INDEX_UNIQUE:
		break;
	default:
		/* Uniqueness option type out of range */
		addStmtError(stmt, "HY100", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Reserved argument */
	switch (Reserved) {
	case SQL_ENSURE:
	case SQL_QUICK:
		break;
	default:
		/* Accuracy option type out of range */
		addStmtError(stmt, "HY101", NULL, 0);
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

	if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
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

	/* construct the query now */
	querylen = 1200 + strlen(stmt->Dbc->dbname) +
		(sch ? strlen(sch) : 0) + (tab ? strlen(tab) : 0);
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

	/* SQLStatistics returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name NOT NULL
	   SMALLINT     non_unique
	   VARCHAR      index_qualifier
	   VARCHAR      index_name
	   SMALLINT     type NOT NULL
	   SMALLINT     ordinal_position
	   VARCHAR      column_name
	   CHAR(1)      asc_or_desc
	   INTEGER      cardinality
	   INTEGER      pages
	   VARCHAR      filter_condition
	 */
	/* TODO: finish the SQL query */
	pos += snprintf(query + pos, querylen - pos,
		"select '%s' as table_cat, "
		       "s.name as table_schem, "
		       "t.name as table_name, "
		       "case when k.name is null then cast(1 as smallint) "
		            "else cast(0 as smallint) end as non_unique, "
		       "cast(null as varchar(1)) as index_qualifier, "
		       "i.name as index_name, "
		       "case i.type when 0 then cast(%d as smallint) "
		                   "else cast(%d as smallint) end as type, "
		       "cast(kc.nr + 1 as smallint) as ordinal_position, "
		       "c.name as column_name, "
		       "cast(null as char(1)) as asc_or_desc, "
		       "cast(null as integer) as cardinality, "
		       "cast(null as integer) as pages, "
		       "cast(null as varchar(1)) as filter_condition "
		"from sys.idxs i, "
		     "sys.schemas s, "
		     "sys.tables t, "
		     "sys.columns c, "
		     "sys.objects kc, "
		     "sys.keys k "
		"where i.table_id = t.id and "
		      "t.schema_id = s.id and "
		      "i.id = kc.id and "
		      "t.id = c.table_id and "
		      "kc.name = c.name and "
		      "k.name = i.name and "
		      "k.type in (0, 1)",
		stmt->Dbc->dbname,
		SQL_INDEX_HASHED, SQL_INDEX_OTHER);
	assert(pos < 1000);

	/* Construct the selection condition query part */
	if (NameLength1 > 0 && CatalogName != NULL) {
		/* filtering requested on catalog name */
		if (strcmp((char *) CatalogName, stmt->Dbc->dbname) != 0) {
			/* catalog name does not match the database name, so return no rows */
			pos += snprintf(query + pos, querylen - pos, " and 1=2");
		}
	}
	if (sch) {
		/* filtering requested on schema name */
		pos += snprintf(query + pos, querylen - pos, " and %s", sch);
		free(sch);
	}
	if (tab) {
		/* filtering requested on table name */
		pos += snprintf(query + pos, querylen - pos, " and %s", tab);
		free(tab);
	}

	/* add the ordering */
	pos += strcpy_len(query + pos, " order by non_unique, type, index_qualifier, index_name, ordinal_position", querylen - pos);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query, (SQLINTEGER) pos);

	free(query);

	return rc;

  nomem:
	/* note that query must be NULL when we get here */
	if (sch)
		free(sch);
	if (tab)
		free(tab);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLStatistics(SQLHSTMT StatementHandle,
	      SQLCHAR *CatalogName,
	      SQLSMALLINT NameLength1,
	      SQLCHAR *SchemaName,
	      SQLSMALLINT NameLength2,
	      SQLCHAR *TableName,
	      SQLSMALLINT NameLength3,
	      SQLUSMALLINT Unique,
	      SQLUSMALLINT Reserved)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLStatistics %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBStatistics(stmt,
			      CatalogName, NameLength1,
			      SchemaName, NameLength2,
			      TableName, NameLength3,
			      Unique,
			      Reserved);
}

SQLRETURN SQL_API
SQLStatisticsA(SQLHSTMT StatementHandle,
	       SQLCHAR *CatalogName,
	       SQLSMALLINT NameLength1,
	       SQLCHAR *SchemaName,
	       SQLSMALLINT NameLength2,
	       SQLCHAR *TableName,
	       SQLSMALLINT NameLength3,
	       SQLUSMALLINT Unique,
	       SQLUSMALLINT Reserved)
{
	return SQLStatistics(StatementHandle,
			     CatalogName, NameLength1,
			     SchemaName, NameLength2,
			     TableName, NameLength3,
			     Unique,
			     Reserved);
}

SQLRETURN SQL_API
SQLStatisticsW(SQLHSTMT StatementHandle,
	       SQLWCHAR *CatalogName,
	       SQLSMALLINT NameLength1,
	       SQLWCHAR *SchemaName,
	       SQLSMALLINT NameLength2,
	       SQLWCHAR *TableName,
	       SQLSMALLINT NameLength3,
	       SQLUSMALLINT Unique,
	       SQLUSMALLINT Reserved)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLStatisticsW %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(TableName, NameLength3, SQLCHAR, table,
		   addStmtError, stmt, goto bailout);

	rc = MNDBStatistics(stmt,
			    catalog, SQL_NTS,
			    schema, SQL_NTS,
			    table, SQL_NTS,
			    Unique,
			    Reserved);

      bailout:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);

	return rc;
}
