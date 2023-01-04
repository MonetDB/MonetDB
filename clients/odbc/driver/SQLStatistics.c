/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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
	bool addTmpQuery = false;

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

	/* determine if we need to add a query against the tmp.* tables */
	addTmpQuery = (SchemaName == NULL)
		   || (SchemaName != NULL
			&& (strcmp((const char *) SchemaName, "tmp") == 0
			 || strchr((const char *) SchemaName, '%') != NULL
			 || strchr((const char *) SchemaName, '_') != NULL));

	/* construct the query */
	querylen = 1200 + (sch ? strlen(sch) : 0) + (tab ? strlen(tab) : 0);
	if (addTmpQuery)
		querylen *= 2;
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

	/* SQLStatistics returns a table with the following columns:
	   VARCHAR      TABLE_CAT
	   VARCHAR      TABLE_SCHEM
	   VARCHAR      TABLE_NAME NOT NULL
	   SMALLINT     NON_UNIQUE
	   VARCHAR      INDEX_QUALIFIER
	   VARCHAR      INDEX_NAME
	   SMALLINT     TYPE NOT NULL
	   SMALLINT     ORDINAL_POSITION
	   VARCHAR      COLUMN_NAME
	   CHAR(1)      ASC_OR_DESC
	   INTEGER      CARDINALITY
	   INTEGER      PAGES
	   VARCHAR      FILTER_CONDITION
	 */
	pos += snprintf(query + pos, querylen - pos,
		"select cast(null as varchar(1)) as \"TABLE_CAT\", "
		       "s.name as \"TABLE_SCHEM\", "
		       "t.name as \"TABLE_NAME\", "
		       "cast(sys.ifthenelse(k.name is null,1,0) as smallint) as \"NON_UNIQUE\", "
		       "cast(null as varchar(1)) as \"INDEX_QUALIFIER\", "
		       "i.name as \"INDEX_NAME\", "
		       "cast(sys.ifthenelse(i.type = 0, %d, %d) as smallint) as \"TYPE\", "
		       "cast(kc.nr + 1 as smallint) as \"ORDINAL_POSITION\", "
		       "c.name as \"COLUMN_NAME\", "
		       "cast(null as char(1)) as \"ASC_OR_DESC\", "
		       "cast(sys.ifthenelse(k.name is null,NULL,st.count) as integer) as \"CARDINALITY\", "
		       "cast(null as integer) as \"PAGES\", "
		       "cast(null as varchar(1)) as \"FILTER_CONDITION\" "
		"from sys.idxs i "
		"join sys._tables t on i.table_id = t.id "
		"join sys.schemas s on t.schema_id = s.id "
		"join sys.objects kc on i.id = kc.id "
		"join sys._columns c on (t.id = c.table_id and kc.name = c.name) "
		"%sjoin sys.keys k on (k.name = i.name and i.table_id = k.table_id and k.type in (0, 1)) "
		"join sys.storage() st on (st.schema = s.name and st.table = t.name and st.column = c.name) "
		"where 1=1",
		SQL_INDEX_HASHED, SQL_INDEX_OTHER,
		(Unique == SQL_INDEX_UNIQUE) ? "" : "left outer ");
		/* by using left outer join we also get indices for tables
		   which have no primary key or unique constraints, so no rows in sys.keys */
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
	}
	if (tab) {
		/* filtering requested on table name */
		pos += snprintf(query + pos, querylen - pos, " and %s", tab);
	}

	if (addTmpQuery) {
		/* we must also include the indexes of local temporary tables
		   which are stored in tmp.idxs, tmp._tables, tmp._columns, tmp.objects and tmp.keys */
		pos += snprintf(query + pos, querylen - pos,
			" UNION ALL "
			"select cast(null as varchar(1)) as \"TABLE_CAT\", "
			       "s.name as \"TABLE_SCHEM\", "
			       "t.name as \"TABLE_NAME\", "
			       "cast(sys.ifthenelse(k.name is null,1,0) as smallint) as \"NON_UNIQUE\", "
			       "cast(null as varchar(1)) as \"INDEX_QUALIFIER\", "
			       "i.name as \"INDEX_NAME\", "
			       "cast(sys.ifthenelse(i.type = 0, %d, %d) as smallint) as \"TYPE\", "
			       "cast(kc.nr + 1 as smallint) as \"ORDINAL_POSITION\", "
			       "c.name as \"COLUMN_NAME\", "
			       "cast(null as char(1)) as \"ASC_OR_DESC\", "
			       "cast(sys.ifthenelse(k.name is null,NULL,st.count) as integer) as \"CARDINALITY\", "
			       "cast(null as integer) as \"PAGES\", "
			       "cast(null as varchar(1)) as \"FILTER_CONDITION\" "
			"from tmp.idxs i "
			"join tmp._tables t on i.table_id = t.id "
			"join sys.schemas s on t.schema_id = s.id "
			"join tmp.objects kc on i.id = kc.id "
			"join tmp._columns c on (t.id = c.table_id and kc.name = c.name) "
			"%sjoin tmp.keys k on (k.name = i.name and i.table_id = k.table_id and k.type in (0, 1))"
			"left outer join sys.storage() st on (st.schema = s.name and st.table = t.name and st.column = c.name) "
			"where 1=1",
			SQL_INDEX_HASHED, SQL_INDEX_OTHER,
			(Unique == SQL_INDEX_UNIQUE) ? "" : "left outer ");

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
		}
		if (tab) {
			/* filtering requested on table name */
			pos += snprintf(query + pos, querylen - pos, " and %s", tab);
		}
	}
	assert(pos < (querylen - 74));

	if (sch)
		free(sch);
	if (tab)
		free(tab);

	/* add the ordering */
	pos += strcpy_len(query + pos, " order by \"NON_UNIQUE\", \"TYPE\", \"INDEX_QUALIFIER\", \"INDEX_NAME\", \"ORDINAL_POSITION\"", querylen - pos);
	assert(pos < querylen);

	/* debug: fprintf(stdout, "SQLStatistics query (pos: %zu, len: %zu):\n%s\n\n", pos, strlen(query), query); */

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
