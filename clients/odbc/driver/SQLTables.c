/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
 * SQLTables()
 * CLI Compliance: X/Open
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
MNDBTables(ODBCStmt *stmt,
	   const SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
	   const SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
	   const SQLCHAR *TableName, SQLSMALLINT NameLength3,
	   const SQLCHAR *TableType, SQLSMALLINT NameLength4)
{
	RETCODE rc;
	char *sch = NULL, *tab = NULL;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;

	/* convert input string parameters to normal null terminated C
	 * strings */
	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableType, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, TableName ? (char *) TableName : "",
		(int) NameLength4, TableType ? (char *) TableType : "");
#endif

	/* SQLTables returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name
	   VARCHAR      table_type
	   VARCHAR      remarks
	 */

	/* Check first on the special cases */
	if (NameLength2 == 0 &&
	    NameLength3 == 0 &&
	    CatalogName &&
	    strcmp((char *) CatalogName, SQL_ALL_CATALOGS) == 0) {
		/* Special case query to fetch all Catalog names. */
		query = strdup("select e.value as table_cat, "
				      "cast(null as varchar(1)) as table_schem, "
				      "cast(null as varchar(1)) as table_name, "
				      "cast(null as varchar(1)) as table_type, "
				      "cast(null as varchar(1)) as remarks "
			       "from sys.env() e "
			       "where e.name = 'gdk_dbname'");
	} else if (NameLength1 == 0 &&
		   NameLength3 == 0 &&
		   SchemaName &&
		   strcmp((char *) SchemaName, SQL_ALL_SCHEMAS) == 0) {
		/* Special case query to fetch all Schema names. */
		query = strdup("select cast(null as varchar(1)) as table_cat, "
				      "name as table_schem, "
				      "cast(null as varchar(1)) as table_name, "
				      "cast(null as varchar(1)) as table_type, "
			       /* ODBC says remarks column contains
				* NULL even though MonetDB supports
				* schema remarks */
				      "cast(null as varchar(1)) as remarks "
			       "from sys.schemas order by table_schem");
	} else if (NameLength1 == 0 &&
		   NameLength2 == 0 &&
		   NameLength3 == 0 &&
		   TableType &&
		   strcmp((char *) TableType, SQL_ALL_TABLE_TYPES) == 0) {
		/* Special case query to fetch all Table type names. */
		query = strdup("select cast(null as varchar(1)) as table_cat, "
				      "cast(null as varchar(1)) as table_schem, "
				      "cast(null as varchar(1)) as table_name, "
				      "table_type_name as table_type, "
				      "cast(null as varchar(1)) as remarks "
			       "from sys.table_types order by table_type");
	} else {
		/* no special case argument values */
		size_t querylen;
		size_t pos = 0;

		if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
			if (NameLength2 > 0) {
				sch = ODBCParsePV("s", "name",
						  (const char *) SchemaName,
						  (size_t) NameLength2);
				if (sch == NULL)
					goto nomem;
			}
			if (NameLength3 > 0) {
				tab = ODBCParsePV("t", "name",
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
		querylen = 2000 + strlen(stmt->Dbc->dbname) +
			(sch ? strlen(sch) : 0) + (tab ? strlen(tab) : 0) +
			((NameLength4 + 1) / 5) * 67;
		query = malloc(querylen);
		if (query == NULL)
			goto nomem;

		pos += snprintf(query + pos, querylen - pos,
		       "select '%s' as table_cat, "
			      "s.name as table_schem, "
			      "t.name as table_name, "
		              "tt.table_type_name as table_type, "
			      "%s as remarks "
		       "from sys.schemas s, "
			    "sys.tables t%s, "
		            "sys.table_types tt "
		       "where s.id = t.schema_id and "
		             "t.type = tt.table_type_id",
			stmt->Dbc->dbname,
			stmt->Dbc->has_comment ? "c.remark" : "cast(null as varchar(1))",
			stmt->Dbc->has_comment ? " left outer join sys.comments c on c.id = t.id" : "");
		assert(pos < 1900);

		/* dependent on the input parameter values we must add a
		   variable selection condition dynamically */

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

		if (NameLength4 > 0) {
			/* filtering requested on table type */
			char buf[32];	/* the longest string is "GLOBAL TEMPORARY TABLE" */
			int i;
			size_t j;

			pos += strcpy_len(query + pos, " and tt.table_type_name in (", querylen - pos);
			for (j = 0, i = 0; i < NameLength4 + 1; i++) {
				if (i == NameLength4 || TableType[i] == ',') {
					if (j > 0 && buf[j - 1] == ' ')
						j--;
					if (j >= sizeof(buf) || j == 0) {
						j = 0;
						continue;
					}
					buf[j] = 0;
					pos += snprintf(query + pos, querylen - pos, "'%s',", buf);
 					j = 0;
				} else if (j < sizeof(buf) &&
					   TableType[i] != '\'' &&
					   (TableType[i] != ' ' ||
					    (j > 0 && buf[j - 1] != ' ')))
					buf[j++] = TableType[i];
			}
			if (query[pos - 1] == ',') {
				query[pos - 1] = ')';
			} else {
				/* no extra tests added, so remove
				 * clause completely */
				pos -= 28;
			}
			query[pos] = 0;
		}

		/* add the ordering */
		pos += strcpy_len(query + pos, " order by table_type, table_schem, table_name", querylen - pos);
	}

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;

  nomem:
	if (sch)
		free(sch);
	if (tab)
		free(tab);
	if (query)
		free(query);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLTables(SQLHSTMT StatementHandle,
	  SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
	  SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
	  SQLCHAR *TableName, SQLSMALLINT NameLength3,
	  SQLCHAR *TableType, SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLTables %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBTables(stmt,
			  CatalogName, NameLength1,
			  SchemaName, NameLength2,
			  TableName, NameLength3,
			  TableType, NameLength4);
}

SQLRETURN SQL_API
SQLTablesA(SQLHSTMT StatementHandle,
	   SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
	   SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
	   SQLCHAR *TableName, SQLSMALLINT NameLength3,
	   SQLCHAR *TableType, SQLSMALLINT NameLength4)
{
	return SQLTables(StatementHandle,
			 CatalogName, NameLength1,
			 SchemaName, NameLength2,
			 TableName, NameLength3,
			 TableType, NameLength4);
}

SQLRETURN SQL_API
SQLTablesW(SQLHSTMT StatementHandle,
	   SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
	   SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
	   SQLWCHAR *TableName, SQLSMALLINT NameLength3,
	   SQLWCHAR *TableType, SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL, *type = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLTablesW %p ", StatementHandle);
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
	fixWcharIn(TableType, NameLength4, SQLCHAR, type,
		   addStmtError, stmt, goto bailout);

	rc = MNDBTables(stmt,
			catalog, SQL_NTS,
			schema, SQL_NTS,
			table, SQL_NTS,
			type, SQL_NTS);

      bailout:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);
	if (type)
		free(type);

	return rc;
}
