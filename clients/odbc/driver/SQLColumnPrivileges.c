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
 * SQLColumnPrivileges()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not implemented (it only sets an error),
 * because monetDB SQL frontend does not support column based authorization.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCUtil.h"
#include "ODBCStmt.h"


static SQLRETURN
MNDBColumnPrivileges(ODBCStmt *stmt,
		     const SQLCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     const SQLCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     const SQLCHAR *TableName,
		     SQLSMALLINT NameLength3,
		     const SQLCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	RETCODE rc;
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *sch = NULL, *tab = NULL, *col = NULL;

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ColumnName, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, TableName ? (char *) TableName : "",
		(int) NameLength4, ColumnName ? (char *) ColumnName : "");
#endif

	if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
		if (NameLength2 > 0) {
			sch = ODBCParseOA("s", "name",
					  (const char *) SchemaName,
					  (size_t) NameLength2);
			if (sch == NULL)
				goto nomem;
		}
		if (NameLength3 > 0) {
			tab = ODBCParseOA("tc", "tname",
					  (const char *) TableName,
					  (size_t) NameLength3);
			if (tab == NULL)
				goto nomem;
		}
		if (NameLength4 > 0) {
			col = ODBCParsePV("tc", "cname",
					  (const char *) ColumnName,
					  (size_t) NameLength4,
					  stmt->Dbc);
			if (col == NULL)
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
			tab = ODBCParseID("tc", "tname",
					  (const char *) TableName,
					  (size_t) NameLength3);
			if (tab == NULL)
				goto nomem;
		}
		if (NameLength4 > 0) {
			col = ODBCParseID("tc", "cname",
					  (const char *) ColumnName,
					  (size_t) NameLength4);
			if (col == NULL)
				goto nomem;
		}
	}

	/* construct the query now */
	querylen = 1300 + (sch ? strlen(sch) : 0) +
		(tab ? strlen(tab) : 0) + (col ? strlen(col) : 0);
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

	/* SQLColumnPrivileges returns a table with the following columns:
	   TABLE_CAT    VARCHAR
	   TABLE_SCHEM  VARCHAR
	   TABLE_NAME   VARCHAR NOT NULL
	   COLUMN_NAME  VARCHAR NOT NULL
	   GRANTOR      VARCHAR
	   GRANTEE      VARCHAR NOT NULL
	   PRIVILEGE    VARCHAR NOT NULL
	   IS_GRANTABLE VARCHAR
	 */

	pos += snprintf(query + pos, querylen - pos,
		"select cast(null as varchar(1)) as \"TABLE_CAT\", "
		       "s.name as \"TABLE_SCHEM\", "
		       "tc.tname as \"TABLE_NAME\", "
		       "tc.cname as \"COLUMN_NAME\", "
		       "case a.id "
			    "when s.owner "
			    "then '_SYSTEM' "
			    "else g.name "
			    "end as \"GRANTOR\", "
		       "case a.name "
			    "when 'public' then 'PUBLIC' "
			    "else a.name "
			    "end as \"GRANTEE\", "
		       "pc.privilege_code_name as \"PRIVILEGE\", "
		       "case p.grantable "
			    "when 1 then 'YES' "
			    "when 0 then 'NO' "
			    "end as \"IS_GRANTABLE\" "
		"from sys.schemas as s, "
		     /* next union all subquery is much more efficient than using sys.tables join sys.columns */
		     "(select t1.id as tid, t1.name as tname, t1.schema_id, c1.id as cid, c1.name as cname"
		     " from sys._tables as t1"
		     " join sys._columns as c1 on t1.id = c1.table_id"
		     " where not t1.system"	/* exclude system tables and views */
		     " union all"
		     " select t2.id as tid, t2.name as tname, t2.schema_id, c2.id as cid, c2.name as cname"
		     " from tmp._tables as t2"
		     " join tmp._columns as c2 on t2.id = c2.table_id)"
		     " as tc(tid, tname, schema_id, cid, cname), "
		     "sys.auths as a, "
		     "sys.privileges as p, "
		     "sys.auths as g, "
		     "%s "
		"where p.obj_id = tc.cid and "
		      "p.auth_id = a.id and "
		      "tc.schema_id = s.id and "
		      "p.grantor = g.id and "
		      "p.privileges = pc.privilege_code_id",
		/* a server that supports sys.comments also supports
		 * sys.privilege_codes */
		stmt->Dbc->has_comment ? "sys.privilege_codes as pc" :
		     "(values (1, 'SELECT'), "
			     "(2, 'UPDATE'), "
			     "(4, 'INSERT'), "
			     "(8, 'DELETE'), "
			     "(16, 'EXECUTE'), "
			     "(32, 'GRANT')) as pc(privilege_code_id, privilege_code_name)");
	assert(pos < 1200);

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
	if (col) {
		/* filtering requested on column name */
		pos += snprintf(query + pos, querylen - pos, " and %s", col);
		free(col);
	}

	/* add the ordering (exclude table_cat as it is the same for all rows) */
	pos += strcpy_len(query + pos, " order by \"TABLE_SCHEM\", \"TABLE_NAME\", \"COLUMN_NAME\", \"PRIVILEGE\"", querylen - pos);
	assert(pos < querylen);

	/* debug: fprintf(stdout, "SQLColumnPrivileges query (pos: %zu, len: %zu):\n%s\n\n", pos, strlen(query), query); */

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
	if (col)
		free(col);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLColumnPrivileges(SQLHSTMT StatementHandle,
		    SQLCHAR *CatalogName,
		    SQLSMALLINT NameLength1,
		    SQLCHAR *SchemaName,
		    SQLSMALLINT NameLength2,
		    SQLCHAR *TableName,
		    SQLSMALLINT NameLength3,
		    SQLCHAR *ColumnName,
		    SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnPrivileges %p", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBColumnPrivileges(stmt,
				    CatalogName, NameLength1,
				    SchemaName, NameLength2,
				    TableName, NameLength3,
				    ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLColumnPrivilegesA(SQLHSTMT StatementHandle,
		     SQLCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLCHAR *TableName,
		     SQLSMALLINT NameLength3,
		     SQLCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	return SQLColumnPrivileges(StatementHandle,
				   CatalogName, NameLength1,
				   SchemaName, NameLength2,
				   TableName, NameLength3,
				   ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLColumnPrivilegesW(SQLHSTMT StatementHandle,
		     SQLWCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLWCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLWCHAR *TableName,
		     SQLSMALLINT NameLength3,
		     SQLWCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL, *column = NULL;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnPrivilegesW %p", StatementHandle);
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
	fixWcharIn(ColumnName, NameLength4, SQLCHAR, column,
		   addStmtError, stmt, goto bailout);

	rc = MNDBColumnPrivileges(stmt,
				  catalog, SQL_NTS,
				  schema, SQL_NTS,
				  table, SQL_NTS,
				  column, SQL_NTS);

      bailout:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);
	if (column)
		free(column);
	return rc;
}
