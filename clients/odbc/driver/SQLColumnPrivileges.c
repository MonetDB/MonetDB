/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
	char *query_end = NULL;
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
			tab = ODBCParseOA("t", "name",
					  (const char *) TableName,
					  (size_t) NameLength3);
			if (tab == NULL)
				goto nomem;
		}
		if (NameLength4 > 0) {
			col = ODBCParsePV("c", "name",
					  (const char *) ColumnName,
					  (size_t) NameLength4);
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
			tab = ODBCParseID("t", "name",
					  (const char *) TableName,
					  (size_t) NameLength3);
			if (tab == NULL)
				goto nomem;
		}
		if (NameLength4 > 0) {
			col = ODBCParseID("c", "name",
					  (const char *) ColumnName,
					  (size_t) NameLength4);
			if (col == NULL)
				goto nomem;
		}
	}

	/* construct the query now */
	query = malloc(1200 + strlen(stmt->Dbc->dbname) + (sch ? strlen(sch) : 0) +
			(tab ? strlen(tab) : 0) + (col ? strlen(col) : 0));
	if (query == NULL)
		goto nomem;
	query_end = query;

	/* SQLColumnPrivileges returns a table with the following columns:
	   table_cat    VARCHAR
	   table_schem  VARCHAR
	   table_name   VARCHAR NOT NULL
	   column_name  VARCHAR NOT NULL
	   grantor      VARCHAR
	   grantee      VARCHAR NOT NULL
	   privilege    VARCHAR NOT NULL
	   is_grantable VARCHAR
	 */

	sprintf(query_end,
		"select '%s' as table_cat, "
		       "s.name as table_schem, "
		       "t.name as table_name, "
		       "c.name as column_name, "
		       "case a.id "
			    "when s.owner "
			    "then '_SYSTEM' "
			    "else g.name "
			    "end as grantor, "
		       "case a.name "
			    "when 'public' then 'PUBLIC' "
			    "else a.name "
			    "end as grantee, "
		       "pc.privilege_code_name as privilege, "
		       "case p.grantable "
			    "when 1 then 'YES' "
			    "when 0 then 'NO' "
			    "end as is_grantable "
		"from sys.schemas as s, "
		     "sys._tables as t, "
		     "sys._columns as c, "
		     "sys.auths as a, "
		     "sys.privileges as p, "
		     "sys.auths as g, "
		     "%s "
		"where p.obj_id = c.id and "
		      "c.table_id = t.id and "
		      "p.auth_id = a.id and "
		      "t.schema_id = s.id and "
		      "not t.system and "
		      "p.grantor = g.id and "
		      "p.privileges = pc.privilege_code_id",
		stmt->Dbc->dbname,
		/* a server that supports sys.comments also supports
		 * sys.privilege_codes */
		stmt->Dbc->has_comment ? "sys.privilege_codes as pc" :
		     "(values (1, 'SELECT'), "
			     "(2, 'UPDATE'), "
			     "(4, 'INSERT'), "
			     "(8, 'DELETE'), "
			     "(16, 'EXECUTE'), "
			     "(32, 'GRANT')) as pc(privilege_code_id, privilege_code_name)");
	assert(strlen(query) < 1100);
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	if (NameLength1 > 0 && CatalogName != NULL) {
		/* filtering requested on catalog name */
		if (strcmp((char *) CatalogName, stmt->Dbc->dbname) != 0) {
			/* catalog name does not match the database name, so return no rows */
			sprintf(query_end, " and 1=2");
			query_end += strlen(query_end);
		}
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
	if (col) {
		/* filtering requested on column name */
		sprintf(query_end, " and %s", col);
		query_end += strlen(query_end);
		free(col);
	}

	/* add the ordering (exclude table_cat as it is the same for all rows) */
	strcpy(query_end, " order by table_schem, table_name, column_name, privilege");
	query_end += strlen(query_end);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query, (SQLINTEGER) (query_end - query));

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
