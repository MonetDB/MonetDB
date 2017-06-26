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
		     SQLCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLCHAR *TableName,
		     SQLSMALLINT NameLength3,
		     SQLCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	RETCODE rc;
	char *query = NULL;
	char *query_end = NULL;
	char *cat = NULL, *sch = NULL, *tab = NULL, *col = NULL;

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT
		      , addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ColumnName, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, (char *) CatalogName,
		(int) NameLength2, (char *) SchemaName,
		(int) NameLength3, (char *) TableName,
		(int) NameLength4, (char *) ColumnName);
#endif

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
		if (NameLength4 > 0) {
			col = ODBCParsePV("c", "name",
					  (const char *) ColumnName,
					  (size_t) NameLength4);
			if (col == NULL)
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
		if (NameLength4 > 0) {
			col = ODBCParseID("c", "name",
					  (const char *) ColumnName,
					  (size_t) NameLength4);
			if (col == NULL)
				goto nomem;
		}
	}

	/* construct the query now */
	query = malloc(1200 + (cat ? strlen(cat) : 0) +
		       (sch ? strlen(sch) : 0) + (tab ? strlen(tab) : 0) +
		       (col ? strlen(col) : 0));
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

	strcpy(query_end,
	       "select e.value as table_cat, "
		      "s.name as table_schem, "
		      "t.name as table_name, "
		      "c.name as column_name, "
		      "case a.id "
			   "when s.owner "
			   "then '_SYSTEM' "
			   "else g.name "
			   "end as grantor, "
		      "case a.name "
			   "when 'public' "
			   "then 'PUBLIC' "
			   "else a.name "
			   "end as grantee, "
		      "pc.privilege_code_name as privilege, "
		      "case p.grantable "
			   "when 1 "
			   "then 'YES' "
			   "when 0 "
			   "then 'NO' "
			   "end as is_grantable "
	       "from sys.schemas as s, "
		    "sys._tables as t, "
		    "sys._columns as c, "
		    "sys.auths as a, "
		    "sys.privileges as p, "
		    "sys.auths as g, "
		    "sys.env() as e, "
	       /* this can eventually be replaced by
		* sys.privilege_codes as pc
		* see 51_sys_schema_extensionl.sql */
		    "(values (1, 'SELECT'), "
			    "(2, 'UPDATE'), "
			    "(4, 'INSERT'), "
			    "(8, 'DELETE'), "
			    "(16, 'EXECUTE'), "
			    "(32, 'GRANT')) as pc(privilege_code_id, privilege_code_name) "
	       "where p.obj_id = c.id and "
		     "c.table_id = t.id and "
		     "p.auth_id = a.id and "
		     "t.schema_id = s.id and "
		     "t.system = false and "
		     "p.grantor = g.id and "
		     "e.name = 'gdk_dbname' and "
		     "p.privileges = pc.privilege_code_id");
	assert(strlen(query) < 1100);
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
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
	if (col) {
		/* filtering requested on column name */
		sprintf(query_end, " and %s", col);
		query_end += strlen(query_end);
		free(col);
	}

	/* add the ordering */
	strcpy(query_end,
	       " order by table_cat, table_schem, table_name, column_name, privilege");
	query_end += strlen(query_end);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query,
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
	ODBCLOG("SQLColumnPrivileges " PTRFMT, PTRFMTCAST StatementHandle);
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
	ODBCLOG("SQLColumnPrivilegesW " PTRFMT, PTRFMTCAST StatementHandle);
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
	fixWcharIn(ColumnName, NameLength4, SQLCHAR, column,
		   addStmtError, stmt, goto exit);

	rc = MNDBColumnPrivileges(stmt,
				  catalog, SQL_NTS,
				  schema, SQL_NTS,
				  table, SQL_NTS,
				  column, SQL_NTS);

      exit:
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
