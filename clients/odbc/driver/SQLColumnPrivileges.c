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
SQLColumnPrivileges_(ODBCStmt *stmt,
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

	/* construct the query now */
	query = malloc(1200 + NameLength2 + NameLength3 + NameLength4);
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
		"select cast(null as varchar(128)) as \"table_cat\","
		" \"s\".\"name\" as \"table_schem\","
		" \"t\".\"name\" as \"table_name\","
		" \"c\".\"name\" as \"column_name\","
		" case \"a\".\"id\""
		"      when \"s\".\"owner\" then '_SYSTEM'"
		"      else \"g\".\"name\""
		"      end as \"grantor\","
		" case \"a\".\"name\""
		"      when 'public' then 'PUBLIC'"
		"      else \"a\".\"name\""
		"      end as \"grantee\","
		" case \"p\".\"privileges\""
		"      when 1 then 'SELECT'"
		"      when 2 then 'UPDATE'"
		"      when 4 then 'INSERT'"
		"      when 8 then 'DELETE'"
		"      when 16 then 'EXECUTE'"
		"      when 32 then 'GRANT'"
		"      end as \"privilege\","
		" case \"p\".\"grantable\""
		"      when 1 then 'YES'"
		"      when 0 then 'NO'"
		"      end as \"is_grantable\" "
		"from \"sys\".\"schemas\" \"s\","
		" \"sys\".\"_tables\" \"t\","
		" \"sys\".\"_columns\" \"c\","
		" \"sys\".\"auths\" \"a\","
		" \"sys\".\"privileges\" \"p\","
		" \"sys\".\"auths\" \"g\" "
		"where \"p\".\"obj_id\" = \"c\".\"id\" and"
		" \"c\".\"table_id\" = \"t\".\"id\" and"
		" \"p\".\"auth_id\" = \"a\".\"id\" and"
		" \"t\".\"schema_id\" = \"s\".\"id\" and"
		" \"t\".\"system\" = false and"
		" \"p\".\"grantor\" = \"g\".\"id\"");
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	if (stmt->Dbc->sql_attr_metadata_id == SQL_TRUE) {
		/* treat arguments as identifiers */
		/* remove trailing blanks */
		while (NameLength2 > 0 &&
		       isspace((int) SchemaName[NameLength2 - 1]))
			NameLength2--;
		while (NameLength3 > 0 &&
		       isspace((int) TableName[NameLength3 - 1]))
			NameLength3--;
		while (NameLength4 > 0 &&
		       isspace((int) ColumnName[NameLength4 - 1]))
			NameLength4--;
		if (NameLength2 > 0) {
			sprintf(query_end, " and \"s\".\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength2-- > 0)
				*query_end++ = tolower(*SchemaName++);
			*query_end++ = '\'';
		}
		if (NameLength3 > 0) {
			sprintf(query_end, " and \"t\".\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength3-- > 0)
				*query_end++ = tolower(*TableName++);
			*query_end++ = '\'';
		}
		if (NameLength4 > 0) {
			sprintf(query_end, " and \"c\".\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength4-- > 0)
				*query_end++ = tolower(*ColumnName++);
			*query_end++ = '\'';
		}
	} else {
		int escape;
		if (NameLength2 > 0) {
			escape = 0;
			sprintf(query_end, " and \"s\".\"name\" like '");
			query_end += strlen(query_end);
			while (NameLength2-- > 0) {
				if (*SchemaName == '\\') {
					escape = 1;
					*query_end++ = '\\';
				}
				*query_end++ = *SchemaName++;
			}
			*query_end++ = '\'';
			if (escape) {
				sprintf(query_end, " escape '\\\\'");
				query_end += strlen(query_end);
			}
		}
		if (NameLength3 > 0) {
			escape = 0;
			sprintf(query_end, " and \"t\".\"name\" like '");
			query_end += strlen(query_end);
			while (NameLength3-- > 0) {
				if (*TableName == '\\') {
					escape = 1;
					*query_end++ = '\\';
				}
				*query_end++ = *TableName++;
			}
			*query_end++ = '\'';
			if (escape) {
				sprintf(query_end, " escape '\\\\'");
				query_end += strlen(query_end);
			}
		}
		if (NameLength4 > 0) {
			escape = 0;
			sprintf(query_end, " and \"t\".\"name\" like '");
			query_end += strlen(query_end);
			while (NameLength4-- > 0) {
				if (*ColumnName == '\\') {
					escape = 1;
					*query_end++ = '\\';
				}
				*query_end++ = *ColumnName++;
			}
			*query_end++ = '\'';
			if (escape) {
				sprintf(query_end, " escape '\\\\'");
				query_end += strlen(query_end);
			}
		}
	}

	/* add the ordering */
	strcpy(query_end,
	       " order by \"table_cat\", \"table_schem\", \"table_name\", \"column_name\", \"privilege\"");
	query_end += strlen(query_end);
	assert((int) (query_end - query) < 1200 + NameLength2 + NameLength3 + NameLength4);

	/* query the MonetDB data dictionary tables */
        rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
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

	return SQLColumnPrivileges_(stmt,
				    CatalogName, NameLength1,
				    SchemaName, NameLength2,
				    TableName, NameLength3,
				    ColumnName, NameLength4);
}

#ifdef WITH_WCHAR
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

	rc = SQLColumnPrivileges_(stmt,
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
#endif /* WITH_WCHAR */
