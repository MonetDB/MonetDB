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
 * SQLForeignKeys()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: catalogs are not supported, we ignore any value set for
 * PKCatalogName and FKCatalogName.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
SQLForeignKeys_(ODBCStmt *stmt,
		SQLCHAR *PKCatalogName,
		SQLSMALLINT NameLength1,
		SQLCHAR *PKSchemaName,
		SQLSMALLINT NameLength2,
		SQLCHAR *PKTableName,
		SQLSMALLINT NameLength3,
		SQLCHAR *FKCatalogName,
		SQLSMALLINT NameLength4,
		SQLCHAR *FKSchemaName,
		SQLSMALLINT NameLength5,
		SQLCHAR *FKTableName,
		SQLSMALLINT NameLength6)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;	/* pointer to end of built-up query */

	/* deal with SQL_NTS and SQL_NULL_DATA */
	fixODBCstring(PKCatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(PKSchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(PKTableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(FKCatalogName, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(FKSchemaName, NameLength5, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(FKTableName, NameLength6, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODCBDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, PKCatalogName,
		(int) NameLength2, PKSchemaName,
		(int) NameLength3, PKTableName,
		(int) NameLength4, FKCatalogName,
		(int) NameLength5, FKSchemaName,
		(int) NameLength6, FKTableName);
#endif
	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	/* first create a string buffer (1200 extra bytes is plenty:
	   we actually need just over 1000) */
	query = (char *) malloc(1200 + NameLength2 + NameLength3 + NameLength5 + NameLength6);
	assert(query);
	query_end = query;

	/* SQLForeignKeys returns a table with the following columns:
	   VARCHAR      pktable_cat
	   VARCHAR      pktable_schem
	   VARCHAR      pktable_name NOT NULL
	   VARCHAR      pkcolumn_name NOT NULL
	   VARCHAR      fktable_cat
	   VARCHAR      fktable_schem
	   VARCHAR      fktable_name NOT NULL
	   VARCHAR      fkcolumn_name NOT NULL
	   SMALLINT     key_seq NOT NULL
	   SMALLINT     update_rule
	   SMALLINT     delete_rule
	   VARCHAR      fk_name
	   VARCHAR      pk_name
	   SMALLINT     deferrability
	 */

	sprintf(query_end,
		"select "
		"cast(null as varchar(1)) as pktable_cat, "
		"pks.\"name\" as pktable_schem, "
		"pkt.\"name\" as pktable_name, "
		"pkkc.\"name\" as pkcolumn_name, "
		"cast(null as varchar(1)) as fktable_cat, "
		"fks.\"name\" as fktable_schem, "
		"fkt.\"name\" as fktable_name, "
		"fkkc.\"name\" as fkcolumn_name, "
		"cast(fkkc.\"nr\" + 1 as smallint) as key_seq, "
		"cast(%d as smallint) as update_rule, "
		"cast(%d as smallint) as delete_rule, "
		"fkk.\"name\" as fk_name, "
		"pkk.\"name\" as pk_name, "
		"cast(%d as smallint) as deferrability "
		"from sys.\"schemas\" fks, sys.\"tables\" fkt, "
		"sys.\"objects\" fkkc, sys.\"keys\" as fkk, "
		"sys.\"schemas\" pks, sys.\"tables\" pkt, "
		"sys.\"objects\" pkkc, sys.\"keys\" as pkk "
		"where fkt.\"id\" = fkk.\"table_id\" and "
		"pkt.\"id\" = pkk.\"table_id\" and "
		"fkk.\"id\" = fkkc.\"id\" and "
		"pkk.\"id\" = pkkc.\"id\" and "
		"fks.\"id\" = fkt.\"schema_id\" and "
		"pks.\"id\" = pkt.\"schema_id\" and "
		"fkk.\"rkey\" = pkk.\"id\" and "
		"fkkc.\"nr\" = pkkc.\"nr\"",
		SQL_NO_ACTION, SQL_NO_ACTION, SQL_NOT_DEFERRABLE);
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	if (PKSchemaName != NULL && NameLength2 > 0) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and pks.\"name\" = '%.*s'",
			NameLength2, (char*)PKSchemaName);
		query_end += strlen(query_end);
	}

	if (PKTableName != NULL && NameLength3 > 0) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and pkt.\"name\" = '%.*s'",
			NameLength3, (char*)PKTableName);
		query_end += strlen(query_end);
	}

	if (FKSchemaName != NULL && NameLength5 > 0) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and fks.\"name\" = '%.*s'",
			NameLength5, (char*)FKSchemaName);
		query_end += strlen(query_end);
	}

	if (FKTableName != NULL && NameLength6 > 0) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and fkt.\"name\" = '%.*s'",
			NameLength6, (char*)FKTableName);
		query_end += strlen(query_end);
	}


/* TODO finish the FROM and WHERE clauses */

	/* add the ordering */
	/* if PKTableName != NULL, selection on primary key, order
	   on FK output columns, else order on PK output columns */
	sprintf(query_end, " order by %stable_schem, %stable_name, key_seq",
		PKTableName != NULL ? "fk" : "pk",
		PKTableName != NULL ? "fk" : "pk");
	query_end += strlen(query_end);
	assert(query_end - query < 1200 + NameLength2 + NameLength3 + NameLength5 + NameLength6);

	/* query the MonetDB data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLForeignKeys(SQLHSTMT StatementHandle,
	       SQLCHAR *PKCatalogName,
	       SQLSMALLINT NameLength1,
	       SQLCHAR *PKSchemaName,
	       SQLSMALLINT NameLength2,
	       SQLCHAR *PKTableName,
	       SQLSMALLINT NameLength3,
	       SQLCHAR *FKCatalogName,
	       SQLSMALLINT NameLength4,
	       SQLCHAR *FKSchemaName,
	       SQLSMALLINT NameLength5,
	       SQLCHAR *FKTableName,
	       SQLSMALLINT NameLength6)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLForeignKeys " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLForeignKeys_(stmt, PKCatalogName, NameLength1,
			       PKSchemaName, NameLength2,
			       PKTableName, NameLength3,
			       FKCatalogName, NameLength4,
			       FKSchemaName, NameLength5,
			       FKTableName, NameLength6);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLForeignKeysA(SQLHSTMT StatementHandle,
		SQLCHAR *PKCatalogName,
		SQLSMALLINT NameLength1,
		SQLCHAR *PKSchemaName,
		SQLSMALLINT NameLength2,
		SQLCHAR *PKTableName,
		SQLSMALLINT NameLength3,
		SQLCHAR *FKCatalogName,
		SQLSMALLINT NameLength4,
		SQLCHAR *FKSchemaName,
		SQLSMALLINT NameLength5,
		SQLCHAR *FKTableName,
		SQLSMALLINT NameLength6)
{
	return SQLForeignKeys(StatementHandle, PKCatalogName, NameLength1,
			      PKSchemaName, NameLength2,
			      PKTableName, NameLength3,
			      FKCatalogName, NameLength4,
			      FKSchemaName, NameLength5,
			      FKTableName, NameLength6);
}

SQLRETURN SQL_API
SQLForeignKeysW(SQLHSTMT StatementHandle,
		SQLWCHAR *PKCatalogName,
		SQLSMALLINT NameLength1,
		SQLWCHAR *PKSchemaName,
		SQLSMALLINT NameLength2,
		SQLWCHAR *PKTableName,
		SQLSMALLINT NameLength3,
		SQLWCHAR *FKCatalogName,
		SQLSMALLINT NameLength4,
		SQLWCHAR *FKSchemaName,
		SQLSMALLINT NameLength5,
		SQLWCHAR *FKTableName,
		SQLSMALLINT NameLength6)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLCHAR *PKcatalog = NULL, *PKschema = NULL, *PKtable = NULL;
	SQLCHAR *FKcatalog = NULL, *FKschema = NULL, *FKtable = NULL;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLForeignKeysW " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(PKCatalogName, NameLength1, SQLCHAR,
		   PKcatalog, addStmtError, stmt, goto exit);
	fixWcharIn(PKSchemaName, NameLength2, SQLCHAR,
		   PKschema, addStmtError, stmt, goto exit);
	fixWcharIn(PKTableName, NameLength3, SQLCHAR,
		   PKtable, addStmtError, stmt, goto exit);
	fixWcharIn(FKCatalogName, NameLength4, SQLCHAR,
		   FKcatalog, addStmtError, stmt, goto exit);
	fixWcharIn(FKSchemaName, NameLength5, SQLCHAR,
		   FKschema, addStmtError, stmt, goto exit);
	fixWcharIn(FKTableName, NameLength6, SQLCHAR,
		   FKtable, addStmtError, stmt, goto exit);

	rc = SQLForeignKeys_(stmt, PKcatalog, SQL_NTS, PKschema, SQL_NTS,
			     PKtable, SQL_NTS, FKcatalog, SQL_NTS,
			     FKschema, SQL_NTS, FKtable, SQL_NTS);

      exit:
	if (PKcatalog)
		free(PKcatalog);
	if (PKschema)
		free(PKschema);
	if (PKtable)
		free(PKtable);
	if (FKcatalog)
		free(FKcatalog);
	if (FKschema)
		free(FKschema);
	if (FKtable)
		free(FKtable);

	return rc;
}
#endif /* WITH_WCHAR */
