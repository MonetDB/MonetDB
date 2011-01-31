/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * szPKCatalogName and szFKCatalogName.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
SQLForeignKeys_(ODBCStmt *stmt,
		SQLCHAR *szPKCatalogName,
		SQLSMALLINT nPKCatalogNameLength,
		SQLCHAR *szPKSchemaName,
		SQLSMALLINT nPKSchemaNameLength,
		SQLCHAR *szPKTableName,
		SQLSMALLINT nPKTableNameLength,
		SQLCHAR *szFKCatalogName,
		SQLSMALLINT nFKCatalogNameLength,
		SQLCHAR *szFKSchemaName,
		SQLSMALLINT nFKSchemaNameLength,
		SQLCHAR *szFKTableName,
		SQLSMALLINT nFKTableNameLength)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;	/* pointer to end of built-up query */

	/* deal with SQL_NTS and SQL_NULL_DATA */
	fixODBCstring(szPKCatalogName, nPKCatalogNameLength, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szPKSchemaName, nPKSchemaNameLength, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szPKTableName, nPKTableNameLength, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szFKCatalogName, nFKCatalogNameLength, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szFKSchemaName, nFKSchemaNameLength, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szFKTableName, nFKTableNameLength, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODCBDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		nPKCatalogNameLength, szPKCatalogName, nPKSchemaNameLength,
		szPKSchemaName, nPKTableNameLength, szPKTableName,
		nFKCatalogNameLength, szFKCatalogName, nFKSchemaNameLength,
		szFKSchemaName, nFKTableNameLength, szFKTableName);
#endif
	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	/* first create a string buffer (1200 extra bytes is plenty:
	   we actually need just over 1000) */
	query = (char *) malloc(1200 + nPKSchemaNameLength + nPKTableNameLength + nFKSchemaNameLength + nFKTableNameLength);
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
	if (szPKSchemaName != NULL && nPKSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and pks.\"name\" = '%.*s'",
			nPKSchemaNameLength, (char*)szPKSchemaName);
		query_end += strlen(query_end);
	}

	if (szPKTableName != NULL && nPKTableNameLength > 0) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and pkt.\"name\" = '%.*s'",
			nPKTableNameLength, (char*)szPKTableName);
		query_end += strlen(query_end);
	}

	if (szFKSchemaName != NULL && nFKSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and fks.\"name\" = '%.*s'",
			nFKSchemaNameLength, (char*)szFKSchemaName);
		query_end += strlen(query_end);
	}

	if (szFKTableName != NULL && nFKTableNameLength > 0) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and fkt.\"name\" = '%.*s'",
			nFKTableNameLength, (char*)szFKTableName);
		query_end += strlen(query_end);
	}


/* TODO finish the FROM and WHERE clauses */

	/* add the ordering */
	/* if szPKTableName != NULL, selection on primary key, order
	   on FK output columns, else order on PK output columns */
	sprintf(query_end, " order by %stable_schem, %stable_name, key_seq",
		szPKTableName != NULL ? "fk" : "pk",
		szPKTableName != NULL ? "fk" : "pk");
	query_end += strlen(query_end);
	assert(query_end - query < 1200 + nPKSchemaNameLength + nPKTableNameLength + nFKSchemaNameLength + nFKTableNameLength);

	/* query the MonetDB data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLForeignKeys(SQLHSTMT hStmt,
	       SQLCHAR *szPKCatalogName,
	       SQLSMALLINT nPKCatalogNameLength,
	       SQLCHAR *szPKSchemaName,
	       SQLSMALLINT nPKSchemaNameLength,
	       SQLCHAR *szPKTableName,
	       SQLSMALLINT nPKTableNameLength,
	       SQLCHAR *szFKCatalogName,
	       SQLSMALLINT nFKCatalogNameLength,
	       SQLCHAR *szFKSchemaName,
	       SQLSMALLINT nFKSchemaNameLength,
	       SQLCHAR *szFKTableName,
	       SQLSMALLINT nFKTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLForeignKeys " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLForeignKeys_(stmt, szPKCatalogName, nPKCatalogNameLength,
			       szPKSchemaName, nPKSchemaNameLength,
			       szPKTableName, nPKTableNameLength,
			       szFKCatalogName, nFKCatalogNameLength,
			       szFKSchemaName, nFKSchemaNameLength,
			       szFKTableName, nFKTableNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLForeignKeysA(SQLHSTMT hStmt,
		SQLCHAR *szPKCatalogName,
		SQLSMALLINT nPKCatalogNameLength,
		SQLCHAR *szPKSchemaName,
		SQLSMALLINT nPKSchemaNameLength,
		SQLCHAR *szPKTableName,
		SQLSMALLINT nPKTableNameLength,
		SQLCHAR *szFKCatalogName,
		SQLSMALLINT nFKCatalogNameLength,
		SQLCHAR *szFKSchemaName,
		SQLSMALLINT nFKSchemaNameLength,
		SQLCHAR *szFKTableName,
		SQLSMALLINT nFKTableNameLength)
{
	return SQLForeignKeys(hStmt, szPKCatalogName, nPKCatalogNameLength,
			      szPKSchemaName, nPKSchemaNameLength,
			      szPKTableName, nPKTableNameLength,
			      szFKCatalogName, nFKCatalogNameLength,
			      szFKSchemaName, nFKSchemaNameLength,
			      szFKTableName, nFKTableNameLength);
}

SQLRETURN SQL_API
SQLForeignKeysW(SQLHSTMT hStmt,
		SQLWCHAR *szPKCatalogName,
		SQLSMALLINT nPKCatalogNameLength,
		SQLWCHAR *szPKSchemaName,
		SQLSMALLINT nPKSchemaNameLength,
		SQLWCHAR *szPKTableName,
		SQLSMALLINT nPKTableNameLength,
		SQLWCHAR *szFKCatalogName,
		SQLSMALLINT nFKCatalogNameLength,
		SQLWCHAR *szFKSchemaName,
		SQLSMALLINT nFKSchemaNameLength,
		SQLWCHAR *szFKTableName,
		SQLSMALLINT nFKTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLCHAR *PKcatalog = NULL, *PKschema = NULL, *PKtable = NULL;
	SQLCHAR *FKcatalog = NULL, *FKschema = NULL, *FKtable = NULL;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLForeignKeysW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szPKCatalogName, nPKCatalogNameLength, SQLCHAR,
		   PKcatalog, addStmtError, stmt, goto exit);
	fixWcharIn(szPKSchemaName, nPKSchemaNameLength, SQLCHAR,
		   PKschema, addStmtError, stmt, goto exit);
	fixWcharIn(szPKTableName, nPKTableNameLength, SQLCHAR,
		   PKtable, addStmtError, stmt, goto exit);
	fixWcharIn(szFKCatalogName, nFKCatalogNameLength, SQLCHAR,
		   FKcatalog, addStmtError, stmt, goto exit);
	fixWcharIn(szFKSchemaName, nFKSchemaNameLength, SQLCHAR,
		   FKschema, addStmtError, stmt, goto exit);
	fixWcharIn(szFKTableName, nFKTableNameLength, SQLCHAR,
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
