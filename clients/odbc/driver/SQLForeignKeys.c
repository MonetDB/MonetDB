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
 * SQLForeignKeys()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
MNDBForeignKeys(ODBCStmt *stmt,
		const SQLCHAR *PKCatalogName,
		SQLSMALLINT NameLength1,
		const SQLCHAR *PKSchemaName,
		SQLSMALLINT NameLength2,
		const SQLCHAR *PKTableName,
		SQLSMALLINT NameLength3,
		const SQLCHAR *FKCatalogName,
		SQLSMALLINT NameLength4,
		const SQLCHAR *FKSchemaName,
		SQLSMALLINT NameLength5,
		const SQLCHAR *FKTableName,
		SQLSMALLINT NameLength6)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *psch = NULL, *ptab = NULL;
	char *fsch = NULL, *ftab = NULL;

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
		(int) NameLength1, PKCatalogName ? (char *) PKCatalogName : "",
		(int) NameLength2, PKSchemaName ? (char *) PKSchemaName : "",
		(int) NameLength3, PKTableName ? (char *) PKTableName : "",
		(int) NameLength4, FKCatalogName ? (char *) FKCatalogName : "",
		(int) NameLength5, FKSchemaName ? (char *) FKSchemaName : "",
		(int) NameLength6, FKTableName ? (char *) FKTableName : "");
#endif
	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
		if (NameLength2 > 0) {
			psch = ODBCParseOA("pks", "name",
					   (const char *) PKSchemaName,
					   (size_t) NameLength2);
			if (psch == NULL)
				goto nomem;
		}
		if (NameLength3 > 0) {
			ptab = ODBCParseOA("pkt", "name",
					   (const char *) PKTableName,
					   (size_t) NameLength3);
			if (ptab == NULL)
				goto nomem;
		}
		if (NameLength5 > 0) {
			fsch = ODBCParseOA("fks", "name",
					   (const char *) FKSchemaName,
					   (size_t) NameLength5);
			if (fsch == NULL)
				goto nomem;
		}
		if (NameLength6 > 0) {
			ftab = ODBCParseOA("fkt", "name",
					   (const char *) FKTableName,
					   (size_t) NameLength6);
			if (ftab == NULL)
				goto nomem;
		}
	} else {
		if (NameLength2 > 0) {
			psch = ODBCParseID("pks", "name",
					   (const char *) PKSchemaName,
					   (size_t) NameLength2);
			if (psch == NULL)
				goto nomem;
		}
		if (NameLength3 > 0) {
			ptab = ODBCParseID("pkt", "name",
					   (const char *) PKTableName,
					   (size_t) NameLength3);
			if (ptab == NULL)
				goto nomem;
		}
		if (NameLength5 > 0) {
			fsch = ODBCParseID("fks", "name",
					   (const char *) FKSchemaName,
					   (size_t) NameLength5);
			if (fsch == NULL)
				goto nomem;
		}
		if (NameLength6 > 0) {
			ftab = ODBCParseID("fkt", "name",
					   (const char *) FKTableName,
					   (size_t) NameLength6);
			if (ftab == NULL)
				goto nomem;
		}
	}

	/* first create a string buffer (1200 extra bytes is plenty:
	   we actually need just over 1000) */
	querylen = 1200 + (2 * strlen(stmt->Dbc->dbname)) +
		(psch ? strlen(psch) : 0) + (ptab ? strlen(ptab) : 0) +
		(fsch ? strlen(fsch) : 0) + (ftab ? strlen(ftab) : 0);
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

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

	pos += snprintf(query + pos, querylen - pos,
		"select '%s' as pktable_cat, "
		       "pks.name as pktable_schem, "
		       "pkt.name as pktable_name, "
		       "pkkc.name as pkcolumn_name, "
		       "'%s' as fktable_cat, "
		       "fks.name as fktable_schem, "
		       "fkt.name as fktable_name, "
		       "fkkc.name as fkcolumn_name, "
		       "cast(fkkc.nr + 1 as smallint) as key_seq, "
		       "cast(%d as smallint) as update_rule, "
		       "cast(%d as smallint) as delete_rule, "
		       "fkk.name as fk_name, "
		       "pkk.name as pk_name, "
		       "cast(%d as smallint) as deferrability "
		"from sys.schemas fks, sys.tables fkt, "
		     "sys.objects fkkc, sys.keys as fkk, "
		     "sys.schemas pks, sys.tables pkt, "
		     "sys.objects pkkc, sys.keys as pkk "
		"where fkt.id = fkk.table_id and "
		      "pkt.id = pkk.table_id and "
		      "fkk.id = fkkc.id and "
		      "pkk.id = pkkc.id and "
		      "fks.id = fkt.schema_id and "
		      "pks.id = pkt.schema_id and "
		      "fkk.rkey = pkk.id and "
		      "fkkc.nr = pkkc.nr",
		stmt->Dbc->dbname,
		stmt->Dbc->dbname,
		SQL_NO_ACTION, SQL_NO_ACTION, SQL_NOT_DEFERRABLE);
	assert(pos < 1100);

	/* Construct the selection condition query part */
	if (NameLength1 > 0 && PKCatalogName != NULL) {
		/* filtering requested on catalog name */
		if (strcmp((char *) PKCatalogName, stmt->Dbc->dbname) != 0) {
			/* catalog name does not match the database name, so return no rows */
			pos += snprintf(query + pos, querylen - pos, " and 1=2");
		}
	}
	if (psch) {
		/* filtering requested on schema name */
		pos += snprintf(query + pos, querylen - pos, " and %s", psch);
		free(psch);
	}
	if (ptab) {
		/* filtering requested on table name */
		pos += snprintf(query + pos, querylen - pos, " and %s", ptab);
		free(ptab);
	}
	if (NameLength4 > 0 && FKCatalogName != NULL) {
		/* filtering requested on catalog name */
		if (strcmp((char *) FKCatalogName, stmt->Dbc->dbname) != 0) {
			/* catalog name does not match the database name, so return no rows */
			pos += snprintf(query + pos, querylen - pos, " and 1=2");
		}
	}
	if (fsch) {
		/* filtering requested on schema name */
		pos += snprintf(query + pos, querylen - pos, " and %s", fsch);
		free(fsch);
	}
	if (ftab) {
		/* filtering requested on table name */
		pos += snprintf(query + pos, querylen - pos, " and %s", ftab);
		free(ftab);
	}

/* TODO finish the FROM and WHERE clauses */

	/* add the ordering */
	/* if PKTableName != NULL, selection on primary key, order
	   on FK output columns, else order on PK output columns */
	pos += snprintf(query + pos, querylen - pos,
			" order by %stable_schem, %stable_name, key_seq",
			PKTableName != NULL ? "fk" : "pk",
			PKTableName != NULL ? "fk" : "pk");

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query, (SQLINTEGER) pos);

	free(query);

	return rc;

  nomem:
	if (psch)
		free(psch);
	if (ptab)
		free(ptab);
	if (fsch)
		free(fsch);
	if (ftab)
		free(ftab);
	if (query)
		free(query);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
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
	ODBCLOG("SQLForeignKeys %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBForeignKeys(stmt, PKCatalogName, NameLength1,
			       PKSchemaName, NameLength2,
			       PKTableName, NameLength3,
			       FKCatalogName, NameLength4,
			       FKSchemaName, NameLength5,
			       FKTableName, NameLength6);
}

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
	ODBCLOG("SQLForeignKeysW %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(PKCatalogName, NameLength1, SQLCHAR,
		   PKcatalog, addStmtError, stmt, goto bailout);
	fixWcharIn(PKSchemaName, NameLength2, SQLCHAR,
		   PKschema, addStmtError, stmt, goto bailout);
	fixWcharIn(PKTableName, NameLength3, SQLCHAR,
		   PKtable, addStmtError, stmt, goto bailout);
	fixWcharIn(FKCatalogName, NameLength4, SQLCHAR,
		   FKcatalog, addStmtError, stmt, goto bailout);
	fixWcharIn(FKSchemaName, NameLength5, SQLCHAR,
		   FKschema, addStmtError, stmt, goto bailout);
	fixWcharIn(FKTableName, NameLength6, SQLCHAR,
		   FKtable, addStmtError, stmt, goto bailout);

	rc = MNDBForeignKeys(stmt, PKcatalog, SQL_NTS, PKschema, SQL_NTS,
			     PKtable, SQL_NTS, FKcatalog, SQL_NTS,
			     FKschema, SQL_NTS, FKtable, SQL_NTS);

      bailout:
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
