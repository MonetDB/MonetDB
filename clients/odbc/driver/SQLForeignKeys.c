/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
	char *pcat = NULL, *psch = NULL, *ptab = NULL;
	char *fcat = NULL, *fsch = NULL, *ftab = NULL;

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

	if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
		if (NameLength1 > 0) {
			pcat = ODBCParseOA("e", "value",
					   (const char *) PKCatalogName,
					   (size_t) NameLength1);
			if (pcat == NULL)
				goto nomem;
		}
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
		if (NameLength4 > 0) {
			fcat = ODBCParseOA("e", "value",
					   (const char *) FKCatalogName,
					   (size_t) NameLength4);
			if (fcat == NULL)
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
		if (NameLength1 > 0) {
			pcat = ODBCParseID("e", "value",
					   (const char *) PKCatalogName,
					   (size_t) NameLength1);
			if (pcat == NULL)
				goto nomem;
		}
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
		if (NameLength4 > 0) {
			fcat = ODBCParseID("e", "value",
					   (const char *) FKCatalogName,
					   (size_t) NameLength4);
			if (fcat == NULL)
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
	query = malloc(1200 + (pcat ? strlen(pcat) : 0) +
		       (psch ? strlen(psch) : 0) + (ptab ? strlen(ptab) : 0) +
		       (fcat ? strlen(fcat) : 0) + (fsch ? strlen(fsch) : 0) +
		       (ftab ? strlen(ftab) : 0));
	if (query == NULL)
		goto nomem;
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
		"select e.value as pktable_cat, "
		       "pks.name as pktable_schem, "
		       "pkt.name as pktable_name, "
		       "pkkc.name as pkcolumn_name, "
		       "e.value as fktable_cat, "
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
		     "sys.objects pkkc, sys.keys as pkk, "
		     "sys.env() e "
		"where fkt.id = fkk.table_id and "
		      "pkt.id = pkk.table_id and "
		      "fkk.id = fkkc.id and "
		      "pkk.id = pkkc.id and "
		      "fks.id = fkt.schema_id and "
		      "pks.id = pkt.schema_id and "
		      "fkk.rkey = pkk.id and "
		      "fkkc.nr = pkkc.nr and "
		      "e.name = 'gdk_dbname'",
		SQL_NO_ACTION, SQL_NO_ACTION, SQL_NOT_DEFERRABLE);
	assert(strlen(query) < 1100);
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	if (pcat) {
		/* filtering requested on catalog name */
		sprintf(query_end, " and %s", pcat);
		query_end += strlen(query_end);
		free(pcat);
	}
	if (psch) {
		/* filtering requested on schema name */
		sprintf(query_end, " and %s", psch);
		query_end += strlen(query_end);
		free(psch);
	}
	if (ptab) {
		/* filtering requested on table name */
		sprintf(query_end, " and %s", ptab);
		query_end += strlen(query_end);
		free(ptab);
	}
	if (fcat) {
		/* filtering requested on catalog name */
		sprintf(query_end, " and %s", fcat);
		query_end += strlen(query_end);
		free(fcat);
	}
	if (fsch) {
		/* filtering requested on schema name */
		sprintf(query_end, " and %s", fsch);
		query_end += strlen(query_end);
		free(fsch);
	}
	if (ftab) {
		/* filtering requested on table name */
		sprintf(query_end, " and %s", ftab);
		query_end += strlen(query_end);
		free(ftab);
	}

/* TODO finish the FROM and WHERE clauses */

	/* add the ordering */
	/* if PKTableName != NULL, selection on primary key, order
	   on FK output columns, else order on PK output columns */
	sprintf(query_end, " order by %stable_schem, %stable_name, key_seq",
		PKTableName != NULL ? "fk" : "pk",
		PKTableName != NULL ? "fk" : "pk");
	query_end += strlen(query_end);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;

  nomem:
	if (pcat)
		free(pcat);
	if (psch)
		free(psch);
	if (ptab)
		free(ptab);
	if (fcat)
		free(fcat);
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
	ODBCLOG("SQLForeignKeys " PTRFMT " ", PTRFMTCAST StatementHandle);
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

	rc = MNDBForeignKeys(stmt, PKcatalog, SQL_NTS, PKschema, SQL_NTS,
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
