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
 * SQLProcedures()
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
MNDBProcedures(ODBCStmt *stmt,
	       const SQLCHAR *CatalogName,
	       SQLSMALLINT NameLength1,
	       const SQLCHAR *SchemaName,
	       SQLSMALLINT NameLength2,
	       const SQLCHAR *ProcName,
	       SQLSMALLINT NameLength3)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *sch = NULL, *pro = NULL;

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ProcName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, ProcName ? (char *) ProcName : "");
#endif

	/* SQLProcedures returns a table with the following columns:
	   VARCHAR      procedure_cat
	   VARCHAR      procedure_schem
	   VARCHAR      procedure_name NOT NULL
	   n/a          num_input_params (reserved for future use)
	   n/a          num_output_params (reserved for future use)
	   n/a          num_result_sets (reserved for future use)
	   VARCHAR      remarks
	   SMALLINT     procedure_type
	 */

	if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
		if (NameLength2 > 0) {
			sch = ODBCParsePV("s", "name",
					  (const char *) SchemaName,
					  (size_t) NameLength2);
			if (sch == NULL)
				goto nomem;
		}
		if (NameLength3 > 0) {
			pro = ODBCParsePV("p", "name",
					  (const char *) ProcName,
					  (size_t) NameLength3);
			if (pro == NULL)
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
			pro = ODBCParseID("p", "name",
					  (const char *) ProcName,
					  (size_t) NameLength3);
			if (pro == NULL)
				goto nomem;
		}
	}

	querylen = 1000 + strlen(stmt->Dbc->dbname) +
		(sch ? strlen(sch) : 0) + (pro ? strlen(pro) : 0);
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

/* see sql_catalog.h */
#define F_FUNC 1
#define F_PROC 2
#define F_UNION 5
#define FUNC_LANG_SQL 2
	pos += snprintf(query + pos, querylen - pos,
		 "select '%s' as procedure_cat, "
			"s.name as procedure_schem, "
			"p.name as procedure_name, "
			"0 as num_input_params, "
			"0 as num_output_params, "
			"0 as num_result_sets, "
			"%s as remarks, "
			"cast(case when p.type = %d then %d else %d end as smallint) as procedure_type "
		 "from sys.schemas as s, "
		      "sys.functions as p%s "
		 "where p.schema_id = s.id and "
		       "p.language >= %d and "
		       "p.type in (%d, %d, %d)",
		 stmt->Dbc->dbname,
		 stmt->Dbc->has_comment ? "c.remark" : "cast(null as varchar(1))",
		 F_PROC, SQL_PT_PROCEDURE, SQL_PT_FUNCTION,
		 stmt->Dbc->has_comment ? " left outer join sys.comments c on p.id = c.id" : "",
		 FUNC_LANG_SQL, F_FUNC, F_PROC, F_UNION);
	assert(pos < 800);

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
	if (pro) {
		/* filtering requested on procedure name */
		pos += snprintf(query + pos, querylen - pos, " and %s", pro);
		free(pro);
	}

	/* add the ordering (exclude procedure_cat as it is the same for all rows) */
	pos += strcpy_len(query + pos, " order by procedure_schem, procedure_name", querylen - pos);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query, (SQLINTEGER) pos);

	free(query);

	return rc;

  nomem:
	/* note that query must be NULL when we get here */
	if (sch)
		free(sch);
	if (pro)
		free(pro);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLProcedures(SQLHSTMT StatementHandle,
	      SQLCHAR *CatalogName,
	      SQLSMALLINT NameLength1,
	      SQLCHAR *SchemaName,
	      SQLSMALLINT NameLength2,
	      SQLCHAR *ProcName,
	      SQLSMALLINT NameLength3)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedures %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBProcedures(stmt,
			      CatalogName, NameLength1,
			      SchemaName, NameLength2,
			      ProcName, NameLength3);
}

SQLRETURN SQL_API
SQLProceduresA(SQLHSTMT StatementHandle,
	       SQLCHAR *CatalogName,
	       SQLSMALLINT NameLength1,
	       SQLCHAR *SchemaName,
	       SQLSMALLINT NameLength2,
	       SQLCHAR *ProcName,
	       SQLSMALLINT NameLength3)
{
	return SQLProcedures(StatementHandle,
			     CatalogName, NameLength1,
			     SchemaName, NameLength2,
			     ProcName, NameLength3);
}

SQLRETURN SQL_API
SQLProceduresW(SQLHSTMT StatementHandle,
	       SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
	       SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
	       SQLWCHAR *ProcName, SQLSMALLINT NameLength3)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *proc = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProceduresW %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(ProcName, NameLength3, SQLCHAR, proc,
		   addStmtError, stmt, goto bailout);

	rc = MNDBProcedures(stmt,
			    catalog, SQL_NTS,
			    schema, SQL_NTS,
			    proc, SQL_NTS);

      bailout:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (proc)
		free(proc);

	return rc;
}
