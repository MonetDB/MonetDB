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
 * SQLProcedures()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not implemented (it only sets an error),
 * because MonetDB SQL frontend does not support stored procedures.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLProcedures_(ODBCStmt *stmt,
	       SQLCHAR *CatalogName,
	       SQLSMALLINT NameLength1,
	       SQLCHAR *SchemaName,
	       SQLSMALLINT NameLength2,
	       SQLCHAR *ProcName,
	       SQLSMALLINT NameLength3)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end;

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ProcName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, (char *) CatalogName,
		(int) NameLength2, (char *) SchemaName,
		(int) NameLength3, (char *) ProcName);
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

	query = (char *) malloc(1000 + NameLength2 + NameLength3);
	assert(query);
	query_end = query;

	snprintf(query_end, 1000,
		 "select cast(null as varchar(1)) as \"procedure_cat\","
		 "       \"s\".\"name\" as \"procedure_schem\","
		 "       \"p\".\"name\" as \"procedure_name\","
		 "       0 as \"num_input_params\","
		 "       0 as \"num_output_params\","
		 "       0 as \"num_result_sets\","
		 "       cast('' as varchar(1)) as \"remarks\","
		 "       cast(case when \"a\".\"name\" is null then %d else %d end as smallint) as \"procedure_type\""
		 "from \"sys\".\"schemas\" as \"s\","
		 "     \"sys\".\"functions\" as \"p\" left outer join \"sys\".\"args\" as \"a\""
		 "     	       on \"p\".\"id\" = \"a\".\"func_id\" and \"a\".\"name\" = 'result'"
		 "where \"p\".\"schema_id\" = \"s\".\"id\" and"
		 "      \"p\".\"sql\" = true",
		 SQL_PT_PROCEDURE, SQL_PT_FUNCTION);
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	if (NameLength1 > 0) {
		/* filtering requested on catalog name */
		/* we do not support catalog names, so ignore it */
	}

	/* Construct the selection condition query part */
	if (stmt->Dbc->sql_attr_metadata_id == SQL_TRUE) {
		/* treat arguments as identifiers */
		/* remove trailing blanks */
		while (NameLength2 > 0 &&
		       isspace((int) SchemaName[NameLength2 - 1]))
			NameLength2--;
		while (NameLength3 > 0 &&
		       isspace((int) ProcName[NameLength3 - 1]))
			NameLength3--;
		if (NameLength2 > 0) {
			sprintf(query_end, " and s.\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength2-- > 0)
				*query_end++ = tolower(*SchemaName++);
			*query_end++ = '\'';
		}
		if (NameLength3 > 0) {
			sprintf(query_end, " and p.\"name\" = '");
			query_end += strlen(query_end);
			while (NameLength3-- > 0)
				*query_end++ = tolower(*ProcName++);
			*query_end++ = '\'';
		}
	} else {
		int escape;
		if (NameLength2 > 0) {
			escape = 0;
			sprintf(query_end, " and s.\"name\" like '");
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
			sprintf(query_end, " and p.\"name\" like '");
			query_end += strlen(query_end);
			while (NameLength3-- > 0) {
				if (*ProcName == '\\') {
					escape = 1;
					*query_end++ = '\\';
				}
				*query_end++ = *ProcName++;
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
	       " order by \"procedure_cat\", \"procedure_schem\", \"procedure_name\"");
	query_end += strlen(query_end);

	/* query the MonetDB data dictionary tables */

	rc = SQLExecDirect_(stmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;
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
	ODBCLOG("SQLProcedures " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLProcedures_(stmt,
			      CatalogName, NameLength1,
			      SchemaName, NameLength2,
			      ProcName, NameLength3);
}

#ifdef WITH_WCHAR
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
	ODBCLOG("SQLProceduresW " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto exit);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto exit);
	fixWcharIn(ProcName, NameLength3, SQLCHAR, proc,
		   addStmtError, stmt, goto exit);

	rc = SQLProcedures_(stmt,
			    catalog, SQL_NTS,
			    schema, SQL_NTS,
			    proc, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (proc)
		free(proc);

	return rc;
}
#endif /* WITH_WCHAR */
