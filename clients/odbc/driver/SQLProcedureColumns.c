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
 * SQLProcedureColumns()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not implemented (it only sets an error),
 * because MonetDB SQL frontend does not support stored procedures.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLProcedureColumns_(ODBCStmt *stmt,
		     SQLCHAR *szCatalogName,
		     SQLSMALLINT nCatalogNameLength,
		     SQLCHAR *szSchemaName,
		     SQLSMALLINT nSchemaNameLength,
		     SQLCHAR *szProcName,
		     SQLSMALLINT nProcNameLength,
		     SQLCHAR *szColumnName,
		     SQLSMALLINT nColumnNameLength)
{
	fixODBCstring(szCatalogName, nCatalogNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szSchemaName, nSchemaNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szProcName, nProcNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szColumnName, nColumnNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) nCatalogNameLength, (char *) szCatalogName,
		(int) nSchemaNameLength, (char *) szSchemaName,
		(int) nProcNameLength, (char *) szProcName,
		(int) nColumnNameLength, (char *) szColumnName);
#endif

	/* SQLProcedureColumns returns a table with the following columns:
	   VARCHAR      procedure_cat
	   VARCHAR      procedure_schem
	   VARCHAR      procedure_name NOT NULL
	   VARCHAR      column_name NOT NULL
	   SMALLINT     column_type NOT NULL
	   SMALLINT     data_type NOT NULL
	   VARCHAR      type_name NOT NULL
	   INTEGER      column_size
	   INTEGER      buffer_length
	   SMALLINT     decimal_digits
	   SMALLINT     num_prec_radix
	   SMALLINT     nullable NOT NULL
	   VARCHAR      remarks
	   VARCHAR      column_def
	   SMALLINT     sql_data_type NOT NULL
	   SMALLINT     sql_datetime_sub
	   INTEGER      char_octet_length
	   INTEGER      ordinal_position NOT NULL
	   VARCHAR      is_nullable
	 */

	/* Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLProcedureColumns(SQLHSTMT hStmt,
		    SQLCHAR *szCatalogName,
		    SQLSMALLINT nCatalogNameLength,
		    SQLCHAR *szSchemaName,
		    SQLSMALLINT nSchemaNameLength,
		    SQLCHAR *szProcName,
		    SQLSMALLINT nProcNameLength,
		    SQLCHAR *szColumnName,
		    SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedureColumns " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLProcedureColumns_(stmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szProcName, nProcNameLength, szColumnName, nColumnNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLProcedureColumnsA(SQLHSTMT hStmt,
		     SQLCHAR *szCatalogName,
		     SQLSMALLINT nCatalogNameLength,
		     SQLCHAR *szSchemaName,
		     SQLSMALLINT nSchemaNameLength,
		     SQLCHAR *szProcName,
		     SQLSMALLINT nProcNameLength,
		     SQLCHAR *szColumnName,
		     SQLSMALLINT nColumnNameLength)
{
	return SQLProcedureColumns(hStmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szProcName, nProcNameLength, szColumnName, nColumnNameLength);
}

SQLRETURN SQL_API
SQLProcedureColumnsW(SQLHSTMT hStmt,
		     SQLWCHAR * szCatalogName,
		     SQLSMALLINT nCatalogNameLength,
		     SQLWCHAR * szSchemaName,
		     SQLSMALLINT nSchemaNameLength,
		     SQLWCHAR * szProcName,
		     SQLSMALLINT nProcNameLength,
		     SQLWCHAR * szColumnName,
		     SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *proc = NULL, *column = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedureColumnsW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, SQLCHAR, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, SQLCHAR, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szProcName, nProcNameLength, SQLCHAR, proc, addStmtError, stmt, goto exit);
	fixWcharIn(szColumnName, nColumnNameLength, SQLCHAR, column, addStmtError, stmt, goto exit);

	rc = SQLProcedureColumns_(stmt, catalog, SQL_NTS, schema, SQL_NTS, proc, SQL_NTS, column, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (proc)
		free(proc);
	if (column)
		free(column);

	return rc;
}
#endif /* WITH_WCHAR */
