/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
MNDBProcedureColumns(ODBCStmt *stmt,
		     SQLCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLCHAR *ProcName,
		     SQLSMALLINT NameLength3,
		     SQLCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ProcName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ColumnName, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, (char *) CatalogName,
		(int) NameLength2, (char *) SchemaName,
		(int) NameLength3, (char *) ProcName,
		(int) NameLength4, (char *) ColumnName);
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
SQLProcedureColumns(SQLHSTMT StatementHandle,
		    SQLCHAR *CatalogName,
		    SQLSMALLINT NameLength1,
		    SQLCHAR *SchemaName,
		    SQLSMALLINT NameLength2,
		    SQLCHAR *ProcName,
		    SQLSMALLINT NameLength3,
		    SQLCHAR *ColumnName,
		    SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedureColumns %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBProcedureColumns(stmt,
				    CatalogName, NameLength1,
				    SchemaName, NameLength2,
				    ProcName, NameLength3,
				    ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLProcedureColumnsA(SQLHSTMT StatementHandle,
		     SQLCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLCHAR *ProcName,
		     SQLSMALLINT NameLength3,
		     SQLCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	return SQLProcedureColumns(StatementHandle,
				   CatalogName, NameLength1,
				   SchemaName, NameLength2,
				   ProcName, NameLength3,
				   ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLProcedureColumnsW(SQLHSTMT StatementHandle,
		     SQLWCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLWCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLWCHAR *ProcName,
		     SQLSMALLINT NameLength3,
		     SQLWCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *proc = NULL, *column = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedureColumnsW %p ", StatementHandle);
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
	fixWcharIn(ColumnName, NameLength4, SQLCHAR, column,
		   addStmtError, stmt, goto exit);

	rc = MNDBProcedureColumns(stmt,
				  catalog, SQL_NTS,
				  schema, SQL_NTS,
				  proc, SQL_NTS,
				  column, SQL_NTS);

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
