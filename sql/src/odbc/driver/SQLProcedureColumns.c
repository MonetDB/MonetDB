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
 * because monetDB SQL frontend does not support stored procedures.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLProcedureColumns(SQLHSTMT hStmt,
		    SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
		    SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
		    SQLCHAR *szProcName, SQLSMALLINT nProcNameLength,
		    SQLCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedureColumns\n");
#endif

	(void) szCatalogName;	/* Stefan: unused!? */
	(void) nCatalogNameLength;	/* Stefan: unused!? */
	(void) szSchemaName;	/* Stefan: unused!? */
	(void) nSchemaNameLength;	/* Stefan: unused!? */
	(void) szProcName;	/* Stefan: unused!? */
	(void) nProcNameLength;	/* Stefan: unused!? */
	(void) szColumnName;	/* Stefan: unused!? */
	(void) nColumnNameLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* SQLProcedureColumns returns a table with the following columns:
	   VARCHAR	procedure_cat
	   VARCHAR	procedure_schem
	   VARCHAR	procedure_name NOT NULL
	   VARCHAR	column_name NOT NULL
	   SMALLINT	column_type NOT NULL
	   SMALLINT	data_type NOT NULL
	   VARCHAR	type_name NOT NULL
	   INTEGER	column_size
	   INTEGER	buffer_length
	   SMALLINT	decimal_digits
	   SMALLINT	num_prec_radix
	   SMALLINT	nullable NOT NULL
	   VARCHAR	remarks
	   VARCHAR	column_def
	   SMALLINT	sql_data_type NOT NULL
	   SMALLINT	sql_datetime_sub
	   INTEGER	char_octet_length
	   INTEGER	ordinal_position NOT NULL
	   VARCHAR	is_nullable
	*/

	/* IM001 = Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}
