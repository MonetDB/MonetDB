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
 * SQLExecute()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

struct msql_types {
	char *name;
	int concise_type;
	int type;
	int code;
} msql_types[] = {
	{"bigint", SQL_BIGINT, SQL_BIGINT, 0},
	{"boolean", SQL_BIT, SQL_BIT, 0},
	{"character", SQL_CHAR, SQL_CHAR, 0},
	{"date", SQL_TYPE_DATE, SQL_DATETIME, SQL_CODE_DATE},
	{"decimal", SQL_DECIMAL, SQL_DECIMAL, 0},
	{"double", SQL_DOUBLE, SQL_DOUBLE, 0},
	{"float", SQL_FLOAT, SQL_FLOAT, 0},
	{"int", SQL_INTEGER, SQL_INTEGER, 0},
	{"mediumint", SQL_INTEGER, SQL_INTEGER, 0},
	{"month_interval", SQL_INTERVAL_MONTH, SQL_INTERVAL, SQL_CODE_MONTH},
	{"sec_interval", SQL_INTERVAL_SECOND, SQL_INTERVAL, SQL_CODE_SECOND},
	{"smallint", SQL_SMALLINT, SQL_SMALLINT, 0},
	{"time", SQL_TYPE_TIME, SQL_DATETIME, SQL_CODE_TIME},
	{"timestamp", SQL_TYPE_TIMESTAMP, SQL_DATETIME, SQL_CODE_TIMESTAMP},
	{"tinyint", SQL_TINYINT, SQL_TINYINT, 0},
	{"varchar", SQL_CHAR, SQL_CHAR, 0},
	{"blob", SQL_BINARY, SQL_BINARY, 0},
	{"datetime", 0, 0, 0},
	{"oid", SQL_GUID, SQL_GUID, 0},
	{"table", 0, 0, 0},
	{"ubyte", SQL_TINYINT, SQL_TINYINT, 0},
	{0, 0, 0, 0},		/* sentinel */
};

SQLRETURN
SQLExecute_(ODBCStmt *stmt)
{
	int i = 0;
	int nrCols;
	ODBCDescRec *rec;
	MapiHdl hdl;
	MapiMsg msg;

	if (!isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be prepared */
	if (stmt->State != PREPARED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* internal state correctness checks */
	assert(stmt->ImplRowDescr->descRec == NULL);

	assert(stmt->Dbc);
	assert(stmt->Dbc->mid);
	hdl = stmt->hdl;
	assert(hdl);

	/* Have the server execute the query */
	msg = mapi_execute(hdl);
	switch (msg) {
	case MOK:
		break;
	case MTIMEOUT:
		/* 08S01 Communication link failure */
		addStmtError(stmt, "08S01", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	default:
		/* General error */
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	}

	/* now get the result data and store it to our internal data structure */

	/* initialize the Result meta data values */
	nrCols = mapi_get_field_count(hdl);
	stmt->currentRow = 0;
	stmt->retrieved = 0;
	stmt->currentCol = 0;

	if (nrCols == 0 && mapi_get_row_count(hdl) == 0) {
		stmt->State = PREPARED;
		return SQL_SUCCESS;
	}

	setODBCDescRecCount(stmt->ImplRowDescr, nrCols);
	if (stmt->ImplRowDescr->descRec == NULL) {
		addStmtError(stmt, "HY001", NULL, 0);
		return SQL_ERROR;
	}

	rec = stmt->ImplRowDescr->descRec + 1;
	for (i = 0; i < nrCols; i++) {
		struct msql_types *p;
		char *s;

		s = mapi_get_name(hdl, i);
		rec->sql_desc_base_column_name = (SQLCHAR *) strdup(s);
		rec->sql_desc_label = (SQLCHAR *) strdup(s);
		rec->sql_desc_name = (SQLCHAR *) strdup(s);
		rec->sql_desc_display_size = strlen(s) + 2;

		s = mapi_get_type(hdl, i);
		rec->sql_desc_type_name = (SQLCHAR *) strdup(s);
		for (p = msql_types; p->name; p++) {
			if (strcmp(p->name, s) == 0) {
				rec->sql_desc_type = p->type;
				rec->sql_desc_concise_type = p->concise_type;
				rec->sql_desc_datetime_interval_code = p->code;
				break;
			}
		}

		rec->sql_desc_base_table_name = (SQLCHAR *) strdup("tablename");
		rec->sql_desc_local_type_name = (SQLCHAR *) strdup("Mtype");
		rec->sql_desc_catalog_name = (SQLCHAR *) strdup("catalog");
		rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("pre");
		rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("suf");
		rec->sql_desc_schema_name = (SQLCHAR *) strdup("schema");
		rec->sql_desc_table_name = (SQLCHAR *) strdup("table");

		rec++;
	}

	stmt->State = EXECUTED;
	return SQL_SUCCESS;
}

SQLRETURN
SQLExecute(SQLHSTMT hStmt)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLExecute\n");
#endif

	return SQLExecute_((ODBCStmt *) hStmt);
}
