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
	int type;
} msql_types[] = {
	{"bit", SQL_C_BIT},
	{"uchr", SQL_C_UTINYINT},
	{"char", SQL_C_CHAR},
	{"varchar", SQL_C_CHAR},
	{"sht", SQL_C_SSHORT},
	{"int", SQL_C_SLONG},
	{"lng", SQL_C_SBIGINT},
	{"flt", SQL_C_FLOAT},
	{"dbl", SQL_C_DOUBLE},
	{"date", SQL_C_TYPE_DATE},
	{"time", SQL_C_TYPE_TIME},
	{"timestamp", SQL_C_TYPE_TIMESTAMP},
	{0, 0},			/* sentinel */
};

SQLRETURN
SQLExecute_(ODBCStmt *stmt)
{
	int i = 0;
	int nrCols;
	ODBCDescRec *pCol;
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

	pCol = stmt->ImplRowDescr->descRec + 1;
	for (i = 0; i < nrCols; i++) {
		struct msql_types *p;
		char *s;

		s = mapi_get_name(hdl, i);
		pCol->sql_desc_base_column_name = strdup(s);
		pCol->sql_desc_label = strdup(s);
		pCol->sql_desc_name = strdup(s);
		pCol->sql_desc_display_size = strlen(s) + 2;

		s = mapi_get_type(hdl, i);
		pCol->sql_desc_type_name = strdup(s);
		for (p = msql_types; p->name; p++) {
			if (strcmp(p->name, s) == 0) {
				pCol->sql_desc_type = p->type;
				break;
			}
		}

		pCol->sql_desc_base_table_name = strdup("tablename");
		pCol->sql_desc_local_type_name = strdup("Mtype");
		pCol->sql_desc_catalog_name = strdup("catalog");
		pCol->sql_desc_literal_prefix = strdup("pre");
		pCol->sql_desc_literal_suffix = strdup("suf");
		pCol->sql_desc_schema_name = strdup("schema");
		pCol->sql_desc_table_name = strdup("table");

		pCol++;
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
