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

struct sql_types {
	char *name;
	int type;
} sql_types[] = {
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
SQLExecute(SQLHSTMT hStmt)
{
	ODBCStmt *hstmt = (ODBCStmt *) hStmt;
	int i = 0;
	ColumnHeader *pCol;
	Mapi mid;

	if (!isValidStmt(hstmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(hstmt);

	/* check statement cursor state, query should be prepared */
	if (hstmt->State != PREPARED) {
		/* 24000 = Invalid cursor state */
		addStmtError(hstmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* internal state correctness checks */
	assert(hstmt->ResultCols == NULL);

	assert(hstmt->Dbc);
	mid = hstmt->Dbc->mid;
	assert(mid);

	/* Have the server execute the query */
	if (mapi_execute(mid) != MOK) {
		/* 08S01 Communication link failure */
		addStmtError(hstmt, "08S01", NULL, 0);
		return SQL_ERROR;
	}

	/* now get the result data and store it to our internal data structure */

	/* initialize the Result meta data values */
	hstmt->nrCols = mapi_get_field_count(mid);
	hstmt->currentRow = 0;
	hstmt->retrieved = 0;
	hstmt->currentCol = -1;

	hstmt->ResultCols = NEW_ARRAY(ColumnHeader, (hstmt->nrCols + 1));
	memset(hstmt->ResultCols, 0, (hstmt->nrCols + 1) * sizeof(ColumnHeader));
	pCol = hstmt->ResultCols + 1;
	for (i = 0; i < hstmt->nrCols; i++) {
		struct sql_types *p;
		char *s;

		s = mapi_get_name(mid, i);
		pCol->pszSQL_DESC_BASE_COLUMN_NAME = strdup(s);
		pCol->pszSQL_DESC_LABEL = strdup(s);
		pCol->pszSQL_DESC_NAME = strdup(s);
		pCol->nSQL_DESC_DISPLAY_SIZE = strlen(s) + 2;

		s = mapi_get_type(mid, i);
		pCol->pszSQL_DESC_TYPE_NAME = strdup(s);
		for (p = sql_types; p->name; p++) {
			if (strcmp(p->name, s) == 0) {
				pCol->nSQL_DESC_TYPE = p->type;
				break;
			}
		}

		pCol->pszSQL_DESC_BASE_TABLE_NAME = strdup("tablename");
		pCol->pszSQL_DESC_LOCAL_TYPE_NAME = strdup("Mtype");
		pCol->pszSQL_DESC_CATALOG_NAME = strdup("catalog");
		pCol->pszSQL_DESC_LITERAL_PREFIX = strdup("pre");
		pCol->pszSQL_DESC_LITERAL_SUFFIX = strdup("suf");
		pCol->pszSQL_DESC_SCHEMA_NAME = strdup("schema");
		pCol->pszSQL_DESC_TABLE_NAME = strdup("table");

		pCol++;
	}

	hstmt->State = EXECUTED;
	return SQL_SUCCESS;
}
