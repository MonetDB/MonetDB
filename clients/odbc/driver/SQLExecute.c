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
 * SQLExecute()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static struct msql_types {
	char *name;
	int concise_type;
} msql_types[] = {
	{"bigint", SQL_BIGINT},
	{"blob", SQL_LONGVARBINARY},
	{"boolean", SQL_BIT},
	{"char", SQL_WCHAR},
	{"clob", SQL_WLONGVARCHAR},
	{"date", SQL_TYPE_DATE},
	{"decimal", SQL_DECIMAL},
	{"double", SQL_DOUBLE},
	{"int", SQL_INTEGER},
	{"month_interval", SQL_INTERVAL_MONTH},
	{"oid", SQL_BIGINT},
	{"real", SQL_REAL},
	{"sec_interval", SQL_INTERVAL_SECOND},
	{"smallint", SQL_SMALLINT},
	{"table", 0},
	{"time", SQL_TYPE_TIME},
	{"timetz", SQL_TYPE_TIME},
	{"timestamp", SQL_TYPE_TIMESTAMP},
	{"timestamptz", SQL_TYPE_TIMESTAMP},
	{"tinyint", SQL_TINYINT},
/* 	{"ubyte", SQL_TINYINT}, */
	{"uuid", SQL_GUID},
	{"varchar", SQL_WVARCHAR},
	{"wrd", SQL_BIGINT},
	{0, 0},			/* sentinel */
};

int
ODBCConciseType(const char *name)
{
	struct msql_types *p;

	for (p = msql_types; p->name; p++)
		if (strcmp(p->name, name) == 0)
			return p->concise_type;
	return 0;
}

#ifdef ODBCDEBUG
static char *
translatequerytype(int querytype)
{
	switch (querytype) {
	case Q_PARSE:
		return "Q_PARSE";
	case Q_TABLE:
		return "Q_TABLE";
	case Q_UPDATE:
		return "Q_UPDATE";
	case Q_SCHEMA:
		return "Q_SCHEMA";
	case Q_TRANS:
		return "Q_TRANS";
	case Q_PREPARE:
		return "Q_PREPARE";
	case Q_BLOCK:
		return "Q_BLOCK";
	default:
		return "unknown";
	}
}
#endif

SQLRETURN
ODBCInitResult(ODBCStmt *stmt)
{
	int i = 0;
	int nrCols;
	ODBCDescRec *rec;
	MapiHdl hdl;
	char *errstr;

	hdl = stmt->hdl;

	/* initialize the Result meta data values */
	stmt->currentRow = 0;
	stmt->startRow = 0;
	stmt->rowSetSize = 0;
	stmt->retrieved = 0;
	stmt->currentCol = 0;

      repeat:
	errstr = mapi_result_error(hdl);
	if (errstr) {
		const char *emsg, *sqlstate;

		if ((sqlstate = ODBCErrorType(errstr, &emsg)) != NULL)
			addStmtError(stmt, sqlstate, emsg, 0);
		else {
			/* Syntax error or access violation */
			addStmtError(stmt, "42000", errstr, 0);
		}
		return SQL_ERROR;
	}
	nrCols = mapi_get_field_count(hdl);
	stmt->querytype = mapi_get_querytype(hdl);
#if SIZEOF_SIZE_T == SIZEOF_INT
	if (mapi_rows_affected(hdl) >= (mapi_int64) 1 << (sizeof(int) * 8)) {
		/* General error */
		addStmtError(stmt, "HY000", "Too many rows to handle", 0);
		return SQL_ERROR;
	}
#endif
	stmt->rowcount = (SQLULEN) mapi_rows_affected(hdl);

#ifdef ODBCDEBUG
	ODBCLOG("ODBCInitResult: querytype %s, rowcount %lu\n",
		translatequerytype(stmt->querytype),
		(unsigned long) stmt->rowcount);
#endif

	switch (stmt->querytype) {
	case Q_TABLE:	/* Q_TABLE */
		/* result set generating query */
		assert(nrCols > 0);
		stmt->State = EXECUTED1;
		break;
	case Q_UPDATE:		/* Q_UPDATE */
		/* result count generating query */
		assert(nrCols == 0);
		stmt->State = EXECUTED0;
		break;
	default:
		/* resultless query */
		if (mapi_result_error(hdl) == NULL && mapi_next_result(hdl) == 1)
			goto repeat;
		stmt->State = EXECUTED0;
		stmt->rowcount = 0;
		nrCols = 0;
		break;
	}

#if 0
	/* XXX is this correct? */
	assert(stmt->ImplRowDescr == NULL ||
	       stmt->ImplRowDescr->sql_desc_count == nrCols ||
	       stmt->ImplRowDescr->sql_desc_count == 0);
#endif
	setODBCDescRecCount(stmt->ImplRowDescr, nrCols);

	if (nrCols == 0)
		return SQL_SUCCESS;
	if (stmt->ImplRowDescr->descRec == NULL) {
		stmt->State = stmt->queryid >= 0 ? (stmt->State == EXECUTED0 ? PREPARED0 : PREPARED1) : INITED;

		/* Memory allocation error */
		addStmtError(stmt, "HY001", NULL, 0);
		return SQL_ERROR;
	}

	rec = stmt->ImplRowDescr->descRec + 1;

	for (i = 0; i < nrCols; i++) {
		struct sql_types *tp;
		int concise_type;
		char *s;

		rec->sql_desc_auto_unique_value = SQL_FALSE;
		rec->sql_desc_nullable = SQL_NULLABLE_UNKNOWN;
		rec->sql_desc_rowver = SQL_FALSE;
		rec->sql_desc_searchable = SQL_PRED_SEARCHABLE;
		rec->sql_desc_updatable = SQL_ATTR_READONLY;

		s = mapi_get_name(hdl, i);
		/* HACK to compensate for generated column names */
		if (s == NULL || strcmp(s, "single_value") == 0)
			s = "";
		if (*s) {
			rec->sql_desc_unnamed = SQL_NAMED;
			if (rec->sql_desc_label)
				free(rec->sql_desc_label);
			rec->sql_desc_label = (SQLCHAR *) strdup(s);
			if (rec->sql_desc_label == NULL)
				goto nomem;
			if (rec->sql_desc_name)
				free(rec->sql_desc_name);
			rec->sql_desc_name = (SQLCHAR *) strdup(s);
			if (rec->sql_desc_name == NULL)
				goto nomem;
		} else {
			rec->sql_desc_unnamed = SQL_UNNAMED;
			rec->sql_desc_label = NULL;
			rec->sql_desc_name = NULL;
		}
		if (rec->sql_desc_base_column_name)
			free(rec->sql_desc_base_column_name);
		rec->sql_desc_base_column_name = NULL; /* see below */

		s = mapi_get_type(hdl, i);
		if (s == NULL)	/* shouldn't happen */
			s = "";
		if (rec->sql_desc_type_name)
			free(rec->sql_desc_type_name);
		rec->sql_desc_type_name = (SQLCHAR *) strdup(s);
		if (rec->sql_desc_type_name == NULL)
			goto nomem;
		concise_type = ODBCConciseType(s);
		if (concise_type == SQL_INTERVAL_MONTH) {
			switch (mapi_get_digits(hdl, i)) {
			case 1:
				concise_type = SQL_INTERVAL_YEAR;
				break;
			case 2:
				concise_type = SQL_INTERVAL_YEAR_TO_MONTH;
				break;
			case 3:
				concise_type = SQL_INTERVAL_MONTH;
				break;
			default:
				assert(0);
			}
		} else if (concise_type == SQL_INTERVAL_SECOND) {
			switch (mapi_get_digits(hdl, i)) {
			case 4:
				concise_type = SQL_INTERVAL_DAY;
				break;
			case 5:
				concise_type = SQL_INTERVAL_DAY_TO_HOUR;
				break;
			case 6:
				concise_type = SQL_INTERVAL_DAY_TO_MINUTE;
				break;
			case 7:
				concise_type = SQL_INTERVAL_DAY_TO_SECOND;
				break;
			case 8:
				concise_type = SQL_INTERVAL_HOUR;
				break;
			case 9:
				concise_type = SQL_INTERVAL_HOUR_TO_MINUTE;
				break;
			case 10:
				concise_type = SQL_INTERVAL_HOUR_TO_SECOND;
				break;
			case 11:
				concise_type = SQL_INTERVAL_MINUTE;
				break;
			case 12:
				concise_type = SQL_INTERVAL_MINUTE_TO_SECOND;
				break;
			case 13:
				concise_type = SQL_INTERVAL_SECOND;
				break;
			default:
				assert(0);
			}
		}
		for (tp = ODBC_sql_types; tp->concise_type; tp++)
			if (concise_type == tp->concise_type)
				break;
		rec->sql_desc_concise_type = tp->concise_type;
		rec->sql_desc_type = tp->type;
		rec->sql_desc_datetime_interval_code = tp->code;
		if (concise_type == SQL_DECIMAL) {
			rec->sql_desc_precision = mapi_get_digits(hdl, i);
			rec->sql_desc_scale = mapi_get_scale(hdl, i);
		} else {
			if (tp->precision != UNAFFECTED)
				rec->sql_desc_precision = tp->precision;
			if (tp->scale != UNAFFECTED)
				rec->sql_desc_scale = tp->scale;
		}
		if (tp->datetime_interval_precision != UNAFFECTED)
			rec->sql_desc_datetime_interval_precision = tp->datetime_interval_precision;
		rec->sql_desc_fixed_prec_scale = tp->fixed;
		rec->sql_desc_num_prec_radix = tp->radix;
		rec->sql_desc_unsigned = tp->radix == 0 ? SQL_TRUE : SQL_FALSE;

		if (rec->sql_desc_concise_type == SQL_CHAR ||
		    rec->sql_desc_concise_type == SQL_VARCHAR ||
		    rec->sql_desc_concise_type == SQL_LONGVARCHAR ||
		    rec->sql_desc_concise_type == SQL_WCHAR ||
		    rec->sql_desc_concise_type == SQL_WVARCHAR ||
		    rec->sql_desc_concise_type == SQL_WLONGVARCHAR)
			rec->sql_desc_case_sensitive = SQL_TRUE;
		else
			rec->sql_desc_case_sensitive = SQL_FALSE;

		s = mapi_get_table(hdl, i);
		if (s) {
			char *p = strchr(s, '.');
			if (rec->sql_desc_table_name)
				free(rec->sql_desc_table_name);
			if (p) {
				if (rec->sql_desc_schema_name)
					free(rec->sql_desc_schema_name);
				rec->sql_desc_schema_name = (SQLCHAR *) dupODBCstring((SQLCHAR *) s, p - s);
				if (rec->sql_desc_schema_name == NULL)
					goto nomem;
				rec->sql_desc_table_name = (SQLCHAR *) strdup(p + 1);
				if (rec->sql_desc_table_name == NULL)
					goto nomem;
				if (p != s) {
					/* base table name and base
					 * column name exist if there
					 * is a schema name */
					if (rec->sql_desc_base_table_name)
						free(rec->sql_desc_base_table_name);
					rec->sql_desc_base_table_name = (SQLCHAR *) strdup(p + 1);
					if (rec->sql_desc_base_table_name == NULL)
						goto nomem;
					if (rec->sql_desc_name) {
						rec->sql_desc_base_column_name = (SQLCHAR *) strdup((char *) rec->sql_desc_name);
						if (rec->sql_desc_base_column_name == NULL)
							goto nomem;
					}
				}
			} else {
				rec->sql_desc_table_name = (SQLCHAR *) strdup(s);
				if (rec->sql_desc_table_name == NULL)
					goto nomem;
			}
		}

		if (rec->sql_desc_type != SQL_INTERVAL &&
		    (rec->sql_desc_length = mapi_get_digits(hdl, i)) == 0)
			rec->sql_desc_length = mapi_get_len(hdl, i);

		rec->sql_desc_local_type_name = NULL;
		if (rec->sql_desc_catalog_name == NULL) {
			if (stmt->Dbc->dbname) {
				rec->sql_desc_catalog_name = (SQLCHAR *) strdup(stmt->Dbc->dbname);
				if (rec->sql_desc_catalog_name == NULL)
					goto nomem;
			} else
				rec->sql_desc_catalog_name = NULL;
		}
		rec->sql_desc_literal_prefix = NULL;
		rec->sql_desc_literal_suffix = NULL;

		/* unused fields */
		rec->sql_desc_data_ptr = NULL;
		rec->sql_desc_indicator_ptr = NULL;
		rec->sql_desc_octet_length_ptr = NULL;
		rec->sql_desc_parameter_type = 0;

		/* this must come after other fields have been
		 * initialized */
		rec->sql_desc_length = ODBCLength(rec, SQL_DESC_LENGTH);
		rec->sql_desc_display_size = ODBCLength(rec, SQL_DESC_DISPLAY_SIZE);
		rec->sql_desc_octet_length = ODBCLength(rec, SQL_DESC_OCTET_LENGTH);
		if (rec->sql_desc_length == 0) {
			rec->sql_desc_length = SQL_NO_TOTAL;
			rec->sql_desc_display_size = SQL_NO_TOTAL;
			rec->sql_desc_octet_length = SQL_NO_TOTAL;
		}

		rec++;
	}

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;

  nomem:
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN
MNDBExecute(ODBCStmt *stmt)
{
	MapiHdl hdl;
	MapiMsg msg;
	char *query;
	char *sep;
	size_t querylen;
	size_t querypos;
	int i;
	ODBCDesc *desc;
	SQLLEN offset;

	/* check statement cursor state, query should be prepared */
	if (stmt->State == INITED ||
	    (stmt->State >= EXECUTED0 && stmt->queryid < 0)) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State >= EXECUTED1 ||
	    (stmt->State == EXECUTED0 && mapi_more_results(stmt->hdl))) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* internal state correctness checks */
	assert(stmt->State == PREPARED0 || stmt->State == EXECUTED0 || stmt->ImplRowDescr->descRec != NULL);

	assert(stmt->Dbc);
	assert(stmt->Dbc->mid);
	hdl = stmt->hdl;

	assert(hdl);

	desc = stmt->ApplParamDescr;

	if (desc->sql_desc_count < stmt->nparams ||
	    stmt->ImplParamDescr->sql_desc_count < stmt->nparams) {
		/* COUNT field incorrect */
		addStmtError(stmt, "07002", NULL, 0);
		return SQL_ERROR;
	}

	querylen = 1024;
	query = malloc(querylen); /* XXX allocate space for parameters */
	if (query == NULL) {
		/* Memory allocation error */
		addStmtError(stmt, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	querypos = snprintf(query, querylen, "execute %d (", stmt->queryid);
	/* XXX fill in parameter values */
	if (desc->sql_desc_bind_offset_ptr)
		offset = *desc->sql_desc_bind_offset_ptr;
	else
		offset = 0;
	sep = "";
	for (i = 1; i <= stmt->nparams; i++) {
		if (ODBCStore(stmt, i, offset, 0, &query, &querypos, &querylen, sep) == SQL_ERROR) {
			if (query)
				free(query);
			return SQL_ERROR;
		}
		sep = ",";
	}
	if (querypos + 1 >= querylen) {
		char *q = realloc(query, querylen += 10);
		if (q == NULL) {
			free(query);
			addStmtError(stmt, "HY001", NULL, 0);
			return SQL_ERROR;
		}
		query = q;
	}
	query[querypos++] = ')';
	query[querypos] = 0;

#ifdef ODBCDEBUG
	ODBCLOG("SQLExecute " PTRFMT " %s\n", PTRFMTCAST stmt, query);
#endif

	/* Have the server execute the query */
	if (stmt->next == NULL && stmt->Dbc->FirstStmt == stmt &&
	    stmt->cursorType == SQL_CURSOR_FORWARD_ONLY) {
		/* we're the only Stmt handle, and we're only going forward */
		if (stmt->Dbc->cachelimit != 1000)
			mapi_cache_limit(stmt->Dbc->mid, 1000);
		stmt->Dbc->cachelimit = 1000;
	} else {
		if (stmt->Dbc->cachelimit != 100)
			mapi_cache_limit(stmt->Dbc->mid, 100);
		stmt->Dbc->cachelimit = 100;
	}
	msg = mapi_query_handle(hdl, query);
	free(query);
	switch (msg) {
	case MOK:
		break;
	case MTIMEOUT:
		/* Timeout expired / Communication link failure */
		addStmtError(stmt, stmt->Dbc->sql_attr_connection_timeout ? "HYT00" : "08S01", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	default:
		/* reuse variable query for error message */
		query = mapi_result_error(hdl);
		if (query == NULL)
			query = mapi_error_str(stmt->Dbc->mid);
		if (query) {
			const char *emsg, *sqlstate;

			if ((sqlstate = ODBCErrorType(query, &emsg)) != NULL) {
				addStmtError(stmt, sqlstate, emsg, 0);
				return SQL_ERROR;
			}
		}
		/* General error */
		addStmtError(stmt, "HY000", query, 0);
		return SQL_ERROR;
	}

	/* now get the result data and store it to our internal data structure */

	return ODBCInitResult(stmt);
}

SQLRETURN SQL_API
SQLExecute(SQLHSTMT StatementHandle)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLExecute " PTRFMT "\n", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return MNDBExecute((ODBCStmt *) StatementHandle);
}
