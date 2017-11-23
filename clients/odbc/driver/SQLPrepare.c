/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 * SQLPrepare
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


void
ODBCResetStmt(ODBCStmt *stmt)
{
	MNDBFreeStmt(stmt, SQL_CLOSE);
	setODBCDescRecCount(stmt->ImplParamDescr, 0);

	if (stmt->queryid >= 0)
		mapi_release_id(stmt->Dbc->mid, stmt->queryid);
	stmt->queryid = -1;
	stmt->nparams = 0;
	stmt->State = INITED;
}

SQLRETURN
MNDBPrepare(ODBCStmt *stmt,
	    SQLCHAR *StatementText,
	    SQLINTEGER TextLength)
{
	char *query, *s;
	MapiMsg ret;
	MapiHdl hdl;
	int nrows;
	int ncols;
	ODBCDescRec *prec, *rrec; /* param and row descriptors */
	ODBCDescRec *rec;
	int i;

	hdl = stmt->hdl;

	if (stmt->State >= EXECUTED1 ||
	    (stmt->State == EXECUTED0 && mapi_more_results(hdl))) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* check input parameter */
	if (StatementText == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	fixODBCstring(StatementText, TextLength, SQLINTEGER, addStmtError, stmt, return SQL_ERROR);
	/* TODO: convert ODBC escape sequences ( {d 'value'} or {t
	 * 'value'} or {ts 'value'} or {escape 'e-char'} or {oj
	 * outer-join} or {fn scalar-function} etc. ) to MonetDB SQL
	 * syntax */
	query = ODBCTranslateSQL(stmt->Dbc, StatementText, (size_t) TextLength,
				 stmt->noScan);
	if (query == NULL) {
		/* Memory allocation error */
		addStmtError(stmt, "HY001", NULL, 0);
		return SQL_ERROR;
	}
#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepare: \"%s\"\n", query);
#endif
	s = malloc(strlen(query) + 9);
	if (s == NULL) {
		free(query);
		/* Memory allocation error */
		addStmtError(stmt, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	strcat(strcpy(s, "prepare "), query);
	free(query);

	ODBCResetStmt(stmt);

	ret = mapi_query_handle(hdl, s);
	free(s);
	s = NULL;
	if (ret != MOK) {
		const char *e, *m;

		/* XXX more fine-grained control required */
		/* Syntax error or access violation */
		if ((m = mapi_result_error(hdl)) == NULL)
			m = mapi_error_str(stmt->Dbc->mid);
		if (m && (e = mapi_result_errorcode(hdl)) != NULL)
			addStmtError(stmt, e, m, 0);
		else
			addStmtError(stmt, "42000", m, 0);
		return SQL_ERROR;
	}
	if (mapi_rows_affected(hdl) > ((int64_t) 1 << 16)) {
		/* arbitrarily limit the number of parameters */
		/* Memory allocation error */
		addStmtError(stmt, "HY001", 0, 0);
		return SQL_ERROR;
	}
	nrows = (int) mapi_rows_affected(hdl);
	ncols = mapi_get_field_count(hdl);
	/* these two will be adjusted later */
	setODBCDescRecCount(stmt->ImplParamDescr, nrows);
	setODBCDescRecCount(stmt->ImplRowDescr, nrows);
	prec = stmt->ImplParamDescr->descRec + 1;
	rrec = stmt->ImplRowDescr->descRec + 1;
	stmt->nparams = 0;
	for (i = 0; i < nrows; i++) {
		struct sql_types *tp;
		int concise_type;
		int length, scale;

		mapi_fetch_row(hdl);
		if (ncols == 3 ||
		    (s = mapi_fetch_field(hdl, 5)) == NULL) {
			/* either old prepare (i.e. old server) or no
			 * column name: either way, this describes a
			 * parameter */
			stmt->nparams++;
			rec = prec++;
			rec->sql_desc_nullable = SQL_NULLABLE;
			rec->sql_desc_searchable = SQL_UNSEARCHABLE;
			rec->sql_desc_unnamed = SQL_UNNAMED;
			rec->sql_desc_label = NULL;
			rec->sql_desc_name = NULL;
			rec->sql_desc_schema_name = NULL;
			rec->sql_desc_table_name = NULL;
			rec->sql_desc_base_table_name = NULL;
			rec->sql_desc_base_column_name = NULL;
			rec->sql_desc_parameter_type = SQL_PARAM_INPUT;
		} else {
			rec = rrec++;
			rec->sql_desc_nullable = SQL_NULLABLE_UNKNOWN;
			rec->sql_desc_searchable = SQL_PRED_SEARCHABLE;
			rec->sql_desc_unnamed = SQL_NAMED;
			rec->sql_desc_label = (SQLCHAR *) strdup(s);
			rec->sql_desc_name = (SQLCHAR *) strdup(s);
			s = mapi_fetch_field(hdl, 3); /* schema name */
			rec->sql_desc_schema_name = s && *s ? (SQLCHAR *) strdup(s) : NULL;
			s = mapi_fetch_field(hdl, 4); /* table name */
			rec->sql_desc_table_name = s && *s ? (SQLCHAR *) strdup(s) : NULL;
			if (rec->sql_desc_schema_name) {
				/* base table name and base column
				 * name exist if there is a schema
				 * name; the extra check is for static
				 * code analyzers and robustness */
				rec->sql_desc_base_table_name = rec->sql_desc_table_name ? (SQLCHAR *) strdup((char *) rec->sql_desc_table_name) : NULL;
				rec->sql_desc_base_column_name = (SQLCHAR *) strdup((char *) rec->sql_desc_name);
			} else {
				rec->sql_desc_base_table_name = NULL;
				rec->sql_desc_base_column_name = NULL;
			}
			rec->sql_desc_parameter_type = 0;
		}

		s = mapi_fetch_field(hdl, 0); /* type */
		rec->sql_desc_type_name = (SQLCHAR *) strdup(s);
		concise_type = ODBCConciseType(s);

		s = mapi_fetch_field(hdl, 1); /* digits */
		length = atoi(s);

		s = mapi_fetch_field(hdl, 2); /* scale */
		scale = atoi(s);

		/* for interval types, length and scale are used
		 * differently */
		if (concise_type == SQL_INTERVAL_MONTH) {
			switch (length) {
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
			rec->sql_desc_scale = 0;
			rec->sql_desc_length = 0;
		} else if (concise_type == SQL_INTERVAL_SECOND) {
			switch (length) {
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
			rec->sql_desc_scale = 0;
			rec->sql_desc_length = 0;
		} else {
			rec->sql_desc_scale = scale;
			rec->sql_desc_length = length;
		}

		for (tp = ODBC_sql_types; tp->concise_type; tp++)
			if (concise_type == tp->concise_type)
				break;
		rec->sql_desc_concise_type = tp->concise_type;
		rec->sql_desc_type = tp->type;
		rec->sql_desc_datetime_interval_code = tp->code;
		if (tp->precision != UNAFFECTED)
			rec->sql_desc_precision = tp->precision;
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

		rec->sql_desc_local_type_name = NULL;
		rec->sql_desc_rowver = SQL_FALSE;
		rec->sql_desc_catalog_name = stmt->Dbc->dbname ? (SQLCHAR *) strdup(stmt->Dbc->dbname) : NULL;

		/* unused fields */
		rec->sql_desc_auto_unique_value = SQL_FALSE;
		rec->sql_desc_data_ptr = NULL;
		rec->sql_desc_display_size = 0;
		rec->sql_desc_indicator_ptr = NULL;
		rec->sql_desc_literal_prefix = NULL;
		rec->sql_desc_literal_suffix = NULL;
		rec->sql_desc_octet_length_ptr = NULL;
		rec->sql_desc_schema_name = NULL;
		rec->sql_desc_table_name = NULL;
		rec->sql_desc_updatable = SQL_ATTR_READONLY;

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
	}

	assert(prec - stmt->ImplParamDescr->descRec == stmt->nparams + 1);
	assert(rrec - stmt->ImplRowDescr->descRec == nrows - stmt->nparams + 1);
	setODBCDescRecCount(stmt->ImplParamDescr, stmt->nparams);
	setODBCDescRecCount(stmt->ImplRowDescr, nrows - stmt->nparams);

	/* update the internal state */
	stmt->queryid = mapi_get_tableid(hdl);
	if (stmt->ImplRowDescr->sql_desc_count == 0)
		stmt->State = PREPARED0; /* no columns: no result set */
	else
		stmt->State = PREPARED1;

	return SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLPrepare(SQLHSTMT StatementHandle,
	   SQLCHAR *StatementText,
	   SQLINTEGER TextLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepare " PTRFMT "\n", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return MNDBPrepare((ODBCStmt *) StatementHandle,
			   StatementText,
			   TextLength);
}

SQLRETURN SQL_API
SQLPrepareA(SQLHSTMT StatementHandle,
	    SQLCHAR *StatementText,
	    SQLINTEGER TextLength)
{
	return SQLPrepare(StatementHandle, StatementText, TextLength);
}

SQLRETURN SQL_API
SQLPrepareW(SQLHSTMT StatementHandle,
	    SQLWCHAR *StatementText,
	    SQLINTEGER TextLength)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLCHAR *sql;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepareW " PTRFMT "\n", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(StatementText, TextLength, SQLCHAR, sql,
		   addStmtError, stmt, return SQL_ERROR);

	rc = MNDBPrepare(stmt, sql, SQL_NTS);

	if (sql)
		free(sql);

	return rc;
}
