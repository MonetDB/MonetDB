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
	SQLFreeStmt_(stmt, SQL_CLOSE);
	setODBCDescRecCount(stmt->ImplParamDescr, 0);

	stmt->queryid = -1;
	stmt->nparams = 0;
	stmt->State = INITED;
}

SQLRETURN
SQLPrepare_(ODBCStmt *stmt,
	    SQLCHAR *StatementText,
	    SQLINTEGER TextLength)
{
	char *query, *s;
	MapiMsg ret;
	MapiHdl hdl;
	int nrParams;
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
	query = ODBCTranslateSQL(StatementText, (size_t) TextLength,
				 stmt->noScan);
#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepare: \"%s\"\n", query);
#endif
	s = malloc(strlen(query) + 9);
	strcat(strcpy(s, "prepare "), query);
	free(query);

	ODBCResetStmt(stmt);

	ret = mapi_query_handle(hdl, s);
	free(s);
	s = NULL;
	if (ret != MOK || (s = mapi_result_error(hdl)) != NULL) {
		/* XXX more fine-grained control required */
		/* Syntax error or access violation */
		addStmtError(stmt, "42000", s, 0);
		return SQL_ERROR;
	}
	if (mapi_rows_affected(hdl) > (1 << 16)) {
		/* arbitrarily limit the number of parameters */
		/* Memory allocation error */
		addStmtError(stmt, "HY001", 0, 0);
		return SQL_ERROR;
	}
	nrParams = (int) mapi_rows_affected(hdl);
	setODBCDescRecCount(stmt->ImplParamDescr, nrParams);
	rec = stmt->ImplParamDescr->descRec + 1;
	for (i = 0; i < nrParams; i++, rec++) {
		struct sql_types *tp;
		int concise_type;
		int length, scale;

		mapi_fetch_row(hdl);
		s = mapi_fetch_field(hdl, 0); /* type */
		rec->sql_desc_type_name = (SQLCHAR *) strdup(s);
		concise_type = ODBCConciseType(s);

		s = mapi_fetch_field(hdl, 1); /* digits */
		length = atoi(s);

		s = mapi_fetch_field(hdl, 2); /* scale */
		scale = atoi(s);

		/* for interval types, length and scale are used
		   differently */
		if (concise_type == SQL_INTERVAL_MONTH) {
			switch (length) {
			case 1:
				assert(scale == 1);
				concise_type = SQL_INTERVAL_YEAR;
				break;
			case 2:
				if (scale == 1)
					concise_type = SQL_INTERVAL_YEAR_TO_MONTH;
				else {
					assert(scale == 2);
					concise_type = SQL_INTERVAL_MONTH;
				}
				break;
			}
			rec->sql_desc_scale = 0;
			rec->sql_desc_length = 0;
		} else if (concise_type == SQL_INTERVAL_SECOND) {
			switch (length) {
			case 3:
				assert(scale == 3);
				concise_type = SQL_INTERVAL_DAY;
				break;
			case 4:
				switch (scale) {
				case 3:
					concise_type = SQL_INTERVAL_DAY_TO_HOUR;
					break;
				case 4:
					concise_type = SQL_INTERVAL_HOUR;
					break;
				default:
					assert(0);
				}
				break;
			case 5:
				switch (scale) {
				case 3:
					concise_type = SQL_INTERVAL_DAY_TO_MINUTE;
					break;
				case 4:
					concise_type = SQL_INTERVAL_HOUR_TO_MINUTE;
					break;
				case 5:
					concise_type = SQL_INTERVAL_MINUTE;
					break;
				default:
					assert(0);
				}
				break;
			case 6:
				switch (scale) {
				case 3:
					concise_type = SQL_INTERVAL_DAY_TO_SECOND;
					break;
				case 4:
					concise_type = SQL_INTERVAL_HOUR_TO_SECOND;
					break;
				case 5:
					concise_type = SQL_INTERVAL_MINUTE_TO_SECOND;
					break;
				case 6:
					concise_type = SQL_INTERVAL_SECOND;
					break;
				default:
					assert(0);
				}
				break;
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
		    rec->sql_desc_concise_type == SQL_LONGVARCHAR)
			rec->sql_desc_case_sensitive = SQL_TRUE;
		else
			rec->sql_desc_case_sensitive = SQL_FALSE;

		rec->sql_desc_local_type_name = (SQLCHAR *) strdup("");
		rec->sql_desc_nullable = SQL_NULLABLE;
		rec->sql_desc_parameter_type = SQL_PARAM_INPUT;
		rec->sql_desc_rowver = SQL_FALSE;
		rec->sql_desc_unnamed = SQL_UNNAMED;

		/* unused fields */
		rec->sql_desc_auto_unique_value = 0;
		rec->sql_desc_base_column_name = NULL;
		rec->sql_desc_base_table_name = NULL;
		rec->sql_desc_catalog_name = NULL;
		rec->sql_desc_data_ptr = NULL;
		rec->sql_desc_display_size = 0;
		rec->sql_desc_indicator_ptr = NULL;
		rec->sql_desc_label = NULL;
		rec->sql_desc_literal_prefix = NULL;
		rec->sql_desc_literal_suffix = NULL;
		rec->sql_desc_octet_length_ptr = NULL;
		rec->sql_desc_schema_name = NULL;
		rec->sql_desc_searchable = 0;
		rec->sql_desc_table_name = NULL;
		rec->sql_desc_updatable = 0;

		/* this must come after other fields have been
		 * initialized */
		rec->sql_desc_length = ODBCDisplaySize(rec);
		rec->sql_desc_display_size = rec->sql_desc_length;
		if (rec->sql_desc_concise_type == SQL_CHAR ||
		    rec->sql_desc_concise_type == SQL_VARCHAR ||
		    rec->sql_desc_concise_type == SQL_LONGVARCHAR) {
			/* in theory, each character (really: Unicode
			 * code point) could need 6 bytes in the UTF-8
			 * encoding, plus we need a byte for the
			 * terminating NUL byte */
			rec->sql_desc_octet_length = 6 * rec->sql_desc_length + 1;
		} else
			rec->sql_desc_octet_length = rec->sql_desc_length;
	}

	/* update the internal state */
	stmt->queryid = mapi_get_tableid(hdl);
	stmt->nparams = nrParams;
	stmt->State = PREPARED1;	/* XXX or PREPARED0, depending on query */

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

	return SQLPrepare_((ODBCStmt *) StatementHandle,
			   StatementText,
			   TextLength);
}

#ifdef WITH_WCHAR
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

	rc = SQLPrepare_(stmt, sql, SQL_NTS);

	if (sql)
		free(sql);

	return rc;
}
#endif /* WITH_WCHAR */
