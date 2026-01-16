/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

#define ODBC_DESC_MAGIC_NR	21845	/* for internal sanity check only */

/*
 * Creates a new allocated ODBCDesc object and initializes it.
 *
 * Precondition: valid ODBCDbc object
 * Postcondition: returns a new ODBCDesc object
 */
ODBCDesc *
newODBCDesc(ODBCDbc *dbc)
{
	ODBCDesc *desc;

	assert(dbc);

	desc = (ODBCDesc *) malloc(sizeof(ODBCDesc));
	if (desc == NULL) {
		/* Memory allocation error */
		addDbcError(dbc, "HY001", NULL, 0);
		return NULL;
	}

	*desc = (ODBCDesc) {
		.Dbc = dbc,
		.sql_desc_alloc_type = SQL_DESC_ALLOC_USER,
		.sql_desc_array_size = 1,
		.sql_desc_bind_type = SQL_BIND_TYPE_DEFAULT,
		.Type = ODBC_DESC_MAGIC_NR,	/* set it valid */
	};
	return desc;
}


/*
 * Check if the descriptor handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns true if it is a valid statement handle,
 * 	returns false if is invalid and thus an unusable handle.
 */
bool
isValidDesc(ODBCDesc *desc)
{
#ifdef ODBCDEBUG
	if (!(desc && desc->Type == ODBC_DESC_MAGIC_NR))
		ODBCLOG("desc %pnot a valid descriptor handle\n", desc);
#endif
	return desc && desc->Type == ODBC_DESC_MAGIC_NR;
}

/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCDesc struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: desc must be valid. SQLState and errMsg may be NULL.
 */
void
addDescError(ODBCDesc *desc, const char *SQLState, const char *errMsg, int nativeErrCode)
{
	ODBCError *error = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("addDescError %p %s %s %d\n", desc, SQLState, errMsg ? errMsg : getStandardSQLStateMsg(SQLState), nativeErrCode);
#endif
	assert(isValidDesc(desc));

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	appendODBCError(&desc->Error, error);
}

/*
 * Extracts an error object from the error list of this ODBCDesc struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: desc and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError *
getDescError(ODBCDesc *desc)
{
	assert(isValidDesc(desc));
	return desc->Error;
}

static void
cleanODBCDescRec(ODBCDesc *desc, ODBCDescRec *rec)
{
	if (rec->sql_desc_base_column_name)
		free(rec->sql_desc_base_column_name);
	if (rec->sql_desc_base_table_name)
		free(rec->sql_desc_base_table_name);
	if (rec->sql_desc_catalog_name)
		free(rec->sql_desc_catalog_name);
	if (rec->sql_desc_label)
		free(rec->sql_desc_label);
	if (rec->sql_desc_literal_prefix)
		free(rec->sql_desc_literal_prefix);
	if (rec->sql_desc_literal_suffix)
		free(rec->sql_desc_literal_suffix);
	if (rec->sql_desc_local_type_name)
		free(rec->sql_desc_local_type_name);
	if (rec->sql_desc_name)
		free(rec->sql_desc_name);
	if (rec->sql_desc_schema_name)
		free(rec->sql_desc_schema_name);
	if (rec->sql_desc_table_name)
		free(rec->sql_desc_table_name);
	if (rec->sql_desc_type_name)
		free(rec->sql_desc_type_name);
	*rec = (ODBCDescRec) {0};
	if (desc) {
		if (isAD(desc)) {
			rec->sql_desc_concise_type = SQL_C_DEFAULT;
			rec->sql_desc_type = SQL_C_DEFAULT;
		} else if (isIPD(desc)) {
			rec->sql_desc_parameter_type = SQL_PARAM_INPUT;
			rec->sql_desc_nullable = SQL_NULLABLE;
		}
	}
}

void
setODBCDescRecCount(ODBCDesc *desc, int count)
{
	assert(count >= 0);
	assert(desc->sql_desc_count >= 0);

	if (count == desc->sql_desc_count)
		return;
	if (count < desc->sql_desc_count) {
		int i;

		for (i = count + 1; i <= desc->sql_desc_count; i++)
			cleanODBCDescRec(NULL, &desc->descRec[i]);
	}
	if (count == 0) {
		assert(desc->descRec != NULL);
		free(desc->descRec);
		desc->descRec = NULL;
	} else if (desc->descRec == NULL) {
		assert(desc->sql_desc_count == 0);
		desc->descRec = malloc((count + 1) * sizeof(*desc->descRec));
	} else {
		ODBCDescRec *p;
		assert(desc->sql_desc_count > 0);
		p = realloc(desc->descRec, (count + 1) * sizeof(*desc->descRec));
		if (p == NULL)
			return;	/* TODO: error handling */
		desc->descRec = p;
	}
	if (count > desc->sql_desc_count) {
		int i;

		memset(desc->descRec + desc->sql_desc_count + 1, 0, (count - desc->sql_desc_count) * sizeof(*desc->descRec));
		if (isAD(desc)) {
			for (i = desc->sql_desc_count + 1; i <= count; i++) {
				desc->descRec[i].sql_desc_concise_type = SQL_C_DEFAULT;
				desc->descRec[i].sql_desc_type = SQL_C_DEFAULT;
			}
		} else if (isIPD(desc)) {
			for (i = desc->sql_desc_count + 1; i <= count; i++) {
				desc->descRec[i].sql_desc_parameter_type = SQL_PARAM_INPUT;
				desc->descRec[i].sql_desc_nullable = SQL_NULLABLE;
			}
		}
	}
	desc->sql_desc_count = count;
}

/*
 * Destroys the ODBCDesc object including its own managed data.
 *
 * Precondition: desc must be valid.
 * Postcondition: desc is completely destroyed, desc handle is become invalid.
 */
void
destroyODBCDesc(ODBCDesc *desc)
{
	assert(isValidDesc(desc));

	desc->Type = 0;
	deleteODBCErrorList(&desc->Error);
	setODBCDescRecCount(desc, 0);
	free(desc);
}

ODBCDescRec *
addODBCDescRec(ODBCDesc *desc, SQLSMALLINT recno)
{
	assert(desc);
	assert(recno > 0);

	if (desc->sql_desc_count < recno)
		setODBCDescRecCount(desc, recno);
	else {
		assert(desc->descRec != NULL);
		cleanODBCDescRec(desc, &desc->descRec[recno]);
	}

	return &desc->descRec[recno];
}

/* Return either the column size or display size for a column or parameter. */
SQLULEN
ODBCLength(ODBCDescRec *rec, int lengthtype)
{
	switch (rec->sql_desc_concise_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return rec->sql_desc_length * 4;
		else
			return rec->sql_desc_length;
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		return rec->sql_desc_length;
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		return rec->sql_desc_length + (lengthtype == SQL_DESC_LENGTH ? 0 : 2);
	case SQL_BIT:
		return 1;
	case SQL_TINYINT:
		switch (lengthtype) {
		case SQL_DESC_LENGTH:
			return 3;
		case SQL_DESC_DISPLAY_SIZE:
			return 3 + !rec->sql_desc_unsigned;
		case SQL_DESC_OCTET_LENGTH:
			return (int) sizeof(char);
		}
		break;
	case SQL_SMALLINT:
		switch (lengthtype) {
		case SQL_DESC_LENGTH:
			return 5;
		case SQL_DESC_DISPLAY_SIZE:
			return 5 + !rec->sql_desc_unsigned;
		case SQL_DESC_OCTET_LENGTH:
			return (int) sizeof(short);
		}
		break;
	case SQL_INTEGER:
		switch (lengthtype) {
		case SQL_DESC_LENGTH:
			return 10;
		case SQL_DESC_DISPLAY_SIZE:
			return 10 + !rec->sql_desc_unsigned;
		case SQL_DESC_OCTET_LENGTH:
			return (int) sizeof(int);
		}
		break;
	case SQL_BIGINT:
		switch (lengthtype) {
		case SQL_DESC_LENGTH:
			return 19 + (rec->sql_desc_unsigned != 0);
		case SQL_DESC_DISPLAY_SIZE:
		case SQL_DESC_OCTET_LENGTH:
			return 20;
		}
		break;
	case SQL_HUGEINT:
		switch (lengthtype) {
		case SQL_DESC_LENGTH:
			return 39 + (rec->sql_desc_unsigned != 0);
		case SQL_DESC_DISPLAY_SIZE:
		case SQL_DESC_OCTET_LENGTH:
			return 40;
		}
		break;
	case SQL_REAL:
		switch (lengthtype) {
		case SQL_DESC_LENGTH:
			return 7;
		case SQL_DESC_DISPLAY_SIZE:
			/* sign, 7 digits, decimal point, E, sign, 2 digits */
			return 14;
		case SQL_DESC_OCTET_LENGTH:
			return (int) sizeof(float);
		}
		break;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		switch (lengthtype) {
		case SQL_DESC_LENGTH:
			return 15;
		case SQL_DESC_DISPLAY_SIZE:
			/* sign, 15 digits, decimal point, E, sign, 3 digits */
			return 24;
		case SQL_DESC_OCTET_LENGTH:
			return (int) sizeof(double);
		}
		break;
	case SQL_TYPE_DATE:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_DATE_STRUCT);
		else {
			/* strlen("yyyy-mm-dd") */
			return 10;
		}
	case SQL_TYPE_TIME:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_TIME_STRUCT);
		else {
			/* strlen("hh:mm:ss.fff") */
			return 12;
		}
	case SQL_TYPE_TIMESTAMP:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_TIMESTAMP_STRUCT);
		else {
			/* strlen("yyyy-mm-dd hh:mm:ss.fff") */
			return 23;
		}
	case SQL_INTERVAL_SECOND:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'sss[.fff]' SECOND(p,q)") */
		return 11 + 13 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			(rec->sql_desc_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			(rec->sql_desc_precision > 0 ?
			 rec->sql_desc_precision + 1 :
			 0);
	case SQL_INTERVAL_DAY_TO_SECOND:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'ddd hh:mm:ss[.fff]' DAY(p) TO SECOND(q)") */
		return 11 + 21 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			(rec->sql_desc_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			9 +
			(rec->sql_desc_precision > 0 ?
			 rec->sql_desc_precision + 1 :
			 0);
	case SQL_INTERVAL_HOUR_TO_SECOND:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'hhh:mm:ss[.fff]' HOUR(p) TO SECOND(q)") */
		return 11 + 22 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			(rec->sql_desc_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			6 +
			(rec->sql_desc_precision > 0 ?
			 rec->sql_desc_precision + 1 :
			 0);
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'mmm:ss[.fff]' MINUTE(p) TO SECOND(q)") */
		return 11 + 24 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			(rec->sql_desc_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			3 +
			(rec->sql_desc_precision > 0 ?
			 rec->sql_desc_precision + 1 :
			 0);
	case SQL_INTERVAL_YEAR:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' YEAR(p)") */
		return 11 + 9 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision;
	case SQL_INTERVAL_MONTH:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' MONTH(p)") */
		return 11 + 10 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision;
	case SQL_INTERVAL_DAY:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' DAY(p)") */
		return 11 + 8 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision;
	case SQL_INTERVAL_HOUR:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' HOUR(p)") */
		return 11 + 9 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision;
	case SQL_INTERVAL_MINUTE:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' MINUTE(p)") */
		return 11 + 11 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision;
	case SQL_INTERVAL_YEAR_TO_MONTH:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' YEAR(p) TO MONTH") */
		return 11 + 18 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			3;
	case SQL_INTERVAL_DAY_TO_HOUR:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' DAY(p) TO HOUR") */
		return 11 + 16 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			3;
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' HOUR(p) TO MINUTE") */
		return 11 + 19 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			3;
	case SQL_INTERVAL_DAY_TO_MINUTE:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return (int) sizeof(SQL_INTERVAL_STRUCT);
		/* strlen("INTERVAL -'yyy' DAY(p) TO MINUTE") */
		return 11 + 18 +
			(rec->sql_desc_datetime_interval_precision > 10) +
			rec->sql_desc_datetime_interval_precision +
			6;
	case SQL_GUID:
		if (lengthtype == SQL_DESC_OCTET_LENGTH)
			return 16; /* sizeof(SQLGUID) */
		/* strlen("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") */
		return 36;
	default:
		break;
	}
	return SQL_NO_TOTAL;
}

/* the literal prefix and suffix strings depend on the sql_desc_concise_type
   and for specific MonetDB types (inet, url, json, timetz, timestamptz)
   on the sql_desc_type_name.
 */
void
fillLiteralPrefixSuffix(ODBCDescRec *rec)
{
	switch (rec->sql_desc_concise_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		if (rec->sql_desc_literal_prefix == NULL) {
			if (strcmp("inet", (char *)rec->sql_desc_type_name) == 0) {
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("inet '");
			} else
			if (strcmp("url", (char *)rec->sql_desc_type_name) == 0) {
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("url '");
			} else
			if (strcmp("json", (char *)rec->sql_desc_type_name) == 0) {
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("json '");
			} else {
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("'");
			}
		}
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("'");
		break;
	case SQL_TYPE_DATE:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("date '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("'");
		break;
	case SQL_TYPE_TIME:
		if (rec->sql_desc_literal_prefix == NULL) {
			if (strcmp("timetz", (char *)rec->sql_desc_type_name) == 0)
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("timetz '");
			else
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("time '");
		}
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("'");
		break;
	case SQL_TYPE_TIMESTAMP:
		if (rec->sql_desc_literal_prefix == NULL) {
			if (strcmp("timestamptz", (char *)rec->sql_desc_type_name) == 0)
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("timestamptz '");
			else
				rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("timestamp '");
		}
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("'");
		break;
	case SQL_INTERVAL_YEAR:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' year");
		break;
	case SQL_INTERVAL_YEAR_TO_MONTH:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' year to month");
		break;
	case SQL_INTERVAL_MONTH:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' month");
		break;
	case SQL_INTERVAL_DAY:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' day");
		break;
	case SQL_INTERVAL_DAY_TO_HOUR:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' day to hour");
		break;
	case SQL_INTERVAL_DAY_TO_MINUTE:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' day to minute");
		break;
	case SQL_INTERVAL_DAY_TO_SECOND:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' day to second");
		break;
	case SQL_INTERVAL_HOUR:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' hour");
		break;
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' hour to minute");
		break;
	case SQL_INTERVAL_HOUR_TO_SECOND:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' hour to second");
		break;
	case SQL_INTERVAL_MINUTE:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' minute");
		break;
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' minute to second");
		break;
	case SQL_INTERVAL_SECOND:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("interval '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("' second");
		break;
	case SQL_GUID:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("uuid '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("'");
		break;
	case SQL_LONGVARBINARY:
		if (rec->sql_desc_literal_prefix == NULL)
			rec->sql_desc_literal_prefix = (SQLCHAR *) strdup("blob '");
		if (rec->sql_desc_literal_suffix == NULL)
			rec->sql_desc_literal_suffix = (SQLCHAR *) strdup("'");
		break;
	default:
		break;
	}
}
