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
 * SQLSetDescField()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

/* SQL_DESC_CONCISE_TYPE, SQL_DESC_DATETIME_INTERVAL_CODE, and
   SQL_DESC_TYPE are interdependend and setting one affects the other.
   Also, setting them affect other fields.  This is all encoded in
   this table.  If a field is equal to UNAFFECTED, it is not
   affected. */
struct sql_types {
	int concise_type;
	int type;
	int code;
	int precision;
	int datetime_interval_precision;
	int length;
	int scale;
};

static struct sql_types sql_types[] = {
	{SQL_CHAR, SQL_CHAR, 0, 0, UNAFFECTED, 1, UNAFFECTED,},
	{SQL_VARCHAR, SQL_VARCHAR, 0, 0, UNAFFECTED, 1, UNAFFECTED,},
	{SQL_LONGVARCHAR, SQL_LONGVARCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_WCHAR, SQL_WCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_WVARCHAR, SQL_WVARCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_WLONGVARCHAR, SQL_WLONGVARCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_DECIMAL, SQL_DECIMAL, 0, 17, UNAFFECTED, UNAFFECTED, 0},
	{SQL_NUMERIC, SQL_NUMERIC, 0, 17, UNAFFECTED, UNAFFECTED, 0},
	{SQL_BIT, SQL_BIT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_TINYINT, SQL_TINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_SMALLINT, SQL_SMALLINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_INTEGER, SQL_INTEGER, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_BIGINT, SQL_BIGINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_REAL, SQL_REAL, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_FLOAT, SQL_FLOAT, 0, 11, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_DOUBLE, SQL_DOUBLE, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_BINARY, SQL_BINARY, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_VARBINARY, SQL_VARBINARY, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_LONGVARBINARY, SQL_LONGVARBINARY, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_GUID, SQL_GUID, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_TYPE_DATE, SQL_DATETIME, SQL_CODE_DATE, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED,},
	{SQL_TYPE_TIME, SQL_DATETIME, SQL_CODE_TIME, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED,},
	{SQL_TYPE_TIMESTAMP, SQL_DATETIME, SQL_CODE_TIMESTAMP, 6, UNAFFECTED, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_MONTH, SQL_INTERVAL, SQL_CODE_MONTH, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_YEAR, SQL_INTERVAL, SQL_CODE_YEAR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_YEAR_TO_MONTH, SQL_INTERVAL, SQL_CODE_YEAR_TO_MONTH, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_DAY, SQL_INTERVAL, SQL_CODE_DAY, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_HOUR, SQL_INTERVAL, SQL_CODE_HOUR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_MINUTE, SQL_INTERVAL, SQL_CODE_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_SECOND, SQL_INTERVAL, SQL_CODE_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL, SQL_CODE_DAY_TO_HOUR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_DAY_TO_MINUTE, SQL_INTERVAL, SQL_CODE_DAY_TO_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL, SQL_CODE_DAY_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL, SQL_CODE_HOUR_TO_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL, SQL_CODE_HOUR_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
	{SQL_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL, SQL_CODE_MINUTE_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
};
#define NSQL_TYPES	(sizeof(sql_types)/sizeof(sql_types[0]))

static struct sql_types c_types[] = {
	{SQL_C_CHAR, SQL_C_CHAR, 0, 0, UNAFFECTED, 1, UNAFFECTED,},
	{SQL_C_WCHAR, SQL_C_WCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_BIT, SQL_C_BIT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_NUMERIC, SQL_C_NUMERIC, 0, 17, UNAFFECTED, UNAFFECTED, 0},
	{SQL_C_STINYINT, SQL_C_STINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_UTINYINT, SQL_C_UTINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_TINYINT, SQL_C_TINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_SBIGINT, SQL_C_SBIGINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_UBIGINT, SQL_C_UBIGINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_SSHORT, SQL_C_SSHORT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_USHORT, SQL_C_USHORT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_SHORT, SQL_C_SHORT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_SLONG, SQL_C_SLONG, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_ULONG, SQL_C_ULONG, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_LONG, SQL_C_LONG, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_FLOAT, SQL_C_FLOAT, 0, 11, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_DOUBLE, SQL_C_DOUBLE, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_BINARY, SQL_C_BINARY, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
	{SQL_C_TYPE_DATE, SQL_DATETIME, SQL_CODE_DATE, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED,},
	{SQL_C_TYPE_TIME, SQL_DATETIME, SQL_CODE_TIME, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED,},
	{SQL_C_TYPE_TIMESTAMP, SQL_DATETIME, SQL_CODE_TIMESTAMP, 6, UNAFFECTED, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_MONTH, SQL_INTERVAL, SQL_CODE_MONTH, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_YEAR, SQL_INTERVAL, SQL_CODE_YEAR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_YEAR_TO_MONTH, SQL_INTERVAL, SQL_CODE_YEAR_TO_MONTH, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_DAY, SQL_INTERVAL, SQL_CODE_DAY, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_HOUR, SQL_INTERVAL, SQL_CODE_HOUR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_MINUTE, SQL_INTERVAL, SQL_CODE_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_SECOND, SQL_INTERVAL, SQL_CODE_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL, SQL_CODE_DAY_TO_HOUR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_DAY_TO_MINUTE, SQL_INTERVAL, SQL_CODE_DAY_TO_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL, SQL_CODE_DAY_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL, SQL_CODE_HOUR_TO_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL, SQL_CODE_HOUR_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
	{SQL_C_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL, SQL_CODE_MINUTE_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED,},
	{SQL_C_GUID, SQL_C_GUID, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED},
};
#define NC_TYPES	(sizeof(c_types)/sizeof(c_types[0]))

SQLRETURN
SQLSetDescField_(ODBCDesc *desc, SQLSMALLINT RecordNumber,
		 SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
		 SQLINTEGER BufferLength)
{
	ODBCDescRec *rec;
	struct sql_types *p;
	struct sql_types *start, *end;

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	if (isAD(desc)) {
		start = c_types;
		end = c_types + NC_TYPES;
	} else {
		start = sql_types;
		end = sql_types + NSQL_TYPES;
	}

	switch (FieldIdentifier) {
	case SQL_DESC_ALLOC_TYPE:
		/* Invalid descriptor field identifier */
		addDescError(desc, "HY091", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_ARRAY_SIZE:
		if (isAD(desc)) {
			if ((SQLUINTEGER) (size_t) Value != 1) {
				/* driver does not support this feature */
				addDescError(desc, "IM001", NULL, 0);
				return SQL_ERROR;
			}
			desc->sql_desc_array_size = (SQLUINTEGER) (size_t) Value;
		}
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_STATUS_PTR:
		desc->sql_desc_array_status_ptr = (SQLUSMALLINT *) Value;
		return SQL_SUCCESS;
	case SQL_DESC_BIND_OFFSET_PTR:
		if (isAD(desc))
			desc->sql_desc_bind_offset_ptr = (SQLINTEGER *) Value;
		return SQL_SUCCESS;
	case SQL_DESC_BIND_TYPE:
		if (isAD(desc)) {
			if ((SQLUINTEGER) (size_t) Value != SQL_BIND_BY_COLUMN) {
				/* driver does not support this feature */
				addDescError(desc, "IM001", NULL, 0);
				return SQL_ERROR;
			}
			desc->sql_desc_bind_type = (SQLUINTEGER) (size_t) Value;
		}
		return SQL_SUCCESS;
	case SQL_DESC_COUNT:
		if (isIRD(desc)) {
			addDescError(desc, "HY091", NULL, 0);
			return SQL_ERROR;
		}
		setODBCDescRecCount(desc, (int) (SQLSMALLINT) (ssize_t) Value);
		return SQL_SUCCESS;
	case SQL_DESC_ROWS_PROCESSED_PTR:
		if (desc->Stmt)
			desc->sql_desc_rows_processed_ptr = (SQLUINTEGER *) Value;
		return SQL_SUCCESS;
	}

	if (RecordNumber <= 0) {
		addDescError(desc, "07009", NULL, 0);
		return SQL_ERROR;
	}
	if (RecordNumber > desc->sql_desc_count)
		return SQL_NO_DATA;

	rec = &desc->descRec[RecordNumber];

	if (isIRD(desc)) {
		/* the Implementation Row Descriptor is read-only */
		/* HY091: Invalid descriptor field identifier */
		addDescError(desc, "HY091", NULL, 0);
		return SQL_ERROR;
	}

	/* break for read-only fields since the error is the same as
	   unknown FieldIdentifier */
	switch (FieldIdentifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_BASE_COLUMN_NAME:
	case SQL_DESC_BASE_TABLE_NAME:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_CATALOG_NAME:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_LABEL:
	case SQL_DESC_LITERAL_PREFIX:
	case SQL_DESC_LITERAL_SUFFIX:
	case SQL_DESC_LOCAL_TYPE_NAME:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_ROWVER:
	case SQL_DESC_SCHEMA_NAME:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_TYPE_NAME:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE:
		break;		/* read-only or unused */
	case SQL_DESC_CONCISE_TYPE:
		for (p = start; p < end; p++) {
			if (p->concise_type == (SQLSMALLINT) (ssize_t) Value) {
				rec->sql_desc_concise_type = p->concise_type;
				rec->sql_desc_type = p->type;
				rec->sql_desc_datetime_interval_code = p->code;
				if (p->precision != UNAFFECTED)
					rec->sql_desc_precision = p->precision;
				if (p->datetime_interval_precision != UNAFFECTED)
					rec->sql_desc_datetime_interval_precision = p->datetime_interval_precision;
				if (p->length != UNAFFECTED)
					rec->sql_desc_length = p->length;
				if (p->scale != UNAFFECTED)
					rec->sql_desc_scale = p->scale;
				return SQL_SUCCESS;
			}
		}
		/* Inconsistent descriptor information */
		addDescError(desc, "HY021", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_DATA_PTR:
		/* TODO: consistency check */
		rec->sql_desc_data_ptr = Value;
		return SQL_SUCCESS;
	case SQL_DESC_DATETIME_INTERVAL_CODE:
		for (p = start; p < end; p++) {
			if (p->code == (SQLSMALLINT) (ssize_t) Value &&
			    p->type == rec->sql_desc_type) {
				rec->sql_desc_concise_type = p->concise_type;
				rec->sql_desc_type = p->type;
				rec->sql_desc_datetime_interval_code = p->code;
				if (p->precision != UNAFFECTED)
					rec->sql_desc_precision = p->precision;
				if (p->datetime_interval_precision != UNAFFECTED)
					rec->sql_desc_datetime_interval_precision = p->datetime_interval_precision;
				if (p->length != UNAFFECTED)
					rec->sql_desc_length = p->length;
				if (p->scale != UNAFFECTED)
					rec->sql_desc_scale = p->scale;
				return SQL_SUCCESS;
			}
		}
		addDescError(desc, "HY021", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_DATETIME_INTERVAL_PRECISION:
		rec->sql_desc_datetime_interval_precision = (SQLINTEGER) (ssize_t) Value;
		return SQL_SUCCESS;
	case SQL_DESC_INDICATOR_PTR:
		if (isAD(desc))
			rec->sql_desc_indicator_ptr = (SQLINTEGER *) Value;
		return SQL_SUCCESS;
	case SQL_DESC_LENGTH:
		rec->sql_desc_length = (SQLUINTEGER) (size_t) Value;
		return SQL_SUCCESS;
	case SQL_DESC_NAME:
		if (isID(desc)) {
			fixODBCstring(Value, BufferLength, addDescError, desc);
			if (rec->sql_desc_name != NULL)
				free(rec->sql_desc_name);
			rec->sql_desc_name = (SQLCHAR *) dupODBCstring((SQLCHAR *) Value, (size_t) BufferLength);
			rec->sql_desc_unnamed = rec->sql_desc_name ? SQL_NAMED : SQL_UNNAMED;
		}
		return SQL_SUCCESS;
	case SQL_DESC_NUM_PREC_RADIX:
		rec->sql_desc_num_prec_radix = (SQLINTEGER) (ssize_t) Value;
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH:
		rec->sql_desc_octet_length = (SQLINTEGER) (ssize_t) Value;
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH_PTR:
		if (isAD(desc))
			rec->sql_desc_octet_length_ptr = (SQLINTEGER *) Value;
		return SQL_SUCCESS;
	case SQL_DESC_PARAMETER_TYPE:
		switch ((SQLINTEGER) (ssize_t) Value) {
		case SQL_PARAM_INPUT:
			break;
		case SQL_PARAM_INPUT_OUTPUT:
		case SQL_PARAM_OUTPUT:
			/* Driver does not support this function */
			addDescError(desc, "IM001", NULL, 0);
			return SQL_ERROR;
		default:
			/* Invalid attribute/option identifier */
			addDescError(desc, "HY092", NULL, 0);
			return SQL_ERROR;
		}
		if (isIPD(desc))
			rec->sql_desc_parameter_type = (SQLINTEGER) (ssize_t) Value;
		return SQL_SUCCESS;
	case SQL_DESC_PRECISION:
		rec->sql_desc_precision = (SQLSMALLINT) (ssize_t) Value;
		return SQL_SUCCESS;
	case SQL_DESC_SCALE:
		rec->sql_desc_scale = (SQLSMALLINT) (ssize_t) Value;
		return SQL_SUCCESS;
	case SQL_DESC_TYPE:
		for (p = start; p < end; p++) {
			if (p->type == (SQLSMALLINT) (ssize_t) Value &&
			    (((SQLSMALLINT) (ssize_t) Value != SQL_DATETIME &&
			      (SQLSMALLINT) (ssize_t) Value != SQL_INTERVAL) ||
			     p->code == rec->sql_desc_datetime_interval_code)) {
				rec->sql_desc_concise_type = p->concise_type;
				rec->sql_desc_type = p->type;
				rec->sql_desc_datetime_interval_code = p->code;
				if (p->precision != UNAFFECTED)
					rec->sql_desc_precision = p->precision;
				if (p->datetime_interval_precision != UNAFFECTED)
					rec->sql_desc_datetime_interval_precision = p->datetime_interval_precision;
				if (p->length != UNAFFECTED)
					rec->sql_desc_length = p->length;
				if (p->scale != UNAFFECTED)
					rec->sql_desc_scale = p->scale;
				return SQL_SUCCESS;
			}
		}
		/* Inconsistent descriptor information */
		addDescError(desc, "HY021", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_UNNAMED:
		if ((SQLSMALLINT) (ssize_t) Value == SQL_NAMED) {
			addDescError(desc, "HY091", NULL, 0);
			return SQL_ERROR;
		} else if ((SQLSMALLINT) (ssize_t) Value == SQL_UNNAMED &&
			   isIPD(desc)) {
			rec->sql_desc_unnamed = SQL_UNNAMED;
			if (rec->sql_desc_name)
				free(rec->sql_desc_name);
			rec->sql_desc_name = NULL;
			return SQL_SUCCESS;
		}
		/* Inconsistent descriptor information */
		addDescError(desc, "HY021", NULL, 0);
		return SQL_ERROR;
	}

	/* HY091: Invalid descriptor field identifier */
	addDescError(desc, "HY091", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN
SQLSetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
		SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
		SQLINTEGER BufferLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetDescField %d %d\n", RecordNumber, FieldIdentifier);
#endif

	return SQLSetDescField_((ODBCDesc *) DescriptorHandle, RecordNumber,
				FieldIdentifier, Value, BufferLength);
}
