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

SQLRETURN
SQLSetDescField_(ODBCDesc *desc, SQLSMALLINT RecordNumber,
		 SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
		 SQLINTEGER BufferLength)
{
	ODBCDescRec *rec;
	struct sql_types *p;

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

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
		for (p = sql_types; p < &sql_types[NSQL_TYPES]; p++) {
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
		for (p = sql_types; p < &sql_types[NSQL_TYPES]; p++) {
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
			rec->sql_desc_name = (SQLCHAR *) dupODBCstring((SQLCHAR *) Value, BufferLength);
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
		for (p = sql_types; p < &sql_types[NSQL_TYPES]; p++) {
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
