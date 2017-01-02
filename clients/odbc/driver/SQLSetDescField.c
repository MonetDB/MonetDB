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
 * SQLSetDescField()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN
MNDBSetDescField(ODBCDesc *desc,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT FieldIdentifier,
		 SQLPOINTER ValuePtr,
		 SQLINTEGER BufferLength)
{
	ODBCDescRec *rec;
	struct sql_types *tp;

	if (isAD(desc))
		tp = ODBC_c_types;
	else
		tp = ODBC_sql_types;

	switch (FieldIdentifier) {
	case SQL_DESC_ALLOC_TYPE:		/* SQLSMALLINT */
		/* Invalid descriptor field identifier */
		addDescError(desc, "HY091", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_ARRAY_SIZE:		/* SQLULEN */
		if ((SQLULEN) (uintptr_t) ValuePtr == 0) {
			/* Invalid attribute/option identifier */
			addDescError(desc, "HY092", NULL, 0);
			return SQL_ERROR;
		}
		if (isAD(desc)) {
			/* limit size to protect against bugs */
			if ((SQLULEN) (uintptr_t) ValuePtr > 10000) {
				/* Driver does not support this function */
				addDescError(desc, "IM001", NULL, 0);
				return SQL_ERROR;
			}
			desc->sql_desc_array_size = (SQLULEN) (uintptr_t) ValuePtr;
		}
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_STATUS_PTR:		/* SQLUSMALLINT * */
		desc->sql_desc_array_status_ptr = (SQLUSMALLINT *) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_BIND_OFFSET_PTR:		/* SQLLEN * */
		if (isAD(desc))
			desc->sql_desc_bind_offset_ptr = (SQLLEN *) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_BIND_TYPE:		/* SQLINTEGER */
		if (isAD(desc))
			desc->sql_desc_bind_type = (SQLINTEGER) (intptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_COUNT:			/* SQLSMALLINT */
		if (isIRD(desc)) {
			/* Invalid descriptor field identifier */
			addDescError(desc, "HY091", NULL, 0);
			return SQL_ERROR;
		}
		setODBCDescRecCount(desc, (int) (SQLSMALLINT) (intptr_t) ValuePtr);
		return SQL_SUCCESS;
	case SQL_DESC_ROWS_PROCESSED_PTR:	/* SQLULEN * */
		if (desc->Stmt)
			desc->sql_desc_rows_processed_ptr = (SQLULEN *) ValuePtr;
		return SQL_SUCCESS;
	}

	if (RecNumber <= 0) {
		/* Invalid descriptor index */
		addDescError(desc, "07009", NULL, 0);
		return SQL_ERROR;
	}
	if (RecNumber > desc->sql_desc_count)
		return SQL_NO_DATA;

	if (isIRD(desc)) {
		/* the Implementation Row Descriptor is read-only */
		/* Invalid descriptor field identifier */
		addDescError(desc, "HY091", NULL, 0);
		return SQL_ERROR;
	}

	rec = &desc->descRec[RecNumber];

	/* break for read-only fields since the error is the same as
	   unknown FieldIdentifier */
	switch (FieldIdentifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:	/* SQLINTEGER */
	case SQL_DESC_BASE_COLUMN_NAME:		/* SQLCHAR * */
	case SQL_DESC_BASE_TABLE_NAME:		/* SQLCHAR * */
	case SQL_DESC_CASE_SENSITIVE:		/* SQLINTEGER */
	case SQL_DESC_CATALOG_NAME:		/* SQLCHAR * */
	case SQL_DESC_DISPLAY_SIZE:		/* SQLLEN */
	case SQL_DESC_FIXED_PREC_SCALE:		/* SQLSMALLINT */
	case SQL_DESC_LABEL:			/* SQLCHAR * */
	case SQL_DESC_LITERAL_PREFIX:		/* SQLCHAR * */
	case SQL_DESC_LITERAL_SUFFIX:		/* SQLCHAR * */
	case SQL_DESC_LOCAL_TYPE_NAME:		/* SQLCHAR * */
	case SQL_DESC_NULLABLE:			/* SQLSMALLINT */
	case SQL_DESC_ROWVER:			/* SQLSMALLINT */
	case SQL_DESC_SCHEMA_NAME:		/* SQLCHAR * */
	case SQL_DESC_SEARCHABLE:		/* SQLSMALLINT */
	case SQL_DESC_TABLE_NAME:		/* SQLCHAR * */
	case SQL_DESC_TYPE_NAME:		/* SQLCHAR * */
	case SQL_DESC_UNSIGNED:			/* SQLSMALLINT */
	case SQL_DESC_UPDATABLE:		/* SQLSMALLINT */
		break;		/* read-only or unused */
	case SQL_DESC_CONCISE_TYPE:		/* SQLSMALLINT */
		while (tp->concise_type != 0) {
			if ((intptr_t) tp->concise_type == (intptr_t) ValuePtr) {
				rec->sql_desc_concise_type = tp->concise_type;
				rec->sql_desc_type = tp->type;
				rec->sql_desc_datetime_interval_code = tp->code;
				if (tp->precision != UNAFFECTED)
					rec->sql_desc_precision = tp->precision;
				if (tp->datetime_interval_precision != UNAFFECTED)
					rec->sql_desc_datetime_interval_precision = tp->datetime_interval_precision;
				if (tp->length != UNAFFECTED)
					rec->sql_desc_length = tp->length;
				if (tp->scale != UNAFFECTED)
					rec->sql_desc_scale = tp->scale;
				rec->sql_desc_fixed_prec_scale = tp->fixed;
				rec->sql_desc_num_prec_radix = tp->radix;
				return SQL_SUCCESS;
			}
			tp++;
		}
		/* Invalid attribute/option identifier */
		addDescError(desc, "HY092", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_DATA_PTR:			/* SQLPOINTER */
		/* TODO: consistency check */
		rec->sql_desc_data_ptr = ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_DATETIME_INTERVAL_CODE:	/* SQLSMALLINT */
		while (tp->concise_type != 0) {
			if ((intptr_t) tp->code == (intptr_t) ValuePtr &&
			    tp->type == rec->sql_desc_type) {
				rec->sql_desc_concise_type = tp->concise_type;
				rec->sql_desc_type = tp->type;
				rec->sql_desc_datetime_interval_code = tp->code;
				if (tp->precision != UNAFFECTED)
					rec->sql_desc_precision = tp->precision;
				if (tp->datetime_interval_precision != UNAFFECTED)
					rec->sql_desc_datetime_interval_precision = tp->datetime_interval_precision;
				if (tp->length != UNAFFECTED)
					rec->sql_desc_length = tp->length;
				if (tp->scale != UNAFFECTED)
					rec->sql_desc_scale = tp->scale;
				rec->sql_desc_fixed_prec_scale = tp->fixed;
				rec->sql_desc_num_prec_radix = tp->radix;
				return SQL_SUCCESS;
			}
			tp++;
		}
		/* Inconsistent descriptor information */
		addDescError(desc, "HY021", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_DATETIME_INTERVAL_PRECISION: /* SQLINTEGER */
		rec->sql_desc_datetime_interval_precision = (SQLINTEGER) (intptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_INDICATOR_PTR:		/* SQLLEN * */
		if (isAD(desc))
			rec->sql_desc_indicator_ptr = (SQLLEN *) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_LENGTH:			/* SQLULEN */
		rec->sql_desc_length = (SQLULEN) (uintptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_NAME:			/* SQLCHAR * */
		if (isID(desc)) {
			fixODBCstring(ValuePtr, BufferLength, SQLINTEGER,
				      addDescError, desc, return SQL_ERROR);
			if (rec->sql_desc_name != NULL)
				free(rec->sql_desc_name);
			rec->sql_desc_name = (SQLCHAR *) dupODBCstring((SQLCHAR *) ValuePtr, (size_t) BufferLength);
			if (rec->sql_desc_name == NULL) {
				/* Memory allocation error */
				addDescError(desc, "HY001", NULL, 0);
				return SQL_ERROR;
			}
			rec->sql_desc_unnamed = *rec->sql_desc_name ? SQL_NAMED : SQL_UNNAMED;
		}
		return SQL_SUCCESS;
	case SQL_DESC_NUM_PREC_RADIX:
		rec->sql_desc_num_prec_radix = (SQLINTEGER) (intptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH:		/* SQLLEN */
		rec->sql_desc_octet_length = (SQLLEN) (intptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH_PTR:		/* SQLLEN * */
		if (isAD(desc))
			rec->sql_desc_octet_length_ptr = (SQLLEN *) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_PARAMETER_TYPE:		/* SQLSMALLINT */
		switch ((SQLSMALLINT) (intptr_t) ValuePtr) {
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
			rec->sql_desc_parameter_type = (SQLSMALLINT) (intptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_PRECISION:		/* SQLSMALLINT */
		rec->sql_desc_precision = (SQLSMALLINT) (intptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_SCALE:			/* SQLSMALLINT */
		rec->sql_desc_scale = (SQLSMALLINT) (intptr_t) ValuePtr;
		return SQL_SUCCESS;
	case SQL_DESC_TYPE:			/* SQLSMALLINT */
		while (tp->concise_type != 0) {
			if ((SQLSMALLINT) (intptr_t) ValuePtr == tp->type &&
			    (((SQLSMALLINT) (intptr_t) ValuePtr != SQL_DATETIME &&
			      (SQLSMALLINT) (intptr_t) ValuePtr != SQL_INTERVAL) ||
			     tp->code == rec->sql_desc_datetime_interval_code)) {
				rec->sql_desc_concise_type = tp->concise_type;
				rec->sql_desc_type = tp->type;
				rec->sql_desc_datetime_interval_code = tp->code;
				if (tp->precision != UNAFFECTED)
					rec->sql_desc_precision = tp->precision;
				if (tp->datetime_interval_precision != UNAFFECTED)
					rec->sql_desc_datetime_interval_precision = tp->datetime_interval_precision;
				if (tp->length != UNAFFECTED)
					rec->sql_desc_length = tp->length;
				if (tp->scale != UNAFFECTED)
					rec->sql_desc_scale = tp->scale;
				rec->sql_desc_fixed_prec_scale = tp->fixed;
				rec->sql_desc_num_prec_radix = tp->radix;
				return SQL_SUCCESS;
			}
			tp++;
		}
		/* Inconsistent descriptor information */
		addDescError(desc, "HY021", NULL, 0);
		return SQL_ERROR;
	case SQL_DESC_UNNAMED:			/* SQLSMALLINT */
		if ((SQLSMALLINT) (intptr_t) ValuePtr == SQL_NAMED) {
			/* Invalid descriptor field identifier */
			addDescError(desc, "HY091", NULL, 0);
			return SQL_ERROR;
		} else if ((SQLSMALLINT) (intptr_t) ValuePtr == SQL_UNNAMED && isIPD(desc)) {
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

	/* Invalid descriptor field identifier */
	addDescError(desc, "HY091", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLSetDescField(SQLHDESC DescriptorHandle,
		SQLSMALLINT RecNumber,
		SQLSMALLINT FieldIdentifier,
		SQLPOINTER ValuePtr,
		SQLINTEGER BufferLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetDescField " PTRFMT " %d %s " PTRFMT " %d\n",
		PTRFMTCAST DescriptorHandle, (int) RecNumber,
		translateFieldIdentifier(FieldIdentifier),
		PTRFMTCAST ValuePtr, (int) BufferLength);
#endif

	if (!isValidDesc((ODBCDesc *) DescriptorHandle))
		return SQL_INVALID_HANDLE;

	clearDescErrors((ODBCDesc *) DescriptorHandle);

	return MNDBSetDescField((ODBCDesc *) DescriptorHandle, RecNumber, FieldIdentifier, ValuePtr, BufferLength);
}

SQLRETURN SQL_API
SQLSetDescFieldW(SQLHDESC DescriptorHandle,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT FieldIdentifier,
		 SQLPOINTER ValuePtr,
		 SQLINTEGER BufferLength)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;
	SQLRETURN rc;
	SQLPOINTER ptr;
	SQLINTEGER n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetDescFieldW " PTRFMT " %d %s " PTRFMT " %d\n",
		PTRFMTCAST DescriptorHandle, (int) RecNumber,
		translateFieldIdentifier(FieldIdentifier),
		PTRFMTCAST ValuePtr, (int) BufferLength);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	clearDescErrors(desc);

	switch (FieldIdentifier) {
	case SQL_DESC_NAME:
		if (BufferLength > 0)	/* convert from bytes to characters */
			BufferLength /= 2;
		fixWcharIn(ValuePtr, BufferLength, SQLCHAR, ptr,
			   addDescError, desc, return SQL_ERROR);
		n = SQL_NTS;
		break;
	default:
		ptr = ValuePtr;
		n = BufferLength;
		break;
	}

	rc = MNDBSetDescField(desc, RecNumber, FieldIdentifier, ptr, n);

	if (ptr && ptr != ValuePtr)
		free(ptr);

	return rc;
}
