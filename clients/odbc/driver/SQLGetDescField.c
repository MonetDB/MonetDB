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
 * SQLGetDescField()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN
MNDBGetDescField(ODBCDesc *desc,
		 SQLSMALLINT RecordNumber,
		 SQLSMALLINT FieldIdentifier,
		 SQLPOINTER ValuePtr,
		 SQLINTEGER BufferLength,
		 SQLINTEGER *StringLengthPtr)
{
	ODBCDescRec *rec;

	if (isIRD(desc)) {
		if (desc->Stmt->State == INITED) {
			/* Function sequence error */
			addDescError(desc, "HY010", NULL, 0);
			return SQL_ERROR;
		}
		if (desc->Stmt->State == EXECUTED0) {
			/* Invalid cursor state */
			addDescError(desc, "24000", NULL, 0);
			return SQL_ERROR;
		}
		if (desc->Stmt->State == PREPARED0)
			return SQL_NO_DATA;
	}

	/* header fields ignore RecordNumber */
	switch (FieldIdentifier) {
	case SQL_DESC_ALLOC_TYPE:		/* SQLSMALLINT */
		WriteData(ValuePtr, desc->sql_desc_alloc_type, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_SIZE:		/* SQLULEN */
		if (isAD(desc))
			WriteData(ValuePtr, desc->sql_desc_array_size, SQLULEN);
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_STATUS_PTR:		/* SQLUSMALLINT * */
		WriteData(ValuePtr, desc->sql_desc_array_status_ptr, SQLUSMALLINT *);
		return SQL_SUCCESS;
	case SQL_DESC_BIND_OFFSET_PTR:		/* SQLLEN * */
		if (isAD(desc))
			WriteData(ValuePtr, desc->sql_desc_bind_offset_ptr, SQLLEN *);
		return SQL_SUCCESS;
	case SQL_DESC_BIND_TYPE:		/* SQLINTEGER */
		if (isAD(desc))
			WriteData(ValuePtr, desc->sql_desc_bind_type, SQLINTEGER);
		return SQL_SUCCESS;
	case SQL_DESC_COUNT:			/* SQLSMALLINT */
		WriteData(ValuePtr, desc->sql_desc_count, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_ROWS_PROCESSED_PTR:	/* SQLULEN * */
		if (desc->Stmt)
			WriteData(ValuePtr, desc->sql_desc_rows_processed_ptr, SQLULEN *);
		return SQL_SUCCESS;
	}

	if (RecordNumber <= 0) {
		/* Invalid descriptor index */
		addDescError(desc, "07009", NULL, 0);
		return SQL_ERROR;
	}
	if (RecordNumber > desc->sql_desc_count)
		return SQL_NO_DATA;

	rec = &desc->descRec[RecordNumber];

	switch (FieldIdentifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:	/* SQLINTEGER */
		if (isIRD(desc))
			WriteData(ValuePtr, rec->sql_desc_auto_unique_value, SQLINTEGER);
		return SQL_SUCCESS;
	case SQL_DESC_BASE_COLUMN_NAME:		/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_base_column_name,
				   strlen((char *) rec->sql_desc_base_column_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_BASE_TABLE_NAME:		/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_base_table_name,
				   strlen((char *) rec->sql_desc_base_table_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_CASE_SENSITIVE:		/* SQLINTEGER */
		if (isID(desc))
			WriteData(ValuePtr, rec->sql_desc_case_sensitive, SQLINTEGER);
		return SQL_SUCCESS;
	case SQL_DESC_CATALOG_NAME:		/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_catalog_name,
				   strlen((char *) rec->sql_desc_catalog_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_CONCISE_TYPE:		/* SQLSMALLINT */
		WriteData(ValuePtr, rec->sql_desc_concise_type, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_DATA_PTR:			/* SQLPOINTER */
#ifndef STATIC_CODE_ANALYSIS
		/* Coverity doesn't like the debug print in WriteData,
		 * so we hide this whole thing */
		if (!isIRD(desc))
			WriteData(ValuePtr, rec->sql_desc_data_ptr, SQLPOINTER);
#endif
		return SQL_SUCCESS;
	case SQL_DESC_DATETIME_INTERVAL_CODE:	/* SQLSMALLINT */
		WriteData(ValuePtr, rec->sql_desc_datetime_interval_code, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_DATETIME_INTERVAL_PRECISION: /* SQLINTEGER */
		WriteData(ValuePtr, rec->sql_desc_datetime_interval_precision, SQLINTEGER);
		return SQL_SUCCESS;
	case SQL_DESC_DISPLAY_SIZE:		/* SQLLEN */
		if (isIRD(desc))
			WriteData(ValuePtr, (SQLINTEGER) rec->sql_desc_display_size, SQLLEN);
		return SQL_SUCCESS;
	case SQL_DESC_FIXED_PREC_SCALE:		/* SQLSMALLINT */
		if (isID(desc))
			WriteData(ValuePtr, rec->sql_desc_fixed_prec_scale, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_INDICATOR_PTR:		/* SQLLEN * */
		if (isAD(desc))
			WriteData(ValuePtr, rec->sql_desc_indicator_ptr, SQLLEN *);
		return SQL_SUCCESS;
	case SQL_DESC_LABEL:			/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_label,
				   strlen((char *) rec->sql_desc_label),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_LENGTH:			/* SQLULEN */
		WriteData(ValuePtr, (SQLUINTEGER) rec->sql_desc_length, SQLULEN);
		return SQL_SUCCESS;
	case SQL_DESC_LITERAL_PREFIX:		/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_literal_prefix,
				   strlen((char *) rec->sql_desc_literal_prefix),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_LITERAL_SUFFIX:		/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_literal_suffix,
				   strlen((char *) rec->sql_desc_literal_suffix),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_LOCAL_TYPE_NAME:		/* SQLCHAR * */
		if (isID(desc))
			copyString(rec->sql_desc_local_type_name,
				   strlen((char *) rec->sql_desc_local_type_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_NAME:			/* SQLCHAR * */
		if (isID(desc))
			copyString(rec->sql_desc_name,
				   strlen((char *) rec->sql_desc_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_NULLABLE:			/* SQLSMALLINT */
		if (isID(desc))
			WriteData(ValuePtr, rec->sql_desc_nullable, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_NUM_PREC_RADIX:		/* SQLINTEGER */
		WriteData(ValuePtr, rec->sql_desc_num_prec_radix, SQLINTEGER);
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH:		/* SQLLEN */
		WriteData(ValuePtr, rec->sql_desc_octet_length, SQLLEN);
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH_PTR:		/* SQLLEN * */
		if (isAD(desc))
			WriteData(ValuePtr, rec->sql_desc_octet_length_ptr, SQLLEN *);
		return SQL_SUCCESS;
	case SQL_DESC_PARAMETER_TYPE:		/* SQLSMALLINT */
		if (isIPD(desc))
			WriteData(ValuePtr, rec->sql_desc_parameter_type, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_PRECISION:		/* SQLSMALLINT */
		WriteData(ValuePtr, rec->sql_desc_precision, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_ROWVER:			/* SQLSMALLINT */
		if (isID(desc))
			WriteData(ValuePtr, rec->sql_desc_rowver, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_SCALE:			/* SQLSMALLINT */
		WriteData(ValuePtr, rec->sql_desc_scale, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_SCHEMA_NAME:		/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_schema_name,
				   strlen((char *) rec->sql_desc_schema_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_SEARCHABLE:		/* SQLSMALLINT */
		if (isIRD(desc))
			WriteData(ValuePtr, rec->sql_desc_searchable, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_TABLE_NAME:		/* SQLCHAR * */
		if (isIRD(desc))
			copyString(rec->sql_desc_table_name,
				   strlen((char *) rec->sql_desc_table_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_TYPE:			/* SQLSMALLINT */
		WriteData(ValuePtr, rec->sql_desc_type, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_TYPE_NAME:		/* SQLCHAR * */
		if (isID(desc))
			copyString(rec->sql_desc_type_name,
				   strlen((char *) rec->sql_desc_type_name),
				   ValuePtr, BufferLength, StringLengthPtr,
				   SQLINTEGER, addDescError, desc,
				   return SQL_ERROR);
		return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
	case SQL_DESC_UNNAMED:			/* SQLSMALLINT */
		if (isID(desc))
			WriteData(ValuePtr, rec->sql_desc_unnamed, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_UNSIGNED:			/* SQLSMALLINT */
		if (isID(desc))
			WriteData(ValuePtr, rec->sql_desc_unsigned, SQLSMALLINT);
		return SQL_SUCCESS;
	case SQL_DESC_UPDATABLE:		/* SQLSMALLINT */
		if (isIRD(desc))
			WriteData(ValuePtr, rec->sql_desc_updatable, SQLSMALLINT);
		return SQL_SUCCESS;
	}

	/* Invalid descriptor field identifier */
	addDescError(desc, "HY091", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLGetDescField(SQLHDESC DescriptorHandle,
		SQLSMALLINT RecordNumber,
		SQLSMALLINT FieldIdentifier,
		SQLPOINTER ValuePtr,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescField " PTRFMT " %d %s " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST DescriptorHandle, (int) RecordNumber,
		translateFieldIdentifier(FieldIdentifier),
		PTRFMTCAST ValuePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr);
#endif

	if (!isValidDesc((ODBCDesc *) DescriptorHandle))
		return SQL_INVALID_HANDLE;
	clearDescErrors((ODBCDesc *) DescriptorHandle);

	return MNDBGetDescField((ODBCDesc *) DescriptorHandle,
				RecordNumber,
				FieldIdentifier,
				ValuePtr,
				BufferLength,
				StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetDescFieldA(SQLHDESC DescriptorHandle,
		 SQLSMALLINT RecordNumber,
		 SQLSMALLINT FieldIdentifier,
		 SQLPOINTER ValuePtr,
		 SQLINTEGER BufferLength,
		 SQLINTEGER *StringLengthPtr)
{
	return SQLGetDescField(DescriptorHandle,
			       RecordNumber,
			       FieldIdentifier,
			       ValuePtr,
			       BufferLength,
			       StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetDescFieldW(SQLHDESC DescriptorHandle,
		 SQLSMALLINT RecordNumber,
		 SQLSMALLINT FieldIdentifier,
		 SQLPOINTER ValuePtr,
		 SQLINTEGER BufferLength,
		 SQLINTEGER *StringLengthPtr)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;
	SQLRETURN rc;
	SQLPOINTER ptr;
	SQLINTEGER n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescFieldW " PTRFMT " %d %s " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST DescriptorHandle, (int) RecordNumber,
		translateFieldIdentifier(FieldIdentifier),
		PTRFMTCAST ValuePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;
	clearDescErrors(desc);

	switch (FieldIdentifier) {
	/* all string attributes */
	case SQL_DESC_BASE_COLUMN_NAME:
	case SQL_DESC_BASE_TABLE_NAME:
	case SQL_DESC_CATALOG_NAME:
	case SQL_DESC_LABEL:
	case SQL_DESC_LITERAL_PREFIX:
	case SQL_DESC_LITERAL_SUFFIX:
	case SQL_DESC_LOCAL_TYPE_NAME:
	case SQL_DESC_NAME:
	case SQL_DESC_SCHEMA_NAME:
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_TYPE_NAME:
		ptr = (SQLPOINTER) malloc(BufferLength);
		if (ptr == NULL) {
			/* Memory allocation error */
			addDescError(desc, "HY001", NULL, 0);
			return SQL_ERROR;
		}
		break;
	default:
		ptr = ValuePtr;
		break;
	}

	rc = MNDBGetDescField(desc, RecordNumber, FieldIdentifier, ptr,
			      BufferLength, &n);

	if (ptr != ValuePtr) {
		if (rc == SQL_SUCCESS_WITH_INFO) {
			clearDescErrors(desc);
			free(ptr);
			ptr = malloc(++n); /* add one for NULL byte */
			if (ptr == NULL) {
				/* Memory allocation error */
				addDescError(desc, "HY001", NULL, 0);
				return SQL_ERROR;
			}
			rc = MNDBGetDescField(desc, RecordNumber,
					      FieldIdentifier, ptr, n, &n);
		}
		if (SQL_SUCCEEDED(rc)) {
			SQLSMALLINT nn = (SQLSMALLINT) n;

			fixWcharOut(rc, ptr, nn, ValuePtr, BufferLength,
				    StringLengthPtr, 2, addDescError, desc);
		}
		free(ptr);
	} else if (StringLengthPtr)
		*StringLengthPtr = n;

	return rc;
}
