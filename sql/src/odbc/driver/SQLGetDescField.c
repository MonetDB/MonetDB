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

#define copyString(str)							\
		do {							\
			if (BufferLength < 0) {				\
				addDescError(desc, "HY090", NULL, 0);	\
				return SQL_ERROR;			\
			}						\
			strncpy((char *) Value, str ? (char *) str : "", BufferLength); \
			*StringLength = str ? strlen(str) : 0;		\
			if (*StringLength >= BufferLength) {		\
				addDescError(desc, "01004", NULL, 0);	\
				return SQL_SUCCESS_WITH_INFO;		\
			}						\
		} while (0)

SQLRETURN
SQLGetDescField_(ODBCDesc *desc, SQLSMALLINT RecordNumber,
		 SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
		 SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
	ODBCDescRec *rec;

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

/*
	if (isIRD(desc) &&
	    (desc->Stmt->State == PREPARED || desc->Stmt->State == EXECUTED) && no open cursor)
		return SQL_NO_DATA;
*/
	if (isIRD(desc) && desc->Stmt->State != PREPARED &&
	    desc->Stmt->State != EXECUTED) {
		/* Associated statement is not prepared */
		addDescError(desc, "HY007", NULL, 0);
		return SQL_ERROR;
	}

	/* header fields ignore RecordNumber */
	switch (FieldIdentifier) {
	case SQL_DESC_ALLOC_TYPE:
		* (SQLSMALLINT *) Value = desc->sql_desc_alloc_type;
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_SIZE:
		if (isAD(desc))
			* (SQLUINTEGER *) Value = desc->sql_desc_array_size;
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_STATUS_PTR:
		* (SQLUSMALLINT **) Value = desc->sql_desc_array_status_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_BIND_OFFSET_PTR:
		if (isAD(desc))
			* (SQLINTEGER **) Value = desc->sql_desc_bind_offset_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_BIND_TYPE:
		if (isAD(desc))
			* (SQLUINTEGER *) Value = desc->sql_desc_bind_type;
		return SQL_SUCCESS;
	case SQL_DESC_COUNT:
		* (SQLSMALLINT *) Value = desc->sql_desc_count;
		return SQL_SUCCESS;
	case SQL_DESC_ROWS_PROCESSED_PTR:
		if (desc->Stmt)
			* (SQLUINTEGER **) Value = desc->sql_desc_rows_processed_ptr;
		return SQL_SUCCESS;
	}

	if (RecordNumber <= 0) {
		addDescError(desc, "07009", NULL, 0);
		return SQL_ERROR;
	}
	if (RecordNumber > desc->sql_desc_count)
		return SQL_NO_DATA;

	rec = &desc->descRec[RecordNumber];

	switch (FieldIdentifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:
		if (isIRD(desc))
			* (SQLINTEGER *) Value = rec->sql_desc_auto_unique_value;
		return SQL_SUCCESS;
	case SQL_DESC_BASE_COLUMN_NAME:
		if (isIRD(desc))
			copyString(rec->sql_desc_base_column_name);
		return SQL_SUCCESS;
	case SQL_DESC_BASE_TABLE_NAME:
		if (isIRD(desc))
			copyString(rec->sql_desc_base_table_name);
		return SQL_SUCCESS;
	case SQL_DESC_CASE_SENSITIVE:
		if (isID(desc))
			* (SQLINTEGER *) Value = rec->sql_desc_case_sensitive;
		return SQL_SUCCESS;
	case SQL_DESC_CATALOG_NAME:
		if (isIRD(desc))
			copyString(rec->sql_desc_catalog_name);
		return SQL_SUCCESS;
	case SQL_DESC_CONCISE_TYPE:
		* (SQLSMALLINT *) Value = rec->sql_desc_concise_type;
		return SQL_SUCCESS;
	case SQL_DESC_DATA_PTR:
		if (!isIRD(desc))
			* (SQLPOINTER *) Value = rec->sql_desc_data_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_DATETIME_INTERVAL_CODE:
		* (SQLSMALLINT *) Value = rec->sql_desc_datetime_interval_code;
		return SQL_SUCCESS;
	case SQL_DESC_DATETIME_INTERVAL_PRECISION:
		* (SQLINTEGER *) Value = rec->sql_desc_datetime_interval_precision;
		return SQL_SUCCESS;
	case SQL_DESC_DISPLAY_SIZE:
		if (isIRD(desc))
			* (SQLINTEGER *) Value = rec->sql_desc_display_size;
		return SQL_SUCCESS;
	case SQL_DESC_FIXED_PREC_SCALE:
		if (isID(desc))
			* (SQLSMALLINT *) Value = rec->sql_desc_fixed_prec_scale;
		return SQL_SUCCESS;
	case SQL_DESC_INDICATOR_PTR:
		if (isAD(desc))
			* (SQLINTEGER **) Value = rec->sql_desc_indicator_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_LABEL:
		if (isIRD(desc))
			copyString(rec->sql_desc_label);
		return SQL_SUCCESS;
	case SQL_DESC_LENGTH:
		* (SQLUINTEGER *) Value = rec->sql_desc_length;
		return SQL_SUCCESS;
	case SQL_DESC_LITERAL_PREFIX:
		if (isIRD(desc))
			copyString(rec->sql_desc_literal_prefix);
		return SQL_SUCCESS;
	case SQL_DESC_LITERAL_SUFFIX:
		if (isIRD(desc))
			copyString(rec->sql_desc_literal_suffix);
		return SQL_SUCCESS;
	case SQL_DESC_LOCAL_TYPE_NAME:
		if (isID(desc))
			copyString(rec->sql_desc_local_type_name);
		return SQL_SUCCESS;
	case SQL_DESC_NAME:
		if (isID(desc))
			copyString(rec->sql_desc_name);
		return SQL_SUCCESS;
	case SQL_DESC_NULLABLE:
		if (isID(desc))
			* (SQLSMALLINT *) Value = rec->sql_desc_nullable;
		return SQL_SUCCESS;
	case SQL_DESC_NUM_PREC_RADIX:
		* (SQLINTEGER *) Value = rec->sql_desc_num_prec_radix;
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH:
		* (SQLINTEGER *) Value = rec->sql_desc_octet_length;
		return SQL_SUCCESS;
	case SQL_DESC_OCTET_LENGTH_PTR:
		if (isAD(desc))
			* (SQLINTEGER **) Value = rec->sql_desc_octet_length_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_PARAMETER_TYPE:
		if (isIPD(desc))
			* (SQLINTEGER *) Value = rec->sql_desc_parameter_type;
		return SQL_SUCCESS;
	case SQL_DESC_PRECISION:
		* (SQLSMALLINT *) Value = rec->sql_desc_precision;
		return SQL_SUCCESS;
	case SQL_DESC_ROWVER:
		if (isID(desc))
			* (SQLSMALLINT *) Value = rec->sql_desc_rowver;
		return SQL_SUCCESS;
	case SQL_DESC_SCALE:
		* (SQLSMALLINT *) Value = rec->sql_desc_scale;
		return SQL_SUCCESS;
	case SQL_DESC_SCHEMA_NAME:
		if (isIRD(desc))
			copyString(rec->sql_desc_schema_name);
		return SQL_SUCCESS;
	case SQL_DESC_SEARCHABLE:
		if (isIRD(desc))
			* (SQLSMALLINT *) Value = rec->sql_desc_searchable;
		return SQL_SUCCESS;
	case SQL_DESC_TABLE_NAME:
		if (isIRD(desc))
			copyString(rec->sql_desc_table_name);
		return SQL_SUCCESS;
	case SQL_DESC_TYPE:
		* (SQLSMALLINT *) Value = rec->sql_desc_type;
		return SQL_SUCCESS;
	case SQL_DESC_TYPE_NAME:
		if (isID(desc))
			copyString(rec->sql_desc_type_name);
		return SQL_SUCCESS;
	case SQL_DESC_UNNAMED:
		if (isID(desc))
			* (SQLSMALLINT *) Value = rec->sql_desc_unnamed;
		return SQL_SUCCESS;
	case SQL_DESC_UNSIGNED:
		if (isID(desc))
			* (SQLSMALLINT *) Value = rec->sql_desc_unsigned;
		return SQL_SUCCESS;
	case SQL_DESC_UPDATABLE:
		if (isIRD(desc))
			* (SQLSMALLINT *) Value = rec->sql_desc_updatable;
		return SQL_SUCCESS;
	}

	/* HY091: Invalid descriptor field identifier */
	addDescError(desc, "HY091", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN
SQLGetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
		SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
		SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescField %d %d\n", RecordNumber, FieldIdentifier);
#endif

	return SQLGetDescField_((ODBCDesc *) DescriptorHandle, RecordNumber,
				FieldIdentifier, Value, BufferLength,
				StringLength);
}
