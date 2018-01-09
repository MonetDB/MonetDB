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
 * SQLColAttribute()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


SQLRETURN
MNDBColAttribute(ODBCStmt *stmt,
		 SQLUSMALLINT ColumnNumber,
		 SQLUSMALLINT FieldIdentifier,
		 SQLPOINTER CharacterAttributePtr,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLengthPtr,
		 LENP_OR_POINTER_T NumericAttributePtr)
{
	ODBCDescRec *rec;

	/* check statement cursor state, query should be prepared or executed */
	if (stmt->State == INITED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == PREPARED0 && FieldIdentifier != SQL_DESC_COUNT) {
		/* Prepared statement not a cursor-specification */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}

	if (stmt->ImplRowDescr->descRec == NULL) {
		/* General error */
		addStmtError(stmt, "HY000", "Cannot return the column info. No result set is available", 0);
		return SQL_ERROR;
	}

	/* check input parameter */
	if (ColumnNumber < 1 || ColumnNumber > stmt->ImplRowDescr->sql_desc_count) {
		/* Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

/* TODO: finish implementation */
	rec = stmt->ImplRowDescr->descRec + ColumnNumber;

	switch (FieldIdentifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:/* SQL_COLUMN_AUTO_INCREMENT */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_auto_unique_value;
		break;
	case SQL_DESC_BASE_COLUMN_NAME:
		copyString(rec->sql_desc_base_column_name,
			   strlen((char *) rec->sql_desc_base_column_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_BASE_TABLE_NAME:
		copyString(rec->sql_desc_base_table_name,
			   strlen((char *) rec->sql_desc_base_table_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_CASE_SENSITIVE:	/* SQL_COLUMN_CASE_SENSITIVE */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_case_sensitive;
		break;
	case SQL_DESC_CATALOG_NAME:	/* SQL_COLUMN_QUALIFIER_NAME */
		copyString(rec->sql_desc_catalog_name,
			   strlen((char *) rec->sql_desc_catalog_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_CONCISE_TYPE:	/* SQL_COLUMN_TYPE */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_concise_type;
		break;
	case SQL_COLUMN_COUNT:
	case SQL_DESC_COUNT:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = stmt->ImplRowDescr->sql_desc_count;
		break;
	case SQL_DESC_DISPLAY_SIZE:	/* SQL_COLUMN_DISPLAY_SIZE */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_display_size;
		break;
	case SQL_DESC_FIXED_PREC_SCALE:	/* SQL_COLUMN_MONEY */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_fixed_prec_scale;
		break;
	case SQL_DESC_LABEL:		/* SQL_COLUMN_LABEL */
		copyString(rec->sql_desc_label,
			   strlen((char *) rec->sql_desc_label),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_COLUMN_LENGTH:
	case SQL_DESC_LENGTH:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_length;
		break;
	case SQL_DESC_LITERAL_PREFIX:
		copyString(rec->sql_desc_literal_prefix,
			   strlen((char *) rec->sql_desc_literal_prefix),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_LITERAL_SUFFIX:
		copyString(rec->sql_desc_literal_suffix, strlen((char *) rec->sql_desc_literal_suffix), CharacterAttributePtr, BufferLength, StringLengthPtr, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
		break;
	case SQL_DESC_LOCAL_TYPE_NAME:
		copyString(rec->sql_desc_local_type_name,
			   strlen((char *) rec->sql_desc_local_type_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_COLUMN_NAME:
	case SQL_DESC_NAME:
		copyString(rec->sql_desc_name,
			   strlen((char *) rec->sql_desc_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_COLUMN_NULLABLE:
		if (NumericAttributePtr)
			*(SQLINTEGER *) NumericAttributePtr = rec->sql_desc_nullable;
		break;
	case SQL_DESC_NULLABLE:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_nullable;
		break;
	case SQL_DESC_NUM_PREC_RADIX:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_num_prec_radix;
		break;
	case SQL_DESC_OCTET_LENGTH:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_octet_length;
		break;
	case SQL_COLUMN_PRECISION:
		if (NumericAttributePtr)
			*(SQLINTEGER *) NumericAttributePtr = rec->sql_desc_precision;
		break;
	case SQL_DESC_PRECISION:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_precision;
		break;
	case SQL_COLUMN_SCALE:
		if (NumericAttributePtr)
			*(SQLINTEGER *) NumericAttributePtr = rec->sql_desc_scale;
		break;
	case SQL_DESC_SCALE:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_scale;
		break;
	case SQL_DESC_SCHEMA_NAME:	/* SQL_COLUMN_OWNER_NAME */
		copyString(rec->sql_desc_schema_name,
			   strlen((char *) rec->sql_desc_schema_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_SEARCHABLE:	/* SQL_COLUMN_SEARCHABLE */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_searchable;
		break;
	case SQL_DESC_TABLE_NAME:	/* SQL_COLUMN_TABLE_NAME */
		copyString(rec->sql_desc_table_name,
			   strlen((char *) rec->sql_desc_table_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_TYPE:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_type;
		break;
	case SQL_DESC_TYPE_NAME:	/* SQL_COLUMN_TYPE_NAME */
		copyString(rec->sql_desc_type_name,
			   strlen((char *) rec->sql_desc_type_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_UNNAMED:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_unnamed;
		break;
	case SQL_DESC_UNSIGNED:		/* SQL_COLUMN_UNSIGNED */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_unsigned;
		break;
	case SQL_DESC_UPDATABLE:	/* SQL_COLUMN_UPDATABLE */
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_updatable;
		break;
	default:
		/* Invalid descriptor field identifier */
		addStmtError(stmt, "HY091", NULL, 0);
		return SQL_ERROR;
	}

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLColAttribute(SQLHSTMT StatementHandle,
		SQLUSMALLINT ColumnNumber,
		SQLUSMALLINT FieldIdentifier,
		SQLPOINTER CharacterAttributePtr,
		SQLSMALLINT BufferLength,
		SQLSMALLINT *StringLengthPtr,
		LENP_OR_POINTER_T NumericAttributePtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLColAttribute " PTRFMT " %d %s " PTRFMT " %d " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST StatementHandle, (int) ColumnNumber,
		translateFieldIdentifier(FieldIdentifier),
		PTRFMTCAST CharacterAttributePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr,
		PTRFMTCAST (void *) NumericAttributePtr);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	switch (FieldIdentifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_BASE_COLUMN_NAME:
	case SQL_DESC_BASE_TABLE_NAME:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_CATALOG_NAME:
	case SQL_DESC_CONCISE_TYPE:
	case SQL_DESC_COUNT:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_LABEL:
	case SQL_DESC_LENGTH:
	case SQL_DESC_LITERAL_PREFIX:
	case SQL_DESC_LITERAL_SUFFIX:
	case SQL_DESC_LOCAL_TYPE_NAME:
	case SQL_DESC_NAME:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_NUM_PREC_RADIX:
	case SQL_DESC_OCTET_LENGTH:
	case SQL_DESC_PRECISION:
	case SQL_DESC_SCALE:
	case SQL_DESC_SCHEMA_NAME:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_TYPE:
	case SQL_DESC_TYPE_NAME:
	case SQL_DESC_UNNAMED:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE:
		break;
	default:
		/* Invalid descriptor field identifier */
		addStmtError((ODBCStmt *) StatementHandle, "HY091", NULL, 0);
		return SQL_ERROR;
	}
	return MNDBColAttribute((ODBCStmt *) StatementHandle,
				ColumnNumber,
				FieldIdentifier,
				CharacterAttributePtr,
				BufferLength,
				StringLengthPtr,
				NumericAttributePtr);
}

SQLRETURN SQL_API
SQLColAttributeA(SQLHSTMT StatementHandle,
		 SQLSMALLINT ColumnNumber,
		 SQLSMALLINT FieldIdentifier,
		 SQLPOINTER CharacterAttributePtr,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLengthPtr,
		 LENP_OR_POINTER_T NumericAttributePtr)
{
	return SQLColAttribute(StatementHandle,
			       (SQLUSMALLINT) ColumnNumber,
			       (SQLUSMALLINT) FieldIdentifier,
			       CharacterAttributePtr,
			       BufferLength,
			       StringLengthPtr,
			       NumericAttributePtr);
}

SQLRETURN SQL_API
SQLColAttributeW(SQLHSTMT StatementHandle,
		 SQLUSMALLINT ColumnNumber,
		 SQLUSMALLINT FieldIdentifier,
		 SQLPOINTER CharacterAttributePtr,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLengthPtr,
		 LENP_OR_POINTER_T NumericAttributePtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLPOINTER ptr;
	SQLRETURN rc;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColAttributeW " PTRFMT " %d %s " PTRFMT " %d " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST StatementHandle, (int) ColumnNumber,
		translateFieldIdentifier(FieldIdentifier),
		PTRFMTCAST CharacterAttributePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr,
		PTRFMTCAST (void *) NumericAttributePtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	switch (FieldIdentifier) {
	/* all string atributes */
	case SQL_DESC_BASE_COLUMN_NAME:
	case SQL_DESC_BASE_TABLE_NAME:
	case SQL_DESC_CATALOG_NAME:	/* SQL_COLUMN_QUALIFIER_NAME */
	case SQL_DESC_LABEL:		/* SQL_COLUMN_LABEL */
	case SQL_DESC_LITERAL_PREFIX:
	case SQL_DESC_LITERAL_SUFFIX:
	case SQL_DESC_LOCAL_TYPE_NAME:
	case SQL_DESC_NAME:
	case SQL_DESC_SCHEMA_NAME:	/* SQL_COLUMN_OWNER_NAME */
	case SQL_DESC_TABLE_NAME:	/* SQL_COLUMN_TABLE_NAME */
	case SQL_DESC_TYPE_NAME:	/* SQL_COLUMN_TYPE_NAME */
		ptr = malloc(BufferLength);
		if (ptr == NULL) {
			/* Memory allocation error */
			addStmtError(stmt, "HY001", NULL, 0);
			return SQL_ERROR;
		}
		break;
	/* all other attributes */
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_CONCISE_TYPE:
	case SQL_DESC_COUNT:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_LENGTH:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_NUM_PREC_RADIX:
	case SQL_DESC_OCTET_LENGTH:
	case SQL_DESC_PRECISION:
	case SQL_DESC_SCALE:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TYPE:
	case SQL_DESC_UNNAMED:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE:
		ptr = CharacterAttributePtr;
		break;
	default:
		/* Invalid descriptor field identifier */
		addStmtError(stmt, "HY091", NULL, 0);
		return SQL_ERROR;
	}

	rc = MNDBColAttribute(stmt, ColumnNumber, FieldIdentifier, ptr,
			      BufferLength, &n, NumericAttributePtr);

	if (ptr != CharacterAttributePtr) {
		if (rc == SQL_SUCCESS_WITH_INFO) {
			clearStmtErrors(stmt);
			free(ptr);
			ptr = malloc(++n); /* add one for NULL byte */
			if (ptr == NULL) {
				/* Memory allocation error */
				addStmtError(stmt, "HY001", NULL, 0);
				return SQL_ERROR;
			}
			rc = MNDBColAttribute(stmt, ColumnNumber,
					      FieldIdentifier, ptr, n, &n,
					      NumericAttributePtr);
		}
		if (SQL_SUCCEEDED(rc)) {
			fixWcharOut(rc, ptr, n, CharacterAttributePtr,
				    BufferLength, StringLengthPtr, 2,
				    addStmtError, stmt);
		}
		free(ptr);
	} else if (StringLengthPtr)
		*StringLengthPtr = n;

	return rc;
}
