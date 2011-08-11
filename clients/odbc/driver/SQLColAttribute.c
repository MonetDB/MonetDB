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
SQLColAttribute_(ODBCStmt *stmt,
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
	case SQL_DESC_AUTO_UNIQUE_VALUE:	/* SQL_COLUMN_AUTO_INCREMENT */
		if (NumericAttributePtr)
			*(int *) NumericAttributePtr = rec->sql_desc_auto_unique_value;
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
			*(int *) NumericAttributePtr = rec->sql_desc_case_sensitive;
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
			*(int *) NumericAttributePtr = rec->sql_desc_concise_type;
		break;
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
			*(int *) NumericAttributePtr = rec->sql_desc_fixed_prec_scale;
		break;
	case SQL_DESC_LABEL:	/* SQL_COLUMN_LABEL */
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
	case SQL_DESC_NAME:
		copyString(rec->sql_desc_name,
			   strlen((char *) rec->sql_desc_name),
			   CharacterAttributePtr, BufferLength,
			   StringLengthPtr, SQLSMALLINT, addStmtError,
			   stmt, return SQL_ERROR);
		break;
	case SQL_DESC_NULLABLE:
		if (NumericAttributePtr)
			*(int *) NumericAttributePtr = rec->sql_desc_nullable;
		break;
	case SQL_DESC_NUM_PREC_RADIX:
		if (NumericAttributePtr)
			*(int *) NumericAttributePtr = rec->sql_desc_num_prec_radix;
		break;
	case SQL_DESC_OCTET_LENGTH:
		if (NumericAttributePtr)
			*(SQLLEN *) NumericAttributePtr = rec->sql_desc_octet_length;
		break;
	case SQL_COLUMN_PRECISION:
	case SQL_DESC_PRECISION:
		if (NumericAttributePtr)
			*(int *) NumericAttributePtr = rec->sql_desc_precision;
		break;
	case SQL_COLUMN_SCALE:
	case SQL_DESC_SCALE:
		if (NumericAttributePtr)
			*(int *) NumericAttributePtr = rec->sql_desc_scale;
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
			*(int *) NumericAttributePtr = rec->sql_desc_searchable;
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
			*(int *) NumericAttributePtr = rec->sql_desc_type;
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
			*(int *) NumericAttributePtr = rec->sql_desc_unnamed;
		break;
	case SQL_DESC_UNSIGNED:	/* SQL_COLUMN_UNSIGNED */
		if (NumericAttributePtr)
			*(int *) NumericAttributePtr = rec->sql_desc_unsigned;
		break;
	case SQL_DESC_UPDATABLE:	/* SQL_COLUMN_UPDATABLE */
		if (NumericAttributePtr)
			*(int *) NumericAttributePtr = rec->sql_desc_updatable;
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
	ODBCLOG("SQLColAttribute " PTRFMT " %u\n",
		PTRFMTCAST StatementHandle, (unsigned int) FieldIdentifier);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return SQLColAttribute_((ODBCStmt *) StatementHandle,
				ColumnNumber,
				FieldIdentifier,
				CharacterAttributePtr,
				BufferLength,
				StringLengthPtr,
				NumericAttributePtr);
}

#ifdef WITH_WCHAR
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
	ODBCLOG("SQLColAttributeW " PTRFMT " %u\n",
		PTRFMTCAST StatementHandle, (unsigned int) FieldIdentifier);
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
		rc = SQLColAttribute_(stmt, ColumnNumber, FieldIdentifier,
				      NULL, 0, &n, NumericAttributePtr);
		if (!SQL_SUCCEEDED(rc))
			return rc;
		clearStmtErrors(stmt);
		n++;		/* account for NUL byte */
		ptr = (SQLPOINTER) malloc(n);
		break;
	default:
		n = BufferLength;
		ptr = CharacterAttributePtr;
		break;
	}

	rc = SQLColAttribute_(stmt, ColumnNumber, FieldIdentifier, ptr,
			      n, &n, NumericAttributePtr);

	if (ptr != CharacterAttributePtr)
		fixWcharOut(rc, ptr, n, CharacterAttributePtr, BufferLength,
			    StringLengthPtr, 2, addStmtError, stmt);
	else if (StringLengthPtr)
		*StringLengthPtr = n;

	return rc;
}
#endif /* WITH_WCHAR */
