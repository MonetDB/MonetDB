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
 * SQLColAttributes()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLColAttribute())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
MNDBColAttributes(ODBCStmt *stmt,
		  SQLUSMALLINT ColumnNumber,
		  SQLUSMALLINT FieldIdentifier,
		  SQLPOINTER CharacterAttributePtr,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLengthPtr,
		  SQLLEN *NumericAttributePtr)
{
	SQLRETURN rc;
	SQLLEN value;

	/* use mapping as described in ODBC 3 SDK Help file */
	switch (FieldIdentifier) {
	case SQL_COLUMN_AUTO_INCREMENT: /* SQL_DESC_AUTO_UNIQUE_VALUE */
	case SQL_COLUMN_CASE_SENSITIVE: /* SQL_DESC_CASE_SENSITIVE */
	case SQL_COLUMN_COUNT:
	case SQL_COLUMN_DISPLAY_SIZE:	/* SQL_DESC_DISPLAY_SIZE */
	case SQL_COLUMN_LABEL:		/* SQL_DESC_LABEL */
	case SQL_COLUMN_LENGTH:
	case SQL_COLUMN_MONEY:		/* SQL_DESC_FIXED_PREC_SCALE */
	case SQL_COLUMN_NAME:
	case SQL_COLUMN_NULLABLE:
		/* SQL_COLUMN_NULLABLE should be translated to
		 * SQL_DESC_NULLABLE, except in the 64 bit
		 * documentation, the former isn't mentioned as
		 * returning a 64 bit value whereas the latter is.
		 * Hence we don't translate but return differently
		 * sized values for the two */
	case SQL_COLUMN_OWNER_NAME:	/* SQL_DESC_SCHEMA_NAME */
	case SQL_COLUMN_PRECISION:
	case SQL_COLUMN_QUALIFIER_NAME: /* SQL_DESC_CATALOG_NAME */
	case SQL_COLUMN_SCALE:
	case SQL_COLUMN_SEARCHABLE:	/* SQL_DESC_SEARCHABLE */
	case SQL_COLUMN_TABLE_NAME:	/* SQL_DESC_TABLE_NAME */
	case SQL_COLUMN_TYPE:		/* SQL_DESC_CONCISE_TYPE */
	case SQL_COLUMN_TYPE_NAME:	/* SQL_DESC_TYPE_NAME */
	case SQL_COLUMN_UNSIGNED:	/* SQL_DESC_UNSIGNED */
	case SQL_COLUMN_UPDATABLE:	/* SQL_DESC_UPDATABLE */
		break;
	default:
		/* Invalid descriptor field identifier */
		addStmtError(stmt, "HY091", NULL, 0);
		return SQL_ERROR;
	}
	rc = MNDBColAttribute(stmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, &value);

	/* TODO: implement special semantics for FieldIdentifiers:
	 * SQL_COLUMN_TYPE, SQL_COLUMN_NAME, SQL_COLUMN_NULLABLE and
	 * SQL_COLUMN_COUNT.  See ODBC 3 SDK Help file,
	 * SQLColAttributes Mapping. */
/*
	if (FieldIdentifier == SQL_COLUMN_TYPE && value == concise datetime type) {
		map return value for date, time, and timestamp codes;
	}
*/
	if (NumericAttributePtr)
		*NumericAttributePtr = value;
	return rc;
}

SQLRETURN SQL_API
SQLColAttributes(SQLHSTMT StatementHandle,
		 SQLUSMALLINT ColumnNumber,
		 SQLUSMALLINT FieldIdentifier,
		 SQLPOINTER CharacterAttributePtr,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLengthPtr,
		 SQLLEN *NumericAttributePtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColAttributes %p %u %s %p %d %p %p\n",
		StatementHandle,
		(unsigned int) ColumnNumber,
		translateFieldIdentifier(FieldIdentifier),
		CharacterAttributePtr, (int) BufferLength,
		StringLengthPtr, NumericAttributePtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBColAttributes(stmt,
				 ColumnNumber,
				 FieldIdentifier,
				 CharacterAttributePtr,
				 BufferLength,
				 StringLengthPtr,
				 NumericAttributePtr);
}

SQLRETURN SQL_API
SQLColAttributesA(SQLHSTMT StatementHandle,
		  SQLUSMALLINT ColumnNumber,
		  SQLUSMALLINT FieldIdentifier,
		  SQLPOINTER CharacterAttributePtr,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLengthPtr,
		  SQLLEN *NumericAttributePtr)
{
	return SQLColAttributes(StatementHandle,
				ColumnNumber,
				FieldIdentifier,
				CharacterAttributePtr,
				BufferLength,
				StringLengthPtr,
				NumericAttributePtr);
}

SQLRETURN SQL_API
SQLColAttributesW(SQLHSTMT StatementHandle,
		  SQLUSMALLINT ColumnNumber,
		  SQLUSMALLINT FieldIdentifier,
		  SQLPOINTER CharacterAttributePtr,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLengthPtr,
		  SQLLEN *NumericAttributePtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLPOINTER ptr;
	SQLRETURN rc;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColAttributesW %p %u %s %p %d %p %p\n",
		StatementHandle,
		(unsigned int) ColumnNumber,
		translateFieldIdentifier(FieldIdentifier),
		CharacterAttributePtr, (int) BufferLength,
		StringLengthPtr, NumericAttributePtr);
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
	default:
		ptr = CharacterAttributePtr;
		break;
	}

	rc = MNDBColAttributes(stmt, ColumnNumber, FieldIdentifier, ptr,
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
			rc = MNDBColAttributes(stmt, ColumnNumber,
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
