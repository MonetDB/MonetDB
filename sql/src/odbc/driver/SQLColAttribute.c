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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLColAttribute_(SQLHSTMT hStmt, SQLUSMALLINT nCol,
		 SQLUSMALLINT nFieldIdentifier, SQLPOINTER pszValue,
		 SQLSMALLINT nValueLengthMax, SQLSMALLINT *pnValueLength,
		 SQLPOINTER pnValue)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	ColumnHeader *pColumnHeader;
	int *valueptr = (int *) pnValue;

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be prepared or executed */
	if (stmt->State == INITED) {
		/* caller should have called SQLPrepare or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* When the query is only prepared (via SQLPrepare) we do not have
	 * the Columns info yet (this is a limitation of the current
	 * MonetDB SQL frontend implementation). */
	/* we only have a correct data when the query is executed and a Result is created */
	if (stmt->State != EXECUTED) {
		/* HY000 = General Error */
		addStmtError(stmt, "HY000",
			     "Cannot return the column info. Query must be executed first",
			     0);
		return SQL_ERROR;
	}
	if (stmt->ResultCols == NULL) {
		addStmtError(stmt, "HY000",
			     "Cannot return the column info. No result set is available",
			     0);
		return SQL_ERROR;
	}

	/* check input parameter */
	if (nCol < 1 || nCol > stmt->nrCols) {
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

/* TODO: finish implementation */
	pColumnHeader = stmt->ResultCols + nCol;

	switch (nFieldIdentifier) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:
		if (valueptr)
			*valueptr = pColumnHeader->bSQL_DESC_AUTO_UNIQUE_VALUE;
		break;
	case SQL_DESC_BASE_COLUMN_NAME:
		if (pszValue)
			strncpy(pszValue,
				pColumnHeader->pszSQL_DESC_BASE_COLUMN_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_BASE_COLUMN_NAME);
		break;
	case SQL_DESC_BASE_TABLE_NAME:
		if (pszValue)
			strncpy(pszValue,
				pColumnHeader->pszSQL_DESC_BASE_TABLE_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_BASE_TABLE_NAME);
		break;
	case SQL_DESC_CASE_SENSITIVE:
		if (valueptr)
			*valueptr = pColumnHeader->bSQL_DESC_CASE_SENSITIVE;
		break;
	case SQL_DESC_CATALOG_NAME:
		if (pszValue)
			strncpy(pszValue,
				pColumnHeader->pszSQL_DESC_CATALOG_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_CATALOG_NAME);
		break;
	case SQL_DESC_CONCISE_TYPE:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_CONCISE_TYPE;
		break;
	case SQL_DESC_COUNT:
		if (valueptr)
			*valueptr = stmt->nrCols;

		break;
	case SQL_DESC_DISPLAY_SIZE:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_DISPLAY_SIZE;
		break;
	case SQL_DESC_FIXED_PREC_SCALE:
		if (valueptr)
			*valueptr = pColumnHeader->bSQL_DESC_FIXED_PREC_SCALE;
		break;
	case SQL_COLUMN_NAME:
	case SQL_DESC_LABEL:
		if (pszValue)
			strncpy(pszValue, pColumnHeader->pszSQL_DESC_LABEL,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_LABEL);
		break;
	case SQL_DESC_LENGTH:
	case SQL_COLUMN_LENGTH:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_LENGTH + 20;
		break;
	case SQL_DESC_LITERAL_PREFIX:
		if (pszValue)
			strncpy(pszValue,
				pColumnHeader->pszSQL_DESC_LITERAL_PREFIX,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_LITERAL_PREFIX);
		break;
	case SQL_DESC_LITERAL_SUFFIX:
		if (pszValue)
			strncpy(pszValue,
				pColumnHeader->pszSQL_DESC_LITERAL_SUFFIX,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_LITERAL_SUFFIX);
		break;
	case SQL_DESC_LOCAL_TYPE_NAME:
		if (pszValue)
			strncpy(pszValue,
				pColumnHeader->pszSQL_DESC_LOCAL_TYPE_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_LOCAL_TYPE_NAME);
		break;
	case SQL_DESC_NAME:
		if (pszValue)
			strncpy(pszValue, pColumnHeader->pszSQL_DESC_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_NAME);
		break;
	case SQL_DESC_NULLABLE:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_NULLABLE;
		break;
	case SQL_DESC_NUM_PREC_RADIX:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_NUM_PREC_RADIX;
		break;
	case SQL_DESC_OCTET_LENGTH:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_OCTET_LENGTH;
		break;
	case SQL_DESC_PRECISION:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_PRECISION;
		break;
	case SQL_DESC_SCALE:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_SCALE;
		break;
	case SQL_DESC_SCHEMA_NAME:
		if (pszValue)
			strncpy(pszValue, pColumnHeader->pszSQL_DESC_SCHEMA_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_SCHEMA_NAME);
		break;
	case SQL_DESC_SEARCHABLE:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_SEARCHABLE;
		break;
	case SQL_DESC_TABLE_NAME:
		if (pszValue)
			strncpy(pszValue,
				pColumnHeader->pszSQL_DESC_TABLE_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_TABLE_NAME);
		break;
	case SQL_DESC_TYPE:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_TYPE;
		break;
	case SQL_DESC_TYPE_NAME:
		if (pszValue)
			strncpy(pszValue, pColumnHeader->pszSQL_DESC_TYPE_NAME,
				nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pColumnHeader->pszSQL_DESC_TYPE_NAME);
		break;
	case SQL_DESC_UNNAMED:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_UNNAMED;
		break;
	case SQL_DESC_UNSIGNED:
		if (valueptr)
			*valueptr = pColumnHeader->bSQL_DESC_UNSIGNED;
		break;
	case SQL_DESC_UPDATABLE:
		if (valueptr)
			*valueptr = pColumnHeader->nSQL_DESC_UPDATABLE;
		break;
	default:
		/* HY091 = Invalid descriptor field identifier */
		addStmtError(stmt, "HY091", NULL, 0);

		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}

SQLRETURN
SQLColAttribute(SQLHSTMT hStmt, SQLUSMALLINT nCol,
		SQLUSMALLINT nFieldIdentifier, SQLPOINTER pszValue,
		SQLSMALLINT nValueLengthMax, SQLSMALLINT *pnValueLength,
		SQLPOINTER pnValue)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLColAttribute\n");
#endif

	return SQLColAttribute_(hStmt, nCol, nFieldIdentifier, pszValue,
				nValueLengthMax, pnValueLength, pnValue);
}
