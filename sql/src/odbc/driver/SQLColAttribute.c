/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
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


SQLRETURN ColAttribute(
	SQLHSTMT	hStmt,
	SQLUSMALLINT	nCol,
	SQLUSMALLINT	nFieldIdentifier,
	SQLPOINTER	pszValue,
	SQLSMALLINT	nValueLengthMax,
	SQLSMALLINT *	pnValueLength,
	SQLPOINTER	pnValue )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	ColumnHeader *	pColumnHeader;
	int		nValue = 0;


	if (! isValidStmt(stmt))
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
		addStmtError(stmt, "HY000", "Cannot return the column info. Query must be executed first", 0);
		return SQL_ERROR;
	}
	if (stmt->ResultCols == NULL) {
		addStmtError(stmt, "HY000", "Cannot return the column info. No result set is available", 0);
		return SQL_ERROR;
	}

	/* check input parameter */
	if (nCol < 1 || nCol > stmt->nrCols)
	{
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

/* TODO: finish implementation */
	pColumnHeader = stmt->ResultCols+nCol;

	switch (nFieldIdentifier)
	{
	case SQL_DESC_AUTO_UNIQUE_VALUE:
		nValue = pColumnHeader->bSQL_DESC_AUTO_UNIQUE_VALUE;
		break;
	case SQL_DESC_BASE_COLUMN_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_BASE_COLUMN_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_BASE_TABLE_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_BASE_TABLE_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_CASE_SENSITIVE:
		nValue = pColumnHeader->bSQL_DESC_CASE_SENSITIVE;
		break;
	case SQL_DESC_CATALOG_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_CATALOG_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_CONCISE_TYPE:
		nValue = pColumnHeader->nSQL_DESC_CONCISE_TYPE;
		break;
	case SQL_DESC_COUNT:
		nValue = stmt->nrCols;
		break;
	case SQL_DESC_DISPLAY_SIZE:
		nValue = pColumnHeader->nSQL_DESC_DISPLAY_SIZE;
		break;
	case SQL_DESC_FIXED_PREC_SCALE:
		nValue = pColumnHeader->bSQL_DESC_FIXED_PREC_SCALE;
		break;
	case SQL_COLUMN_NAME:
	case SQL_DESC_LABEL:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_LABEL, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_LENGTH:
	case SQL_COLUMN_LENGTH:
		nValue = pColumnHeader->nSQL_DESC_LENGTH;
		break;
	case SQL_DESC_LITERAL_PREFIX:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_LITERAL_PREFIX, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_LITERAL_SUFFIX:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_LITERAL_SUFFIX, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_LOCAL_TYPE_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_LOCAL_TYPE_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_NULLABLE:
		nValue = pColumnHeader->nSQL_DESC_NULLABLE;
		break;
	case SQL_DESC_NUM_PREC_RADIX:
		nValue = pColumnHeader->nSQL_DESC_NUM_PREC_RADIX;
		break;
	case SQL_DESC_OCTET_LENGTH:
		nValue = pColumnHeader->nSQL_DESC_OCTET_LENGTH;
		break;
	case SQL_DESC_PRECISION:
		nValue = pColumnHeader->nSQL_DESC_PRECISION;
		break;
	case SQL_DESC_SCALE:
		nValue = pColumnHeader->nSQL_DESC_SCALE;
		break;
	case SQL_DESC_SCHEMA_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_SCHEMA_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_SEARCHABLE:
		nValue = pColumnHeader->nSQL_DESC_SEARCHABLE;
		break;
	case SQL_DESC_TABLE_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_TABLE_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_TYPE:
		nValue = pColumnHeader->nSQL_DESC_TYPE;
		break;
	case SQL_DESC_TYPE_NAME:
		strncpy(pszValue, pColumnHeader->pszSQL_DESC_TYPE_NAME, nValueLengthMax);
		if (pnValueLength)
			*pnValueLength = strlen(pszValue);
		break;
	case SQL_DESC_UNNAMED:
		nValue = pColumnHeader->nSQL_DESC_UNNAMED;
		break;
	case SQL_DESC_UNSIGNED:
		nValue = pColumnHeader->bSQL_DESC_UNSIGNED;
		break;
	case SQL_DESC_UPDATABLE:
		nValue = pColumnHeader->nSQL_DESC_UPDATABLE;
		break;
	default:
		/* HY091 = Invalid descriptor field identifier */
		addStmtError(stmt, "HY091", NULL, 0);
		return SQL_ERROR;
	}

	if (pnValue)
		*(int*)pnValue = nValue;

	return SQL_SUCCESS;
}

SQLRETURN SQLColAttribute(
	SQLHSTMT	hStmt,
	SQLUSMALLINT	nCol,
	SQLUSMALLINT	nFieldIdentifier,
	SQLPOINTER	pszValue,
	SQLSMALLINT	nValueLengthMax,
	SQLSMALLINT *	pnValueLength,
	SQLPOINTER	pnValue )
{
	ColAttribute( hStmt, nCol, nFieldIdentifier, pszValue, nValueLengthMax,
				pnValueLength, pnValue);
}
