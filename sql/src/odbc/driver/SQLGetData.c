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
 * SQLGetData()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

#include <stdlib.h>		/* for strtoll() & strtoull() on SunOS; doesn't work yet */

SQLRETURN
SQLGetData(SQLHSTMT hStmt, SQLUSMALLINT nCol, SQLSMALLINT nTargetType,	/* C DATA TYPE */
	   SQLPOINTER pTarget, SQLINTEGER nTargetLength,
	   SQLINTEGER *pnLengthOrIndicator)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	int mapitype;
	void *dst = pTarget;
	int dstsz;
	char *strptr;
	SQLRETURN ret = SQL_SUCCESS;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetData\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	assert(stmt->Dbc);
	assert(stmt->Dbc->mid);
	assert(stmt->hdl);

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->currentRow <= 0) {
		/* caller should have called SQLFetch first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (nCol <= 0 || nCol > mapi_get_field_count(stmt->hdl)) {
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

	if (nCol != stmt->currentCol)
		stmt->retrieved = 0;
	stmt->currentCol = nCol;

	switch (nTargetType) {
	case SQL_C_CHAR:
#ifdef SQL_C_XML
	case SQL_C_XML:
#endif
	case SQL_C_VARBOOKMARK:
		/* mapi_store_field doesn't copy the data but only the
		   pointer to the data */
		mapitype = MAPI_VARCHAR;
		dst = &strptr;
		dstsz = -1;	/* not yet known */
		break;
	case SQL_C_LONG:
	case SQL_C_SLONG:
		mapitype = MAPI_LONG;
		dstsz = sizeof(SQLINTEGER);
		break;
	case SQL_C_ULONG:
		mapitype = MAPI_ULONG;
		dstsz = sizeof(SQLUINTEGER);
		break;
	case SQL_C_SHORT:
	case SQL_C_SSHORT:
		mapitype = MAPI_SHORT;
		dstsz = sizeof(SQLSMALLINT);
		break;
	case SQL_C_USHORT:
		mapitype = MAPI_USHORT;
		dstsz = sizeof(SQLUSMALLINT);
		break;
	case SQL_C_BIT:
	case SQL_C_TINYINT:
	case SQL_C_STINYINT:
		mapitype = MAPI_TINY;
		dstsz = sizeof(SQLSCHAR);
		break;
	case SQL_C_UTINYINT:
		mapitype = MAPI_UTINY;
		dstsz = sizeof(SQLCHAR);
		break;
	case SQL_C_SBIGINT:
		mapitype = MAPI_LONGLONG;
		dstsz = sizeof(SQLBIGINT);
		break;
	case SQL_C_UBIGINT:
		mapitype = MAPI_ULONGLONG;
		dstsz = sizeof(SQLUBIGINT);
		break;
	case SQL_C_FLOAT:
		mapitype = MAPI_FLOAT;
		dstsz = sizeof(SQLREAL);
		break;
	case SQL_C_DOUBLE:
		mapitype = MAPI_DOUBLE;
		dstsz = sizeof(SQLDOUBLE);
		break;
	case SQL_C_TYPE_DATE:
		mapitype = MAPI_DATE;
		dstsz = sizeof(SQL_DATE_STRUCT);
		break;
	case SQL_C_TYPE_TIME:
		mapitype = MAPI_TIME;
		dstsz = sizeof(SQL_TIME_STRUCT);
		break;
	case SQL_C_TYPE_TIMESTAMP:
		mapitype = MAPI_DATETIME;
		dstsz = sizeof(SQL_TIMESTAMP_STRUCT);
		break;
	default:
/* TODO: finish implementation */
		/* unimplemented conversion */
		addStmtError(stmt, "07006", NULL, 0);
		return SQL_ERROR;
	}

	if (dstsz > 0 && stmt->retrieved > 0) {
		/* already retrieved, can't retrieve again */
		return SQL_NO_DATA;
	}

	if (dst != NULL) {
		mapi_store_field(stmt->hdl, nCol - 1, mapitype, dst);
		if (mapi_error(stmt->Dbc->mid)) {
			if (strcmp(mapi_error_str(stmt->Dbc->mid), "Field value is nil") == 0) {
				if (pnLengthOrIndicator) {
					*pnLengthOrIndicator = SQL_NULL_DATA;
					return SQL_SUCCESS;
				} else {
					addStmtError(stmt, "22002", NULL, 0);
					return SQL_ERROR;
				}
			}
			addStmtError(stmt, "HY000",
				     mapi_error_str(stmt->Dbc->mid), 0);
			return SQL_ERROR;
		}
	}

	if (mapitype == MAPI_VARCHAR) {
		dstsz = strlen(strptr) + 1;
		if (dstsz == stmt->retrieved)
			return SQL_NO_DATA;
		if (pTarget && nTargetLength > 0) {
			int len = dstsz - stmt->retrieved;

			if (len > nTargetLength) {
				len = nTargetLength;
				addStmtError(stmt, "01004", NULL, 0);
				ret = SQL_SUCCESS_WITH_INFO;
			}
			len--;	/* space for NULL byte */
			strncpy((char *) pTarget, strptr + stmt->retrieved, len);
			((char *) pTarget)[len] = 0;
			stmt->retrieved += len;
		} else {
			addStmtError(stmt, "01004", NULL, 0);
			ret = SQL_SUCCESS_WITH_INFO;
		}
	} else {
		  stmt->retrieved = dstsz;
	}

	if (pnLengthOrIndicator)
		*pnLengthOrIndicator = dstsz;

	return ret;
}
