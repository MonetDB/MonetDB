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
 * SQLDescribeCol()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLDescribeCol(SQLHSTMT hStmt, SQLUSMALLINT nCol, SQLCHAR *szColName,
	       SQLSMALLINT nColNameMax, SQLSMALLINT *pnColNameLength,
	       SQLSMALLINT *pnSQLDataType, SQLUINTEGER *pnColSize,
	       SQLSMALLINT *pnDecDigits, SQLSMALLINT *pnNullable)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	ODBCDescRec *rec = NULL;
	int colNameLen = 0;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeCol\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	/* and it should return a result set */
	if (stmt->ImplRowDescr->sql_desc_count == 0) {
		/* 07005 = Prepared statement not a cursor specification */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}
	if (nCol < 1 || nCol > stmt->ImplRowDescr->sql_desc_count) {
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}

	/* OK */
	rec = stmt->ImplRowDescr->descRec + nCol;

	if (rec->sql_desc_name)
		colNameLen = strlen((char *) rec->sql_desc_name);

	/* now copy the data */
	if (szColName) {
		if (rec->sql_desc_name) {
			strncpy((char *) szColName, (char *) rec->sql_desc_name, nColNameMax - 1);
			szColName[nColNameMax - 1] = 0;/* null terminate it */
		} else if (nColNameMax > 0)
			szColName[0] = 0; /* return empty string */
	}
	if (pnColNameLength)
		*pnColNameLength = rec->sql_desc_name ? colNameLen : 0;
	if (colNameLen >= nColNameMax) {
		/* 01004 = String data, right truncation */
		addStmtError(stmt, "01004", NULL, 0);
	}

	if (pnSQLDataType)
		*pnSQLDataType = rec->sql_desc_concise_type;

	/* also see SQLDescribeParam */
	if (pnColSize) {
		switch (rec->sql_desc_concise_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_LONGVARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_WLONGVARCHAR:
			*pnColSize = rec->sql_desc_length;
			break;
		case SQL_DECIMAL:
		case SQL_NUMERIC:
			*pnColSize = rec->sql_desc_length;
			break;
		case SQL_BIT:
			*pnColSize = 1;
			break;
		case SQL_TINYINT:
			*pnColSize = 3;
			break;
		case SQL_SMALLINT:
			*pnColSize = 5;
			break;
		case SQL_INTEGER:
			*pnColSize = 10;
			break;
		case SQL_BIGINT:
			*pnColSize = rec->sql_desc_unsigned ? 20 : 19;
			break;
		case SQL_REAL:
			*pnColSize = 7;
			break;
		case SQL_FLOAT:
		case SQL_DOUBLE:
			*pnColSize = 15;
			break;
		case SQL_TYPE_DATE:
			*pnColSize = 10; /* strlen("yyyy-mm-dd") */
			break;
		case SQL_TYPE_TIME:
			*pnColSize = 12; /* strlen("hh:mm:ss.fff") */
			break;
		case SQL_TYPE_TIMESTAMP:
			*pnColSize = 23; /* strlen("yyyy-mm-dd hh:mm:ss.fff") */
			break;
		case SQL_INTERVAL_SECOND:
			/* strlen("sss.fff") */
			*pnColSize = rec->sql_desc_datetime_interval_precision + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_DAY_TO_SECOND:
			/* strlen("ddd hh:mm:ss.fff") */
			*pnColSize = rec->sql_desc_datetime_interval_precision + 9 + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_HOUR_TO_SECOND:
			/* strlen("hhh:mm:ss.fff") */
			*pnColSize = rec->sql_desc_datetime_interval_precision + 6 + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_MINUTE_TO_SECOND:
			/* strlen("mmm:ss.fff") */
			*pnColSize = rec->sql_desc_datetime_interval_precision + 3 + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_YEAR:
		case SQL_INTERVAL_MONTH:
		case SQL_INTERVAL_DAY:
		case SQL_INTERVAL_HOUR:
		case SQL_INTERVAL_MINUTE:
			*pnColSize = rec->sql_desc_datetime_interval_precision;
			break;
		case SQL_INTERVAL_YEAR_TO_MONTH:
		case SQL_INTERVAL_DAY_TO_HOUR:
		case SQL_INTERVAL_HOUR_TO_MINUTE:
			*pnColSize = rec->sql_desc_datetime_interval_precision + 3;
			break;
		case SQL_INTERVAL_DAY_TO_MINUTE:
			*pnColSize = rec->sql_desc_datetime_interval_precision + 6;
			break;
		case SQL_GUID:
			/* strlen("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") */
			*pnColSize = 36;
			break;
		default:
			*pnColSize = SQL_NO_TOTAL;
			break;
		}
	}

	/* also see SQLDescribeParam */
	if (pnDecDigits) {
		switch (rec->sql_desc_concise_type) {
		case SQL_DECIMAL:
		case SQL_NUMERIC:
			*pnDecDigits = rec->sql_desc_scale;
			break;
		case SQL_BIT:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			*pnDecDigits = 0;
			break;
		case SQL_TYPE_TIME:
		case SQL_TYPE_TIMESTAMP:
		case SQL_INTERVAL_SECOND:
		case SQL_INTERVAL_DAY_TO_SECOND:
		case SQL_INTERVAL_HOUR_TO_SECOND:
		case SQL_INTERVAL_MINUTE_TO_SECOND:
			*pnDecDigits = rec->sql_desc_precision;
			break;
		}
	}

	if (pnNullable)
		*pnNullable = rec->sql_desc_nullable;

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
