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
 * SQLDescribeParam()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLDescribeParam(SQLHSTMT hStmt, SQLUSMALLINT nParmNumber,
		 SQLSMALLINT *pnDataType, SQLUINTEGER *pnSize,
		 SQLSMALLINT *pnDecDigits, SQLSMALLINT *pnNullable)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	ODBCDescRec *rec;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeParam\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be prepared or executed */
	if (stmt->State == INITED) {
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	if (nParmNumber < 1 ||
	    nParmNumber > stmt->ImplRowDescr->sql_desc_count) {
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

	rec = &stmt->ImplRowDescr->descRec[nParmNumber];
	if (pnDataType)
		*pnDataType = rec->sql_desc_concise_type;
	if (pnNullable)
		*pnNullable = rec->sql_desc_nullable;
	if (pnSize) {
		switch (rec->sql_desc_concise_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_LONGVARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_WLONGVARCHAR:
			*pnSize = rec->sql_desc_length;
			break;
		case SQL_DECIMAL:
		case SQL_NUMERIC:
			*pnSize = rec->sql_desc_length;
			break;
		case SQL_BIT:
			*pnSize = 1;
			break;
		case SQL_TINYINT:
			*pnSize = 3;
			break;
		case SQL_SMALLINT:
			*pnSize = 5;
			break;
		case SQL_INTEGER:
			*pnSize = 10;
			break;
		case SQL_BIGINT:
			*pnSize = rec->sql_desc_unsigned ? 20 : 19;
			break;
		case SQL_REAL:
			*pnSize = 7;
			break;
		case SQL_FLOAT:
		case SQL_DOUBLE:
			*pnSize = 15;
			break;
		case SQL_TYPE_DATE:
			*pnSize = 10; /* strlen("yyyy-mm-dd") */
			break;
		case SQL_TYPE_TIME:
			*pnSize = 12; /* strlen("hh:mm:ss.fff") */
			break;
		case SQL_TYPE_TIMESTAMP:
			*pnSize = 23; /* strlen("yyyy-mm-dd hh:mm:ss.fff") */
			break;
		case SQL_INTERVAL_SECOND:
			/* strlen("sss.fff") */
			*pnSize = rec->sql_desc_datetime_interval_precision + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_DAY_TO_SECOND:
			/* strlen("ddd hh:mm:ss.fff") */
			*pnSize = rec->sql_desc_datetime_interval_precision + 9 + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_HOUR_TO_SECOND:
			/* strlen("hhh:mm:ss.fff") */
			*pnSize = rec->sql_desc_datetime_interval_precision + 6 + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_MINUTE_TO_SECOND:
			/* strlen("mmm:ss.fff") */
			*pnSize = rec->sql_desc_datetime_interval_precision + 3 + (rec->sql_desc_precision > 0 ? rec->sql_desc_precision + 1 : 0);
			break;
		case SQL_INTERVAL_YEAR:
		case SQL_INTERVAL_MONTH:
		case SQL_INTERVAL_DAY:
		case SQL_INTERVAL_HOUR:
		case SQL_INTERVAL_MINUTE:
			*pnSize = rec->sql_desc_datetime_interval_precision;
			break;
		case SQL_INTERVAL_YEAR_TO_MONTH:
		case SQL_INTERVAL_DAY_TO_HOUR:
		case SQL_INTERVAL_HOUR_TO_MINUTE:
			*pnSize = rec->sql_desc_datetime_interval_precision + 3;
			break;
		case SQL_INTERVAL_DAY_TO_MINUTE:
			*pnSize = rec->sql_desc_datetime_interval_precision + 6;
			break;
		case SQL_GUID:
			/* strlen("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") */
			*pnSize = 36;
			break;
		default:
			*pnSize = SQL_NO_TOTAL;
			break;
		}
	}
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

	return SQL_SUCCESS;
}
