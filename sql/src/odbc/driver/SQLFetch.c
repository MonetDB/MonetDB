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
 * SQLFetch()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLFetch(SQLHSTMT hStmt)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN retCode = SQL_SUCCESS;
	OdbcOutHostVar *outVars = NULL;
	int idx = 0;


	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	if (stmt->ResultRows == NULL)
		return SQL_NO_DATA;
	if (stmt->nrRows <= 0)
		return SQL_NO_DATA;
	if (stmt->currentRow >= stmt->nrRows)
		return SQL_NO_DATA;

	/* increase the current Row number */
	stmt->currentRow++;

	outVars = stmt->bindCols.array;

	if (outVars == NULL) {
		/* there are no bound output columns, so we are done */
		return SQL_SUCCESS;
	}

	/* transfer result column data to bound column buffers as requested */
	/* do this for each bound column */
	for (idx = 1; idx <= stmt->bindCols.size; idx++) {
		OdbcOutHostVar var = outVars[idx];

		if (var != NULL) {
			/* it is a bound column */
			SQLRETURN rc = ODBCGetData(stmt, var->icol,
						   var->fCType, var->rgbValue,
						   var->cbValueMax,
						   var->pcbValue);

			/* remember the intermediate return value to be
			 * returned when we have processed all bound columns.
			 */
			switch (rc) {
			case SQL_ERROR:
				retCode = rc;
				break;
			case SQL_NO_DATA:
				/* change only when NOT error detected before */
				if (retCode != SQL_ERROR)
					retCode = rc;
				break;
			case SQL_SUCCESS_WITH_INFO:
				/* change only when up till now all went successful */
				if (retCode == SQL_SUCCESS)
					retCode = rc;
				break;
			case SQL_SUCCESS:
			default:
				/* do nothing */
				break;
			}
		}
	}

	return retCode;
}
