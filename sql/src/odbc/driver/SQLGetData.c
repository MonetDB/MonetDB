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
 * SQLGetData()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQLGetData(
	SQLHSTMT	hStmt,
	SQLUSMALLINT	nCol,
	SQLSMALLINT	nTargetType,	/* C DATA TYPE */
	SQLPOINTER	pTarget,
	SQLINTEGER	nTargetLength,
	SQLINTEGER *	pnLengthOrIndicator )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;

	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return ODBCGetData(stmt, nCol, nTargetType, pTarget, nTargetLength, pnLengthOrIndicator);
}


/* Internal help function which is used both by SQLGetData() and SQLFetch().
 * It does not clear the errors (only adds any when needed) so it can
 * be called multiple times from SQLFetch().
 * It gets the data of one field in the current result row of the result set.
 */
SQLRETURN ODBCGetData(
	ODBCStmt *	stmt,
	SQLUSMALLINT	nCol,
	SQLSMALLINT	nTargetType,	/* C DATA TYPE */
	SQLPOINTER	pTarget,
	SQLINTEGER	nTargetLength,
	SQLINTEGER *	pnLengthOrIndicator )
{
	char *	pSourceData    = NULL;

	/* in the next cases this function should not have been called */
	assert(stmt != NULL);
	assert(isValidStmt(stmt));


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
	if (nCol > stmt->nrCols) {
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

	/* when no Result or rows are their return SQL_NO_DATA */
	if (stmt->ResultRows == NULL) {
		return SQL_NO_DATA;
	}
	if (stmt->nrRows <= 0) {
		return SQL_NO_DATA;
	}
	if (stmt->currentRow > stmt->nrRows) {
		return SQL_NO_DATA;
	}



/* TODO: finish implementation */

	/**********************************************************************
	 * GET pSourceData FOR NORMAL RESULT SETS
	 **********************************************************************/
	pSourceData = (stmt->ResultRows)[(stmt->currentRow * stmt->nrCols) + nCol];

	/****************************
	 * ALL cols are stored as SQL_CHAR... bad for storage... good for code
	 * SO no need to determine the source type when translating to destination
	 ***************************/
	if (pSourceData != NULL)
	{
		/*********************
		 * Now get the col when we have a value
		 *********************/
			if (pnLengthOrIndicator != NULL) {
			*pnLengthOrIndicator = SQL_NULL_DATA;
		}

		if (pTarget != NULL) {
		switch (nTargetType)
		{
		case SQL_C_CHAR:
			strncpy(pTarget, pSourceData, nTargetLength -1);
			if (pnLengthOrIndicator != NULL) {
				*pnLengthOrIndicator = strlen(pTarget);
			}
			break;
		case SQL_C_LONG:
			*((int *)pTarget) = atoi(pSourceData);
			if (pnLengthOrIndicator != NULL) {
				*pnLengthOrIndicator = sizeof( int );
			}
			break;
		case SQL_C_FLOAT:
			sscanf( pSourceData, "%g", pTarget );
			if (pnLengthOrIndicator != NULL) {
				*pnLengthOrIndicator = sizeof( float );
			}
			break;
		default:
			/* TODO: add error: unsuported conversion */
			break;
#if 0
#define SQL_C_CHAR    SQL_CHAR             /* CHAR, VARCHAR, DECIMAL, NUMERIC */#define SQL_C_LONG    SQL_INTEGER          /* INTEGER                      */
#define SQL_C_SHORT   SQL_SMALLINT         /* SMALLINT                     */
#define SQL_C_FLOAT   SQL_REAL             /* REAL                         */
#define SQL_C_DOUBLE  SQL_DOUBLE           /* FLOAT, DOUBLE                */
#define SQL_C_DEFAULT 99
#define SQL_C_DATE       SQL_DATE
#define SQL_C_TIME       SQL_TIME
#define SQL_C_TIMESTAMP  SQL_TIMESTAMP
#define SQL_C_TYPE_DATE                                 SQL_TYPE_DATE
#define SQL_C_TYPE_TIME                                 SQL_TYPE_TIME
#define SQL_C_TYPE_TIMESTAMP                    SQL_TYPE_TIMESTAMP
#define SQL_C_INTERVAL_YEAR                             SQL_INTERVAL_YEAR
#define SQL_C_INTERVAL_MONTH                    SQL_INTERVAL_MONTH
#define SQL_C_INTERVAL_DAY                              SQL_INTERVAL_DAY
#define SQL_C_INTERVAL_HOUR                             SQL_INTERVAL_HOUR
#define SQL_C_INTERVAL_MINUTE                   SQL_INTERVAL_MINUTE
#define SQL_C_INTERVAL_SECOND                   SQL_INTERVAL_SECOND
#define SQL_C_INTERVAL_YEAR_TO_MONTH    SQL_INTERVAL_YEAR_TO_MONTH
#define SQL_C_INTERVAL_DAY_TO_HOUR              SQL_INTERVAL_DAY_TO_HOUR
#define SQL_C_INTERVAL_DAY_TO_MINUTE    SQL_INTERVAL_DAY_TO_MINUTE
#define SQL_C_INTERVAL_DAY_TO_SECOND    SQL_INTERVAL_DAY_TO_SECOND
#define SQL_C_INTERVAL_HOUR_TO_MINUTE   SQL_INTERVAL_HOUR_TO_MINUTE
#define SQL_C_INTERVAL_HOUR_TO_SECOND   SQL_INTERVAL_HOUR_TO_SECOND
#define SQL_C_INTERVAL_MINUTE_TO_SECOND SQL_INTERVAL_MINUTE_TO_SECOND
#define SQL_C_BINARY     SQL_BINARY
#define SQL_C_BIT        SQL_BIT
#define SQL_C_SBIGINT   (SQL_BIGINT+SQL_SIGNED_OFFSET)     /* SIGNED BIGINT */
#define SQL_C_TINYINT    SQL_TINYINT
#define SQL_C_SLONG      (SQL_C_LONG+SQL_SIGNED_OFFSET)    /* SIGNED INTEGER  */
#define SQL_C_SSHORT     (SQL_C_SHORT+SQL_SIGNED_OFFSET)   /* SIGNED SMALLINT */
#define SQL_C_STINYINT   (SQL_TINYINT+SQL_SIGNED_OFFSET)   /* SIGNED TINYINT  */
#define SQL_C_ULONG      (SQL_C_LONG+SQL_UNSIGNED_OFFSET)  /* UNSIGNED INTEGER*/
#define SQL_C_USHORT     (SQL_C_SHORT+SQL_UNSIGNED_OFFSET) /* UNSIGNED SMALLINT*/
#define SQL_C_UTINYINT   (SQL_TINYINT+SQL_UNSIGNED_OFFSET) /* UNSIGNED TINYINT*/
#define SQL_C_BOOKMARK   SQL_C_ULONG                       /* BOOKMARK        */
#define SQL_C_GUID      SQL_GUID
#endif
		}
		}
	}
	else
	{
		/* it is a NULL value */

		if (pnLengthOrIndicator != NULL) {
			*pnLengthOrIndicator = SQL_NULL_DATA;
		}

		if (pTarget != NULL) {
		switch (nTargetType)
		{
		case SQL_C_CHAR:
			*((char *)pTarget) = '\0';
			break;
		case SQL_C_LONG:
			memset( pTarget, 0, sizeof(int) );
			break;
		case SQL_C_FLOAT:
			memset( pTarget, 0, sizeof(float) );
			break;
		default:
			/* TODO: add error: unsuported conversion */
			break;
		}
		}
	}

	return SQL_SUCCESS;
}
