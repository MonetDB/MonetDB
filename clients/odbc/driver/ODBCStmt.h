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

/**********************************************
 * ODBCStmt.h
 *
 * Description:
 * This file contains the ODBC statement structure
 * and function prototypes on this structure.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCSTMT
#define _H_ODBCSTMT

#include "ODBCGlobal.h"
#include "ODBCError.h"
#include "ODBCDbc.h"
#include "ODBCDesc.h"

/* some statement related ODBC driver defines */
#define MONETDB_MAX_BIND_COLS	8192


/* these states parallel the Statement Transitions section from
   Appendix B: ODBC State Transition Tables */
enum StatementState {
	/* order is important */
	INITED,		/* S1: allocated statement */
	PREPARED0,	/* S2: statement prepared, no result set */
	PREPARED1,	/* S3: statement prepared, expect result set */
	EXECUTED0,	/* S4: statement executed, no result set */
	EXECUTED1,	/* S5: statement executed, with result set */
	FETCHED,	/* S6: cursor positioned with SQLFetch(Scroll) */
	EXTENDEDFETCHED	/* S7: cursor positioned with SQLExtendedFetch */
};

typedef struct tODBCDRIVERSTMT {
	/* Stmt properties */
	int Type;		/* structure type, used for handle validy test */
	ODBCError *Error;	/* pointer to an Error object or NULL */
	int RetrievedErrors;	/* # of errors already retrieved by SQLError */
	ODBCDbc *Dbc;		/* Connection context */
	struct tODBCDRIVERSTMT *next;	/* the linked list of stmt's in this Dbc */
	enum StatementState State;	/* needed to detect invalid cursor state */
	MapiHdl hdl;

	SQLULEN rowcount;	/* # affected rows */

	/* startRow is the row number of first row in the result
	   set (0 based); rowSetSize is the number of rows in the
	   current result set; currentRow is the row number of the
	   current row within the current result set */
	SQLLEN currentRow;
	SQLLEN startRow;
	SQLLEN rowSetSize;

	unsigned int currentCol; /* used by SQLGetData() */
	SQLINTEGER retrieved;	/* amount of data retrieved */
	int queryid;		/* the query to be executed */
	int nparams;		/* the number of parameters expected */

	int querytype;		/* query type as returned by server */

	SQLULEN qtimeout;	/* query timeout requested */

	SQLUINTEGER cursorType;
	SQLULEN cursorScrollable;
	SQLULEN retrieveData;
	SQLULEN noScan;

	ODBCDesc *ApplRowDescr;	/* Application Row Descriptor (ARD) */
	ODBCDesc *ApplParamDescr; /* Application Parameter Descriptor (APD) */
	ODBCDesc *ImplRowDescr;	/* Implementation Row Descriptor (IRD) */
	ODBCDesc *ImplParamDescr; /* Implementation Parameter Descriptor (IPD) */

	ODBCDesc *AutoApplRowDescr; /* Auto-allocated ARD */
	ODBCDesc *AutoApplParamDescr; /* Auto-allocated APD */

	/* Stmt children: none yet */
} ODBCStmt;



/*
 * Creates a new allocated ODBCStmt object and initializes it.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCStmt object
 */
ODBCStmt *newODBCStmt(ODBCDbc *dbc);


/*
 * Check if the statement handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns 1 if it is a valid statement handle,
 * 	returns 0 if is invalid and thus an unusable handle.
 */
int isValidStmt(ODBCStmt *stmt);


/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCStmt struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: stmt must be valid. SQLState and errMsg may be NULL.
 */
void addStmtError(ODBCStmt *stmt, const char *SQLState, const char *errMsg, int nativeErrCode);


/*
 * Extracts an error object from the error list of this ODBCStmt struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: stmt and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError *getStmtError(ODBCStmt *stmt);


/* utility macro to quickly remove any none collected error msgs */
#define clearStmtErrors(stmt) do {					\
				assert(stmt);				\
				if ((stmt)->Error) {			\
					deleteODBCErrorList(&(stmt)->Error); \
					(stmt)->RetrievedErrors = 0;	\
				}					\
			} while (0)


/*
 * Destroys the ODBCStmt object including its own managed data.
 *
 * Precondition: stmt must be valid.
 * Postcondition: stmt is completely destroyed, stmt handle is invalid.
 */
void destroyODBCStmt(ODBCStmt *stmt);


/* Internal help function which is used both by SQLGetData() and SQLFetch().
 * It does not clear the errors (only adds any when needed) so it can
 * be called multiple times from SQLFetch().
 * It gets the data of one field in the current result row of the result set.
 */
SQLRETURN ODBCGetData(ODBCStmt *stmt, SQLUSMALLINT nCol,
		      SQLSMALLINT nTargetType, SQLPOINTER pTarget,
		      SQLINTEGER nTargetLength,
		      SQLINTEGER *pnLengthOrIndicator);


SQLRETURN ODBCFetch(ODBCStmt *stmt, SQLUSMALLINT col, SQLSMALLINT type,
		    SQLPOINTER ptr, SQLLEN buflen, SQLLEN *lenp,
		    SQLLEN *nullp, SQLSMALLINT precision, SQLSMALLINT scale,
		    SQLINTEGER datetime_interval_precision, SQLLEN offset,
		    SQLULEN row);
SQLRETURN ODBCStore(ODBCStmt *stmt, SQLUSMALLINT param, SQLLEN offset,
		    SQLULEN row, char **bufp, size_t *bufposp, size_t *buflenp,
		    char *sep);
SQLRETURN ODBCFreeStmt_(ODBCStmt *stmt);
SQLRETURN ODBCInitResult(ODBCStmt *stmt);
const char *ODBCGetTypeInfo(int concise_type, int *data_type,
			    int *sql_data_type, int *sql_datetime_sub);
int ODBCConciseType(const char *name);
void ODBCResetStmt(ODBCStmt *stmt);
SQLRETURN MNDBBindParameter(ODBCStmt *stmt, SQLUSMALLINT ParameterNumber,
			    SQLSMALLINT InputOutputType, SQLSMALLINT ValueType,
			    SQLSMALLINT ParameterType, SQLULEN ColumnSize,
			    SQLSMALLINT DecimalDigits,
			    SQLPOINTER ParameterValuePtr, SQLLEN BufferLength,
			    SQLLEN *StrLen_or_IndPtr);
SQLRETURN MNDBColAttribute(ODBCStmt *stmt,
			   SQLUSMALLINT nCol,
			   SQLUSMALLINT nFieldIdentifier,
			   SQLPOINTER pszValue, SQLSMALLINT nValueLengthMax,
			   SQLSMALLINT *pnValueLength, LENP_OR_POINTER_T pnValue);
SQLRETURN MNDBExecDirect(ODBCStmt *stmt, SQLCHAR *szSqlStr,
			 SQLINTEGER nSqlStr);
SQLRETURN MNDBExecute(ODBCStmt *stmt);
SQLRETURN MNDBFetch(ODBCStmt *stmt, SQLUSMALLINT *RowStatusArray);
SQLRETURN MNDBFetchScroll(ODBCStmt *stmt, SQLSMALLINT nOrientation,
			  SQLLEN nOffset, SQLUSMALLINT *RowStatusArray);
SQLRETURN MNDBFreeStmt(ODBCStmt *stmt, SQLUSMALLINT option);
SQLRETURN MNDBGetStmtAttr(ODBCStmt *stmt, SQLINTEGER Attribute,
			  SQLPOINTER Value, SQLINTEGER BufferLength,
			  SQLINTEGER *StringLength);
SQLRETURN MNDBPrepare(ODBCStmt *stmt, SQLCHAR *szSqlStr,
		      SQLINTEGER nSqlStrLength);
SQLRETURN MNDBSetStmtAttr(ODBCStmt *stmt, SQLINTEGER Attribute,
			  SQLPOINTER Value, SQLINTEGER StringLength);

#endif
