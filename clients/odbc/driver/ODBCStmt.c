/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 * ODBCStmt.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC statement structures/objects (see ODBCStmt.h).
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

#define ODBC_STMT_MAGIC_NR  5461	/* for internal sanity check only */


/*
 * Creates a new allocated ODBCStmt object and initializes it.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCStmt object
 */
ODBCStmt *
newODBCStmt(ODBCDbc *dbc)
{
	ODBCStmt *stmt = (ODBCStmt *) malloc(sizeof(ODBCStmt));

	assert(dbc);
	assert(dbc->mid);

	if (stmt == NULL) {
		/* Memory allocation error */
		addDbcError(dbc, "HY001", NULL, 0);
		return NULL;
	}

	stmt->Dbc = dbc;
	stmt->Error = NULL;
	stmt->RetrievedErrors = 0;

	stmt->State = INITED;
	stmt->hdl = mapi_new_handle(dbc->mid);
	if (stmt->hdl == NULL) {
		/* Memory allocation error */
		addDbcError(dbc, "HY001", NULL, 0);
		free(stmt);
		return NULL;
	}
	assert(stmt->hdl);

	stmt->currentRow = 0;
	stmt->startRow = 0;
	stmt->rowSetSize = 0;
	stmt->queryid = -1;
	stmt->nparams = 0;
	stmt->querytype = -1;
	stmt->rowcount = 0;

	/* add this stmt to the administrative linked stmt list */
	stmt->next = dbc->FirstStmt;
	dbc->FirstStmt = stmt;

	stmt->cursorType = SQL_CURSOR_FORWARD_ONLY;
	stmt->cursorScrollable = SQL_NONSCROLLABLE;
	stmt->retrieveData = SQL_RD_ON;
	stmt->noScan = SQL_NOSCAN_OFF;

	stmt->ApplRowDescr = newODBCDesc(dbc);
	stmt->ApplParamDescr = newODBCDesc(dbc);
	stmt->ImplRowDescr = newODBCDesc(dbc);
	stmt->ImplParamDescr = newODBCDesc(dbc);
	stmt->AutoApplRowDescr = stmt->ApplRowDescr;
	stmt->AutoApplParamDescr = stmt->ApplParamDescr;

	if (stmt->ApplRowDescr == NULL || stmt->ApplParamDescr == NULL ||
	    stmt->ImplRowDescr == NULL || stmt->ImplParamDescr == NULL) {
		destroyODBCStmt(stmt);
		return NULL;
	}

	stmt->ApplRowDescr->sql_desc_alloc_type = SQL_DESC_ALLOC_AUTO;
	stmt->ApplParamDescr->sql_desc_alloc_type = SQL_DESC_ALLOC_AUTO;
	stmt->ImplRowDescr->sql_desc_alloc_type = SQL_DESC_ALLOC_AUTO;
	stmt->ImplParamDescr->sql_desc_alloc_type = SQL_DESC_ALLOC_AUTO;
	stmt->ImplRowDescr->Stmt = stmt;
	stmt->ImplParamDescr->Stmt = stmt;

	stmt->Type = ODBC_STMT_MAGIC_NR;	/* set it valid */

	return stmt;
}


/*
 * Check if the statement handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns 1 if it is a valid statement handle,
 * 	returns 0 if is invalid and thus an unusable handle.
 */
int
isValidStmt(ODBCStmt *stmt)
{
#ifdef ODBCDEBUG
	if (!(stmt &&stmt->Type == ODBC_STMT_MAGIC_NR))
		ODBCLOG("stmt " PTRFMT " not a valid statement handle\n", PTRFMTCAST stmt);
#endif
	return stmt &&stmt->Type == ODBC_STMT_MAGIC_NR;
}


/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCStmt struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: stmt must be valid. SQLState and errMsg may be NULL.
 */
void
addStmtError(ODBCStmt *stmt, const char *SQLState, const char *errMsg, int nativeErrCode)
{
	ODBCError *error = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("addStmtError " PTRFMT " %s %s %d\n", PTRFMTCAST stmt, SQLState, errMsg ? errMsg : getStandardSQLStateMsg(SQLState), nativeErrCode);
#endif
	assert(isValidStmt(stmt));

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	appendODBCError(&stmt->Error, error);
}


/*
 * Extracts an error object from the error list of this ODBCStmt struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: stmt and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError *
getStmtError(ODBCStmt *stmt)
{
	assert(isValidStmt(stmt));
	return stmt->Error;
}



/*
 * Destroys the ODBCStmt object including its own managed data.
 *
 * Precondition: stmt must be valid.
 * Postcondition: stmt is completely destroyed, stmt handle is invalid.
 */
void
destroyODBCStmt(ODBCStmt *stmt)
{
	ODBCStmt **stmtp;

	assert(isValidStmt(stmt));

	/* first set this object to invalid */
	stmt->Type = 0;

	/* remove this stmt from the dbc */
	assert(stmt->Dbc);
	assert(stmt->Dbc->FirstStmt);

	/* search for stmt in linked list */
	stmtp = &stmt->Dbc->FirstStmt;

	while (*stmtp && *stmtp != stmt)
		stmtp = &(*stmtp)->next;
	/* stmtp points to location in list where stmt is found */

	assert(*stmtp == stmt);	/* we must have found it */

	/* now remove it from the linked list */
	*stmtp = stmt->next;

	/* cleanup own managed data */
	deleteODBCErrorList(&stmt->Error);

	destroyODBCDesc(stmt->ImplParamDescr);
	destroyODBCDesc(stmt->ImplRowDescr);
	destroyODBCDesc(stmt->AutoApplParamDescr);
	destroyODBCDesc(stmt->AutoApplRowDescr);

	if (stmt->hdl)
		mapi_close_handle(stmt->hdl);

	free(stmt);
}
