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
 * Author: Martin van Dinther
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
	assert(stmt);

	assert(dbc);
	assert(dbc->mid);

	stmt->Dbc = dbc;
	stmt->Error = NULL;
	stmt->RetrievedErrors = 0;

	stmt->State = INITED;
	stmt->hdl = mapi_new_handle(dbc->mid);
	assert(stmt->hdl);

	stmt->nrCols = 0;
	stmt->ResultCols = NULL;
	stmt->currentRow = 0;

	stmt->maxbindings = 0;
	stmt->bindings = NULL;

	/* add this stmt to the administrative linked stmt list */
	stmt->next = dbc->FirstStmt;
	dbc->FirstStmt = stmt;

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
	return stmt && stmt->Type == ODBC_STMT_MAGIC_NR;
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
addStmtError(ODBCStmt *stmt, const char *SQLState, const char *errMsg,
	     int nativeErrCode)
{
	ODBCError *error = NULL;

	assert(isValidStmt(stmt));

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	if (stmt->Error == NULL)
		stmt->Error = error;
	else
		appendODBCError(stmt->Error, error);
}


/*
 * Adds an error msg object to the end of the error list of
 * this ODBCStmt struct.
 *
 * Precondition: stmt and error must be valid.
 */
void
addStmtErrorObj(ODBCStmt *stmt, ODBCError *error)
{
	assert(isValidStmt(stmt));

	assert(error);

	if (stmt->Error == NULL) {
		stmt->Error = error;
	} else {
		appendODBCError(stmt->Error, error);
	}
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



void
ODBCfreeResultCol(ODBCStmt *stmt)
{
	if (stmt->ResultCols) {
		ColumnHeader *pCol;

		for (pCol = stmt->ResultCols + 1;
		     pCol <= stmt->ResultCols + stmt->nrCols;
		     pCol++) {
			if (pCol->pszSQL_DESC_BASE_COLUMN_NAME)
				free(pCol->pszSQL_DESC_BASE_COLUMN_NAME);
			if (pCol->pszSQL_DESC_LABEL)
				free(pCol->pszSQL_DESC_LABEL);
			if (pCol->pszSQL_DESC_NAME)
				free(pCol->pszSQL_DESC_NAME);
			if (pCol->pszSQL_DESC_TYPE_NAME)
				free(pCol->pszSQL_DESC_TYPE_NAME);
			if (pCol->pszSQL_DESC_BASE_TABLE_NAME)
				free(pCol->pszSQL_DESC_BASE_TABLE_NAME);
			if (pCol->pszSQL_DESC_LOCAL_TYPE_NAME)
				free(pCol->pszSQL_DESC_LOCAL_TYPE_NAME);
			if (pCol->pszSQL_DESC_CATALOG_NAME)
				free(pCol->pszSQL_DESC_CATALOG_NAME);
			if (pCol->pszSQL_DESC_LITERAL_PREFIX)
				free(pCol->pszSQL_DESC_LITERAL_PREFIX);
			if (pCol->pszSQL_DESC_LITERAL_SUFFIX)
				free(pCol->pszSQL_DESC_LITERAL_SUFFIX);
			if (pCol->pszSQL_DESC_SCHEMA_NAME)
				free(pCol->pszSQL_DESC_SCHEMA_NAME);
			if (pCol->pszSQL_DESC_TABLE_NAME)
				free(pCol->pszSQL_DESC_TABLE_NAME);
		}
		free(stmt->ResultCols);
		stmt->ResultCols = NULL;
	}

}

/*
 * Destroys the ODBCStmt object including its own managed data.
 *
 * Precondition: stmt must be valid and inactive (internal State == INITED or
 * State == PREPARED, so NO active result set).
 * Postcondition: stmt is completely destroyed, stmt handle is become invalid.
 */
void
destroyODBCStmt(ODBCStmt *stmt)
{
	ODBCStmt **stmtp;

	assert(isValidStmt(stmt));
	assert(stmt->State == INITED || stmt->State == PREPARED);

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

	assert(*stmtp == stmt);/* we must have found it */

	/* now remove it from the linked list */
	*stmtp = stmt->next;

	/* cleanup own managed data */
	deleteODBCErrorList(stmt->Error);

	ODBCfreebindcol(stmt);
	if (stmt->hdl)
		mapi_close_handle(stmt->hdl);

	ODBCfreeResultCol(stmt);

	free(stmt);
}

void *
ODBCaddbindcol(ODBCStmt *stmt, SQLUSMALLINT nCol, SQLPOINTER pTargetValue,
	       SQLINTEGER nTargetValueMax, SQLINTEGER *pnLengthOrIndicator)
{
	int i, f = -1;

	for (i = 0; i < stmt->maxbindings; i++) {
		if (stmt->bindings[i].column == nCol)
			break;
		if (stmt->bindings[i].column == 0)
			f = i;
	}
	if (i == stmt->maxbindings) {
		/* not found */
		if (f >= 0)
			i = f;	/* use free entry */
		else if (stmt->maxbindings > 0)
			stmt->bindings = realloc(stmt->bindings, ++stmt->maxbindings * sizeof(ODBCBIND));
		else
			stmt->bindings = malloc(++stmt->maxbindings * sizeof(ODBCBIND));
	}

	stmt->bindings[i].column = nCol;
	stmt->bindings[i].pTargetValue = pTargetValue;
	stmt->bindings[i].pnLengthOrIndicator = pnLengthOrIndicator;
	stmt->bindings[i].nTargetValueMax = nTargetValueMax;
	stmt->bindings[i].pszTargetStr = 0;

	return &stmt->bindings[i].pszTargetStr;
}

void
ODBCdelbindcol(ODBCStmt *stmt, SQLUSMALLINT nCol)
{
	int i;

	for (i = 0; i < stmt->maxbindings; i++)
		if (stmt->bindings[i].column == nCol) {
			memset(&stmt->bindings[i], 0, sizeof(ODBCBIND));
			break;
		}
}

void
ODBCfreebindcol(ODBCStmt *stmt)
{
	if (stmt->bindings)
		free(stmt->bindings);
	stmt->bindings = NULL;
	stmt->maxbindings = 0;
}
