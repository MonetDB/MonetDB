/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
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
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
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
ODBCStmt * newODBCStmt(ODBCDbc * dbc)
{
	ODBCStmt * stmt = (ODBCStmt *)GDKmalloc(sizeof(ODBCStmt));
	assert(stmt);
	assert(dbc);

	stmt->Dbc = dbc;
	stmt->next = NULL;
	stmt->Error = NULL;

	stmt->State = INITED;
	stmt->Query = NULL;

	stmt->bindParams.array = NULL;
	stmt->bindParams.size = 0;

	stmt->bindCols.array = NULL;
	stmt->bindCols.size = 0;

	stmt->nrCols = 0;
	stmt->nrRows = 0;
	stmt->Result = NULL;
	stmt->currentRow = 0;

	/* add this stmt to the administrative linked stmt list */
	if (dbc->FirstStmt == NULL) {
		/* it is the first stmt within this dbc */
		dbc->FirstStmt = stmt;
	} else {
		/* add it in front of the list */
		stmt->next = dbc->FirstStmt;
		dbc->FirstStmt = stmt;
	}

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
int isValidStmt(ODBCStmt * stmt)
{
	return (stmt && (stmt->Type == ODBC_STMT_MAGIC_NR)) ? 1 : 0;
}


/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCStmt struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: stmt must be valid. SQLState and errMsg may be NULL.
 */
void addStmtError(ODBCStmt * stmt, char * SQLState, char * errMsg, int nativeErrCode)
{
	ODBCError * error = NULL;

	assert(isValidStmt(stmt));

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	if (stmt->Error == NULL) {
		stmt->Error = error;
	} else {
		appendODBCError(stmt->Error, error);
	}
}


/*
 * Adds an error msg object to the end of the error list of
 * this ODBCStmt struct.
 *
 * Precondition: stmt and error must be valid.
 */
void addStmtErrorObj(ODBCStmt * stmt, ODBCError * error)
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
ODBCError * getStmtError(ODBCStmt * stmt)
{
	ODBCError * err;
	assert(isValidStmt(stmt));


	err = stmt->Error;	/* get first error */
	stmt->Error = (err) ? err->next : NULL;	/* set new first error */

	return err;
}



/*
 * Destroys the ODBCStmt object including its own managed data.
 *
 * Precondition: stmt must be valid and inactive (No prepared query or
 * result sets active, internal State == INITED).
 * Postcondition: stmt is completely destroyed, stmt handle is become invalid.
 */
void destroyODBCStmt(ODBCStmt * stmt)
{
	assert(isValidStmt(stmt));
	assert(stmt->State == INITED);

	/* first set this object to invalid */
	stmt->Type = 0;

	/* remove this stmt from the dbc */
	assert(stmt->Dbc);
	assert(stmt->Dbc->FirstStmt);
	{
		/* search for this stmt in the list */
		ODBCStmt * tmp_stmt = (ODBCStmt *) stmt->Dbc->FirstStmt;
		ODBCStmt * prv_stmt = NULL;

		while ((tmp_stmt != NULL) && (tmp_stmt != stmt)) {
			prv_stmt = tmp_stmt;
			tmp_stmt = tmp_stmt->next;
		}

		assert(tmp_stmt == stmt); /* we must have found it */

		/* now remove it from the linked list */
		if (prv_stmt != NULL) {
			prv_stmt->next = stmt->next;
		} else {
			stmt->Dbc->FirstStmt = stmt->next;
		}
	}

	/* cleanup own managed data */
	deleteODBCErrorList(stmt->Error);
	if (stmt->Query) {
		GDKfree(stmt->Query);
	}

	destroyOdbcInArray(&(stmt->bindParams));
	destroyOdbcOutArray(&(stmt->bindCols));

	if (stmt->Result) {
		GDKfree(stmt->Result);
	}

	GDKfree(stmt);
}

