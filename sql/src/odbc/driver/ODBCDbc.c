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
 * ODBCDbc.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC connection structures/objects (see ODBCDbc.h).
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"

#define ODBC_DBC_MAGIC_NR  1365	/* for internal sanity check only */


/*
 * Creates a new allocated ODBCDbc object and initializes it.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCDbc object
 */
ODBCDbc *
newODBCDbc(ODBCEnv *env)
{
	ODBCDbc *dbc = (ODBCDbc *) malloc(sizeof(ODBCDbc));

	assert(dbc);
	assert(env);

	dbc->Env = env;
	dbc->next = NULL;
	dbc->Error = NULL;

	dbc->DSN = NULL;
	dbc->UID = NULL;
	dbc->PWD = NULL;
	dbc->DBNAME = NULL;

	dbc->Connected = 0;
	dbc->autocommit = 0;
	dbc->socket = 0;
	dbc->Mrs = NULL;
	dbc->Mws = NULL;
	dbc->Mdebug = 0;

	dbc->FirstStmt = NULL;

	/* add this dbc to the administrative linked dbc list */
	if (env->FirstDbc == NULL) {
		/* it is the first dbc within this env */
		env->FirstDbc = dbc;
	} else {
		/* add it in front of the list */
		dbc->next = env->FirstDbc;
		env->FirstDbc = dbc;
	}

	dbc->Type = ODBC_DBC_MAGIC_NR;	/* set it valid */

	return dbc;
}


/*
 * Check if the connection handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns 1 if it is a valid connection handle,
 * 	returns 0 if is invalid and thus an unusable handle.
 */
int
isValidDbc(ODBCDbc *dbc)
{
	return dbc && dbc->Type == ODBC_DBC_MAGIC_NR;
}



/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCDbc struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: dbc must be valid. SQLState and errMsg may be NULL.
 */
void
addDbcError(ODBCDbc *dbc, const char *SQLState, const char *errMsg,
	    int nativeErrCode)
{
	ODBCError *error = NULL;

	assert(isValidDbc(dbc));

	fprintf(stderr, errMsg);

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	if (dbc->Error == NULL) {
		dbc->Error = error;
	} else {
		appendODBCError(dbc->Error, error);
	}
}


/*
 * Adds an error msg object to the end of the error list of
 * this ODBCDbc struct.
 *
 * Precondition: dbc and error must be valid.
 */
void
addDbcErrorObj(ODBCDbc *dbc, ODBCError *error)
{
	assert(isValidDbc(dbc));
	assert(error);

	if (dbc->Error == NULL) {
		dbc->Error = error;
	} else {
		appendODBCError(dbc->Error, error);
	}
}


/*
 * Extracts an error object from the error list of this ODBCDbc struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: dbc and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError *
getDbcError(ODBCDbc *dbc)
{
	assert(isValidDbc(dbc));

	return dbc->Error;
}


/*
 * Destroys the ODBCDbc object including its own managed data.
 *
 * Precondition: dbc must be valid, inactive (not connected) and
 * no ODBCStmt (or ODBCDesc) objects may refer to this dbc.
 * Postcondition: dbc is completely destroyed, dbc handle is become invalid.
 */
void
destroyODBCDbc(ODBCDbc *dbc)
{
	assert(isValidDbc(dbc));
	assert(dbc->Connected == 0);
	assert(dbc->FirstStmt == NULL);

	/* first set this object to invalid */
	dbc->Type = 0;

	/* remove this dbc from the env */
	assert(dbc->Env);
	assert(dbc->Env->FirstDbc);
	{
		/* search for this dbc in the list */
		ODBCDbc *tmp_dbc = (ODBCDbc *) dbc->Env->FirstDbc;
		ODBCDbc *prv_dbc = NULL;

		while ((tmp_dbc != NULL) && (tmp_dbc != dbc)) {
			prv_dbc = tmp_dbc;
			tmp_dbc = tmp_dbc->next;
		}

		assert(tmp_dbc == dbc);	/* we must have found it */

		/* now remove it from the linked list */
		if (prv_dbc != NULL) {
			prv_dbc->next = dbc->next;
		} else {
			dbc->Env->FirstDbc = dbc->next;
		}
	}

	/* cleanup own managed data */
	deleteODBCErrorList(dbc->Error);
	if (dbc->DSN) {
		free(dbc->DSN);
	}
	if (dbc->UID) {
		free(dbc->UID);
	}
	if (dbc->PWD) {
		free(dbc->PWD);
	}
	if (dbc->DBNAME) {
		free(dbc->DBNAME);
	}

	free(dbc);
}
