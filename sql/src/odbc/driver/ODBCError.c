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

/**********************************************
 * ODBCError.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC error structures/objects (see ODBCError.h)
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCGlobal.h"
#include "ODBCError.h"


const char ODBCErrorMsgPrefix[] = "[MonetDB][ODBC Driver 1.0]";
const int ODBCErrorMsgPrefixLength = sizeof(ODBCErrorMsgPrefix)-1;

/* forward declaration of internal function */
static char * getStandardSQLStateMsg(char * SQLState);


/*
 * Creates a new allocated ODBCError object, initializes it and
 * adds copies of the SQLstate, msg and nativeErrorCode to the object.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCError object
 */
ODBCError * newODBCError(char * SQLState, char * msg, int nativeCode)
{
	ODBCError * error = (ODBCError *)malloc(sizeof(ODBCError));
	assert(error);

	if (SQLState) {
		strncpy(error->sqlState, SQLState, SQL_SQLSTATE_SIZE);
		error->sqlState[SQL_SQLSTATE_SIZE] = '\0';
	} else {
		/* initialize it with nulls */
		int i = 0;
		for (; i <= SQL_SQLSTATE_SIZE; i++)
			error->sqlState[i] = '\0';
	}

	error->message = (msg) ? strdup(msg) : NULL;
	error->nativeErrorCode = nativeCode;
	error->next = NULL;

	return error;
}



/*
 * Get the SQL State code string.
 *
 * Precondition: error must be valid
 * Returns: a none NULL string pointer, intended for reading only.
 */
char * getSqlState(ODBCError * error)
{
	assert(error);
	return error->sqlState;
}


/*
 * Get the Message string.
 *
 * Precondition: error must be valid
 * Returns: a string pointer, intended for reading only, which can be NULL !!.
 */
char * getMessage(ODBCError * error)
{
	assert(error);

	/* check special case */
	if (error->message == NULL) {
		/* No error message was set, use the default error msg
		   for the set sqlState (if a msg is available) */
		char * SQLStateMsg = getStandardSQLStateMsg(error->sqlState);
		assert(SQLStateMsg);
		/* check if a usefull (not empty) msg was found */
		if (strcmp(SQLStateMsg, "") != 0) {
			/* use this message instead of the NULL */
			error->message = strdup(SQLStateMsg);
		}
	}
	return error->message;
}


/*
 * Get the native error code value.
 *
 * Precondition: error must be valid
 * Returns: an int value representing the native (MonetDB) error code.
 */
int getNativeErrorCode(ODBCError * error)
{
	assert(error);
	return error->nativeErrorCode;
}


/*
 * Get the pointer to the next ODBCError object or NULL when there no next object.
 *
 * Precondition: error must be valid
 * Returns: the pointer to the next ODBCError object or NULL when there no next object.
 */
ODBCError * getNextError(ODBCError * error)
{
	assert(error);
	return error->next;
}


/*
 * Local utility function which retuns the standard ODBC/ISO error
 * message text for a given ISO SQLState code.
 * When no message could be found for a given SQLState a msg is
 * printed to stderr to warn that the programmer has forgotten to
 * add the message for the SQLState code.
 *
 * Precondition: SQLState is a valid string (non null, 5 chars long).
 * Postcondition: returns a valid pointer to a string (which may be empty).
 */
static char * getStandardSQLStateMsg(char * SQLState)
{
	assert(SQLState);

#ifdef SQLStateEQ
#undef SQLStateEQ
#endif
#define SQLStateEQ(s1) (strcmp(SQLState,s1) == 0)

	switch (SQLState[0])
	{
	case '0':
		if (SQLStateEQ("01000")) return "General warning";
		if (SQLStateEQ("01004")) return "String data, right truncation";
		if (SQLStateEQ("07005")) return "Prepared statement not a cursor-specification";
		if (SQLStateEQ("07009")) return "Invalid descriptor index";
		if (SQLStateEQ("08001")) return "Client unable to establish connection";
		if (SQLStateEQ("08002")) return "Connection already in use";
		if (SQLStateEQ("08003")) return "Connection does not exist";
		if (SQLStateEQ("08S01")) return "Communication link failure";
		break;
	case '2':
		if (SQLStateEQ("24000")) return "Invalid cursor state";
		if (SQLStateEQ("25000")) return "Invalid transaction state";
		break;
	case 'H':
		if (SQLStateEQ("HY000")) return "General error";
		if (SQLStateEQ("HY001")) return "Memory allocation error";
		if (SQLStateEQ("HY003")) return "Invalid application buffer type";
		if (SQLStateEQ("HY004")) return "Invalid SQL data type";
		if (SQLStateEQ("HY009")) return "Invalid use of null pointer";
		if (SQLStateEQ("HY010")) return "Function sequence error";
		if (SQLStateEQ("HY012")) return "Invalid transaction operation code";
		if (SQLStateEQ("HY013")) return "Memory management error";
		if (SQLStateEQ("HY015")) return "No cursor name available";
		if (SQLStateEQ("HY090")) return "Invalid string or buffer length";
		if (SQLStateEQ("HY091")) return "Invalid descriptor field identifier";
		if (SQLStateEQ("HY092")) return "Invalid attribute/option identifier";
		if (SQLStateEQ("HY096")) return "Information type out of range";
		if (SQLStateEQ("HY097")) return "Column type out of range";
		if (SQLStateEQ("HY098")) return "Scope type out of range";
		if (SQLStateEQ("HY099")) return "Nullable type out of range";
		if (SQLStateEQ("HY100")) return "Uniqueness option type out of range";
		if (SQLStateEQ("HY101")) return "Accuracy option type out of range";
		if (SQLStateEQ("HYC00")) return "Optional feature not implemented";
		if (SQLStateEQ("HYT01")) return "Connection timeout expired";
		break;
	case 'I':
		if (SQLStateEQ("IM001")) return "Driver does not support this function";
		if (SQLStateEQ("IM002")) return "Data source not found";
		break;
	default:
		break;
	}

#undef SQLStateEQ

	/* Present a msg to notify the system administrator/programmer */
	fprintf(stderr, "\nMonetDB, ODBC Driver, ODBCError.c: No message defined for SQLState: %s. Please report this error.\n", SQLState);

	return "";	/* always return a string */
}


/*
 * Appends a valid ODBCError object 'this' to the end of the list
 * of a valid ODBCError object 'head'.
 *
 * Precondition: both head and this must be valid (non NULL)
 */
void appendODBCError(ODBCError * head, ODBCError * this)
{
	ODBCError * error = head;

	assert(head);
	assert(this);	/* if this could be NULL this function would do nothing */

	/* search for the last ODBCError in the linked list */
	while (error->next) {
		error = error->next;
	}

	/* add 'this' in the last ODBCError */
	error->next = this;
}


/*
 * Prepends a valid ODBCError object 'this' to the front of the list
 * of a valid ODBCError object 'head' and return the new head.
 *
 * Precondition: both head and this must be valid (non NULL)
 * Returns: the new head (which is the same as the prepended 'this').
 */
ODBCError * prependODBCError(ODBCError * head, ODBCError * this)
{
	assert(head);	/* if head could be NULL this function would do nothing */
	assert(this);

	this->next = head;

	return this;
}


/*
 * Frees the internally allocated data (msg) and the ODBCError object itself.
 *
 * Precondition: error must be valid
 */
void deleteODBCError(ODBCError * error)
{
	assert(error);

	if (error->message) {
		free(error->message);
	}
	error->next = NULL;
	free(error);
}


/*
 * Frees the ODBCError object including its linked ODBCError objects.
 *
 * Precondition: none (error may be NULL)
 */
void deleteODBCErrorList(ODBCError * error)
{
	ODBCError * nxt = NULL;

	while (error) {
		nxt = error->next;
		deleteODBCError(error);
		error = nxt;
	}
}

