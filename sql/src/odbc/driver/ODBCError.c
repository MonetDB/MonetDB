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

struct tODBCError {
	char sqlState[SQL_SQLSTATE_SIZE + 1];	/* +1 for the string terminator */
	char *message;		/* pointer to the allocated error msg */
	int nativeErrorCode;

	struct tODBCError *next;	/* pointer to the next Error object or NULL */
};

const char ODBCErrorMsgPrefix[] = "[MonetDB][ODBC Driver 1.0]";
const int ODBCErrorMsgPrefixLength = sizeof(ODBCErrorMsgPrefix) - 1;

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
static struct SQLStateMsg {
	const char *SQLState;
	const char *SQLMsg;
} SQLStateMsg[] = {
	{"01000", "General warning"},
	{"01004", "String data, right truncation"},
	{"01S07", "Fractional truncation"},
	{"07005", "Prepared statement not a cursor-specification"},
	{"07006", "Restricted data type attribute violation"},
	{"07009", "Invalid descriptor index"},
	{"08001", "Client unable to establish connection"},
	{"08002", "Connection already in use"},
	{"08003", "Connection does not exist"},
	{"08S01", "Communication link failure"},
	{"22002", "Indicator variable required but not supplied"},
	{"22003", "Numeric value out of range"},
	{"22007", "Invalid datetime format"},
	{"22012", "Division by zero"},
	{"22015", "Interval field overflow"},
	{"22018", "Invalid character value for cast specification"},
	{"24000", "Invalid cursor state"},
	{"25000", "Invalid transaction state"},
	{"HY000", "General error"},
	{"HY001", "Memory allocation error"},
	{"HY003", "Invalid application buffer type"},
	{"HY004", "Invalid SQL data type"},
	{"HY008", "Operation canceled"},
	{"HY009", "Invalid use of null pointer"},
	{"HY010", "Function sequence error"},
	{"HY012", "Invalid transaction operation code"},
	{"HY013", "Memory management error"},
	{"HY015", "No cursor name available"},
	{"HY090", "Invalid string or buffer length"},
	{"HY091", "Invalid descriptor field identifier"},
	{"HY092", "Invalid attribute/option identifier"},
	{"HY095", "Function type out of range"},
	{"HY096", "Information type out of range"},
	{"HY097", "Column type out of range"},
	{"HY098", "Scope type out of range"},
	{"HY099", "Nullable type out of range"},
	{"HY100", "Uniqueness option type out of range"},
	{"HY101", "Accuracy option type out of range"},
	{"HY109", "Invalid cursor position"},
	{"HYC00", "Optional feature not implemented"},
	{"HYT01", "Connection timeout expired"},
	{"IM001", "Driver does not support this function"},
	{"IM002", "Data source not found"},
	{0, 0},
};

#ifndef ODBCDEBUG
static
#endif
const char *
getStandardSQLStateMsg(const char *SQLState)
{
	struct SQLStateMsg *p;

	assert(SQLState);

	for (p = SQLStateMsg; p->SQLState; p++)
		if (strcmp(p->SQLState, SQLState) == 0)
			return p->SQLMsg;

	/* Present a msg to notify the system administrator/programmer */
	fprintf(stderr,
		"\nMonetDB, ODBC Driver, ODBCError.c: No message defined for SQLState: %s. Please report this error.\n",
		SQLState);

	return SQLState;	/* always return a string */
}


/*
 * Creates a new allocated ODBCError object, initializes it and
 * adds copies of the SQLstate, msg and nativeErrorCode to the object.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCError object
 */
ODBCError *
newODBCError(const char *SQLState, const char *msg, int nativeCode)
{
	ODBCError *error = (ODBCError *) malloc(sizeof(ODBCError));

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

	error->message = msg ? strdup(msg) : NULL;
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
char *
getSqlState(ODBCError *error)
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
char *
getMessage(ODBCError *error)
{
	assert(error);

	/* check special case */
	if (error->message == NULL) {
		/* No error message was set, use the default error msg
		   for the set sqlState (if a msg is available) */
		const char *SQLStateMsg = getStandardSQLStateMsg(error->sqlState);
		assert(SQLStateMsg);
		/* use this message instead of the NULL */
		error->message = strdup(SQLStateMsg);
	}

	return error->message;
}


/*
 * Get the native error code value.
 *
 * Precondition: error must be valid
 * Returns: an int value representing the native (MonetDB) error code.
 */
int
getNativeErrorCode(ODBCError *error)
{
	assert(error);
	return error->nativeErrorCode;
}


/*
 * Get the pointer to the recNumber'th (starting at 1) ODBCError
 * object or NULL when there no next object.
 *
 * Precondition: error must be valid or NULL
 * Returns: the pointer to the next ODBCError object or NULL when
 * the record does not exist.
 */
ODBCError *
getErrorRec(ODBCError *error, int recNumber)
{
	while (error && --recNumber > 0) {
		error = error->next;
		if (!error)
			return NULL;
	}

	return error;
}


/*
 * Appends a valid ODBCError object 'this' to the end of the list
 * of a valid ODBCError object 'head'.
 *
 * Precondition: both head and this must be valid (non NULL)
 */
void
appendODBCError(ODBCError **head, ODBCError *this)
{
	assert(head);
	assert(this);

	while (*head)
		head = &(*head)->next;
	*head = this;
	this->next = NULL;	/* just to be sure */
}


/*
 * Prepends a valid ODBCError object 'this' to the front of the list
 * of a valid ODBCError object 'head' and return the new head.
 *
 * Precondition: both head and this must be valid (non NULL)
 * Returns: the new head (which is the same as the prepended 'this').
 */
ODBCError *
prependODBCError(ODBCError *head, ODBCError *this)
{
	assert(head);		/* if head could be NULL this function would do nothing */
	assert(this);
	assert(this->next == NULL);

	this->next = head;

	return this;
}


/*
 * Frees the ODBCError object including its linked ODBCError objects.
 *
 * Precondition: none (error may be NULL)
 */
void
deleteODBCErrorList(ODBCError *error)
{
	ODBCError *nxt = NULL;

	while (error) {
		nxt = error->next;
		if (error->message)
			free(error->message);
		free(error);
		error = nxt;
	}
}
