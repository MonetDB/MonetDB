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
 * ODBCError.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC error structures/objects (see ODBCError.h)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
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

const char ODBCErrorMsgPrefix[] = "[MonetDB][ODBC Driver " PACKAGE_VERSION "]";
const int ODBCErrorMsgPrefixLength = (int) sizeof(ODBCErrorMsgPrefix) - 1;

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
	{"01001", "Cursor operation conflict"},
	{"01002", "Disconnect error"},
	{"01003", "NULL value eliminated in set function"},
	{"01004", "String data, right truncated"},
	{"01006", "Privilege not revoked"},
	{"01007", "Privilege not granted"},
	{"01S00", "Invalid connection string attribute"},
	{"01S01", "Error in row"},
	{"01S02", "Option value changed"},
	{"01S06", "Attempt to fetch before the result set returned the first "
		  "rowset"},
	{"01S07", "Fractional truncation"},
	{"01S08", "Error saving file DSN"},
	{"01S09", "Invalid keyword"},
	{"07002", "COUNT field incorrect"},
	{"07005", "Prepared statement not a cursor-specification"},
	{"07006", "Restricted data type attribute violation"},
	{"07007", "Restricted parameter value violation"},
	{"07009", "Invalid descriptor index"},
	{"07S01", "Invalid use of default parameter"},
	{"08001", "Client unable to establish connection"},
	{"08002", "Connection name in use"},
	{"08003", "Connection not open"},
	{"08004", "Server rejected the connection"},
	{"08007", "Connection failure during transaction"},
	{"08S01", "Communication link failure"},
	{"21S01", "Insert value list does not match column list"},
	{"21S02", "Degree of derived table does not match column list"},
	{"22001", "String data, right truncated"},
	{"22002", "Indicator variable required but not supplied"},
	{"22003", "Numeric value out of range"},
	{"22007", "Invalid datetime format"},
	{"22008", "Datetime field overflow"},
	{"22012", "Division by zero"},
	{"22015", "Interval field overflow"},
	{"22018", "Invalid character value for cast specification"},
	{"22019", "Invalid escape character"},
	{"22025", "Invalid escape sequence"},
	{"22026", "String data, length mismatch"},
	{"23000", "Integrity constraint violation"},
	{"24000", "Invalid cursor state"},
	{"25000", "Invalid transaction state"},
	{"25S01", "Transaction state unknown"},
	{"25S02", "Transaction is still active"},
	{"25S03", "Transaction is rolled back"},
	{"28000", "Invalid authorization specification"},
	{"34000", "Invalid cursor name"},
	{"3C000", "Duplicate cursor name"},
	{"3D000", "Invalid catalog name"},
	{"3F000", "Invalid schema name"},
	{"40001", "Serialization failure"},
	{"40002", "Integrity constraint violation"},
	{"40003", "Statement completion unknown"},
	{"42000", "Syntax error or access violation"},
	{"42S01", "Base table or view already exists"},
	{"42S02", "Base table or view not found"},
	{"42S11", "Index already exists"},
	{"42S12", "Index not found"},
	{"42S21", "Column already exists"},
	{"42S22", "Column not found"},
	{"44000", "WITH CHECK OPTION violation"},
	{"HY000", "General error"},
	{"HY001", "Memory allocation error"},
	{"HY003", "Invalid application buffer type"},
	{"HY004", "Invalid SQL data type"},
	{"HY007", "Associated statement is not prepared"},
	{"HY008", "Operation canceled"},
	{"HY009", "Invalid argument value"},
	{"HY010", "Function sequence error"},
	{"HY011", "Attribute cannot be set now"},
	{"HY012", "Invalid transaction operation code"},
	{"HY013", "Memory management error"},
	{"HY014", "Limit on the number of handles exceeded"},
	{"HY015", "No cursor name available"},
	{"HY016", "Cannot modify an implementation row descriptor"},
	{"HY017", "Invalid use of an automatically allocated descriptor "
		  "handle"},
	{"HY018", "Server declined cancel request"},
	{"HY019", "Non-character and non-binary data sent in pieces"},
	{"HY020", "Attempt to concatenate a null value"},
	{"HY021", "Inconsistent descriptor information"},
	{"HY024", "Invalid attribute value"},
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
	{"HY103", "Invalid retrieval code"},
	{"HY104", "Invalid precision or scale value"},
	{"HY105", "Invalid parameter type"},
	{"HY106", "Fetch type out of range"},
	{"HY107", "Row value out of range"},
	{"HY109", "Invalid cursor position"},
	{"HY110", "Invalid driver completion"},
	{"HY111", "Invalid bookmark value"},
	{"HY114", "Driver does not support connection-level asynchronous "
		  "function execution"},
	{"HY115", "SQLEndTran is not allowed for an environment that contains "
		  "a connection with asynchronous function execution enabled"},
	{"HY117", "Connection is suspended due to unknown transaction state.  "
		  "Only disconnect and read-only functions are allowed."},
	{"HY121", "Cursor Library and Driver-Aware Pooling cannot be enabled "
		  "at the same time"},
	{"HYC00", "Optional feature not implemented"},
	{"HYT00", "Timeout expired"},
	{"HYT01", "Connection timeout expired"},
	{"IM001", "Driver does not support this function"},
	{"IM002", "Data source not found and no default driver specified"},
	{"IM003", "Specified driver could not be connected to"},
	{"IM004", "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"},
	{"IM005", "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"},
	{"IM006", "Driver's SQLSetConnectAttr failed"},
	{"IM007", "No data source or driver specified; dialog prohibited"},
	{"IM008", "Dialog failed"},
	{"IM009", "Unable to connect to translation DLL"},
	{"IM010", "Data source name too long"},
	{"IM011", "Driver name too long"},
	{"IM012", "DRIVER keyword syntax error"},
	{"IM014", "The specified DSN contains an architecture mismatch "
		  "between the Driver and Application"},
	{"IM015", "Driver's SQLConnect on SQL_HANDLE_DBC_INFO_HANDLE failed"},
	{"IM017", "Polling is disabled in asynchronous notification mode"},
	{"IM018", "SQLCompleteAsync has not been called to complete the "
		  "previous asynchronous operation on this handle."},
	{"S1118", "Driver does not support asynchronous notification"},
	{0, 0}
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
		if (strncmp(p->SQLState, SQLState, 5) == 0)
			return p->SQLMsg;

	/* Present a msg to notify the system administrator/programmer */
	fprintf(stderr,
		"\nMonetDB, ODBC Driver, ODBCError.c: "
		"No message defined for SQLState: %.5s. "
		"Please report this error.\n", SQLState);

	return SQLState;	/* always return a string */
}


static ODBCError malloc_error = {
	"HY001",
	NULL,
	0,
	NULL,
};

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

	if (error == NULL) {
		/* malloc failure, override anything given to us */
		return &malloc_error;
	}

	if (SQLState) {
		strncpy(error->sqlState, SQLState, SQL_SQLSTATE_SIZE);
		error->sqlState[SQL_SQLSTATE_SIZE] = '\0';
	} else {
		/* initialize it with nulls */
		int i = 0;

		for (; i <= SQL_SQLSTATE_SIZE; i++)
			error->sqlState[i] = 0;
	}

	if (msg) {
		size_t len;

		error->message = strdup(msg);
		if (error->message == NULL) {
			free(error);
			return &malloc_error;
		}

		/* remove trailing newlines */
		len = strlen(error->message);
		while (len > 0 && error->message[len - 1] == '\n') {
			error->message[len - 1] = 0;
			len--;
		}
	} else {
		error->message = NULL;
	}
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
		const char *StandardSQLStateMsg = getStandardSQLStateMsg(error->sqlState);

		assert(StandardSQLStateMsg);
		/* use this message instead of the NULL */
		error->message = strdup(StandardSQLStateMsg);
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
	while (error && --recNumber > 0)
		error = error->next;
	return error;
}

int
getErrorRecCount(ODBCError *error)
{
	int n = 0;

	while (error) {
		error = error->next;
		n++;
	}
	return n;
}

/*
 * Appends a valid ODBCError object 'this' to the end of the list
 * of a valid ODBCError object 'head'.
 *
 * Precondition: both head and this must be valid (non NULL)
 */
void
appendODBCError(ODBCError **head, ODBCError *err)
{
	assert(head);
	assert(err);

	while (*head)
		head = &(*head)->next;
	*head = err;
	err->next = NULL;	/* just to be sure */
}


#if 0				/* unused */
/*
 * Prepends a valid ODBCError object 'err' to the front of the list
 * of a valid ODBCError object 'head' and return the new head.
 *
 * Precondition: both head and err must be valid (non NULL)
 * Returns: the new head (which is the same as the prepended 'err').
 */
void
prependODBCError(ODBCError **head, ODBCError *err)
{
	assert(head);
	assert(err);
	assert(err->next == NULL);

	err->next = *head;
	*head = err;
}
#endif


/*
 * Frees the ODBCError object including its linked ODBCError objects.
 *
 * Precondition: none (error may be NULL)
 */
void
deleteODBCErrorList(ODBCError **error)
{
	ODBCError *cur;

	while (*error) {
		cur = *error;
		*error = cur->next;
		if (cur->message)
			free(cur->message);
		if (cur != &malloc_error)
			free(cur);
		else
			cur->next = NULL;
	}
}
