/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
 * ODBCDbc.h
 *
 * Description:
 * This file contains the ODBC connection structure
 * and function prototypes on this structure.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCDBC
#define _H_ODBCDBC

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCError.h"
#include "mapi.h"
#include "msettings.h"


typedef struct tODBCDRIVERDBC {
	/* Dbc properties */
	int Type;		/* structure type, used for handle validy test */
	ODBCEnv *Env;		/* the Env object it belongs to, NOT NULL */
	struct tODBCDRIVERDBC *next;	/* the linked list of dbc's in this Env */

	ODBCError *Error;	/* pointer to an Error object or NULL */
	int RetrievedErrors;	/* # of errors already retrieved by SQLError */

	/* connection information */
	msettings *settings;	/* All connection parameters */
	char *dsn;		/* Data source name or NULL */
	bool Connected;		/* whether we are connecte to a server */
	bool has_comment;	/* whether the server has sys.comments */
	bool allow_hugeint;	/* whether the application deals with HUGEINT */
	bool raw_strings;	/* server uses raw strings */
	int mapToLongVarchar;	/* when > 0 we map WVARCHAR to WLONGVARCHAR, default 0 */
	SQLUINTEGER sql_attr_autocommit;
	SQLUINTEGER sql_attr_metadata_id;
	SQLUINTEGER sql_attr_connection_timeout;

	/* MonetDB connection handle & status information */
	Mapi mid;		/* connection with server */
	int cachelimit;		/* cache limit we requested */
	SQLULEN qtimeout;	/* current query timeout */
	short major, minor, patch; /* version of server */
	SQLUSMALLINT maxclients;   /* mserver5 max_clients value */
	int Mdebug;

	/* Dbc children: list of ODBC Statement handles created within
	   this Connection */
	/* can't use ODBCStmt *FirstStmt here because of ordering of
	   include files */
	struct tODBCDRIVERSTMT *FirstStmt;	/* first in list or NULL */
	char setting_touched[MP__MAX];  /* for SQLBrowseConnect. set 0 on init, 1 on touch, other is up to SQLBrowseConnect */
} ODBCDbc;


/*
 * Creates a new allocated ODBCDbc object and initializes it.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCDbc object
 */
ODBCDbc *newODBCDbc(ODBCEnv *env);


/*
 * Check if the connection handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns 1 if it is a valid connection handle,
 * 	returns 0 if is invalid and thus an unusable handle.
 */
int isValidDbc(ODBCDbc *dbc);


/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCDbc struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: dbc must be valid. SQLState and errMsg may be NULL.
 */
void addDbcError(ODBCDbc *dbc, const char *SQLState, const char *errMsg, int nativeErrCode);


/*
 * Extracts an error object from the error list of this ODBCDbc struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: dbc and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError *getDbcError(ODBCDbc *dbc);


/* utility macro to quickly remove any none collected error msgs */
#define clearDbcErrors(dbc) do {					\
				assert(dbc);				\
				if ((dbc)->Error) {			\
					deleteODBCErrorList(&(dbc)->Error); \
					(dbc)->RetrievedErrors = 0;	\
				}					\
			} while (0)


/*
 * Destroys the ODBCDbc object including its own managed data.
 *
 * Precondition: dbc must be valid, inactive (not connected) and
 * no ODBCStmt (or ODBCDesc) objects may refer to this dbc.
 * Postcondition: dbc is completely destroyed, dbc handle is become invalid.
 */
void destroyODBCDbc(ODBCDbc *dbc);

int ODBCGetKeyAttr(const SQLCHAR **conn, SQLSMALLINT *nconn, char **key, char **attr);
SQLRETURN ODBCConnectionString(SQLRETURN rc, ODBCDbc *dbc,
			       SQLCHAR *OutConnectionString,
			       SQLSMALLINT BufferLength,
			       SQLSMALLINT *StringLength2Ptr,
			       const char *dsn, const char *uid,
			       const char *pwd, const char *host,
			       int port, const char *database,
			       int mapToLongVarchar);
SQLRETURN MNDBAllocStmt(ODBCDbc *dbc, SQLHANDLE *pnOutputHandle);
SQLRETURN MNDBConnect(ODBCDbc *dbc, const SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength,
			const SQLCHAR *szUID, SQLSMALLINT nUIDLength,
			const SQLCHAR *szPWD, SQLSMALLINT nPWDLength,
			const char *host, int port, const char *dbname, int mapToLongVarchar);
SQLRETURN MNDBGetConnectAttr(ODBCDbc *dbc, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER BufferLength, SQLINTEGER *StringLength);
SQLRETURN MNDBSetConnectAttr(ODBCDbc *dbc, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength);

/*
 * Function to translate an ODBC SQL query to native format.
 * The return value is a freshly allocated null-terminated string.
 */
extern char *ODBCTranslateSQL(ODBCDbc *dbc, const SQLCHAR *query, size_t length, SQLULEN noscan);

extern SQLRETURN MNDBConnectSettings(ODBCDbc *dbc, const char *dsn, msettings *settings);
extern SQLRETURN MNDBDriverConnect(ODBCDbc *dbc, SQLHWND WindowHandle, const SQLCHAR *InConnectionString, SQLSMALLINT StringLength1, SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion, int tryOnly);

extern bool makeNulTerminated(const SQLCHAR **argument, ssize_t argument_len, void **scratch);
extern const char* takeFromDataSource(ODBCDbc *dbc, msettings *settings, const char *dsn);
extern SQLRETURN takeFromConnString(ODBCDbc *dbc, msettings *settings, const SQLCHAR *InConnectionString, SQLSMALLINT StringLength1, char  **dsn_out);
extern char* buildConnectionString(const char *dsn, const msettings *settings);


#endif
