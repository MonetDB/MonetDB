/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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


typedef struct tODBCDRIVERDBC {
	/* Dbc properties */
	int Type;		/* structure type, used for handle validy test */
	ODBCEnv *Env;		/* the Env object it belongs to, NOT NULL */
	struct tODBCDRIVERDBC *next;	/* the linked list of dbc's in this Env */

	ODBCError *Error;	/* pointer to an Error object or NULL */
	int RetrievedErrors;	/* # of errors already retrieved by SQLError */

	/* connection information */
	char *dsn;		/* Data source name or NULL */
	char *uid;		/* User ID or NULL */
	char *pwd;		/* Password for User ID or NULL */
	char *host;		/* Server host */
	int port;		/* Server port */
	char *dbname;		/* Database Name or NULL */
	int Connected;		/* 1 is Yes, 0 is No */
	SQLUINTEGER sql_attr_autocommit;

	/* MonetDB connection handle & status information */
	Mapi mid;		/* connection with server */
	int cachelimit;		/* cache limit we requested */
	int Mdebug;

	/* Dbc children: list of ODBC Statement handles created within
	   this Connection */
	/* can't use ODBCStmt *FirstStmt here because of ordering of
	   include files */
	struct tODBCDRIVERSTMT *FirstStmt;	/* first in list or NULL */
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

int ODBCGetKeyAttr(SQLCHAR **conn, SQLSMALLINT *nconn, char **key, char **attr);
SQLRETURN SQLAllocStmt_(ODBCDbc *dbc, SQLHANDLE *pnOutputHandle);
SQLRETURN SQLConnect_(ODBCDbc *dbc, SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength, SQLCHAR *szUID, SQLSMALLINT nUIDLength, SQLCHAR *szPWD, SQLSMALLINT nPWDLength, char *host, int port, char *schema);
SQLRETURN SQLGetConnectAttr_(ODBCDbc *dbc, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER BufferLength, SQLINTEGER *StringLength);
SQLRETURN SQLSetConnectAttr_(ODBCDbc *dbc, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength);

#endif
