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
 * ODBCError.h
 *
 * Description:
 * This file contains the ODBC error structure used
 * internally by the ODBC driver.
 * It consists of a structure for storing attributes of
 * an ODBC error including a ref to the next ODBC error.
 * This next-ref is needed because one ODBC function call
 * can result in multiple ODBC errors. These are stored
 * as a linked list.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCERROR
#define _H_ODBCERROR

#include "ODBCGlobal.h"		/* for SQL_SQLSTATE_SIZE */


typedef struct tODBCError ODBCError;


extern const char ODBCErrorMsgPrefix[];	/* the prefix required by ODBC for each error msg. */
extern const int ODBCErrorMsgPrefixLength;


/* function prototypes */

/*
 * Creates a new allocated ODBCError object, initializes it and
 * adds copies of the SQLstate, msg and nativeErrorCode to the object.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCError object
 */
ODBCError *newODBCError(const char *SQLState, const char *msg, int nativeCode);


/*
 * Get the SQL State code string.
 *
 * Precondition: error must be valid
 * Returns: a none NULL string pointer, intended for reading only.
 */
char *getSqlState(ODBCError *err);


/*
 * Get the Message string.
 *
 * Precondition: error must be valid
 * Returns: a string pointer, intended for reading only, which can be NULL !!.
 */
char *getMessage(ODBCError *err);


/*
 * Get the native error code value.
 *
 * Precondition: error must be valid
 * Returns: an int value representing the native (MonetDB) error code.
 */
int getNativeErrorCode(ODBCError *err);


/*
 * Get the pointer to the recNumber'th (starting at 1) ODBCError
 * object or NULL when there no next object.
 *
 * Precondition: error must be valid
 * Returns: the pointer to the next ODBCError object or NULL when
 * the record does not exist.
 */
ODBCError *getErrorRec(ODBCError *error, int recNumber);

int getErrorRecCount(ODBCError *error);

/*
 * Appends a valid ODBCError object 'this' to the end of the list
 * of a valid ODBCError object 'head'.
 *
 * Precondition: both head and this must be valid (non NULL)
 */
void appendODBCError(ODBCError **head, ODBCError *err);


#if 0				/* unused */
/*
 * Prepends a valid ODBCError object 'this' to the front of the list
 * of a valid ODBCError object 'head' and return the new head.
 *
 * Precondition: both head and this must be valid (non NULL)
 * Returns: the new head (which is the same as the prepended 'this').
 */
void prependODBCError(ODBCError **head, ODBCError *err);
#endif


/*
 * Frees the ODBCError object including its linked ODBCError objects.
 *
 * Precondition: none (error may be NULL)
 */
void deleteODBCErrorList(ODBCError **err);

#ifdef ODBCDEBUG
const char *getStandardSQLStateMsg(const char *SQLState);
#endif

#endif /* _H_ODBCERROR */
