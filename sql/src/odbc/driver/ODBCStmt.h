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
 * ODBCStmt.h
 *
 * Description:
 * This file contains the ODBC statement structure
 * and function prototypes on this structure.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCSTMT
#define _H_ODBCSTMT

#include "ODBCGlobal.h"
#include "ODBCHostVar.h"
#include "ODBCError.h"
#include "ODBCDbc.h"

/* some statement related ODBC driver defines */
#define MONETDB_MAX_BIND_COLS	8192


/* utility struct to decribe result set column info */
typedef struct tColumnHeader
{
	/* BINDING INFO	(used by SQLBindCol()) */
	SQLSMALLINT	nTargetType;		/* C DATA TYPE ie SQL_C_CHAR */
	SQLPOINTER	pTargetValue;		/* POINTER FROM APPLICATION TO COPY TO */
	SQLINTEGER	nTargetValueMax;	/* MAX SPACE IN pTargetValue */
	SQLINTEGER *	pnLengthOrIndicator;	/* TO RETURN LENGTH OR NULL INDICATOR */


	/* COLUMN DESCRIPTION (used by SQLColAttribute()) */
	int	bSQL_DESC_AUTO_UNIQUE_VALUE;	/* IS AUTO INCREMENT COL? */
	char *	pszSQL_DESC_BASE_COLUMN_NAME;	/* empty string if N/A */
	char *	pszSQL_DESC_BASE_TABLE_NAME;	/* empty string if N/A */
	int	bSQL_DESC_CASE_SENSITIVE;	/* IS CASE SENSITIVE COLUMN? */
	char *	pszSQL_DESC_CATALOG_NAME;	/* empty string if N/A */
	int	nSQL_DESC_CONCISE_TYPE;		/* ie SQL_CHAR, SQL_TYPE_TIME...*/
	int	nSQL_DESC_DISPLAY_SIZE;		/* max digits required to display */
	int	bSQL_DESC_FIXED_PREC_SCALE;	/* has data source specific precision? */
	char *	pszSQL_DESC_LABEL;		/* display label, col name or empty string */
	int	nSQL_DESC_LENGTH;		/* strlen or bin size */
	char *	pszSQL_DESC_LITERAL_PREFIX;	/* empty string if N/A */
	char *	pszSQL_DESC_LITERAL_SUFFIX;	/* empty string if N/A */
	char *	pszSQL_DESC_LOCAL_TYPE_NAME;	/* empty string if N/A */
	char *	pszSQL_DESC_NAME;	/* col alias, col name or empty string */
	int	nSQL_DESC_NULLABLE;		/* SQL_NULLABLE, _NO_NULLS or _UNKNOWN */
	int	nSQL_DESC_NUM_PREC_RADIX;	/* 2, 10, or if N/A... 0 */
	int	nSQL_DESC_OCTET_LENGTH;		/* max size */
	int	nSQL_DESC_PRECISION;	/* */
	int	nSQL_DESC_SCALE;	/* */
	char *	pszSQL_DESC_SCHEMA_NAME; /* empty string if N/A */
	int	nSQL_DESC_SEARCHABLE;	/* can be in a filter ie SQL_PRED_NONE... */
	char *	pszSQL_DESC_TABLE_NAME; /* empty string if N/A */
	int	nSQL_DESC_TYPE;		/* SQL data type ie SQL_CHAR, SQL_INTEGER.. */
	char *	pszSQL_DESC_TYPE_NAME;	/* DBMS data type ie VARCHAR, MONEY...*/
	int	nSQL_DESC_UNNAMED;	/* qualifier for SQL_DESC_NAME ie SQL_NAMED */
	int	bSQL_DESC_UNSIGNED;	/* if signed FALSE else TRUE */
	int	nSQL_DESC_UPDATABLE;	/* ie SQL_ATTR_READONLY, SQL_ATTR_WRITE... */
} ColumnHeader;


typedef enum {
	INITED,
	PREPARED,
	EXECUTED,
} StatementState;


typedef struct tODBCDRIVERSTMT
{
	/* Stmt properties */
	int		Type;	/* structure type, used for handle validy test */
	ODBCError *	Error;	/* pointer to an Error object or NULL */
	ODBCDbc *	Dbc;	/* Connection context */
	struct tODBCDRIVERSTMT * next;	/* the linked list of stmt's in this Dbc */
	StatementState	State;	/* needed to detect invalid cursor state */
	char *		Query;	/* SQL command string */

	OdbcInArray	bindParams;	/* list of bind input parameters */
	OdbcOutArray	bindCols;	/* list of bind output columns */

	unsigned int	nrCols;	/* nr of result output columns */
	unsigned int	nrRows;	/* nr of valid rows in Result */
	char **		Result; /* 1+nrCols x 1+nrRows of char data pointers */
				/* col 0 is bookmark column (not used) */
				/* row 0 is not used (count starts at 1) */
	unsigned int	currentRow;	/* used by SQLFetch() */

	/* Stmt children: none yet */
} ODBCStmt;



/*
 * Creates a new allocated ODBCStmt object and initializes it.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCStmt object
 */
ODBCStmt * newODBCStmt();


/*
 * Check if the statement handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns 1 if it is a valid statement handle,
 * 	returns 0 if is invalid and thus an unusable handle.
 */
int isValidStmt(ODBCStmt * stmt);


/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCStmt struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: stmt must be valid. SQLState and errMsg may be NULL.
 */
void addStmtError(ODBCStmt * stmt, char * SQLState, char * errMsg, int nativeErrCode);


/*
 * Adds an error msg object to the end of the error list of
 * this ODBCStmt struct.
 *
 * Precondition: stmt and error must be valid.
 */
void addStmtErrorObj(ODBCStmt * stmt, ODBCError * error);


/*
 * Extracts an error object from the error list of this ODBCStmt struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: stmt and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError * getStmtError(ODBCStmt * stmt);


/* utility macro to quickly remove any none collected error msgs */
#define clearStmtErrors(stm) { assert(stm); \
        if (stm->Error) { deleteODBCErrorList(stm->Error); stm->Error = NULL; } }


/*
 * Destroys the ODBCStmt object including its own managed data.
 *
 * Precondition: stmt must be valid and inactive (No prepared query or
 * result sets active, internal State == INITED).
 * Postcondition: stmt is completely destroyed, stmt handle is become invalid.
 */
void destroyODBCStmt(ODBCStmt * stmt);


/* Internal help function which is used both by SQLGetData() and SQLFetch().
 * It does not clear the errors (only adds any when needed) so it can
 * be called multiple times from SQLFetch().
 * It gets the data of one field in the current result row of the result set.
 */
SQLRETURN ODBCGetData(
        ODBCStmt *      stmt,
        SQLUSMALLINT    nCol,
        SQLSMALLINT     nTargetType,    /* C DATA TYPE */
        SQLPOINTER      pTarget,
        SQLINTEGER      nTargetLength,
        SQLINTEGER *    pnLengthOrIndicator);


#endif
