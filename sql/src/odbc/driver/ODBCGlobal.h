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
 * ODBCGlobal.h
 *
 * Description:
 * The global MonetDB ODBC include file which
 * includes all needed external include files.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCGLOBAL
#define _H_ODBCGLOBAL

#include "monetdb_config.h"
#ifdef NATIVE_WIN32
/* indicate to sqltypes.h that windows.h has already been included and
   that it doesn't have to define Windows constants */
#define ALLREADY_HAVE_WINDOWS_TYPE
#endif

/**** Define the ODBC Version this ODBC driver complies with ****/
#define ODBCVER 0x0351		/* Important: this must be defined before include of sqlext.h */

/* some general defines */
#define MONETDB_ODBC_VER     "3.51"	/* must be synchronous with ODBCVER */
#define MONETDB_DRIVER_NAME  "MonetDBODBClib"
#define MONETDB_DRIVER_VER   "1.00"
#define MONETDB_PRODUCT_NAME "MonetDB ODBC driver"
#define MONETDB_SERVER_NAME  "MonetDB"

#ifdef _MSC_VER
#ifndef LIBMONETODBC
#define odbc_export extern __declspec(dllimport) 
#else
#define odbc_export extern __declspec(dllexport) 
#endif
#else
#define odbc_export extern 
#endif

/* standard ODBC driver include files */
#include <sqltypes.h>		/* ODBC C typedefs */
/* Note: sqlext.h includes sql.h so it is not needed here to be included */
/* Note2: if you include sql.h it will give an error because it will find
	src/sql/common/sql.h instead, which is not the one we need */
#include <sqlext.h>		/* ODBC API definitions and prototypes */
#include <sqlucode.h>		/* ODBC Unicode defs and prototypes */

/* standard ODBC driver installer & configurator include files */
#include <odbcinst.h>		/* ODBC installer definitions and prototypes */

/* standard C include files */
#include <string.h>		/* for strcpy() etc. */
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <assert.h>

#ifndef HAVE_SSIZE_T
#if SIZEOF_SIZE_T == SIZEOF_INT
typedef int ssize_t;
#define HAVE_SSIZE_T 1
#else
#if SIZEOF_SIZE_T == SIZEOF_LONG
typedef long ssize_t;
#define HAVE_SSIZE_T 1
#endif
#endif
#endif

/* these functions are called from within the library */
SQLRETURN SQLAllocHandle_(SQLSMALLINT nHandleType, SQLHANDLE nInputHandle,
			  SQLHANDLE *pnOutputHandle);
SQLRETURN SQLEndTran_(SQLSMALLINT nHandleType, SQLHANDLE nHandle,
		      SQLSMALLINT nCompletionType);
SQLRETURN SQLFreeHandle_(SQLSMALLINT handleType, SQLHANDLE handle);
SQLRETURN SQLGetDiagRec_(SQLSMALLINT handleType, SQLHANDLE handle,
			 SQLSMALLINT recNumber, SQLCHAR *sqlState,
			 SQLINTEGER *nativeErrorPtr, SQLCHAR *messageText,
			 SQLSMALLINT bufferLength, SQLSMALLINT *textLengthPtr);

#ifdef ODBCDEBUG
#define ODBCLOG(...)	do {						\
				char *s = getenv("ODBCDEBUG");		\
				if (s && *s) {				\
					FILE *f;			\
					f = fopen(s, "a");		\
					if (f) {			\
						fprintf(f, __VA_ARGS__); \
						fclose(f);		\
					}				\
				}					\
			} while (0)
#endif

#endif
