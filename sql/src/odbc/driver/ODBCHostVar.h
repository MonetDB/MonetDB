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
 * ODBCHostVar.c
 *
 * Description:
 * This file contains the structures and function
 * prototypes which operate on ODBC statement
 * input parameters (InHostVar) and
 * output columns (OutHostVar).
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCHOSTVAR
#define _H_ODBCHOSTVAR

#include "ODBCGlobal.h"

typedef struct _OdbcInHostVarRec {
	/* supplied by SQLBindParameter() or SQLSetParam() */
	SQLUSMALLINT ParameterNumber;
	SQLSMALLINT InputOutputType;
	SQLSMALLINT ValueType;
	SQLSMALLINT ParameterType;
	SQLUINTEGER ColumnSize;
	SQLSMALLINT DecimalDigits;
	SQLPOINTER ParameterValuePtr;
	SQLINTEGER BufferLength;
	SQLINTEGER *StrLen_or_IndPtr;


	/* TODO: if *StrLen_or_IndPtr == SQL_DATA_AT_EXEC then we need to
	 * administer extra information in a separate OdbcInDAEParRec.
	 */
} OdbcInHostVarRec, *OdbcInHostVar;


typedef struct _OdbcOutHostVarRec {
	SQLUSMALLINT icol;
	SQLSMALLINT fCType;
	SQLPOINTER rgbValue;
	SQLINTEGER cbValueMax;
	SQLINTEGER *pcbValue;
} OdbcOutHostVarRec, *OdbcOutHostVar;


OdbcInHostVar makeOdbcInHostVar(SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
				SQLSMALLINT, SQLUINTEGER, SQLSMALLINT,
				SQLPOINTER, SQLINTEGER, SQLINTEGER *);
OdbcOutHostVar makeOdbcOutHostVar(SQLUSMALLINT, SQLSMALLINT, SQLPOINTER,
				  SQLINTEGER, SQLINTEGER *);

void destroyOdbcInHostVar(OdbcInHostVar);
void destroyOdbcOutHostVar(OdbcOutHostVar);



typedef struct _OdbcInArray {
	OdbcInHostVar *array;
	int size;
} OdbcInArray;

typedef struct _OdbcOutArray {
	OdbcOutHostVar *array;
	int size;
} OdbcOutArray;


void addOdbcInArray(OdbcInArray *, OdbcInHostVar);
void addOdbcOutArray(OdbcOutArray *, OdbcOutHostVar);

void delOdbcInArray(OdbcInArray *, int n);
void delOdbcOutArray(OdbcOutArray *, int n);

void destroyOdbcInArray(OdbcInArray *);
void destroyOdbcOutArray(OdbcOutArray *);

#endif
