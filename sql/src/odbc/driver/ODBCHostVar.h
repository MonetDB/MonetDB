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

typedef struct _OdbcInHostVarRec
{
	/* supplied by SQLBindParameter() or SQLSetParam() */
	SQLUSMALLINT	ParameterNumber;
	SQLSMALLINT	InputOutputType;
	SQLSMALLINT	ValueType;
	SQLSMALLINT	ParameterType;
	SQLUINTEGER	ColumnSize;
	SQLSMALLINT	DecimalDigits;
	SQLPOINTER	ParameterValuePtr;
	SQLINTEGER	BufferLength;
	SQLINTEGER*	StrLen_or_IndPtr;


	/* TODO: if *StrLen_or_IndPtr == SQL_DATA_AT_EXEC then we need to
	 * administer extra information in a separate OdbcInDAEParRec.
	 */
} OdbcInHostVarRec, *OdbcInHostVar;


typedef struct _OdbcOutHostVarRec
{
	SQLUSMALLINT	icol;
	SQLSMALLINT	fCType;
	SQLPOINTER	rgbValue;
	SQLINTEGER	cbValueMax;
	SQLINTEGER *	pcbValue;
} OdbcOutHostVarRec, *OdbcOutHostVar;


OdbcInHostVar makeOdbcInHostVar(SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT, SQLSMALLINT, SQLUINTEGER, SQLSMALLINT, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
OdbcOutHostVar makeOdbcOutHostVar(SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLINTEGER, SQLINTEGER *);

void destroyOdbcInHostVar(OdbcInHostVar);
void destroyOdbcOutHostVar(OdbcOutHostVar);



typedef struct _OdbcInArray
{
	OdbcInHostVar *	array;
	int		size;
} OdbcInArray;

typedef struct _OdbcOutArray
{
	OdbcOutHostVar * array;
	int		size;
} OdbcOutArray;


void addOdbcInArray(OdbcInArray *, OdbcInHostVar);
void addOdbcOutArray(OdbcOutArray *, OdbcOutHostVar);

void delOdbcInArray(OdbcInArray *, int n);
void delOdbcOutArray(OdbcOutArray *, int n);

void destroyOdbcInArray(OdbcInArray *);
void destroyOdbcOutArray(OdbcOutArray *);

#endif
