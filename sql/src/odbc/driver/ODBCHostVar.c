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
 * This file contains the functions which operate on
 * ODBC statement input parameters (InHostVar) and
 * output columns (OutHostVar), see ODBCHostVar.h
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************/

#include "ODBCGlobal.h"
#include "ODBCHostVar.h"


OdbcInHostVar
makeOdbcInHostVar(SQLUSMALLINT ParameterNumber, SQLSMALLINT InputOutputType,
		  SQLSMALLINT ValueType, SQLSMALLINT ParameterType,
		  SQLUINTEGER ColumnSize, SQLSMALLINT DecimalDigits,
		  SQLPOINTER ParameterValuePtr, SQLINTEGER BufferLength,
		  SQLINTEGER *StrLen_or_IndPtr)
{
	OdbcInHostVar this = (OdbcInHostVar) malloc(sizeof(OdbcInHostVarRec));

	assert(this);
	this->ParameterNumber = ParameterNumber;
	this->InputOutputType = InputOutputType;
	this->ValueType = ValueType;
	this->ParameterType = ParameterType;
	this->ColumnSize = ColumnSize;
	this->DecimalDigits = DecimalDigits;
	this->ParameterValuePtr = ParameterValuePtr;
	this->BufferLength = BufferLength;
	this->StrLen_or_IndPtr = StrLen_or_IndPtr;

	return this;
}


OdbcOutHostVar
makeOdbcOutHostVar(SQLUSMALLINT icol, SQLSMALLINT fCType, SQLPOINTER rgbValue,
		   SQLINTEGER cbValueMax, SQLINTEGER *pcbValue)
{
	OdbcOutHostVar this = (OdbcOutHostVar) malloc(sizeof(OdbcOutHostVarRec));

	assert(this);
	this->icol = icol;
	this->fCType = fCType;
	this->rgbValue = rgbValue;
	this->cbValueMax = cbValueMax;
	this->pcbValue = pcbValue;

	return this;
}


void
destroyOdbcInHostVar(OdbcInHostVar this)
{
	assert(this);
	free((void *) this);
}


void
destroyOdbcOutHostVar(OdbcOutHostVar this)
{
	assert(this);
	free((void *) this);
}
