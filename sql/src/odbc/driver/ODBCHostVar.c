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



/* This will add the specified ODBC in host var to the array.
 * If the array is NULL, a new one will be created.
 * If a ODBC in host var for the specified parameter already exists
 * in the array, the previous one is destroyed.
 */
void
addOdbcInArray(OdbcInArray *this, OdbcInHostVar var)
{
	SQLUSMALLINT ParameterNumber;

	assert(this);
	assert(var);

	ParameterNumber = var->ParameterNumber;

	if (ParameterNumber > this->size) {
		int idx = this->size;
		int new_size = ParameterNumber + 7;	/* grow in steps of 8 for efficiency */
		OdbcInHostVar *new_array = NULL;

		if (this->array == NULL) {
			/* create a new array of pointers */
			new_array = (OdbcInHostVar *) malloc((new_size + 1) *
							     sizeof(OdbcInHostVar));
			idx = 0;
		} else {
			/* enlarge the array of pointers */
			new_array = (OdbcInHostVar *) realloc(this->array,
							      ((new_size + 1) *
							       sizeof(OdbcInHostVar)));
			idx = this->size;
		}
		assert(new_array);

		/* initialize the new places with NULL */
		for (; idx <= new_size; idx++)
			new_array[idx] = NULL;

		this->array = new_array;
		this->size = new_size;
	}

	/* add the InHostVar to the array */
	assert(ParameterNumber <= this->size);
	assert(this->array);
	if (this->array[ParameterNumber] != NULL) {
		destroyOdbcInHostVar(this->array[ParameterNumber]);
	}
	this->array[ParameterNumber] = var;
}


/* This will add the specified ODBC out host var to the array.
 * If the array is NULL, a new one will be created.
 * If a ODBC out host var for the column is already in the array,
 * the previous one is destroyed.
 */
void
addOdbcOutArray(OdbcOutArray *this, OdbcOutHostVar var)
{
	UWORD icol;

	assert(this);
	assert(var);

	icol = var->icol;

	if (icol > this->size) {
		int idx = this->size;
		int new_size = icol + 7;	/* grow in steps of 8 for efficiency */
		OdbcOutHostVar *new_array = NULL;

		if (this->array == NULL) {
			/* create a new array of pointers */
			new_array = (OdbcOutHostVar *) malloc((new_size + 1) *
							      sizeof(OdbcOutHostVar));
			idx = 0;
		} else {
			/* enlarge the array of pointers */
			new_array = (OdbcOutHostVar *) realloc(this->array,
							       ((new_size + 1) *
								sizeof(OdbcOutHostVar)));
			idx = this->size;
		}
		assert(new_array);
		/* initialize the new places with NULL */
		for (; idx <= new_size; idx++)
			new_array[idx] = NULL;

		this->array = new_array;
		this->size = new_size;
	}

	/* add the OutHostVar to the array */
	assert(icol <= this->size);
	assert(this->array);
	if (this->array[icol] != NULL)
		destroyOdbcOutHostVar(this->array[icol]);
	this->array[icol] = var;
}

void
delOdbcInArray(OdbcInArray *this, int n)
{
	assert(this);

	if (n < this->size && this->array != NULL && this->array[n] != NULL) {
		destroyOdbcInHostVar(this->array[n]);
		this->array[n] = NULL;
	}
}

void
delOdbcOutArray(OdbcOutArray *this, int n)
{
	assert(this);

	if (n < this->size && this->array != NULL && this->array[n] != NULL) {
		destroyOdbcOutHostVar(this->array[n]);
		this->array[n] = NULL;
	}
}


void
destroyOdbcInArray(OdbcInArray *this)
{
	assert(this);

	if (this->array != NULL) {
		/* first remove any allocated Host Var */
		int idx = this->size;

		for (; idx > 0; idx--) {
			if (this->array[idx] != NULL)
				destroyOdbcInHostVar(this->array[idx]);
		}

		/* next remove the allocated array */
		free((void *) this->array);
		this->array = NULL;
	}
	this->size = 0;
}


void
destroyOdbcOutArray(OdbcOutArray *this)
{
	assert(this);

	if (this->array != NULL) {
		/* first remove any allocated Host Var */
		int idx = this->size;

		for (; idx > 0; idx--) {
			if (this->array[idx] != NULL)
				destroyOdbcOutHostVar(this->array[idx]);
		}

		/* next remove the allocated array */
		free((void *) this->array);
		this->array = NULL;
	}
	this->size = 0;
}
