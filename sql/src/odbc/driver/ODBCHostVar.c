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
 * ODBCHostVar.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC statement input parameters (InHostVar) and
 * output columns (OutHostVar), see ODBCHostVar.h
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCGlobal.h"
#include "ODBCHostVar.h"


OdbcInHostVar makeOdbcInHostVar(
	UWORD		ipar,
	SWORD		fCType,
	SWORD		fSqlType,
	UDWORD		cbColDef,
	SWORD		ibScale,
	PTR		rgbValue,
	SDWORD *	pcbValue )
{
	OdbcInHostVar this = (OdbcInHostVar)GDKmalloc(sizeof(OdbcInHostVarRec));

	this->ipar = ipar;
	this->fCType = fCType;
	this->fSqlType = fSqlType;
	this->cbColDef = cbColDef;
	this->ibScale = ibScale;
	this->rgbValue = rgbValue;
	this->pcbValue = pcbValue;

	return this;
}


OdbcOutHostVar makeOdbcOutHostVar(
	UWORD		icol,
	SWORD		fCType,
	PTR		rgbValue,
	SDWORD		cbValueMax,
	SDWORD *	pcbValue )
{
	OdbcOutHostVar this = (OdbcOutHostVar)GDKmalloc(sizeof(OdbcOutHostVarRec));

	this->icol = icol;
	this->fCType = fCType;
	this->rgbValue = rgbValue;
	this->cbValueMax = cbValueMax;
	this->pcbValue = pcbValue;

	return this;
}


void destroyOdbcInHostVar(OdbcInHostVar this)
{
	assert(this);
	GDKfree((void *)this);
}


void destroyOdbcOutHostVar(OdbcOutHostVar this)
{
	assert(this);
	GDKfree((void *)this);
}



/* This will add the specified ODBC in host var to the array.
 * If the array is NULL, a new one will be created.
 * If a ODBC in host var for the specified parameter already exists
 * in the array, the previous one is destroyed.
 */
void addOdbcInArray(OdbcInArray * this, OdbcInHostVar var)
{
	UWORD ipar;
	assert(this);
	assert(var);

	ipar = var->ipar;
	if (ipar > this->size) {
		int idx = this->size;
		int new_size = ipar + 7;	/* grow in steps of 8 for efficiency */
		OdbcInHostVar * new_array = NULL;

		if (this->array == NULL) {
			/* create a new array of pointers */
			new_array = (OdbcInHostVar *) GDKmalloc((new_size +1) * sizeof(OdbcInHostVar));
			idx = 0;
		} else {
			/* enlarge the array of pointers */
			new_array = (OdbcInHostVar *) GDKrealloc(this->array, ((new_size +1) * sizeof(OdbcInHostVar)));
			idx = this->size;
		}
		assert(new_array);

		/* initialize the new places with NULL */
		for (; idx <= new_size; idx++) {
			new_array[idx] = NULL;
		}

		this->array = new_array;
		this->size = new_size;
	}

	/* add the InHostVar to the array */
	assert(ipar <= this->size);
	assert(this->array);
	if (this->array[ipar] != NULL) {
		destroyOdbcInHostVar(this->array[ipar]);
	}
	this->array[ipar] = var;
}


/* This will add the specified ODBC out host var to the array.
 * If the array is NULL, a new one will be created.
 * If a ODBC out host var for the column is already in the array,
 * the previous one is destroyed.
 */
void addOdbcOutArray(OdbcOutArray * this, OdbcOutHostVar var)
{
	UWORD icol;
	assert(this);
	assert(var);

	icol = var->icol;
	if (icol > this->size) {
		int idx = this->size;
		int new_size = icol + 7;	/* grow in steps of 8 for efficiency */
		OdbcOutHostVar * new_array = NULL;

		if (this->array == NULL) {
			/* create a new array of pointers */
			new_array = (OdbcOutHostVar *) GDKmalloc((new_size +1) * sizeof(OdbcOutHostVar));
			idx = 0;
		} else {
			/* enlarge the array of pointers */
			new_array = (OdbcOutHostVar *) GDKrealloc(this->array, ((new_size +1) * sizeof(OdbcOutHostVar)));
			idx = this->size;
		}
		assert(new_array);
		/* initialize the new places with NULL */
		for (; idx <= new_size; idx++) {
			new_array[idx] = NULL;
		}

		this->array = new_array;
		this->size = new_size;
	}

	/* add the OutHostVar to the array */
	assert(icol <= this->size);
	assert(this->array);
	if (this->array[icol] != NULL) {
		destroyOdbcOutHostVar(this->array[icol]);
	}
	this->array[icol] = var;
}


void destroyOdbcinArray(OdbcInArray * this)
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
		GDKfree((void *)this->array);
		this->array = NULL;
	}
	this->size = 0;
}


void destroyOdbcOutArray(OdbcOutArray * this)
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
		GDKfree((void *)this->array);
		this->array = NULL;
	}
	this->size = 0;
}
