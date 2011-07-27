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
 * @-
 * @+ Implementation
 */
#ifndef _DCOPERATOR_
#define _DCOPERATOR_
#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "basket.h"
#include "algebra.h"
#include "bat5.h"

/* #define _DEBUG_DCOPERATOR_ */
#define DCout GDKout
/*#define  _BASKET_SIZE_*/

#ifdef WIN32
#ifndef LIBDATACELL
#define datacell_export extern __declspec(dllimport)
#else
#define datacell_export extern __declspec(dllexport)
#endif
#else
#define datacell_export extern
#endif

datacell_export str DCselect(int *ret, int *bid, ptr low, ptr high);
datacell_export str DCselectInsert(int *ret, int *res, int *bid, lng *low, lng *hgh);
datacell_export str DCselectInsertDelete(int *ret, int *res, int *bid, lng *low, lng *hgh);
datacell_export str DCdeleteUpperSlice(int *ret, int *bid, int *pos);
datacell_export str DCreplaceTailBasedOnHead(int *ret, int *res, int *bid);
datacell_export str DCsliceStrict(int *ret, bat *bid, lng *start, lng *end);
#endif

