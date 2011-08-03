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
#ifndef _PETRINET_
#define _PETRINET_
#include "mal_interpreter.h"
#include "sql_scenario.h"
#include "basket.h"

/* #define _DEBUG_PETRINET_ */

#define PNout GDKout
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

datacell_export str PNregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str PNpauseScheduler(int *ret);
datacell_export str PNresumeScheduler(int *ret);
datacell_export str PNstopScheduler(int *ret);
datacell_export str PNdump(int *ret);
datacell_export str PNsource(int *ret, str *fcn, str *tbl);
datacell_export str PNtarget(int *ret, str *fcn, str *tbl);
datacell_export str PNanalysis(Client cntxt, MalBlkPtr mb);
datacell_export str PNanalyseWrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str PNtable(int *ret);
#endif

