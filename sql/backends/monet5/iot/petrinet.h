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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _PETRINET_
#define _PETRINET_
#include "mal_interpreter.h"
#include "sql_scenario.h"
#include "basket.h"

#define _DEBUG_PETRINET_ 

#define PNout GDKout
/*#define  _BASKET_SIZE_*/

#ifdef WIN32
#ifndef LIBDATACELL
#define iot_export extern __declspec(dllimport)
#else
#define iot_export extern __declspec(dllexport)
#endif
#else
#define iot_export extern
#endif

iot_export str PNregisterInternal(Client cntxt, MalBlkPtr mb);
iot_export str PNregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNdrop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNdump(void *ret);

iot_export str PNsource(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNtarget(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNanalysis(Client cntxt, MalBlkPtr mb);
iot_export str PNanalyseWrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
iot_export str PNtable(bat *modnameId, bat *fcnnameId, bat *statusId, bat *seenId, bat *cyclesId, bat *eventsId, bat *timeId, bat * errorId);
#endif

