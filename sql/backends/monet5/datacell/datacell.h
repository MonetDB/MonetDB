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

#ifndef _DATACELLS_
#define _DATACELLS_

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "sql.h"
#include "sql_scenario.h"
#include "basket.h"
#include "receptor.h"
#include "emitter.h"
#include "petrinet.h"

#ifdef WIN32
#ifndef LIBDATACELL
#define datacell_export extern __declspec(dllimport)
#else
#define datacell_export extern __declspec(dllexport)
#endif
#else
#define datacell_export extern
#endif

/* #define _DEBUG_DATACELL     debug this module */

datacell_export str DCprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCreceptor(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCremove(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCresumeObject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCmode(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCprotocol(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCdump(int *ret);
datacell_export str DCthreshold(int *ret, str *bskt, int *mi);
datacell_export str DCwindow(int *ret, str *bskt, int *sz, int *slide);
datacell_export str DCtimewindow(int *ret, str *bskt, int *sz, int *slide);
datacell_export str DCbeat(int *ret, str *bskt, int *t);

datacell_export str DCpauseScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCresumeScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
datacell_export str DCpostlude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

datacell_export str DCemitter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
