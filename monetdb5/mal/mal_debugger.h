/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MAL_DEBUGGER_H
#define _MAL_DEBUGGER_H

#include "mal.h"
#include "mal_scenario.h"
#include "mal_client.h"

#define MAL_DEBUGGER		/* debugger is active */

#define MAXBREAKS 32

mal_export int MDBdelay;	/* do not immediately react */

mal_export void mdbSetBreakRequest(Client cntxt, MalBlkPtr mb, str request, char cmd);
mal_export int mdbSetTrap(Client cntxt, str modnme, str fcnnme, int flag);
mal_export str mdbGrab(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str mdbTrapClient(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str mdbTrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export int mdbSession(void);
mal_export void mdbDump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void printStack(stream *f, MalBlkPtr mb, MalStkPtr s);

mal_export str runMALDebugger(Client cntxt, Symbol s);

mal_export str debugOptimizers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void debugLifespan(Client cntxt, MalBlkPtr mb, Lifespan span);
#endif /* _MAL_DEBUGGER_h */
