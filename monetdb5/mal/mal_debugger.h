/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MAL_DEBUGGER_H
#define _MAL_DEBUGGER_H

#include "mal.h"
#include "mal_scenario.h"
#include "mal_client.h"

#define MAXBREAKS 32

mal_export void mdbDump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export void printStack(stream *f, MalBlkPtr mb, MalStkPtr s);

mal_export void mdbSetBreakRequest(Client cntxt, MalBlkPtr mb, str request, char cmd);
mal_export str runMALDebugger(Client cntxt, MalBlkPtr mb);
mal_export str BATinfo(BAT **key, BAT **val, const bat bid);

#endif /* _MAL_DEBUGGER_h */
