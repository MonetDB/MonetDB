/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _MAL_DEBUGGER_H
#define _MAL_DEBUGGER_H

#include "mal.h"
#include "mal_client.h"

mal_export str runMALDebugger(Client cntxt, MalBlkPtr mb);

#ifdef LIBMONETDB5
/* only available in monetdb5 */

#define MAXBREAKS 32

extern void mdbDump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern void printStack(stream *f, MalBlkPtr mb, MalStkPtr s);

extern void mdbSetBreakRequest(Client cntxt, MalBlkPtr mb, str request, char cmd);
extern str BATinfo(BAT **key, BAT **val, const bat bid);

#endif

#endif /* _MAL_DEBUGGER_h */
