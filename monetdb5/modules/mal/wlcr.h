/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _WLCR_H
#define _WLCR_H

#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_exception.h"
#include "mal_interpreter.h"

/* #define _DEBUG_WLCR_ */

#define WLCR_QUERY		1
#define WLCR_UPDATE 	2
#define WLCR_CATALOG 	3

mal_export int wlcr_duration; // how long to capture default= 0
mal_export int wlcr_threshold; // threshold (seconds) for sending readonly queries
mal_export int wlcr_deltas;  // sent the delta values
mal_export int wlcr_all;	// also ship failed transaction
mal_export str wlcr_snapshot;	// name assigned to the snapshot
mal_export int wlcr_lastunit;	// last job executed

mal_export str WLCRproperties(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRfin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRcommit(Client cntxt);
mal_export str WLCRcommitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCRrollback(Client cntxt);
mal_export str WLCRrollbackCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _WLCR_H */
