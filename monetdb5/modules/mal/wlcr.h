/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _WLC_H
#define _WLC_H

#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_exception.h"
#include "mal_interpreter.h"

/* #define _WLC_DEBUG_ */

#define WLCR_QUERY		1
#define WLCR_UPDATE 	2
#define WLCR_CATALOG 	3
#define WLCR_IGNORE		4

mal_export int wlcr_threshold;
mal_export int wlcr_batches;
mal_export int wlcr_drift;
mal_export str wlcr_dbname;
mal_export int wlcr_rollback;

mal_export str WLCinit(Client cntxt);
mal_export str WLCexit(void);
mal_export int WLCused(void);
mal_export str WLCgetConfig(void);
mal_export str WLCinitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCstopmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLClogthreshold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLClogrollback(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCdrift(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCfinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCchange(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCcommit(int clientid);
mal_export str WLCcommitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCrollback(int clientid);
mal_export str WLCrollbackCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _WLC_H */
