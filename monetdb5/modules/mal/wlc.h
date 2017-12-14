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
#include <time.h>
#include "mal_exception.h"
#include "mal_interpreter.h"

/* #define _WLC_DEBUG_ */

#define WLC_QUERY		1
#define WLC_UPDATE 		2
#define WLC_CATALOG 	3
#define WLC_IGNORE		4

/* WLC modes */
#define WLC_STARTUP	0	// wlc not yet initialized
#define WLC_RUN		1	// started for the current snapshot
#define WLC_STOP		2	// finished last log file for this snapsho
#define WLC_CLONE		3	// logs used in replica construction

/*
 * returns 1 if the file exists
 */
#ifndef F_OK
#define F_OK 0
#endif
#ifdef _MSC_VER
#define access(f, m)    _access(f, m)
#endif

mal_export MT_Lock wlc_lock;
mal_export char wlc_dir[FILENAME_MAX];
mal_export lng wlc_id;
mal_export int wlc_batches;
mal_export int wlc_state;
mal_export int wlc_beat;
mal_export char wlc_write[26];

mal_export str WLCinit(void);
mal_export int WLCused(void);
mal_export void WLCreset(void);
mal_export str WLCgetConfig(void);
mal_export void WLCreadConfig(FILE *fd);
mal_export str WLCinitCmd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCstopmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCsetmasterbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCgetmasterbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCgetmasterclock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCgetmastertick(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str WLCfinish(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
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
