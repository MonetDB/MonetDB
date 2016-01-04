/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/* Author(s) M.Ivanova, M.Kersten
 * The srvpool abstracts away specifics from the experimental octopus and centipede
 * to reach a re-useable framework for ochestrating multiple mservers
 * an execute distributed queries.
*/
#ifndef _RUN_SRVPOOL
#define _RUN_SRVPOOL
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"

#define DEBUG_RUN_SRVPOOL 	/* to trace processing */

#ifdef WIN32
#define mpool_export extern __declspec(dllexport)
#else
#define mpool_export extern
#endif

mpool_export str SRVPOOLscheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mpool_export str SRVPOOLexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mpool_export str SRVPOOLregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mpool_export str SRVPOOLquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mpool_export str SRVPOOLreset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mpool_export str SRVPOOLconnect(str *c, str *dbname);
mpool_export str SRVPOOLlocal(void *res, bit *flag);
mpool_export str SRVsetServers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* MAL_RUN_SRVPOOL */

