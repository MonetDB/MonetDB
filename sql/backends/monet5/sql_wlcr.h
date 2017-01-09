/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef SQL_WLCR_H
#define SQL_WLCR_H

#include <streams.h>
#include <mal.h>
#include <mal_client.h>
#include <sql_mvc.h>
#include <sql_qc.h>

/*
 */
extern void WLCRprocess(void *arg);
extern str WLCRreplay(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLCRclone(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

extern str CLONEjob(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEupdateOID(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEupdateValue(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str CLONEclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /*SQL_CLONE_H*/
