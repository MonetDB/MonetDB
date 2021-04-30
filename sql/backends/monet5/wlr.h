/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef SQL_WLR_H
#define SQL_WLR_H

#include <mal.h>
#include <mal_client.h>
#include <sql_mvc.h>
#include <sql_qc.h>

extern str WLRstart(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRreplicate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRgetmaster(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRgetclock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRgettick(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRsetbeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRcatalog(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRgeneric(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRclear_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRrollback(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLRaccept(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /*SQL_WLR_H*/
