/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_RANK_H
#define _SQL_RANK_H

#include "sql.h"

sql5_export str SQLdiff(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLwindow_bound(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLrow_number(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLrank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLdense_rank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLpercent_rank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcume_dist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLntile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLfirst_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLlast_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLnth_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLlag(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLlead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_RANK_H */
