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
sql5_export str SQLrow_number(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLrank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLdense_rank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLcount_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLscalarsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define SQLVECTORSUM(TPE) sql_export str SQLvectorsum_##TPE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

SQLVECTORSUM(lng)
SQLVECTORSUM(flt)
SQLVECTORSUM(dbl)
#ifdef HAVE_HGE
SQLVECTORSUM(hge)
#endif

#undef SQLVECTORSUM

sql5_export str SQLscalarprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define SQLVECTORPROD(TPE) sql_export str SQLvectorprod_##TPE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

SQLVECTORPROD(lng)
SQLVECTORPROD(flt)
SQLVECTORPROD(dbl)
#ifdef HAVE_HGE
SQLVECTORPROD(hge)
#endif

#undef SQLVECTORPROD

sql5_export str SQLavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_RANK_H */
