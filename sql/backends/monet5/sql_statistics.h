/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*  (co) M.L. Kersten */
#ifndef _SQL_STATISTICS_DEF
#define _SQL_STATISTICS_DEF

#include "sql.h"

sql5_export str sql_analyze(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str sql_drop_statistics(mvc *m, sql_table *t);

#endif /* _SQL_STATISTICS_DEF */
