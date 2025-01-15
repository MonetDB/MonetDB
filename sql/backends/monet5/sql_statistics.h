/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*  (co) M.L. Kersten */
#ifndef _SQL_STATISTICS_DEF
#define _SQL_STATISTICS_DEF

#include "sql.h"

extern str sql_set_count_distinct(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str sql_set_min(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str sql_set_max(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str sql_analyze(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str sql_statistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_STATISTICS_DEF */
