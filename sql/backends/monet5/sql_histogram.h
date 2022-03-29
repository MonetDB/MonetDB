/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _SQL_HISTOGRAM_DEF
#define _SQL_HISTOGRAM_DEF

#include "sql.h"

extern str sql_createhistogram(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str sql_printhistogram(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_HISTOGRAM_DEF */
