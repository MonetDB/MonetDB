/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _QLOG_H
#define _QLOG_H
#include "mal.h"
#include "mal_interpreter.h"

mal_export str initQlog(void);
mal_export str QLOGcatalog(BAT **r);
mal_export str QLOGcalls(BAT **r);
mal_export str QLOGenable(void *ret);
mal_export str QLOGenableThreshold(void *ret, int *threshold);
mal_export str QLOGdisable(void *ret);
mal_export int QLOGisset(void);
mal_export str QLOGissetFcn(int *ret);
mal_export str QLOGempty(void *ret);
mal_export str QLOGappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str QLOGdefineNaive(void *ret, str *qry, str *opt, int *nr);
mal_export str QLOGcall(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _QLOG_H */
