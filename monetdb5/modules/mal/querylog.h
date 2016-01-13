/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _QLOG_H
#define _QLOG_H
#include "mal.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define qlog_export extern __declspec(dllimport)
#else
#define qlog_export extern __declspec(dllexport)
#endif
#else
#define qlog_export extern
#endif

qlog_export int initQlog(void);
qlog_export void QLOGcatalog(BAT **r);
qlog_export void QLOGcalls(BAT **r);
qlog_export str QLOGenable(void *ret);
qlog_export str QLOGenableThreshold(void *ret, int *threshold);
qlog_export str QLOGdisable(void *ret);
qlog_export int QLOGisset(void);
qlog_export str QLOGissetFcn(int *ret);
qlog_export str QLOGempty(void *ret);
qlog_export str QLOGinsert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
qlog_export str QLOGdefineNaive(void *ret, str *qry, str *opt, int *nr);
qlog_export str QLOGcall(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _QLOG_H */
