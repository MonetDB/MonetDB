/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * @-
 * @+ Implementation
 * The commands merely encapsulate the functionality provided by
 * mal_profiler, which should be explicitly compiled with the kernel, because
 * it generates a noticable overhead.
 */

#ifndef _PROFILER_
#define _PROFILER_

#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_stack.h"
#include "mal_resolve.h"
#include "mal_exception.h"
#include "mal_client.h"
#include "mal_profiler.h"
#include "mal_interpreter.h"
#include "mal_runtime.h"

mal_export str CMDstartProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDstopProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDstartTrace(void *res);
mal_export str CMDstartTracePath(void *res, str *path);
mal_export str CMDstopTrace(void *res);
mal_export str CMDstopTracePath(void *res, str *path);
mal_export str CMDnoopProfiler(void *res);
mal_export str CMDsetHeartbeat(void *res, int *ev);
mal_export str CMDopenProfilerStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDcloseProfilerStream(void *res);
mal_export str CMDcleanupTraces(void *res);
mal_export str CMDgetTrace(bat *res, str *ev);
mal_export str CMDgetprofilerlimit(int *res);
mal_export str CMDsetprofilerlimit(void *res, int *lim);

mal_export str CMDgetDiskReads(lng *ret);
mal_export str CMDgetDiskWrites(lng *ret);
mal_export str CMDgetUserTime(lng *ret);
mal_export str CMDgetSystemTime(lng *ret);
mal_export str CMDcpustats(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait);
mal_export str CMDcpuloadPercentage(int *cycles, int *io, lng *user, lng *nice, lng *sys, lng *idle, lng *iowait);
#endif  /* _PROFILER_*/
