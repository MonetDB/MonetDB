/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define profiler_export extern __declspec(dllimport)
#else
#define profiler_export extern __declspec(dllexport)
#endif
#else
#define profiler_export extern
#endif

profiler_export str CMDstartProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDstopProfiler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDnoopProfiler(void *res);
profiler_export str CMDsetHeartbeat(void *res, int *ev);
profiler_export str CMDopenProfilerStream(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
profiler_export str CMDcloseProfilerStream(void *res);
profiler_export str CMDcleanup(void *ret);
profiler_export str CMDclearTrace(void *res);
profiler_export str CMDcleanupTraces(void *res);
profiler_export str CMDgetTrace(bat *res, str *ev);

profiler_export str CMDgetDiskReads(lng *ret);
profiler_export str CMDgetDiskWrites(lng *ret);
profiler_export str CMDgetUserTime(lng *ret);
profiler_export str CMDgetSystemTime(lng *ret);
profiler_export str CMDcpustats(lng *user, lng *nice, lng *sys, lng *idle, lng *iowait);
profiler_export str CMDcpuloadPercentage(int *cycles, int *io, lng *user, lng *nice, lng *sys, lng *idle, lng *iowait);
#endif  /* _PROFILER_*/
