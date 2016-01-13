/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MAL_SCHEDULER
#define _MAL_SCHEDULER
#include "mal.h"
#include "mal_instruction.h"
#include "mal_client.h"

#define DEBUG_MAL_SCHEDULER

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

struct {
	/*mallinfo memory; */
	/* rusage memused; */
	int cpuload;		/* hard to get */
} runtime;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define pipeline_export extern __declspec(dllimport)
#else
#define pipeline_export extern __declspec(dllexport)
#endif
#else
#define pipeline_export extern
#endif

pipeline_export str MALpipeline(Client c);
pipeline_export str debugScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* MAL_SCEDULER */
