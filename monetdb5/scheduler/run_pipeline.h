/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
