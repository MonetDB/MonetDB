/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _MAL_RESOURCE_H
#define _MAL_RESOURCE_H

#include "mal_interpreter.h"
#include "gdk_atomic.h"

#define TIMESLICE  (3 * 60 * 1000 * 1000) /* usec , 3 minute high priority */
#define DELAYUNIT 2 /* ms delay in parallel processing decisions */
#define MAX_DELAYS 1000 /* never wait more then 2000 ms */

//#define heapinfo(X,Id)	(((X) && (X)->base && ((X)->parentid == 0 || (X)->parentid == Id)) ? (X)->free : 0)
#define heapinfo(X,Id)	(((X) && (X)->base ) ? (X)->free : 0)
#define hashinfo(X,Id) ((X) && (X) != (Hash *) 1 ? heapinfo((X)->heap, Id) : 0)

#define USE_MAL_ADMISSION
#ifdef USE_MAL_ADMISSION
mal_export int MALadmission(lng argclaim, lng hotclaim);
#endif

#define FAIRNESS_THRESHOLD (MAX_DELAYS * DELAYUNIT)

mal_export lng getMemoryClaim(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int i, int flag);
mal_export void MALresourceFairness(lng usec);
mal_export size_t MALrunningThreads(void);

#endif /*  _MAL_RESOURCE_H*/
