/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _MAL_RESOURCE_H
#define _MAL_RESOURCE_H

#include "mal_interpreter.h"

#define TIMESLICE  2000000 /* usec */
#define DELAYUNIT 2 /* ms delay in parallel processing decisions */
#define MAX_DELAYS 1000 /* never wait forever */

#define USE_MAL_ADMISSION
#ifdef USE_MAL_ADMISSION
mal_export int MALadmission(lng argclaim, lng hotclaim);
#endif

mal_export lng getMemoryClaim(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int i, int flag);
mal_export void MALresourceFairness(lng usec);
mal_export size_t MALrunningThreads(void);

#endif /*  _MAL_RESOURCE_H*/
