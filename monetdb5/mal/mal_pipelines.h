/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _MAL_PIPELINES_H
#define _MAL_PIPELINES_H

#include "mal.h"
#include "mal_client.h"

typedef struct Pipelines {
	Client cntxt;   /* for debugging and client resolution */
	MalBlkPtr mb;   /* carry the context */
	MalStkPtr stk;
	int start, stop;    /* guarded block under consideration*/
	ATOMIC_PTR_TYPE error;		/* error encountered */
	MT_Sema s;	/* threads wait on empty queues */
	MT_Lock l;
	int maxparts;
	int counter;
	int nr_workers;
	ATOMIC_TYPE workers;
} Pipelines;

typedef struct Pipeline {
	Pipelines *p;	/* the shared pipelines */
	int wid;	/* worker id [ 0 .. nr_workers ] */
	void *wls;	/* worker local storage */
} Pipeline;

mal_export str runMALpipelines(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, int maxparts, MalStkPtr stk);

#endif /*  _MAL_PIPELINES_H*/
