/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
	char *error;		/* error encountered */
	char *errbuf;		/* parent errbuf */
	MT_Sema s;	/* threads wait on empty queues */
	MT_Lock l;
	MT_Cond cond;	/* condition variable for full worker synchronisation and worker execution order */
	int maxparts;
	int nr_workers;
	int seqnr;
	ATOMIC_TYPE workers;
	ATOMIC_TYPE master_counter;
	int channel[8]; /* sinks use 8, 7 etc, sources 0, 1 etc */
	int status;
	bat sink;
} Pipelines;

typedef struct Pipeline {
	Pipelines *p;	/* the shared pipelines */
	int wid;	/* worker id [ 0 .. nr_workers ] */
	int tseqnr;	/* multi sources pipelines aggregated seqnr (for previous sources) */
	int seqnr;	/* needed to keep things align/ordered */
	void *wls;	/* worker local storage */
} Pipeline;

mal_export str runMALpipelines(Client cntxt, MalBlkPtr mb, int startpc, int stoppc, int maxparts, bat sink, MalStkPtr stk);
//mal_export int PIPELINEnext_counter(Pipeline *p);
//mal_export void PIPELINEclear_counter(Pipeline *p);
//

mal_export int pipeline_pass_token(Pipeline *p, int channel, int id); /* pass the token to the worker id+1 */
mal_export int pipeline_get_token(Pipeline *p, int channel, int id, bool *done); /* wait for token addressed to id, done flag could change and
																			ends waiting */

#endif /*  _MAL_PIPELINES_H*/
