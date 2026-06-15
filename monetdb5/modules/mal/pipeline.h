/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PIPELINE_H_
#define _PIPELINE_H_

#include "gdk.h"
#include "mal_pipelines.h"

#define pipeline_lock(pl) MT_lock_set(&pl->p->l)
#define pipeline_unlock(pl) MT_lock_unset(&pl->p->l)

#define pipeline_lock1(r) MT_lock_set(&r->batIdxLock)
#define pipeline_unlock1(r) MT_lock_unset(&r->batIdxLock)

#define pipeline_lock2(r) MT_lock_set(&r->theaplock)
#define pipeline_unlock2(r) MT_lock_unset(&r->theaplock)

#define pipeline_get_thread_private_pipeline() MT_thread_getdata()
#define pipeline_set_thread_private_pipeline(p) MT_thread_setdata(p)

#define SLICE_SIZE 100000

// TODO a better way to define/add/register sinks, similar to types
#define PIPELINE_IO_HASH_TABLE 1
#define PIPELINE_IO_SOP        2 /* set of ordered parts */
#define PIPELINE_IO_TOPN       3
#define PIPELINE_IO_HEAP       4
#define PIPELINE_IO_PART       5
#define PIPELINE_IO_MAT        6
#define PIPELINE_IO_COPY       7
#define PIPELINE_IO_COUNTER    8
#define PIPELINE_IO_CONCAT     9
#define PIPELINE_IO_PARQUET    10
#define PIPELINE_IO_MPARQUET   11

struct pipeline_counter {
	struct pipeline_io pl_io;
	MT_Lock l;
	int nr;
	int current;
	bool sync;
	int scnt;
	int *cur; /* nr per worker */
};

struct pipeline_concat {
	struct pipeline_io pl_io;
	MT_Lock l;
	int current;
	int max;
	bool started;
	int *cur;
	BAT *srcs[];
};

struct pipeline_resultset {
	struct pipeline_io pl_io;
	ATOMIC_TYPE claimed;
	MT_Lock l;
};

extern int BATupgrade(BAT *r, BAT *b, bool locked);
extern void BATswap_heaps(BAT *u, BAT *b, Pipeline *p);

#endif /*_PIPELINE_H_*/
