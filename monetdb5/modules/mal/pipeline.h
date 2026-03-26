/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PIPELINE_H_
#define _PIPELINE_H_

#define pipeline_lock(pl) MT_lock_set(&pl->p->l)
#define pipeline_unlock(pl) MT_lock_unset(&pl->p->l)

#define pipeline_lock1(r) MT_lock_set(&r->batIdxLock)
#define pipeline_unlock1(r) MT_lock_unset(&r->batIdxLock)

#define pipeline_lock2(r) MT_lock_set(&r->theaplock)
#define pipeline_unlock2(r) MT_lock_unset(&r->theaplock)

#define SLICE_SIZE 100000

// TODO a better way to define/add/register sinks, similar to types
#define OA_HASH_TABLE_SINK 1
#define OA_HASH_PAYLOAD_SINK 2
#define TOPN_SINK 3
#define HEAP_SINK 4
#define PART_SINK 5
#define MAT_SINK  6
#define COPY_SINK 42

extern int BATupgrade(BAT *r, BAT *b, bool locked);
extern void BATswap_heaps(BAT *u, BAT *b, Pipeline *p);

mal_export void counter_wait(Sink *s, int nr, Pipeline *p);
mal_export void counter_next(Sink *s);

#endif /*_PIPELINE_H_*/
