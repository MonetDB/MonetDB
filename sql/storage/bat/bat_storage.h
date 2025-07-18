/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef BATSTORAGE_H
#define BATSTORAGE_H

#include "sql_storage.h"
#include "bat_logger.h"

typedef struct column_storage {
	ATOMIC_TYPE refcnt;
	int bid;
	int ebid;		/* extra bid */
	int uibid;		/* bat with positions of updates */
	int uvbid;		/* bat with values of updates */
	storage_type st; /* ST_DEFAULT, ST_DICT, ST_FOR */
	bool cleared;
	bool merged;	/* only merge changes once */
	size_t ucnt;	/* number of updates */
	ulng ts;		/* version timestamp */
} column_storage;

typedef struct sql_delta {
	column_storage cs;
	lng nr_updates;
	struct sql_delta *next;	/* possibly older version of the same column/idx */
} sql_delta;

typedef struct segment {
	BUN start;
	BUN end;
	bool deleted;	/* we need to keep a dense segment set, 0 - end of last segment,
					   some segments maybe deleted */
	ulng ts;		/* timestamp on this segment, ie tid of some active transaction or commit time of append/delete or
					   rollback time, ie ready for reuse */
	ulng oldts;		/* keep previous ts, for rollbacks */
	ATOMIC_PTR_TYPE next;	/* usually one should be enough */
	struct segment *prev;	/* used in destruction list */
} segment;

/* container structure to allow sharing this structure */
typedef struct segments {
	sql_ref r;
	ulng nr_reused;
	ATOMIC_TYPE deleted;
	struct segment *h;
	struct segment *t;
} segments;

typedef struct storage {
	column_storage cs;	/* storage on disk */
	segments *segs;	/* local used segments */
	struct storage *next;
} storage;

/* initialize bat storage call back functions interface */
extern void bat_storage_init( store_functions *sf );

storage *
bind_del_data(sql_trans *tr, sql_table *t, bool *clear);

#endif /*BATSTORAGE_H */
