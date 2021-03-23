/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef BATSTORAGE_H
#define BATSTORAGE_H

#include "sql_storage.h"
#include "bat_logger.h"

typedef struct column_storage {
	int refcnt;
	int bid;
	int uibid;		/* bat with positions of updates */
	int uvbid;		/* bat with values of updates */
	bool cleared;
	bool alter;		/* set when the delta is created for an alter statement */
	size_t cnt;		/* number of tuples (excluding? the deletes) */
	size_t ucnt;	/* number of updates */
	ulng ts;		/* version timestamp */
} column_storage;

typedef struct sql_delta {
	column_storage cs;
	struct sql_delta *next;	/* possibly older version of the same column/idx */
} sql_delta;

typedef struct segment {
	BUN start;
	BUN end;
	bool deleted;	/* we need to keep a dense segment set, 0 - end of last segemnt,
					   some segments maybe deleted */
	ulng ts;		/* timestamp on this segment, ie tid of some active transaction or commit time of append/delete or
					   rollback time, ie ready for reuse */
	ulng oldts;		/* keep previous ts, for rollbacks */
	struct segment *next;	/* usualy one should be enough */
	struct segment *prev;	/* used in destruction list */
} segment;

/* container structure too allow sharing this structure */
typedef struct segments {
	sql_ref r;
	struct segment *h;
	struct segment *t;
} segments;

typedef struct storage {
	column_storage cs;	/* storage on disk */
	segments *segs;	/* local used segements */
	struct storage *next;
} storage;

/* initialize bat storage call back functions interface */
extern void bat_storage_init( store_functions *sf );
extern sql_delta * col_timestamp_delta( sql_trans *tr, sql_column *c);
extern storage * tab_timestamp_dbat( sql_trans *tr, sql_table *t);

#endif /*BATSTORAGE_H */

