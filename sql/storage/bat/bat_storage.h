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

typedef struct sql_delta {
	int refcnt;
	char *name;		/* name of the main bat */
	int bid;
	oid ibase;		/* ibase: first id of inserts */
	int ibid;		/* bat with inserts */
	int uibid;		/* bat with positions of updates */
	int uvbid;		/* bat with values of updates */
	bool cleared;
	bool alter;		/* set when the delta is created for an alter statement */
	size_t cnt;		/* number of tuples (excluding the deletes) */
	size_t ucnt;		/* number of updates */
	BAT *cached;		/* cached copy, used for schema bats only */
	ulng ts;		/* version timestamp */
	struct sql_delta *next;	/* possibly older version of the same column/idx */
} sql_delta;

typedef struct sql_dbat {
	int refcnt;
	char *dname;		/* name of the persistent deletes bat */
	int dbid;		/* bat with deletes */
	int cleared;
	size_t cnt;
	BAT *cached;		/* cached copy, used for schema bats only */
	ulng ts;		/* version timestamp */
	struct sql_dbat *next;	/* possibly older version of the same deletes */
} sql_dbat;

/* initialize bat storage call back functions interface */
extern void bat_storage_init( store_functions *sf );
extern sql_delta * col_timestamp_delta( sql_trans *tr, sql_column *c);

#endif /*BATSTORAGE_H */

