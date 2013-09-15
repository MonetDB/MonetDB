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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef BATSTORAGE_H
#define BATSTORAGE_H

#include "sql_storage.h"
#include "bat_logger.h"

typedef struct sql_delta {	
	char *name;		/* name of the main bat */
	int bid;
	oid ibase;		/* ibase: first id of inserts */
	int ibid;		/* bat with inserts */
	int ubid;		/* bat with updates */
	size_t cnt;		/* number of tuples (excluding the deletes) */
	size_t ucnt;		/* number of updates */
	BAT *cached;		/* cached copy, used for schema bats only */
	int wtime;		/* time stamp */
	struct sql_delta *next;	/* possibly older version of the same column/idx */
} sql_delta;

typedef struct sql_dbat {
	char *dname;		/* name of the persistent deletes bat */
	int dbid;		/* bat with deletes */
	size_t cnt;
	int wtime;		/* time stamp */
	struct sql_dbat *next;	/* possibly older version of the same deletes */
} sql_dbat;

/* initialize bat storage call back functions interface */
extern int bat_storage_init( store_functions *sf );

extern int tr_update_delta( sql_trans *tr, sql_delta *obat, sql_delta *cbat );
extern int tr_update_dbat(sql_trans *tr, sql_dbat *tdb, sql_dbat *fdb, int cleared);
extern int tr_log_delta( sql_trans *tr, sql_delta *cbat, int cleared);
extern int tr_log_dbat(sql_trans *tr, sql_dbat *fdb, int cleared);

extern int dup_bat(sql_trans *tr, sql_table *t, sql_delta *obat, sql_delta *bat, int type, int oc_isnew, int c_isnew);
extern sql_delta * timestamp_delta( sql_delta *d, int ts);

#endif /*BATSTORAGE_H */

