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
 * Copyright August 2008-2011 MonetDB B.V.
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
	BAT *cached;		/* cached copy, used for schema bats only */
} sql_delta;

typedef struct sql_dbat {
	char *dname;		/* name of the persistent deletes bat */
	int dbid;		/* bat with deletes */
	size_t cnt;
} sql_dbat;

/* initialize bat storage call back functions interface */
extern int bat_storage_init( store_functions *sf );

extern void create_delta( sql_delta *d, BAT *b, BAT *i, bat u);
extern BAT * delta_bind_ubat(sql_delta *bat, int access);
extern BAT * delta_bind_bat( sql_delta *bat, int access, int temp);
extern BAT * delta_bind_del(sql_dbat *bat, int access);

extern void delta_update_bat( sql_delta *bat, BAT *upd, int is_new);
extern void delta_update_val( sql_delta *bat, oid rid, void *upd);
extern void delta_append_bat( sql_delta *bat, BAT *i );
extern void delta_append_val( sql_delta *bat, void *i );
extern void delta_delete_bat( sql_dbat *bat, BAT *i );
extern void delta_delete_val( sql_dbat *bat, oid rid );

extern int load_delta(sql_delta *bat, int bid, int type);
extern int load_dbat(sql_dbat *bat, int bid);
extern int new_persistent_delta( sql_delta *bat, int sz );
extern int new_persistent_dbat( sql_dbat *bat);

extern int log_create_delta(sql_delta *bat);
extern int log_create_dbat( sql_dbat *bat );

extern int dup_delta(sql_trans *tr, sql_delta *obat, sql_delta *bat, int type, int oc_isnew, int c_isnew, int temp, int sz);
extern int dup_dbat(sql_trans *tr, sql_dbat *obat, sql_dbat *bat, int isnew, int temp);
extern int destroy_delta(sql_delta *b);
extern int destroy_dbat(sql_dbat *bat);
extern int log_destroy_delta(sql_delta *b);
extern int log_destroy_dbat(sql_dbat *bat);
extern BUN clear_delta(sql_trans *tr, sql_delta *bat);
extern BUN clear_dbat(sql_trans *tr, sql_dbat *bat);
extern int tr_update_delta( sql_trans *tr, sql_delta *obat, sql_delta *cbat, BUN snapshot_minsize);
extern int tr_update_dbat(sql_trans *tr, sql_dbat *tdb, sql_dbat *fdb, int cleared);
extern int tr_log_delta( sql_trans *tr, sql_delta *cbat, int cleared);
extern int tr_log_dbat(sql_trans *tr, sql_dbat *fdb, int cleared);

#endif /*BATSTORAGE_H */

