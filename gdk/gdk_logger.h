/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _LOGGER_H_
#define _LOGGER_H_

#define LOGFILE "log"

typedef struct logaction {
	int type;		/* type of change */
	lng nr;
	int ht;			/* vid(-1),void etc */
	int tt;
	lng id;
	char *name;		/* optional */
	char tpe;		/* tpe of column */
	oid cid;		/* id of object */
	BAT *b;			/* temporary bat with changes */
	BAT *uid;		/* temporary bat with bun positions to update */
} logaction;

/* during the recover process a number of transactions could be active */
typedef struct trans {
	int tid;		/* transaction id */
	int sz;			/* sz of the changes array */
	int nr;			/* nr of changes */

	logaction *changes;

	struct trans *tr;
} trans;

typedef gdk_return (*preversionfix_fptr)(int oldversion, int newversion);
typedef gdk_return (*postversionfix_fptr)(void *lg);

typedef struct logger {
	int debug;
	int version;
	lng changes;
	lng id;
	int tid;
	bool with_ids;
	bool inmemory;
#ifdef GDKLIBRARY_OLDDATE
	/* convert old date values to new */
	bool convert_date;
#endif
	char *fn;
	char *dir;
	char *local_dir; /* the directory in which the log is written */
	preversionfix_fptr prefuncp;
	postversionfix_fptr postfuncp;
	stream *log;
	lng end;		/* end of pre-allocated blocks for faster f(data)sync */
	/* Store log_bids (int) to circumvent trouble with reference counting */
	BAT *catalog_bid;	/* int bid column */
	BAT *catalog_nme;	/* str name column */
	BAT *catalog_tpe;	/* type of column */
	BAT *catalog_oid;	/* object identifier of column (the pair type,oid is unique) */
	BAT *dcatalog;		/* deleted from catalog table */
	BAT *seqs_id;		/* int id column */
	BAT *seqs_val;		/* lng value column */
	BAT *dseqs;		/* deleted from seqs table */
	BAT *snapshots_bid;	/* int bid column */
	BAT *snapshots_tid;	/* int tid column */
	BAT *dsnapshots;	/* deleted from snapshots table */
	BAT *freed;		/* snapshots can be created and destroyed,
				   in a single logger transaction.
				   These snapshot bats should be freed
				   directly (on transaction
				   commit). */
	BAT *freed_lid; 	/* lid when bat got deleted from catalog table */
	void *buf;
	size_t bufsize;
} logger;

#define BATSIZE 0

typedef int log_bid;

/*
 * @+ Sequence numbers
 * The logger also keeps sequence numbers. A sequence needs to store
 * each requested (block) of sequence numbers. This is done using the
 * log_sequence function. The logger_sequence function can be used to
 * return the last logged sequence number. Sequences identifiers
 * should be unique. The first OBJ_SID is for
 * frontend objects, for example the sql objects have a global
 * sequence counter such that each table, trigger, sequence etc. has a
 * unique number.
 */
/* the sequence identifier for frontend objects */
#define OBJ_SID	1

/* type of object id's, to log */
#define LOG_NONE 0
#define LOG_TAB 1
#define LOG_COL 2
#define LOG_IDX 3

gdk_export logger *logger_create(int debug, const char *fn, const char *logdir, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp);
gdk_export void logger_destroy(logger *lg);
gdk_export gdk_return logger_exit(logger *lg);
gdk_export gdk_return logger_restart(logger *lg);
gdk_export gdk_return logger_flush(logger *lg, lng save_id);
gdk_export gdk_return logger_cleanup(logger *lg);
gdk_export void logger_with_ids(logger *lg);
gdk_export lng logger_changes(logger *lg);
gdk_export int logger_sequence(logger *lg, int seq, lng *id);

/* todo pass the transaction id */
gdk_export gdk_return log_bat(logger *lg, BAT *b, const char *n, char tpe, oid id);
gdk_export gdk_return log_bat_clear(logger *lg, const char *n, char tpe, oid id);
gdk_export gdk_return log_bat_persists(logger *lg, BAT *b, const char *n, char tpe, oid id);
gdk_export gdk_return log_bat_transient(logger *lg, const char *n, char tpe, oid id);
gdk_export gdk_return log_delta(logger *lg, BAT *uid, BAT *uval, const char *n, char tpe, oid id);

gdk_export gdk_return log_tstart(logger *lg);
gdk_export gdk_return log_tend(logger *lg);
gdk_export gdk_return log_abort(logger *lg);
gdk_export lng log_save_id(logger *lg);	/* return the current transaction id */

gdk_export gdk_return log_sequence(logger *lg, int seq, lng id);

gdk_export gdk_return logger_add_bat(logger *lg, BAT *b, const char *name, char tpe, oid id)
	__attribute__ ((__warn_unused_result__));
gdk_export gdk_return logger_del_bat(logger *lg, log_bid bid)
	__attribute__ ((__warn_unused_result__));
gdk_export log_bid logger_find_bat(logger *lg, const char *name, char tpe, oid id);
gdk_export gdk_return logger_upgrade_bat(logger *lg, const char *name, char tpe, oid id)
	__attribute__ ((__warn_unused_result__));

typedef int (*geomcatalogfix_fptr)(void *, int);
gdk_export void geomcatalogfix_set(geomcatalogfix_fptr);
gdk_export geomcatalogfix_fptr geomcatalogfix_get(void);

typedef str (*geomsqlfix_fptr)(int);
gdk_export void geomsqlfix_set(geomsqlfix_fptr);
gdk_export geomsqlfix_fptr geomsqlfix_get(void);

gdk_export void geomversion_set(void);
gdk_export int geomversion_get(void);

#endif /*_LOGGER_H_*/
