/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _LOGGER_H_
#define _LOGGER_H_

#define LOG_OK 0
#define LOG_ERR (-1)

typedef struct logaction {
	int type;		/* type of change */
	lng nr;
	int ht;			/* vid(-1),void etc */
	int tt;
	lng id;
	char *name;		/* optional */
	BAT *b;			/* temporary bat with changes */
} logaction;

/* during the recover process a number of transactions could be active */
typedef struct trans {
	int tid;		/* transaction id */
	int sz;			/* sz of the changes array */
	int nr;			/* nr of changes */

	logaction *changes;

	struct trans *tr;
} trans;

typedef int (*preversionfix_fptr)(int oldversion, int newversion);
typedef void (*postversionfix_fptr)(void *lg);

typedef struct logger {
	int debug;
	lng changes;
	int version;
	lng id;
	int tid;
#if SIZEOF_OID == 8
	/* on 64-bit architecture, read OIDs as 32 bits (for upgrading
	 * oid size) */
	int read32bitoid;
#endif
	char *fn;
	char *dir;
	preversionfix_fptr prefuncp;
	postversionfix_fptr postfuncp;
	stream *log;
	lng end;		/* end of pre-allocated blocks for faster f(data)sync */
	/* Store log_bids (int) to circumvent trouble with reference counting */
	BAT *catalog_bid;	/* int bid column */
	BAT *catalog_nme;	/* str name column */
	BAT *seqs_id;		/* int id column */
	BAT *seqs_val;		/* lng value column */
	BAT *snapshots_bid;	/* int bid column */
	BAT *snapshots_tid;	/* int tid column */
	BAT *freed;		/* snapshots can be created and destroyed,
				   in a single logger transaction.
				   These snapshot bats should be freed
				   directly (on transaction
				   commit). */
} logger;

#define BATSIZE 0

typedef int log_bid;

/*
 * @+ Sequence numbers
 * The logger also keeps sequence numbers. A sequence needs to store
 * each requested (block) of sequence numbers. This is done using the
 * log_sequence function. The logger_sequence function can be used to
 * return the last logged sequence number. Sequences identifiers
 * should be unique, and 2 are already used. The first LOG_SID is used
 * internally for the log files sequence. The second OBJ_SID is for
 * frontend objects, for example the sql objects have a global
 * sequence counter such that each table, trigger, sequence etc. has a
 * unique number.
 */
/* the sequence identifier for the sequence of log files */
#define LOG_SID	0
/* the sequence identifier for frontend objects */
#define OBJ_SID	1

gdk_export logger *logger_create(int debug, const char *fn, const char *logdir, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp);
gdk_export void logger_destroy(logger *lg);
gdk_export int logger_exit(logger *lg);
gdk_export int logger_restart(logger *lg);
gdk_export int logger_cleanup(logger *lg);
gdk_export lng logger_changes(logger *lg);
gdk_export int logger_sequence(logger *lg, int seq, lng *id);

/* todo pass the transaction id */
gdk_export int log_bat(logger *lg, BAT *b, const char *n);
gdk_export int log_bat_clear(logger *lg, const char *n);
gdk_export int log_bat_persists(logger *lg, BAT *b, const char *n);
gdk_export int log_bat_transient(logger *lg, const char *n);
gdk_export int log_delta(logger *lg, BAT *b, const char *n);

gdk_export int log_tstart(logger *lg);	/* TODO return transaction id */
gdk_export int log_tend(logger *lg);
gdk_export int log_abort(logger *lg);

gdk_export int log_sequence(logger *lg, int seq, lng id);

gdk_export log_bid logger_add_bat(logger *lg, BAT *b, const char *name);
gdk_export void logger_del_bat(logger *lg, log_bid bid);
gdk_export log_bid logger_find_bat(logger *lg, const char *name);

#endif /*_LOGGER_H_*/
