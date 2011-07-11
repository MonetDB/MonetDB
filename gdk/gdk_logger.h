/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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

#ifndef _LOGGER_H_
#define _LOGGER_H_

#define LOG_OK 0
#define LOG_ERR (-1)

typedef struct logaction {
	int type;		/* type of change */
	int nr;
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
	size_t changes;
	int version;
	lng id;
	int tid;
	char *fn;
	char *dir;
	preversionfix_fptr prefuncp;
	postversionfix_fptr postfuncp;
	stream *log;
	lng end;		/* end of pre-allocated blocks for faster f(data)sync */
	/* Store log_bids (int) to circumvent trouble with reference counting */
	BAT *catalog;		/* int, str */
	BAT *seqs;		/* int, lng */
	BAT *snapshots;		/* int, int the bid and tid of snapshot bat */
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
 * The logger also keeps sequence numbers. A sequence needs to store each
 * requested (block) of sequence numbers. This is done using the log_sequence
 * function. The logger_sequence function can be used to return the
 * last logged sequence number. Sequences identifiers should be unique, and
 * 2 are already used. The first LOG_SID is used internally for the log files
 * sequence. The second OBJ_SID is for frontend objects, for example the sql
 * objects have a global sequence counter such that each table, trigger, sequence
 * etc. has a unique number.
 */
/* the sequence identifier for the sequence of log files */
#define LOG_SID	0
/* the sequence identifier for frontend objects */
#define OBJ_SID	1

gdk_export logger *logger_create(int debug, char *fn, char *logdir, char *dbname, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp);
gdk_export void logger_destroy(logger *lg);
gdk_export int logger_exit(logger *lg);
gdk_export int logger_restart(logger *lg);
gdk_export int logger_cleanup(logger *lg);
gdk_export size_t logger_changes(logger *lg);
gdk_export int logger_sequence(logger *lg, int seq, lng *id);

/* todo pass the transaction id */
gdk_export int log_bat(logger *lg, BAT *b, char *n);
gdk_export int log_bat_clear(logger *lg, char *n);
gdk_export int log_bat_persists(logger *lg, BAT *b, char *n);
gdk_export int log_bat_transient(logger *lg, char *n);
gdk_export int log_delta(logger *lg, BAT *b, char *n);

gdk_export int log_tstart(logger *lg);	/* TODO return transaction id */
gdk_export int log_tend(logger *lg);
gdk_export int log_abort(logger *lg);

gdk_export int log_sequence(logger *lg, int seq, lng id);

gdk_export log_bid logger_add_bat(logger *lg, BAT *b, char *name);
gdk_export void logger_del_bat(logger *lg, log_bid bid);
gdk_export log_bid logger_find_bat(logger *lg, char *name);

#endif /*_LOGGER_H_*/
