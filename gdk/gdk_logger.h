/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _LOGGER_H_
#define _LOGGER_H_

#define LOGFILE "log"

typedef gdk_return (*preversionfix_fptr)(void *data, int oldversion, int newversion);
typedef gdk_return (*postversionfix_fptr)(void *data, void *lg);

typedef struct logger logger;

typedef int log_bid;
typedef int log_id;

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
#define LOG_BATGROUP 4
#define LOG_BATGROUP_ID 5
#define LOG_BATGROUP_END 6

/* interface for the "old" logger */
typedef struct old_logger old_logger;
gdk_export gdk_return old_logger_load(logger *lg, const char *fn, const char *logdir, FILE *fp, int version, const char *filename);
gdk_export log_bid old_logger_find_bat(old_logger *lg, const char *name, char tpe, oid id);

gdk_export logger *logger_create(int debug, const char *fn, const char *logdir, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp, void *funcdata);
gdk_export void logger_destroy(logger *lg);
gdk_export gdk_return logger_flush(logger *lg, ulng saved_id); /* try too flush a part of the logs, including the passed saved_id transaction id */

gdk_export lng logger_changes(logger *lg);
gdk_export int logger_sequence(logger *lg, int seq, lng *id);

/* todo pass the transaction id */
gdk_export gdk_return log_constant(logger *lg, int type, ptr val, log_id id, lng offset, lng cnt);
gdk_export gdk_return log_bat(logger *lg, BAT *b, log_id id, lng offset, lng cnt); /* log slice from b */
gdk_export gdk_return log_bat_clear(logger *lg, log_id id);
gdk_export gdk_return log_bat_persists(logger *lg, BAT *b, log_id id);
gdk_export gdk_return log_bat_transient(logger *lg, log_id id);
gdk_export gdk_return log_delta(logger *lg, BAT *uid, BAT *uval, log_id id);

/* insert/clear groups of bats */
//gdk_export gdk_return log_batgroup(logger *lg, char tpe, oid id, bool cleared, lng nr_inserted, lng offset_inserted, lng nr_deleted, lng offset_deleted);
/* mark end of batgroup insert or clear */
//gdk_export gdk_return log_batgroup_end(logger *lg, oid id);

gdk_export gdk_return log_tstart(logger *lg, ulng commit_ts, bool flush);
gdk_export gdk_return log_tend(logger *lg);

gdk_export gdk_return log_sequence(logger *lg, int seq, lng id);
gdk_export log_bid logger_find_bat(logger *lg, log_id id);

#endif /*_LOGGER_H_*/
