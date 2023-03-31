/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _LOGGER_INTERNALS_H_
#define _LOGGER_INTERNALS_H_

#define FLUSH_QUEUE_SIZE 2048 /* maximum size of the flush queue, i.e. maximum number of transactions committing simultaneously */

typedef struct logged_range_t {
	ulng id;			/* log file id */
	ulng last_ts;		/* last stored timestamp */
	struct logged_range_t *next;
} logged_range;

struct logger {
	int debug;
	int version;
	ulng id;		/* current log output file id */
	ulng saved_id;		/* id of last fully handled log file */
	int tid;		/* current transaction id */
	int saved_tid;		/* id of transaction which was flushed out (into BBP storage)  */
	bool flushing;
	bool flushnow;
	ulng drops;
	bool request_rotation;
	logged_range *pending;
	logged_range *current;

	lng total_cnt; /* When logging the content of a bats in multiple runs, total_cnt is used the very first to signal this and keep track in the logging*/
	bool inmemory;
	char *fn;
	char *dir;
	preversionfix_fptr prefuncp;
	postversionfix_fptr postfuncp;
	void *funcdata;
	stream *output_log;
	stream *input_log;	/* current stream to flush */
	lng end;		/* end of pre-allocated blocks for faster f(data)sync */

	ATOMIC_TYPE refcount; /* Number of active writers and flushers in the logger */ // TODO check refcount in c->log and c->end
	bool flushing_output_log; /* prevent output_log that is currently being flushed from being closed */
	MT_Lock rotation_lock;
	MT_Lock lock;
	/* Store log_bids (int) to circumvent trouble with reference counting */
	BAT *catalog_bid;	/* int bid column */
	BAT *catalog_id;	/* object identifier is unique */
	BAT *catalog_cnt;	/* count of ondisk buns (transient) */
	BAT *catalog_lid;	/* last tid, after which it gets released/destroyed */
	BAT *dcatalog;		/* deleted from catalog table */
	BUN cnt;		/* number of persistent bats, incremented on log flushing */
	BUN deleted;		/* number of destroyed persistent bats, needed for catalog vacuum */

	BAT *seqs_id;		/* int id column */
	BAT *seqs_val;		/* lng value column */
	BAT *dseqs;		/* deleted from seqs table */

	/* we map type names into internal log ids, split in 2 ranges
	 * (0-127 fixed size types and 129 - 255 varsized) */
	uint8_t type_nr[256];	/* mapping from logger type id to GDK type nr */
	int8_t type_id[128];	/* mapping from GDK type nr to logger type id */

	void *buf;
	size_t bufsize;

	/* flush variables */
	unsigned int flush_queue[FLUSH_QUEUE_SIZE]; /* circular array with the current transactions' ids waiting to be flushed */
	int flush_queue_begin; /* start index of the queue */
	int flush_queue_length; /* length of the queue */
	MT_Sema flush_queue_semaphore; /*to protect the queue against ring buffer overflows */
	MT_Lock flush_queue_lock; /* to protect the queue against concurrent reads and writes */
	MT_Lock flush_lock; /* so only one transaction can flush to disk at any given time */
};

struct old_logger {
	logger *lg;		/* the new logger instance */
	const char *filename;	/* name of log file */
	lng changes;
	lng id;
	int tid;
	bool with_ids;
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
	BAT *add;		/* bat ids of bats being added by upgrade */
	BAT *del;		/* bat ids of bats being deleted by upgrade */
};

gdk_return log_create_types_file(logger *lg, const char *filename, bool append);

#endif /* _LOGGER_INTERNALS_H_ */
