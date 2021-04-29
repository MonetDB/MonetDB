/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _LOGGER_INTERNALS_H_
#define _LOGGER_INTERNALS_H_

typedef struct logged_range_t {
	ulng id;			/* log file id */
	int first_tid;		/* first */
	int last_tid;		/* last tid */
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
	logged_range *pending;
	logged_range *current;

	int row_insert_nrcols;	/* nrcols == 0 no rowinsert, log_update will include the logformat */

	bool inmemory;
	char *fn;
	char *dir;
	char *local_dir; /* the directory in which the log is written */
	preversionfix_fptr prefuncp;
	postversionfix_fptr postfuncp;
	void *funcdata;
	stream *output_log;
	stream *input_log;	/* current stream too flush */
	lng end;		/* end of pre-allocated blocks for faster f(data)sync */

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

	/* we map type names into internal log ids, split in 2 ranges (0-127 fixed size types and 128 - 254 varsized) */
	BAT *type_id;		/* id of a type */
	BAT *type_nme;		/* names of types */
	BAT *type_nr;		/* atom number of this type (transient) */

	void *buf;
	size_t bufsize;
};

struct old_logger {
	logger *lg;		/* the new logger instance */
	const char *filename;	/* name of log file */
	lng changes;
	lng id;
	int tid;
	bool with_ids;
	char *local_dir; /* the directory in which the log is written */
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

gdk_return logger_create_types_file(logger *lg, const char *filename);

#endif /* _LOGGER_INTERNALS_H_ */
