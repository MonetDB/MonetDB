/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _LOGGER_INTERNALS_H_
#define _LOGGER_INTERNALS_H_

struct logger {
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
	void *funcdata;
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
};

#endif /* _LOGGER_INTERNALS_H_ */
