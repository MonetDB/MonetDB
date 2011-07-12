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

/*
 * @f gdk_logger
 * @t Transactions
 * @a N. J. Nes
 * @v 2.0
 *
 * @* Introduction
 * In the philosophy of MonetDB, transaction management overhead should only
 * be paid when necessary. Transaction management is for this purpose
 * implemented as a separate module and applications are required to
 * obey the transaction policy, e.g. obtaining/releasing locks.
 *
 * This module is designed to support efficient logging of the SQL database.
 * Once loaded, the SQL compiler will insert the proper calls at
 * transaction commit to include the changes in the log file.
 *
 * The logger uses a directory to store its log files. One master log file
 * stores information about the version of the logger and the transaction
 * log files. This file is a simple ascii file with the following format:
 *  @code{6DIGIT-VERSION\n[log file number \n]*]*}
 * The transaction log files have a binary format, which stores fixed size
 * logformat headers (flag,nr,bid), where the flag is the type of update logged.
 * The nr field indicates how many changes there were (in case of inserts/deletes).
 * The bid stores the bid identifier.
 *
 * The key decision to be made by the user is the location of the log file.
 * Ideally, it should be stored in fail-safe environment, or at least
 * the log and databases should be on separate disk columns.
 *
 * This file system may reside on the same hardware as the database server
 * and therefore the writes are done to the same disk, but could also
 * reside on another system and then the changes are flushed through the network.
 * The logger works under the assumption that it is called to safeguard
 * updates on the database when it has an exclusive lock on
 * the latest version. This lock should be guaranteed by the calling
 * transaction manager first.
 *
 * Finding the updates applied to a BAT is relatively easy, because each
 * BAT contains a delta structure. On commit these changes are
 * written to the log file and the delta management is reset. Since each
 * commit is written to the same log file, the beginning and end are
 * marked by a log identifier.
 *
 * A server restart should only (re)process blocks which are completely
 * written to disk. A log replay therefore ends in a commit or abort on
 * the changed bats. Once all logs have been read, the changes to
 * the bats are made persistent, i.e. a bbp sub-commit is done.
 *
 * @* Implementation Code
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_logger.h"
#include <string.h>


/*
 * @-
 * The log record encoding is geared at reduced storage space, but
 * at the expense of readability. A user can not easily inspect the
 * log a posteriori to check what has happened.
 *
 */
#define LOG_START	1
#define LOG_END		2
#define LOG_INSERT	3
#define LOG_DELETE	4
#define LOG_UPDATE	5
#define LOG_CREATE	6
#define LOG_DESTROY	7
#define LOG_USE		8
#define LOG_CLEAR	9
#define LOG_SEQ		10

static char *log_commands[] = {
	NULL,
	"LOG_START",
	"LOG_END",
	"LOG_INSERT",
	"LOG_DELETE",
	"LOG_UPDATE",
	"LOG_CREATE",
	"LOG_DESTROY",
	"LOG_USE",
	"LOG_CLEAR",
	"LOG_SEQ",
};

typedef struct logformat_t {
	char flag;
	int tid;
	int nr;
} logformat;

#define LOGFILE "log"

static int bm_commit(logger *lg);
static int tr_grow(trans *tr);

static void
logbat_destroy(BAT *b)
{
	if (b)
		BBPunfix(b->batCacheid);
}

static BAT *
logbat_new(int ht, int tt, BUN size)
{
	BAT *nb = BATnew(ht, tt, size);

	if (nb) {
		if (ht == TYPE_void)
			BATseqbase(nb, 0);
		nb->batDirty |= 2;
	}
	return nb;
}

static int
log_read_format(logger *l, logformat *data)
{
	int res = 1;

	if (mnstr_read(l->log, &data->flag, 1, 1) != 1)
		return 0;
	res = mnstr_readInt(l->log, &data->nr);
	if (res)
		res = mnstr_readInt(l->log, &data->tid);
	return res;
}

static int
log_write_format(logger *l, logformat *data)
{
	if (mnstr_write(l->log, &data->flag, 1, 1) != 1 ||
	    !mnstr_writeInt(l->log, data->nr) ||
	    !mnstr_writeInt(l->log, data->tid))
		return LOG_ERR;
	return LOG_OK;
}

static char *
log_read_string(logger *l)
{
	int len;
	ssize_t nr;
	char *buf;

	if (!mnstr_readInt(l->log, &len))
		return NULL;
	if (len == 0)
		return NULL;
	buf = (char *) GDKmalloc(len);
	if (buf == NULL)
		return NULL;

	if ((nr = mnstr_read(l->log, buf, 1, len)) != (ssize_t) len) {
		buf[len - 1] = 0;
		fprintf(stderr, "!ERROR: couldn't read name (%s) " SSZFMT "\n", buf, nr);
		GDKfree(buf);
		return NULL;
	}
	buf[len - 1] = 0;
	return buf;
}

static int
log_write_string(logger *l, char *n)
{
	size_t len = strlen(n) + 1;	/* log including EOS */

	assert(len > 1);
	assert(len <= INT_MAX);
	if (!mnstr_writeInt(l->log, (int) len) ||
	    mnstr_write(l->log, n, 1, len) != (ssize_t) len)
		return LOG_ERR;
	return LOG_OK;
}

static void
log_read_clear(logger *lg, trans *tr, char *name)
{
	if (lg->debug & 1)
		fprintf(stderr, "logger found log_read_clear %s\n", name);

	if (tr_grow(tr)) {
		tr->changes[tr->nr].type = LOG_CLEAR;
		tr->changes[tr->nr].name = GDKstrdup(name);
		tr->nr++;
	}
}

static void
la_bat_clear(logger *lg, logaction *la)
{
	log_bid bid = logger_find_bat(lg, la->name);
	BAT *b;

	/* do we need to skip these old updates */
	if (BATcount(lg->snapshots)) {
		BUN p = BUNfnd(lg->snapshots, &bid);

		if (p != BUN_NONE) {
			BATiter i = bat_iterator(lg->snapshots);
			int tid = *(int *) BUNtloc(i, p);

			if (lg->tid <= tid)
				return;
		}
	}

	b = BATdescriptor(bid);
	if (b) {
		int access = b->P->restricted;
		b->P->restricted = BAT_WRITE;
		BATclear(b);
		b->P->restricted = access;
		logbat_destroy(b);
	}
}

static int
log_read_seq(logger *lg, logformat *l)
{
	int seq = l->nr;
	lng id;

	if (!mnstr_readLng(lg->log, &id))
		 return LOG_ERR;

	if (BUNfnd(lg->seqs, &seq) != BUN_NONE) {
		BUNdelHead(lg->seqs, &seq, FALSE);
	}
	BUNins(lg->seqs, &seq, &id, FALSE);

	return LOG_OK;
}

static int
log_read_updates(logger *lg, trans *tr, logformat *l, char *name)
{
	log_bid bid = logger_find_bat(lg, name);
	BAT *b = BATdescriptor(bid);
	int res = LOG_OK;
	int ht = -1, tt = -1, hseq = 0, tseq = 0;

	if (lg->debug & 1)
		fprintf(stderr, "logger found log_read_updates %s %s %d\n", name, l->flag == LOG_INSERT ? "insert" : l->flag == LOG_DELETE ? "delete" : "update", l->nr);

	if (b) {
		ht = b->htype;
		if (ht == TYPE_void && b->hseqbase != oid_nil)
			hseq = 1;
		tt = b->ttype;
		if (tt == TYPE_void && b->tseqbase != oid_nil)
			tseq = 1;
	} else {		/* search trans action for create statement */
		int i;

		for (i = 0; i < tr->nr; i++) {
			if (tr->changes[i].type == LOG_CREATE && strcmp(tr->changes[i].name, name) == 0) {
				ht = tr->changes[i].ht;
				if (ht < 0) {
					hseq = 1;
					ht = TYPE_void;
				}
				tt = tr->changes[i].tt;
				if (tt < 0) {
					tseq = 1;
					tt = TYPE_void;
				}
				break;
			}
		}
	}
	if (ht >= 0 && tt >= 0) {
		BAT *r;
		void *(*rt) (ptr, stream *, size_t) = BATatoms[tt].atomRead;
		void *tv = ATOMnil(tt);

		r = BATnew(ht, tt, l->nr);

		if (hseq)
			BATseqbase(r, 0);
		if (tseq)
			BATseqbase(BATmirror(r), 0);

		if (ht == TYPE_void && l->flag == LOG_INSERT) {
			for (; l->nr > 0; l->nr--) {
				void *t = rt(tv, lg->log, 1);

				if (!t) {
					res = LOG_ERR;
					break;
				}
				if (l->flag == LOG_INSERT)
					BUNappend(r, t, TRUE);
				if (t != tv)
					GDKfree(t);
			}
		} else {
			void *(*rh) (ptr, stream *, size_t) = ht == TYPE_void ? BATatoms[TYPE_oid].atomRead : BATatoms[ht].atomRead;
			void *hv = ATOMnil(ht);

			for (; l->nr > 0; l->nr--) {
				void *h = rh(hv, lg->log, 1);
				void *t = rt(tv, lg->log, 1);

				if (!h || !t) {
					res = LOG_ERR;
					break;
				}
				BUNins(r, h, t, TRUE);
				if (h != hv)
					GDKfree(h);
				if (t != tv)
					GDKfree(t);
			}
			GDKfree(hv);
		}
		GDKfree(tv);
		logbat_destroy(b);

		if (tr_grow(tr)) {
			tr->changes[tr->nr].type = l->flag;
			tr->changes[tr->nr].nr = l->nr;
			tr->changes[tr->nr].ht = ht;
			tr->changes[tr->nr].tt = tt;
			tr->changes[tr->nr].name = GDKstrdup(name);
			tr->changes[tr->nr].b = r;
			tr->nr++;
		}
	} else {		/* bat missing ERROR or ignore ? currently error. */
		res = LOG_ERR;
	}
	return res;
}

static void
la_bat_updates(logger *lg, logaction *la)
{
	log_bid bid = logger_find_bat(lg, la->name);
	BAT *b;

	if (bid == 0)
		return;		/* ignore bats no longer in the catalog */

	/* do we need to skip these old updates */
	if (BATcount(lg->snapshots)) {
		BUN p = BUNfnd(lg->snapshots, &bid);

		if (p != BUN_NONE) {
			BATiter i = bat_iterator(lg->snapshots);
			int tid = *(int *) BUNtloc(i, p);

			if (lg->tid <= tid)
				return;
		}
	}

	b = BATdescriptor(bid);
	assert(b);
	if (b) {
		if (b->htype == TYPE_void && la->type == LOG_INSERT) {
			BATappend(b, la->b, TRUE);
		} else {
			if (la->type == LOG_INSERT)
				BATins(b, la->b, TRUE);
			else if (la->type == LOG_DELETE)
				BATdel(b, la->b, TRUE);
			else if (la->type == LOG_UPDATE) {
				BATiter bi = bat_iterator(la->b);
				BUN p, q;

				BATloop(la->b, p, q) {
					ptr h = BUNhead(bi, p);
					ptr t = BUNtail(bi, p);

					if (BUNfnd(b, h) == BUN_NONE) {
						/* if value doesn't exist, insert it
						   if b void headed, maintain that by inserting nils */
						if (b->htype == TYPE_void) {
							if (b->batCount == 0 && *(oid *) h != oid_nil)
								b->hseqbase = *(oid *) h;
							if (b->hseqbase != oid_nil && *(oid *) h != oid_nil) {
								void *tv = ATOMnilptr(b->ttype);

								while (b->hseqbase + b->batCount < *(oid *) h)
									BUNappend(b, tv, TRUE);
							}
							BUNappend(b, t, TRUE);
						} else {
							BUNins(b, h, t, TRUE);
						}
					} else {
						BUNreplace(b, h, t, TRUE);
					}
				}
			}
		}
		logbat_destroy(b);
	}
}

static void
log_read_destroy(logger *lg, trans *tr, char *name)
{
	(void) lg;
	if (tr_grow(tr)) {
		tr->changes[tr->nr].type = LOG_DESTROY;
		tr->changes[tr->nr].name = GDKstrdup(name);
		tr->nr++;
	}
}

static void
la_bat_destroy(logger *lg, logaction *la)
{
	log_bid bid = logger_find_bat(lg, la->name);

	if (bid) {
		logger_del_bat(lg, bid);
		if (BUNfnd(lg->snapshots, &bid) != BUN_NONE) {
			BUNdelHead(lg->snapshots, &bid, FALSE);
			BUNins(lg->snapshots, &bid, &lg->tid, FALSE);
		}
	}
}

static int
log_read_create(logger *lg, trans *tr, char *name)
{
	char *buf = log_read_string(lg);

	if (lg->debug & 1)
		fprintf(stderr, "log_read_create %s\n", name);

	if (!buf) {
		return LOG_ERR;
	} else {
		int ht, tt;
		char *ha = buf, *ta = strchr(buf, ',');

		if (!ta)
			return LOG_ERR;
		*ta = 0;
		ta++;		/* skip over , */
		if (strcmp(ha, "vid") == 0) {
			ht = -1;
		} else {
			ht = ATOMindex(ha);
		}
		if (strcmp(ta, "vid") == 0) {
			tt = -1;
		} else {
			tt = ATOMindex(ta);
		}
		if (tr_grow(tr)) {
			tr->changes[tr->nr].type = LOG_CREATE;
			tr->changes[tr->nr].ht = ht;
			tr->changes[tr->nr].tt = tt;
			tr->changes[tr->nr].name = GDKstrdup(name);
			tr->changes[tr->nr].b = NULL;
			tr->nr++;
		}
	}
	if (buf)
		GDKfree(buf);
	return LOG_OK;
}

static void
la_bat_create(logger *lg, logaction *la)
{
	int ht = (la->ht < 0) ? TYPE_void : la->ht;
	int tt = (la->tt < 0) ? TYPE_void : la->tt;
	BAT *b = BATnew(ht, tt, BATSIZE);

	if (b != NULL) {
		if (la->ht < 0)
			BATseqbase(b, 0);
		if (la->tt < 0)
			BATseqbase(BATmirror(b), 0);

		BATsetaccess(b, BAT_READ);
		logger_add_bat(lg, b, la->name);
		logbat_destroy(b);
	}
}

static void
log_read_use(logger *lg, trans *tr, logformat *l, char *name)
{
	(void) lg;
	if (tr_grow(tr)) {
		tr->changes[tr->nr].type = LOG_USE;
		tr->changes[tr->nr].nr = l->nr;
		tr->changes[tr->nr].name = GDKstrdup(name);
		tr->changes[tr->nr].b = NULL;
		tr->nr++;
	}
}

static void
la_bat_use(logger *lg, logaction *la)
{
	log_bid bid = la->nr;
	BAT *b = BATdescriptor(bid);

	if (!b) {
		GDKerror("logger: could not use bat (" OIDFMT ") for %s\n", bid, la->name);
		return;
	}
	logger_add_bat(lg, b, la->name);
	if (BUNfnd(lg->snapshots, &b->batCacheid) != BUN_NONE)
		BUNdelHead(lg->snapshots, &b->batCacheid, FALSE);
	BUNins(lg->snapshots, &b->batCacheid, &lg->tid, FALSE);
	logbat_destroy(b);
}


#define TR_SIZE 	1024

static trans *
tr_create(trans *tr, int tid)
{
	trans *ntr = (trans *) GDKmalloc(sizeof(trans));

	if (ntr == NULL)
		return NULL;
	ntr->tid = tid;
	ntr->sz = TR_SIZE;
	ntr->nr = 0;
	ntr->changes = (logaction *) GDKmalloc(sizeof(logaction) * TR_SIZE);
	if (ntr->changes == NULL) {
		GDKfree(ntr);
		return NULL;
	}
	ntr->tr = tr;
	return ntr;
}

static trans *
tr_find(trans *tr, int tid)
/* finds the tid and reorders the chain list, puts trans with tid first */
{
	trans *t = tr, *p = NULL;

	while (t && t->tid != tid) {
		p = t;
		t = t->tr;
	}
	if (!t)
		return NULL;	/* BAD missing transaction */
	if (t == tr)
		return tr;
	if (t->tr)		/* get this tid out of the list */
		p->tr = t->tr;
	t->tr = tr;		/* and move it to the front */
	return t;
}

static void
la_apply(logger *lg, logaction *c)
{
	switch (c->type) {
	case LOG_INSERT:
	case LOG_DELETE:
	case LOG_UPDATE:
		la_bat_updates(lg, c);
		break;
	case LOG_CREATE:
		la_bat_create(lg, c);
		break;
	case LOG_USE:
		la_bat_use(lg, c);
		break;
	case LOG_DESTROY:
		la_bat_destroy(lg, c);
		break;
	case LOG_CLEAR:
		la_bat_clear(lg, c);
		break;
	}
}

static void
la_destroy(logaction *c)
{
	if (c->name)
		GDKfree(c->name);
	if (c->b)
		logbat_destroy(c->b);
}

static int
tr_grow(trans *tr)
{
	if (tr->nr == tr->sz) {
		tr->sz <<= 1;
		tr->changes = (logaction *) GDKrealloc(tr->changes, tr->sz * sizeof(logaction));
		if (tr->changes == NULL)
			return 0;
	}
	/* cleanup the next */
	tr->changes[tr->nr].name = NULL;
	tr->changes[tr->nr].b = NULL;
	return 1;
}

static trans *
tr_destroy(trans *tr)
{
	trans *r = tr->tr;

	GDKfree(tr->changes);
	GDKfree(tr);
	return r;
}

static trans *
tr_commit(logger *lg, trans *tr)
{
	int i;

	if (lg->debug & 1)
		fprintf(stderr, "tr_commit\n");

	for (i = 0; i < tr->nr; i++) {
		la_apply(lg, &tr->changes[i]);
		la_destroy(&tr->changes[i]);
	}
	return tr_destroy(tr);
}

static trans *
tr_abort(logger *lg, trans *tr)
{
	int i;

	if (lg->debug & 1)
		fprintf(stderr, "tr_abort\n");

	for (i = 0; i < tr->nr; i++)
		la_destroy(&tr->changes[i]);
	return tr_destroy(tr);
}

static int
logger_open(logger *lg)
{
	char filename[BUFSIZ];

	snprintf(filename, BUFSIZ, "%s%s." LLFMT, lg->dir, LOGFILE, lg->id);

	lg->log = open_wstream(filename);
	if (mnstr_errnr(lg->log))
		return LOG_ERR;

	return LOG_OK;
}

static void
logger_close(logger *lg)
{
	stream *log = lg->log;

	if (log) {
		mnstr_close(log);
		mnstr_destroy(log);
	}
	lg->log = NULL;
}

static int
logger_readlog(logger *lg, char *filename)
{
	trans *tr = NULL;
	logformat l;
	int err = 0;

	lg->log = open_rstream(filename);

	/* if the file doesn't exist, there is nothing to be read back */
	if (!lg->log || mnstr_errnr(lg->log)) {
		if (lg->log)
			mnstr_destroy(lg->log);
		lg->log = NULL;
		return 0;
	}
	while (!err && log_read_format(lg, &l)) {
		char *name = NULL;

		if (l.flag != LOG_START && l.flag != LOG_END && l.flag != LOG_SEQ) {
			name = log_read_string(lg);

			if (!name) {
				err = -1;
				break;
			}
		}
		if (lg->debug & 1) {
			fprintf(stderr, "logger_readlog: ");
			if (l.flag > 0 &&
			    l.flag < (char) (sizeof(log_commands) / sizeof(log_commands[0])))
				fprintf(stderr, "%s", log_commands[(int) l.flag]);
			else
				fprintf(stderr, "%d", l.flag);
			fprintf(stderr, " %d %d", l.tid, l.nr);
			if (name)
				fprintf(stderr, " %s", name);
			fprintf(stderr, "\n");
		}
		/* find proper transaction record */
		if (l.flag != LOG_START)
			tr = tr_find(tr, l.tid);
		switch (l.flag) {
		case LOG_START:
			if (l.nr > lg->tid)
				lg->tid = l.nr;
			tr = tr_create(tr, l.nr);
			if (lg->debug & 1)
				fprintf(stderr, "logger tstart %d\n", tr->tid);
			break;
		case LOG_END:
			if (l.tid != l.nr)	/* abort record */
				tr = tr_abort(lg, tr);
			else
				tr = tr_commit(lg, tr);
			break;
		case LOG_SEQ:
			err = (log_read_seq(lg, &l) != LOG_OK);
			break;
		case LOG_INSERT:
		case LOG_DELETE:
		case LOG_UPDATE:
			if (name == NULL)
				err = 1;
			else
				err = (log_read_updates(lg, tr, &l, name) != LOG_OK);
			break;
		case LOG_CREATE:
			if (name == NULL)
				err = 1;
			else
				err = (log_read_create(lg, tr, name) != LOG_OK);
			break;
		case LOG_USE:
			if (name == NULL)
				err = 1;
			else
				log_read_use(lg, tr, &l, name);
			break;
		case LOG_DESTROY:
			if (name == NULL)
				err = 1;
			else
				log_read_destroy(lg, tr, name);
			break;
		case LOG_CLEAR:
			if (name == NULL)
				err = 1;
			else
				log_read_clear(lg, tr, name);
			break;
		default:
			err = -2;
		}
		if (name)
			GDKfree(name);
		lg->changes++;
	}
	logger_close(lg);

	/* remaining transactions are not committed, ie abort */
	while (tr)
		tr = tr_abort(lg, tr);
	return 0;
}

/*
 * @-
 * The log files are incrementally numbered. They are processed in the
 * same sequence.
 */
static int
logger_readlogs(logger *lg, FILE *fp, char *filename)
{
	int res = 0;
	char id[BUFSIZ];

	if (lg->debug & 1)
		fprintf(stderr, "logger_readlogs %s\n", filename);

	while (fgets(id, BUFSIZ, fp) != NULL) {
		char buf[BUFSIZ];
		lng lid = strtoll(id, NULL, 10);

		if (lid >= lg->id) {
			lg->id = lid;
			snprintf(buf, BUFSIZ, "%s." LLFMT, filename, lg->id);

			if ((res = logger_readlog(lg, buf)) != 0) {
				/* we cannot distinguish errors from
				   incomplete transactions (even if we
				   would log aborts in the logs).
				   So we simply abort and move to the next
				   log file */
				(void) res;
			}
		}
	}
	return res;
}

static int
logger_commit(logger *lg)
{
	int id = LOG_SID;

	if (lg->debug & 1)
		fprintf(stderr, "logger_commit\n");

	BUNdelHead(lg->seqs, &id, FALSE);
	BUNins(lg->seqs, &id, &lg->id, FALSE);

	/* cleanup old snapshots */
	if (BATcount(lg->snapshots)) {
		BATclear(lg->snapshots);
		BATcommit(lg->snapshots);
	}
	return bm_commit(lg);
}

static int
check_version(logger *lg, FILE *fp)
{
	int version = 0;

	if (fscanf(fp, "%6d", &version) != 1) {
		GDKerror("Could not read the version number from the file '%s/log'.\n",
			 lg->dir);

		return -1;
	}
	if (version != lg->version) {
		if (lg->prefuncp == NULL ||
		    (*lg->prefuncp)(version, lg->version) != 0) {
			GDKerror("Incompatible database version %06d, "
				 "this server supports version %06d\n"
				 "Please move away %s and its corresponding dbfarm.",
				 version, lg->version, lg->dir);

			return -1;
		}
	} else
		lg->postfuncp = NULL;	 /* don't call */
	fgetc(fp);		/* skip \n */
	fgetc(fp);		/* skip \n */
	return 0;
}

static int
bm_subcommit(BAT *list, BAT *catalog, BAT *extra, int debug)
{
	BUN p, q;
	BUN nn = 2 + (list->batFirst > list->batDeleted ? list->batFirst - list->batDeleted : 0) + BATcount(list) + (extra ? BATcount(extra) : 0);
	bat *n = (bat*)GDKmalloc(sizeof(bat) * nn);
	int i = 0;
	BATiter iter = bat_iterator(list);
	int res;

	n[i++] = 0;		/* n[0] is not used */

	/* first loop over deleted then over current and new */
	for (p = list->batDeleted; p < list->batFirst; p++) {
		bat col = *(log_bid *) BUNhead(iter, p);

		if (debug & 1)
			fprintf(stderr, "commit deleted %s (%d) %s\n",
				BBPname(col), col,
				(list == catalog) ? BUNtail(iter, p) : "snapshot");
		n[i++] = ABS(col);
	}
	BATloop(list, p, q) {
		bat col = *(log_bid *) BUNhead(iter, p);

		if (debug & 1)
			fprintf(stderr, "commit new %s (%d) %s\n",
				BBPname(col), col,
				(list == catalog) ? BUNtail(iter, p) : "snapshot");
		n[i++] = ABS(col);
	}
	if (extra) {
		iter = bat_iterator(extra);
		BATloop(extra, p, q) {
			str name = (str) BUNtail(iter, p);

			if (debug & 1)
				fprintf(stderr, "commit extra %s %s\n",
					name,
					(list == catalog) ? BUNtail(iter, p) : "snapshot");
			n[i++] = ABS(BBPindex(name));
		}
	}
	/* now commit catalog, so it's also up to date on disk */
	n[i++] = ABS(catalog->batCacheid);
	assert((BUN) i <= nn);
	BATcommit(catalog);
	res = TMsubcommit_list(n, i);
	GDKfree(n);
	return res;
}

static void
logger_fatal(const char *format, const char *arg1, const char *arg2, const char *arg3)
{
	char *buf;

	GDKerror(format, arg1, arg2, arg3);
	GDKlog(format, arg1, arg2, arg3);
	if ((buf = GDKerrbuf) != NULL) {
		fprintf(stderr, "%s", buf);
		fflush(stderr);
	}
	GDKexit(1);
}

static logger *
logger_new(int debug, char *fn, char *logdir, char *dbname, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp)
{
	int id = LOG_SID;
	logger *lg = (struct logger *) GDKmalloc(sizeof(struct logger));
	FILE *fp;
	char filename[BUFSIZ];
	char bak[BUFSIZ];
	log_bid seqs = 0;
	bat catalog;

	if (lg == NULL)
		return NULL;

	lg->debug = debug;

	lg->changes = 0;
	lg->version = version;
	lg->id = 1;

	lg->tid = 0;

	/* if the path is absolute, it means someone is still calling
	 * logger_create/logger_new "manually" */
	assert(!MT_path_absolute(logdir));

	snprintf(filename, BUFSIZ, "%s%c%s%c%s%c%s%c",
		 GDKgetenv("gdk_dbfarm"), DIR_SEP, dbname, DIR_SEP,
		 logdir, DIR_SEP, fn, DIR_SEP);
	lg->fn = GDKstrdup(fn);
	lg->dir = GDKstrdup(filename);
	lg->prefuncp = prefuncp;
	lg->postfuncp = postfuncp;
	lg->log = NULL;
	lg->end = 0;
	lg->catalog = NULL;
	lg->snapshots = NULL;
	lg->seqs = NULL;

	snprintf(filename, BUFSIZ, "%s%s", lg->dir, LOGFILE);
	snprintf(bak, BUFSIZ, "%s.bak", filename);

	/* try to open logfile backup, or failing that, the file itself.
	   we need to know whether this file exists when checking the
	   database consistency later on */
	if ((fp = fopen(bak, "r")) != NULL) {
		fclose(fp);
		GDKunlink(lg->dir, LOGFILE, NULL);
		if (GDKmove(lg->dir, LOGFILE, "bak", lg->dir, LOGFILE, NULL) != 0)
			logger_fatal("logger_new: cannot move log.bak file back.\n", 0, 0, 0);
	}
	fp = fopen(filename, "r");

	snprintf(bak, BUFSIZ, "%s_catalog", fn);
	catalog = BBPindex(bak);

	if (catalog == 0) {
		log_bid bid = 0;

		/* catalog does not exist, so the log file also shouldn't exist */
		if (fp != NULL) {
			logger_fatal("logger_new: there is no logger catalog, but there is a log file.\n"
				     "Are you sure you are using the correct combination of database\n"
				     "(--dbfarm / --dbname) and log directory (--set %s_logdir)?\n",
				     fn, 0, 0);
			goto error;
		}

		lg->catalog = logbat_new(TYPE_int, TYPE_str, BATSIZE);
		if (debug)
			fprintf(stderr, "create %s catalog\n", fn);

		bid = lg->catalog->batCacheid;

		/* Make persistent */
		BBPincref(bid, TRUE);
		BATmode(lg->catalog, PERSISTENT);
		BBPrename(lg->catalog->batCacheid, bak);

		if (!GDKcreatedir(filename)) {
			logger_fatal("logger_new: cannot create directory for log file %s\n",
				     filename, 0, 0);
			goto error;
		}
		if ((fp = fopen(filename, "w")) == NULL) {
			logger_fatal("logger_new: cannot create log file %s\n",
				     filename, 0, 0);
			goto error;
		}
		fprintf(fp, "%06d\n\n", lg->version);
		lg->id ++;
		fprintf(fp, LLFMT "\n", lg->id);
		fclose(fp);
		fp = NULL;

		if (bm_subcommit(lg->catalog, lg->catalog, NULL, lg->debug) != 0) {
			/* cannot commit catalog, so remove log */
			unlink(filename);
			goto error;
		}
	} else {
		/* find the persistent catalog. As non persistent bats
		   require a logical reference we also add a logical
		   reference for the persistent bats */
		BUN p, q;
		BAT *b = BATdescriptor(catalog);
		BATiter bi = bat_iterator(b);

		/* the catalog exists, and so should the log file */
		if (fp == NULL) {
			logger_fatal("logger_new: there is a logger catalog, but no log file.\n"
				     "Are you sure you are using the correct combination of database\n"
				     "(--dbfarm / --dbname) and log directory (--set %s_logdir)?\n"
				     "If you have done a recent update of the server, it may be that your\n"
				     "logs are in an old location.  You should then either use\n"
				     "--set %s_logdir=<path to old log directory> or move the old log\n"
				     "directory to the new location (%s).\n",
				     fn, fn, lg->dir);
			goto error;
		}
		lg->catalog = b;
		BATloop(b, p, q) {
			bat bid = *(log_bid *) BUNhead(bi, p);

			BBPincref(bid, TRUE);
		}
	}
	seqs = logger_find_bat(lg, "seqs");
	if (seqs == 0) {
		lg->seqs = logbat_new(TYPE_int, TYPE_lng, 1);
		BATmode(lg->seqs, PERSISTENT);
		snprintf(bak, BUFSIZ, "%s_seqs", fn);
		BBPrename(lg->seqs->batCacheid, bak);

		logger_add_bat(lg, lg->seqs, "seqs");
		BUNins(lg->seqs, &id, &lg->id, FALSE);

		lg->snapshots = logbat_new(TYPE_int, TYPE_int, 1);
		BATmode(lg->snapshots, PERSISTENT);
		snprintf(bak, BUFSIZ, "%s_snapshots", fn);
		BBPrename(lg->snapshots->batCacheid, bak);
		logger_add_bat(lg, lg->snapshots, "snapshots");
		bm_subcommit(lg->catalog, lg->catalog, NULL, lg->debug);
	} else {
		bat snapshots = logger_find_bat(lg, "snapshots");

		lg->seqs = BATdescriptor(seqs);
		if (BATcount(lg->seqs)) {
			BATiter seqsi = bat_iterator(lg->seqs);
			lg->id = *(lng *) BUNtail(seqsi, BUNfnd(lg->seqs, &id));
		} else {
			BUNins(lg->seqs, &id, &lg->id, FALSE);
		}
		lg->snapshots = BATdescriptor(snapshots);
	}
	lg->freed = BATnew(TYPE_int, TYPE_void, 1);
	snprintf(bak, BUFSIZ, "%s_freed", fn);
	BBPrename(lg->freed->batCacheid, bak);

	if (fp != NULL) {
		if (check_version(lg, fp)) {
			goto error;
		}

		lg->changes++;
		logger_readlogs(lg, fp, filename);
		fclose(fp);
		fp = NULL;
		if (lg->postfuncp)
			(*lg->postfuncp)(lg);
	}
	return lg;
      error:
	if (fp)
		fclose(fp);
	if (lg)
		GDKfree(lg);
	return NULL;
}

logger *
logger_create(int debug, char *fn, char *logdir, char *dbname, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp)
{
	logger *lg = logger_new(debug, fn, logdir, dbname, version, prefuncp, postfuncp);

	if (!lg)
		return NULL;
	if (logger_open(lg) == LOG_ERR) {
		logger_destroy(lg);

		return NULL;
	}
	if (lg->changes &&
	    (logger_restart(lg) != LOG_OK ||
	     logger_cleanup(lg) != LOG_OK)) {
		logger_destroy(lg);

		return NULL;
	}
	return lg;
}

void
logger_destroy(logger *lg)
{
	if (lg->catalog) {
		BUN p, q;
		BAT *b = lg->catalog;
		BATiter bi = bat_iterator(b);

		logger_cleanup(lg);

		/* destroy the deleted */
/* would be an error ....
		for (p = b->batDeleted; p < b->batFirst; p++) {
			bat bid = *(log_bid *) BUNhead(b, p);

			BBPdecref(bid, TRUE);
		}
*/
		/* free resources */
		BATloop(b, p, q) {
			bat bid = *(log_bid *) BUNhead(bi, p);

			BBPdecref(bid, TRUE);
		}

		BBPdecref(lg->catalog->batCacheid, TRUE);
		logbat_destroy(lg->catalog);
		logbat_destroy(lg->freed);
	}
	GDKfree(lg->fn);
	GDKfree(lg->dir);
	logger_close(lg);
	GDKfree(lg);
}

int
logger_exit(logger *lg)
{
	FILE *fp;
	char filename[BUFSIZ];

	logger_close(lg);
	if (GDKmove(lg->dir, LOGFILE, NULL, lg->dir, LOGFILE, "bak") < 0) {
		return LOG_ERR;
	}

	snprintf(filename, BUFSIZ, "%s%s", lg->dir, LOGFILE);
	if ((fp = fopen(filename, "w")) != NULL) {
		char ext[BUFSIZ];

		fprintf(fp, "%06d\n\n", lg->version);
		lg->id ++;

		if (logger_commit(lg) != LOG_OK)
			return LOG_ERR;

		fprintf(fp, LLFMT "\n", lg->id);
		fclose(fp);

		/* atomic action, switch to new log, keep old for later
		   cleanup actions
		 */
		snprintf(ext, BUFSIZ, "bak-" LLFMT, lg->id);

		if (GDKmove(lg->dir, LOGFILE, "bak", lg->dir, LOGFILE, ext) < 0)
			return LOG_ERR;

		lg->changes = 0;
	} else {
		GDKerror("logger_exit: could not open %s\n", filename);
		return LOG_ERR;
	}
	return LOG_OK;
}

int
logger_restart(logger *lg)
{
	int res = 0;

	if ((res = logger_exit(lg)) == LOG_OK)
		res = logger_open(lg);

	return res;
}

int
logger_cleanup(logger *lg)
{
	char buf[BUFSIZ];
	char id[BUFSIZ];
	FILE *fp = NULL;

	snprintf(buf, BUFSIZ, "%s%s.bak-" LLFMT, lg->dir, LOGFILE, lg->id);

	if (lg->debug & 1)
		fprintf(stderr, "logger_cleanup %s\n", buf);

	if ((fp = fopen(buf, "r")) == NULL)
		return LOG_ERR;

	/* skip catalog */
	while (fgets(id, BUFSIZ, fp) != NULL && id[0] != '\n')
		;

	while (fgets(id, BUFSIZ, fp) != NULL) {
		char *e = strchr(id, '\n');

		if (e)
			*e = 0;
		GDKunlink(lg->dir, LOGFILE, id);
	}
	fclose(fp);
	snprintf(buf, BUFSIZ, "bak-" LLFMT, lg->id);

	GDKunlink(lg->dir, LOGFILE, buf);

	return LOG_OK;
}

size_t
logger_changes(logger *lg)
{
	return lg->changes;
}

int
logger_sequence(logger *lg, int seq, lng *id)
{
	BUN p = BUNfnd(lg->seqs, &seq);

	if (p != BUN_NONE) {
		BATiter i = bat_iterator(lg->seqs);
		*id = *(lng *) BUNtail(i, p);

		return 1;
	}
	return 0;
}

/*
 * @-
 * Changes made to the BAT descriptor should be stored in the log files.
 * Actually, we need to save the descriptor file, perhaps we should simply
 * introduce a versioning scheme.
 */
int
log_bat_persists(logger *lg, BAT *b, char *name)
{
	char *ha, *ta;
	int len;
	char buf[BUFSIZ];
	logformat l;
	int havevoid = 0;
	int flag = (b->batPersistence == PERSISTENT) ? LOG_USE : LOG_CREATE;

	l.nr = 0;
	if (flag == LOG_USE)
		l.nr = b->batCacheid;
	l.flag = flag;
	l.tid = lg->tid;
	lg->changes++;
	if (log_write_format(lg, &l) == LOG_ERR ||
	    log_write_string(lg, name) == LOG_ERR)
		return LOG_ERR;

	if (lg->debug & 1)
		fprintf(stderr, "persists bat %s (%d) %s\n",
			name, b->batCacheid,
			(flag == LOG_USE) ? "use" : "create");

	if (flag == LOG_USE) {
		if (BUNfnd(lg->snapshots, &b->batCacheid) != BUN_NONE)
			BUNdelHead(lg->snapshots, &b->batCacheid, FALSE);
		BUNins(lg->snapshots, &b->batCacheid, &lg->tid, FALSE);
		return LOG_OK;
	}

	ha = ATOMname(b->htype);
	if (b->htype == TYPE_void && BAThdense(b)) {
		ha = "vid";
		havevoid = 1;
	}
	ta = ATOMname(b->ttype);
	if (!havevoid && b->ttype == TYPE_void && BATtdense(b)) {
		ta = "vid";
	}
	len = snprintf(buf, BUFSIZ, "%s,%s", ha, ta);
	len++;			/* include EOS */
	if (!mnstr_writeInt(lg->log, len) ||
	    mnstr_write(lg->log, buf, 1, len) != (ssize_t) len)
		return LOG_ERR;

	if (lg->debug & 1)
		fprintf(stderr, "Logged new bat [%s,%s] %s " BUNFMT " (%d)\n",
			ha, ta, name, BATcount(b), b->batCacheid);
	return log_bat(lg, b, name);
}

int
log_bat_transient(logger *lg, char *name)
{
	log_bid bid = logger_find_bat(lg, name);
	logformat l;

	l.flag = LOG_DESTROY;
	l.tid = lg->tid;
	l.nr = 0;
	lg->changes++;

	/* if this is a snapshot bat, we need to skip all changes */
	if (BUNfnd(lg->snapshots, &bid) != BUN_NONE) {
		BUNdelHead(lg->snapshots, &bid, FALSE);
		BUNins(lg->snapshots, &bid, &lg->tid, FALSE);
	}

	if (log_write_format(lg, &l) == LOG_ERR ||
	    log_write_string(lg, name) == LOG_ERR)
		return LOG_ERR;

	if (lg->debug & 1)
		fprintf(stderr, "Logged destroyed bat %s\n", name);
	return LOG_OK;
}

int
log_delta(logger *lg, BAT *b, char *name)
{
	int ok = GDK_SUCCEED;
	logformat l;
	BUN p;
	BUN nr;

	if (lg->debug & 128) {
		/* logging is switched off */
		return LOG_OK;
	}

	l.tid = lg->tid;
	nr = (BUNlast(b) - BUNfirst(b));
	assert(nr <= GDK_int_max);
	l.nr = (int) nr;
	lg->changes += l.nr;

	if (l.nr) {
		BATiter bi = bat_iterator(b);
		int (*wh) (ptr, stream *, size_t) = b->htype == TYPE_void ? BATatoms[TYPE_oid].atomWrite : BATatoms[b->htype].atomWrite;
		int (*wt) (ptr, stream *, size_t) = BATatoms[b->ttype].atomWrite;

		l.flag = LOG_UPDATE;
		if (log_write_format(lg, &l) == LOG_ERR ||
		    log_write_string(lg, name) == LOG_ERR)
			return LOG_ERR;

		for (p = BUNfirst(b); p < BUNlast(b) && ok == GDK_SUCCEED; p++) {
			ptr h = BUNhead(bi, p);
			ptr t = BUNtail(bi, p);

			ok = wh(h, lg->log, 1);
			ok = (ok == GDK_FAIL) ? ok : wt(t, lg->log, 1);
		}

		if (lg->debug)
			fprintf(stderr, "Logged %s %d inserts\n", name, l.nr);
	}
	return (ok == GDK_SUCCEED) ? LOG_OK : LOG_ERR;
}

int
log_bat(logger *lg, BAT *b, char *name)
{
	int ok = GDK_SUCCEED;
	logformat l;
	BUN p;

	if (lg->debug & 128) {
		/* logging is switched off */
		return LOG_OK;
	}

	l.tid = lg->tid;
	l.nr = (int) (BUNlast(b) - b->batInserted);
	lg->changes += l.nr;

	if (l.nr) {
		BATiter bi = bat_iterator(b);
		int (*wh) (ptr, stream *, size_t) = BATatoms[b->htype].atomWrite;
		int (*wt) (ptr, stream *, size_t) = BATatoms[b->ttype].atomWrite;

		l.flag = LOG_INSERT;
		if (log_write_format(lg, &l) == LOG_ERR ||
		    log_write_string(lg, name) == LOG_ERR)
			return LOG_ERR;

		if (b->htype == TYPE_void &&
		    b->ttype < TYPE_str &&
		    !isVIEW(b)) {
			ptr t = BUNtail(bi, b->batInserted);

			ok = wt(t, lg->log, l.nr);
		} else {
			for (p = b->batInserted; p < BUNlast(b) && ok == GDK_SUCCEED; p++) {
				ptr h = BUNhead(bi, p);
				ptr t = BUNtail(bi, p);

				ok = wh(h, lg->log, 1);
				ok = (ok == GDK_FAIL) ? ok : wt(t, lg->log, 1);
			}
		}

		if (lg->debug)
			fprintf(stderr, "Logged %s %d inserts\n", name, l.nr);
	}
	l.nr = (int) (b->batFirst - b->batDeleted);
	lg->changes += l.nr;

	if (l.nr && ok == GDK_SUCCEED) {
		BATiter bi = bat_iterator(b);
		int (*wh) (ptr, stream *, size_t) = BATatoms[b->htype].atomWrite;
		int (*wt) (ptr, stream *, size_t) = BATatoms[b->ttype].atomWrite;

		l.flag = LOG_DELETE;
		if (log_write_format(lg, &l) == LOG_ERR ||
		    log_write_string(lg, name) == LOG_ERR)
			return LOG_ERR;

		for (p = b->batDeleted; p < b->batFirst && ok == GDK_SUCCEED; p++) {
			ptr h = BUNhead(bi, p);
			ptr t = BUNtail(bi, p);

			ok = wh(h, lg->log, 1);
			ok = (ok == GDK_FAIL) ? ok : wt(t, lg->log, 1);
		}

		if (lg->debug)
			fprintf(stderr, "Logged %s %d deletes\n", name, l.nr);
	}
	return (ok == GDK_SUCCEED) ? LOG_OK : LOG_ERR;
}

int
log_bat_clear(logger *lg, char *name)
{
	int ok = GDK_SUCCEED;
	logformat l;

	if (lg->debug & 128) {
		/* logging is switched off */
		return LOG_OK;
	}

	l.nr = 1;
	l.tid = lg->tid;
	lg->changes += l.nr;

	l.flag = LOG_CLEAR;
	if (log_write_format(lg, &l) == LOG_ERR ||
	    log_write_string(lg, name) == LOG_ERR)
		return LOG_ERR;

	if (lg->debug)
		fprintf(stderr, "Logged clear %s\n", name);

	return (ok == GDK_SUCCEED) ? LOG_OK : LOG_ERR;
}

int
log_tstart(logger *lg)
{
	logformat l;

	l.flag = LOG_START;
	l.tid = ++lg->tid;
	l.nr = lg->tid;

	if (lg->debug)
		fprintf(stderr, "log_tstart %d\n", lg->tid);

	return log_write_format(lg, &l);
}

#define DBLKSZ 8192
#define DBLKMASK 8191
#define SEGSZ 16*DBLKSZ
static char zeros[DBLKSZ] = { 0 };

static void
pre_allocate(logger *lg)
{
	lng p;

	mnstr_fgetpos(lg->log, &p);
	if (p + DBLKSZ > lg->end) {
		lng s = p;

		if (p > lg->end) {
			lg->end = p & ~DBLKMASK;
			if (p > DBLKSZ)
				p -= DBLKSZ;
		}
		if (p < lg->end) {
			p = (lg->end - p);
			mnstr_write(lg->log, zeros, (size_t) p, 1);
			lg->end += p;
			p = 0;
		}
		for (; p < SEGSZ; p += DBLKSZ, lg->end += DBLKSZ)
			mnstr_write(lg->log, zeros, DBLKSZ, 1);
		mnstr_fsetpos(lg->log, s);
	}
}

int
log_tend(logger *lg)
{
	logformat l;
	int res = 0;

	if (lg->debug)
		fprintf(stderr, "log_tend %d\n", lg->tid);

	if (DELTAdirty(lg->snapshots)) {
		/* sub commit all new snapshots */
		BAT *b = BATselect(lg->snapshots, &lg->tid, &lg->tid);

		if (b == NULL)
			return LOG_ERR;
		res = bm_subcommit(b, lg->snapshots, NULL, lg->debug);
		BBPunfix(b->batCacheid);
	}
	l.flag = LOG_END;
	l.tid = lg->tid;
	l.nr = lg->tid;
	if (res ||
	    log_write_format(lg, &l) == LOG_ERR ||
	    mnstr_flush(lg->log) ||
	    mnstr_fsync(lg->log))
		return LOG_ERR;
	pre_allocate(lg);
	return LOG_OK;
}

int
log_abort(logger *lg)
{
	logformat l;

	if (lg->debug)
		fprintf(stderr, "log_abort %d\n", lg->tid);

	l.flag = LOG_END;
	l.tid = lg->tid;
	l.nr = -1;

	if (log_write_format(lg, &l) == LOG_ERR)
		return LOG_ERR;

	return LOG_OK;
}

/* a transaction in it self */
int
log_sequence(logger *lg, int seq, lng id)
{
	logformat l;

	l.flag = LOG_SEQ;
	l.tid = lg->tid;
	l.nr = seq;

	if (lg->debug)
		fprintf(stderr, "log_sequence (%d," LLFMT ")\n", seq, id);

	if (BUNfnd(lg->seqs, &seq) != BUN_NONE) {
		BUNdelHead(lg->seqs, &seq, FALSE);
	}
	BUNins(lg->seqs, &seq, &id, FALSE);

	if (log_write_format(lg, &l) == LOG_ERR ||
	    !mnstr_writeLng(lg->log, id) ||
	    mnstr_flush(lg->log) ||
	    mnstr_fsync(lg->log))
		 return LOG_ERR;

	pre_allocate(lg);
	return LOG_OK;
}

static int
bm_commit(logger *lg)
{
	BUN p, q;
	BAT *b = lg->catalog;
	BAT *n = logbat_new(TYPE_void, TYPE_str, BATcount(lg->freed));
	BATiter bi = bat_iterator(b);
	int res;

	/* remove the destroyed bats */
	for (p = b->batDeleted; p < b->batFirst; p++) {
		bat bid = *(log_bid *) BUNhead(bi, p);
		BAT *lb = BATdescriptor(bid);

		BATmode(lb, TRANSIENT);
		BBPdecref(bid, TRUE);
		logbat_destroy(lb);

		if (lg->debug & 1)
			fprintf(stderr, "bm_commit: delete %d (%d)\n",
				bid, BBP[bid].lrefs);
	}

	/* subcommit the freed snapshots */
	BATseqbase(n, 0);
	if (BATcount(lg->freed)) {

		BATloop(lg->freed, p, q) {
			bat bid = *(log_bid *) Hloc(lg->freed, p);
			str name = BBPname(bid);

			if (lg->debug & 1)
				fprintf(stderr,
					"commit deleted (snapshot) %s (%d)\n",
					name, bid);
			BUNappend(n, name, FALSE);
			BBPdecref(bid, TRUE);
		}
	}

	for (p = b->batInserted; p < BUNlast(b); p++) {
		log_bid bid = *(log_bid *) BUNhead(bi, p);
		BAT *lb = BATdescriptor(bid);

		BATmode(lb, PERSISTENT);
assert(lb->P->restricted > BAT_WRITE);
		if (BATcount(lb) > (BUN) REMAP_PAGE_MAXSIZE)
			BATmmap(lb, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 0);
		logbat_destroy(lb);

		if (lg->debug & 1)
			fprintf(stderr, "bm_commit: create %d (%d)\n",
				bid, BBP[bid].lrefs);
	}
	res = bm_subcommit(lg->catalog, lg->catalog, n, lg->debug);
	BBPreclaim(n);
	BATclear(lg->freed);
	BATcommit(lg->freed);
	return res != 0 ? LOG_ERR : LOG_OK;
}

log_bid
logger_add_bat(logger *lg, BAT *b, char *name)
{
	log_bid bid = logger_find_bat(lg, name);

	assert(b->P->restricted > 0 || (b == lg->snapshots || b == lg->catalog || b == lg->seqs));
	if (bid) {
		if (bid != b->batCacheid) {
			logger_del_bat(lg, bid);
		} else {
			return bid;
		}
	}
	bid = b->batCacheid;
	if (lg->debug)
		fprintf(stderr, "create %s\n", name);
	lg->changes += BATcount(b) + 1;
	BUNins(lg->catalog, &bid, name, FALSE);
	BBPincref(bid, TRUE);
	return bid;
}

void
logger_del_bat(logger *lg, log_bid bid)
{
	BAT *b = BATdescriptor(bid);
	BUN p = BUNfnd(lg->catalog, &bid);

	assert(p != BUN_NONE);

	/* if this is a not logger commited snapshot bat, make it transient */
	if (p >= lg->catalog->batInserted &&
	    BUNfnd(lg->snapshots, &bid) != BUN_NONE) {

		BUNdelHead(lg->snapshots, &bid, FALSE);
		BATmode(b, TRANSIENT);
		if (lg->debug & 1)
			fprintf(stderr,
				"logger_del_bat release snapshot %d (%d)\n",
				bid, BBP[bid].lrefs);
		BUNins(lg->freed, &bid, NULL, FALSE);
	} else if (p >= lg->catalog->batInserted)
		BBPdecref(bid, TRUE);

	if (b) {
		lg->changes += BATcount(b) + 1;
		BBPunfix(b->batCacheid);
	}
	BUNdelHead(lg->catalog, &bid, FALSE);

/*assert( BBP[bid].lrefs == 0 );*/
}

log_bid
logger_find_bat(logger *lg, char *name)
{
	BAT *r_catalog = BATmirror(lg->catalog);
	log_bid res = 0;
	BUN p = BUNfnd(r_catalog, name);

	if (p != BUN_NONE) {
		BATiter i = bat_iterator(r_catalog);
		res = *(log_bid *) BUNtail(i, p);
	}
	return res;
}

