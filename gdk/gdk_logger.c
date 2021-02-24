/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * (author) N. J. Nes
 *
 * In the philosophy of MonetDB, transaction management overhead
 * should only be paid when necessary. Transaction management is for
 * this purpose implemented as a separate module and applications are
 * required to obey the transaction policy, e.g. obtaining/releasing
 * locks.
 *
 * This module is designed to support efficient logging of the SQL
 * database.  Once loaded, the SQL compiler will insert the proper
 * calls at transaction commit to include the changes in the log file.
 *
 * The logger uses a directory to store its log files. One master log
 * file stores information about the version of the logger and the
 * transaction log files. This file is a simple ascii file with the
 * following format:
 *  {6DIGIT-VERSION\n[log file number \n]*]*}
 * The transaction log files have a binary format, which stores fixed
 * size logformat headers (flag,nr,bid), where the flag is the type of
 * update logged.  The nr field indicates how many changes there were
 * (in case of inserts/deletes).  The bid stores the bid identifier.
 *
 * The key decision to be made by the user is the location of the log
 * file.  Ideally, it should be stored in fail-safe environment, or at
 * least the log and databases should be on separate disk columns.
 *
 * This file system may reside on the same hardware as the database
 * server and therefore the writes are done to the same disk, but
 * could also reside on another system and then the changes are
 * flushed through the network.  The logger works under the assumption
 * that it is called to safeguard updates on the database when it has
 * an exclusive lock on the latest version. This lock should be
 * guaranteed by the calling transaction manager first.
 *
 * Finding the updates applied to a BAT is relatively easy, because
 * each BAT contains a delta structure. On commit these changes are
 * written to the log file and the delta management is reset. Since
 * each commit is written to the same log file, the beginning and end
 * are marked by a log identifier.
 *
 * A server restart should only (re)process blocks which are
 * completely written to disk. A log replay therefore ends in a commit
 * or abort on the changed bats. Once all logs have been read, the
 * changes to the bats are made persistent, i.e. a bbp sub-commit is
 * done.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_logger.h"
#include <string.h>

/*
 * The log record encoding is geared at reduced storage space, but at
 * the expense of readability. A user can not easily inspect the log a
 * posteriori to check what has happened.
 *
 */
#define LOG_START	1
#define LOG_END		2
#define LOG_INSERT	3
#define LOG_UPDATE	5
#define LOG_CREATE	6
#define LOG_DESTROY	7
#define LOG_USE		8
#define LOG_CLEAR	9
#define LOG_SEQ		10
#define LOG_INSERT_ID	11
#define LOG_UPDATE_ID	12
#define LOG_CREATE_ID	13
#define LOG_DESTROY_ID	14
#define LOG_USE_ID	15
#define LOG_CLEAR_ID	16
#define LOG_UPDATE_PAX	17

#ifdef NATIVE_WIN32
#define getfilepos _ftelli64
#else
#ifdef HAVE_FSEEKO
#define getfilepos ftello
#else
#define getfilepos ftell
#endif
#endif

#define NAME(name,tpe,id) (name?name:"tpe id")

#define LOG_DISABLED(lg) ((lg)->debug&128 || (lg)->inmemory)

static const char *log_commands[] = {
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
	"LOG_INSERT_ID",
	"LOG_DELETE_ID",
	"LOG_UPDATE_ID",
	"LOG_CREATE_ID",
	"LOG_DESTROY_ID",
	"LOG_USE_ID",
	"LOG_CLEAR_ID",
	"LOG_UPDATE_PAX",
};

typedef struct logformat_t {
	char flag;
	int tid;
	lng nr;
} logformat;

typedef enum {LOG_OK, LOG_EOF, LOG_ERR} log_return;

/* When reading an old format database, we may need to read the geom
 * Well-known Binary (WKB) type differently.  This variable is used to
 * indicate that to the function wkbREAD during reading of the log. */
static int geomisoldversion;

static gdk_return bm_commit(logger *lg);
static gdk_return tr_grow(trans *tr);

static BUN
log_find(BAT *b, BAT *d, int val)
{
	BATiter cni = bat_iterator(b);
	BUN p;

	assert(b->ttype == TYPE_int);
	assert(d->ttype == TYPE_oid);
	if (BAThash(b) == GDK_SUCCEED) {
		HASHloop_int(cni, cni.b->thash, p, &val) {
			oid pos = p;
			if (BUNfnd(d, &pos) == BUN_NONE)
				return p;
		}
	} else {		/* unlikely: BAThash failed */
		BUN q;
		int *t = (int *) Tloc(b, 0);

		for (p = 0, q = BUNlast(b); p < q; p++) {
			if (t[p] == val) {
				oid pos = p;
				if (BUNfnd(d, &pos) == BUN_NONE)
					return p;
			}
		}
	}
	return BUN_NONE;
}

static void
logbat_destroy(BAT *b)
{
	if (b)
		BBPunfix(b->batCacheid);
}

static BAT *
logbat_new(int tt, BUN size, role_t role)
{
	BAT *nb = COLnew(0, tt, size, role);

	if (nb) {
		if (role == PERSISTENT)
			BATmode(nb, false);
	} else {
		TRC_CRITICAL(GDK, "creating new BAT[void:%s]#" BUNFMT " failed\n", ATOMname(tt), size);
	}
	return nb;
}

static int
log_read_format(logger *l, logformat *data)
{
	assert(!l->inmemory);
	return mnstr_read(l->log, &data->flag, 1, 1) == 1 &&
		mnstr_readLng(l->log, &data->nr) == 1 &&
		mnstr_readInt(l->log, &data->tid) == 1;
}

static gdk_return
log_write_format(logger *l, logformat *data)
{
	assert(!l->inmemory);
	if (mnstr_write(l->log, &data->flag, 1, 1) == 1 &&
	    mnstr_writeLng(l->log, data->nr) &&
	    mnstr_writeInt(l->log, data->tid))
		return GDK_SUCCEED;
	TRC_CRITICAL(GDK, "write failed\n");
	return GDK_FAIL;
}

static char *
log_read_string(logger *l)
{
	int len;
	ssize_t nr;
	char *buf;

	assert(!l->inmemory);
	if (mnstr_readInt(l->log, &len) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
//MK This leads to non-repeatable log structure?
		return NULL;
	}
	if (len == 0)
		return NULL;
	buf = GDKmalloc(len);
	if (buf == NULL) {
		TRC_CRITICAL(GDK, "malloc failed\n");
		/* this is bad */
		return (char *) -1;
	}

	if ((nr = mnstr_read(l->log, buf, 1, len)) != (ssize_t) len) {
		buf[len - 1] = 0;
		TRC_CRITICAL(GDK, "couldn't read name (%s) %zd\n", buf, nr);
		GDKfree(buf);
		return NULL;
	}
	buf[len - 1] = 0;
	return buf;
}

static gdk_return
log_write_string(logger *l, const char *n)
{
	size_t len = strlen(n) + 1;	/* log including EOS */

	assert(!l->inmemory);
	assert(len > 1);
	assert(len <= INT_MAX);
	if (!mnstr_writeInt(l->log, (int) len) ||
	    mnstr_write(l->log, n, 1, len) != (ssize_t) len) {
		TRC_CRITICAL(GDK, "write failed\n");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static log_return
log_read_clear(logger *lg, trans *tr, char *name, char tpe, oid id)
{
	if (lg->debug & 1)
		fprintf(stderr, "#logger found log_read_clear %s\n", NAME(name, tpe, id));
	if (tr_grow(tr) != GDK_SUCCEED)
		return LOG_ERR;
	tr->changes[tr->nr].type = LOG_CLEAR;
	tr->changes[tr->nr].tpe = tpe;
	tr->changes[tr->nr].cid = id;
	if (name && (tr->changes[tr->nr].name = GDKstrdup(name)) == NULL)
		return LOG_ERR;
	tr->nr++;
	return LOG_OK;
}

static int
avoid_snapshot(logger *lg, log_bid bid)
{
	if (BATcount(lg->snapshots_bid)-BATcount(lg->dsnapshots)) {
		BUN p = log_find(lg->snapshots_bid, lg->dsnapshots, bid);

		if (p != BUN_NONE) {
			int tid = *(int *) Tloc(lg->snapshots_tid, p);

			if (lg->tid <= tid)
				return 1;
		}
	}
	return 0;
}

static gdk_return
la_bat_clear(logger *lg, logaction *la)
{
	log_bid bid = logger_find_bat(lg, la->name, la->tpe, la->cid);
	BAT *b;

	if (lg->debug & 1)
		fprintf(stderr, "#la_bat_clear %s\n", NAME(la->name, la->tpe, la->cid));

	/* do we need to skip these old updates */
	if (avoid_snapshot(lg, bid))
		return GDK_SUCCEED;

	b = BATdescriptor(bid);
	if (b) {
		restrict_t access = (restrict_t) b->batRestricted;
		b->batRestricted = BAT_WRITE;
		BATclear(b, true);
		b->batRestricted = access;
		logbat_destroy(b);
	}
	return GDK_SUCCEED;
}

static log_return
log_read_seq(logger *lg, logformat *l)
{
	int seq = (int) l->nr;
	lng val;
	BUN p;

	assert(!lg->inmemory);
	assert(l->nr <= (lng) INT_MAX);
	if (mnstr_readLng(lg->log, &val) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}

	if ((p = log_find(lg->seqs_id, lg->dseqs, seq)) != BUN_NONE &&
	    p >= lg->seqs_id->batInserted) {
		if (BUNinplace(lg->seqs_val, p, &val, false) != GDK_SUCCEED)
			return LOG_ERR;
	} else {
		if (p != BUN_NONE) {
			oid pos = p;
			if (BUNappend(lg->dseqs, &pos, false) != GDK_SUCCEED)
				return LOG_ERR;
		}
		if (BUNappend(lg->seqs_id, &seq, false) != GDK_SUCCEED ||
		    BUNappend(lg->seqs_val, &val, false) != GDK_SUCCEED)
			return LOG_ERR;
	}
	return LOG_OK;
}

static gdk_return
log_write_id(logger *l, char tpe, oid id)
{
	lng lid = id;
	assert(!l->inmemory);
	assert(lid >= 0);
	if (mnstr_writeChr(l->log, tpe) &&
	    mnstr_writeLng(l->log, lid))
		return GDK_SUCCEED;
	TRC_CRITICAL(GDK, "write failed\n");
	return GDK_FAIL;
}

static int
log_read_id(logger *lg, char *tpe, oid *id)
{
	lng lid;

	assert(!lg->inmemory);
	if (mnstr_readChr(lg->log, tpe) != 1 ||
	    mnstr_readLng(lg->log, &lid) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}
	*id = (oid)lid;
	return LOG_OK;
}

#ifdef GDKLIBRARY_OLDDATE
static void *
dateRead(void *dst, stream *s, size_t cnt)
{
	int *ptr;

	if ((ptr = BATatoms[TYPE_int].atomRead(dst, s, cnt)) == NULL)
		return NULL;
	for (size_t i = 0; i < cnt; i++) {
		if (!is_int_nil(ptr[i]))
			ptr[i] = cvtdate(ptr[i]);
	}
	return ptr;
}

static void *
daytimeRead(void *dst, stream *s, size_t cnt)
{
	int *ptr;
	lng *lptr;

	if ((dst = BATatoms[TYPE_int].atomRead(dst, s, cnt)) == NULL)
		return NULL;
	ptr = dst;
	lptr = dst;
	/* work backwards so that we do this in place */
	for (size_t i = cnt; i > 0; ) {
		i--;
		if (is_int_nil(ptr[i]))
			lptr[i] = lng_nil;
		else
			lptr[i] = ptr[i] * LL_CONSTANT(1000);
	}
	return dst;
}

static void *
timestampRead(void *dst, stream *s, size_t cnt)
{
	union timestamp {
		lng l;
		struct {
#ifndef WORDS_BIGENDIAN
			int p_msecs;
			int p_days;
#else
			int p_days;
			int p_msecs;
#endif
		} t;
	} *ptr;

	if ((ptr = BATatoms[TYPE_lng].atomRead(dst, s, cnt)) == NULL)
		return NULL;
	for (size_t i = 0; i < cnt; i++) {
		if (!is_lng_nil(ptr[i].l))
			ptr[i].t.p_days = cvtdate(ptr[i].t.p_days);
	}
	return ptr;
}
#endif

static log_return
log_read_updates(logger *lg, trans *tr, logformat *l, char *name, int tpe, oid id, int pax)
{
	log_bid bid = logger_find_bat(lg, name, tpe, id);
	BAT *b = BATdescriptor(bid);
	log_return res = LOG_OK;
	int ht = -1, tt = -1, tseq = 0;

	assert(!lg->inmemory);
	if (lg->debug & 1)
		fprintf(stderr, "#logger found log_read_updates %s %s " LLFMT "\n", name, l->flag == LOG_INSERT ? "insert" : "update", l->nr);

	if (b) {
		ht = TYPE_void;
		tt = b->ttype;
		if (tt == TYPE_void && BATtdense(b))
			tseq = 1;
	} else {		/* search trans action for create statement */
		int i;

		for (i = 0; i < tr->nr; i++) {
			if (tr->changes[i].type == LOG_CREATE &&
			    (tpe == 0
			     ? strcmp(tr->changes[i].name, name) == 0
			     : tr->changes[i].tpe == tpe && tr->changes[i].cid == id)) {
				ht = tr->changes[i].ht;
				if (ht < 0) {
					ht = TYPE_void;
				}
				tt = tr->changes[i].tt;
				if (tt < 0) {
					tseq = 1;
					tt = TYPE_void;
				}
				break;
			} else if (tr->changes[i].type == LOG_USE &&
				   (tpe == 0
				    ? strcmp(tr->changes[i].name, name) == 0
				    : tr->changes[i].tpe == tpe && tr->changes[i].cid == id)) {
				log_bid bid = (log_bid) tr->changes[i].nr;
				BAT *b = BATdescriptor(bid);

				if (b) {
					ht = TYPE_void;
					tt = b->ttype;
				}
				break;
			}
		}
		assert(i < tr->nr); /* found one */
	}
	assert((ht == TYPE_void && l->flag == LOG_INSERT) ||
	       ((ht == TYPE_oid || !ht) && l->flag == LOG_UPDATE));
	if ((ht != TYPE_void && l->flag == LOG_INSERT) ||
	   ((ht != TYPE_void && ht != TYPE_oid) && l->flag == LOG_UPDATE))
		return LOG_ERR;
	if (ht >= 0 && tt >= 0) {
		BAT *uid = NULL;
		BAT *r;
		void *(*rt) (ptr, stream *, size_t) = BATatoms[tt].atomRead;
		void *tv = NULL;

		if (ATOMstorage(tt) < TYPE_str)
			tv = lg->buf;
#ifdef GDKLIBRARY_OLDDATE
		if (lg->convert_date && tt >= TYPE_date) {
			if (strcmp(BATatoms[tt].name, "date") == 0)
				rt = dateRead;
			else if (strcmp(BATatoms[tt].name, "daytime") == 0)
				rt = daytimeRead;
			else if (strcmp(BATatoms[tt].name, "timestamp") == 0)
				rt = timestampRead;
		}
#endif

		assert(l->nr <= (lng) BUN_MAX);
		if (l->flag == LOG_UPDATE) {
			uid = COLnew(0, ht, (BUN) l->nr, PERSISTENT);
			if (uid == NULL) {
				logbat_destroy(b);
				return LOG_ERR;
			}
		} else {
			assert(ht == TYPE_void);
		}
		r = COLnew(0, tt, (BUN) l->nr, PERSISTENT);
		if (r == NULL) {
			BBPreclaim(uid);
			logbat_destroy(b);
			return LOG_ERR;
		}

		if (tseq)
			BATtseqbase(r, 0);

		if (ht == TYPE_void && l->flag == LOG_INSERT) {
			lng nr = l->nr;
			for (; res == LOG_OK && nr > 0; nr--) {
				void *t = rt(tv, lg->log, 1);

				if (t == NULL) {
					/* see if failure was due to
					 * malloc or something less
					 * serious (in the current
					 * context) */
					if (strstr(GDKerrbuf, "alloc") == NULL)
						res = LOG_EOF;
					else
						res = LOG_ERR;
					break;
				}
				if (BUNappend(r, t, true) != GDK_SUCCEED)
					res = LOG_ERR;
				if (t != tv)
					GDKfree(t);
			}
		} else {
			void *(*rh) (ptr, stream *, size_t) = ht == TYPE_void ? BATatoms[TYPE_oid].atomRead : BATatoms[ht].atomRead;
			void *hv = ATOMnil(ht);

			if (hv == NULL)
				res = LOG_ERR;

			if (!pax) {
				lng nr = l->nr;
				for (; res == LOG_OK && nr > 0; nr--) {
					void *h = rh(hv, lg->log, 1);
					void *t = rt(tv, lg->log, 1);

					if (h == NULL)
						res = LOG_EOF;
					else if (t == NULL) {
						if (strstr(GDKerrbuf, "malloc") == NULL)
							res = LOG_EOF;
						else
							res = LOG_ERR;
					} else if (BUNappend(uid, h, true) != GDK_SUCCEED ||
					   	BUNappend(r, t, true) != GDK_SUCCEED)
						res = LOG_ERR;
					if (t != tv)
						GDKfree(t);
				}
			} else {
				char compressed = 0;
				lng nr = l->nr;

				if (mnstr_read(lg->log, &compressed, 1, 1) != 1)
					return LOG_ERR;

				if (compressed) {
					void *h = rh(hv, lg->log, 1);

					assert(uid->ttype == TYPE_void);
					if (h == NULL)
						res = LOG_EOF;
					else {
						BATtseqbase(uid, *(oid*)h);
						BATsetcount(uid, (BUN) l->nr);
					}
				} else {
					for (; res == LOG_OK && nr > 0; nr--) {
						void *h = rh(hv, lg->log, 1);

						if (h == NULL)
							res = LOG_EOF;
						else if (BUNappend(uid, h, true) != GDK_SUCCEED)
							res = LOG_ERR;
					}
				}
				nr = l->nr;
				for (; res == LOG_OK && nr > 0; nr--) {
					void *t = rt(tv, lg->log, 1);

					if (t == NULL) {
						if (strstr(GDKerrbuf, "malloc") == NULL)
							res = LOG_EOF;
						else
							res = LOG_ERR;
					} else if (BUNappend(r, t, true) != GDK_SUCCEED)
						res = LOG_ERR;
					if (t != tv)
						GDKfree(t);
				}
			}
			GDKfree(hv);
		}
		if (tv != lg->buf)
			GDKfree(tv);

		if (res == LOG_OK) {
			if (tr_grow(tr) == GDK_SUCCEED) {
				tr->changes[tr->nr].type = l->flag;
				tr->changes[tr->nr].nr = l->nr;
				tr->changes[tr->nr].ht = ht;
				tr->changes[tr->nr].tt = tt;
				tr->changes[tr->nr].tpe = tpe;
				tr->changes[tr->nr].cid = id;
				if (name && (tr->changes[tr->nr].name = GDKstrdup(name)) == NULL) {
					logbat_destroy(b);
					BBPreclaim(uid);
					BBPreclaim(r);
					return LOG_ERR;
				}
				tr->changes[tr->nr].b = r;
				tr->changes[tr->nr].uid = uid;
				tr->nr++;
			} else {
				res = LOG_ERR;
			}
		}
	} else {
		/* bat missing ERROR or ignore ? currently error. */
		res = LOG_ERR;
	}
	logbat_destroy(b);
	return res;
}

static gdk_return
la_bat_updates(logger *lg, logaction *la)
{
	log_bid bid = logger_find_bat(lg, la->name, la->tpe, la->cid);
	BAT *b;

	if (bid == 0)
		return GDK_SUCCEED; /* ignore bats no longer in the catalog */

	/* do we need to skip these old updates */
	if (avoid_snapshot(lg, bid))
		return GDK_SUCCEED;

	b = BATdescriptor(bid);
	if (b == NULL)
		return GDK_FAIL;
	if (la->type == LOG_INSERT) {
		if (BATappend(b, la->b, NULL, true) != GDK_SUCCEED) {
			logbat_destroy(b);
			return GDK_FAIL;
		}
	} else if (la->type == LOG_UPDATE) {
		BATiter vi = bat_iterator(la->b);
		BUN p, q;

		BATloop(la->b, p, q) {
			oid h = BUNtoid(la->uid, p);
			const void *t = BUNtail(vi, p);

			if (h < b->hseqbase || h >= b->hseqbase + BATcount(b)) {
				/* if value doesn't exist, insert it;
				 * if b void headed, maintain that by
				 * inserting nils */
				if (b->batCount == 0 && !is_oid_nil(h))
					b->hseqbase = h;
				if (!is_oid_nil(b->hseqbase) && !is_oid_nil(h)) {
					const void *tv = ATOMnilptr(b->ttype);

					while (b->hseqbase + b->batCount < h) {
						if (BUNappend(b, tv, true) != GDK_SUCCEED) {
							logbat_destroy(b);
							return GDK_FAIL;
						}
					}
				}
				if (BUNappend(b, t, true) != GDK_SUCCEED) {
					logbat_destroy(b);
					return GDK_FAIL;
				}
			} else {
				if (BUNreplace(b, h, t, true) != GDK_SUCCEED) {
					logbat_destroy(b);
					return GDK_FAIL;
				}
			}
		}
	}
	logbat_destroy(b);
	return GDK_SUCCEED;
}

static log_return
log_read_destroy(logger *lg, trans *tr, char *name, char tpe, oid id)
{
	(void) lg;
	assert(!lg->inmemory);
	if (tr_grow(tr) == GDK_SUCCEED) {
		tr->changes[tr->nr].type = LOG_DESTROY;
		tr->changes[tr->nr].tpe = tpe;
		tr->changes[tr->nr].cid = id;
		if (name && (tr->changes[tr->nr].name = GDKstrdup(name)) == NULL)
			return LOG_ERR;
		tr->nr++;
	}
	return LOG_OK;
}

static gdk_return
la_bat_destroy(logger *lg, logaction *la)
{
	log_bid bid = logger_find_bat(lg, la->name, la->tpe, la->cid);

	if (bid) {
		BUN p;

		if (logger_del_bat(lg, bid) != GDK_SUCCEED)
			return GDK_FAIL;

		if ((p = log_find(lg->snapshots_bid, lg->dsnapshots, bid)) != BUN_NONE) {
			oid pos = (oid) p;
#ifndef NDEBUG
			assert(BBP_desc(bid)->batRole == PERSISTENT);
			assert(0 <= BBP_desc(bid)->theap.farmid && BBP_desc(bid)->theap.farmid < MAXFARMS);
			assert(BBPfarms[BBP_desc(bid)->theap.farmid].roles & (1 << PERSISTENT));
			if (BBP_desc(bid)->tvheap) {
				assert(0 <= BBP_desc(bid)->tvheap->farmid && BBP_desc(bid)->tvheap->farmid < MAXFARMS);
				assert(BBPfarms[BBP_desc(bid)->tvheap->farmid].roles & (1 << PERSISTENT));
			}
#endif
			if (BUNappend(lg->dsnapshots, &pos, false) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
}

static log_return
log_read_create(logger *lg, trans *tr, char *name, char tpe, oid id)
{
	char *buf = log_read_string(lg);
	int ht, tt;
	char *ha, *ta;

	assert(!lg->inmemory);
	if (lg->debug & 1)
		fprintf(stderr, "#log_read_create %s\n", name);

	if (buf == NULL)
		return LOG_EOF;
	if (buf == (char *) -1)
		return LOG_ERR;
	ha = buf;
	ta = strchr(buf, ',');
	if (ta == NULL) {
		TRC_CRITICAL(GDK, "inconsistent data read\n");
		return LOG_ERR;
	}
	*ta++ = 0;		/* skip over , */
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
	GDKfree(buf);
	if (tr_grow(tr) == GDK_SUCCEED) {
		tr->changes[tr->nr].type = LOG_CREATE;
		tr->changes[tr->nr].ht = ht;
		tr->changes[tr->nr].tt = tt;
		tr->changes[tr->nr].tpe = tpe;
		tr->changes[tr->nr].cid = id;
		if ((tr->changes[tr->nr].name = GDKstrdup(name)) == NULL)
			return LOG_ERR;
		tr->changes[tr->nr].b = NULL;
		tr->nr++;
	}

	return LOG_OK;
}

static gdk_return
la_bat_create(logger *lg, logaction *la)
{
	int tt = (la->tt < 0) ? TYPE_void : la->tt;
	BAT *b;

	/* formerly head column type, should be void */
	assert(((la->ht < 0) ? TYPE_void : la->ht) == TYPE_void);
	if ((b = COLnew(0, tt, BATSIZE, PERSISTENT)) == NULL)
		return GDK_FAIL;

	if (la->tt < 0)
		BATtseqbase(b, 0);

	if (BATsetaccess(b, BAT_READ) != GDK_SUCCEED ||
	    logger_add_bat(lg, b, la->name, la->tpe, la->cid) != GDK_SUCCEED)
		return GDK_FAIL;
	logbat_destroy(b);
	return GDK_SUCCEED;
}

static log_return
log_read_use(logger *lg, trans *tr, logformat *l, char *name, char tpe, oid id)
{
	(void) lg;

	assert(!lg->inmemory);
	if (tr_grow(tr) != GDK_SUCCEED)
		return LOG_ERR;
	tr->changes[tr->nr].type = LOG_USE;
	tr->changes[tr->nr].nr = l->nr;
	tr->changes[tr->nr].tpe = tpe;
	tr->changes[tr->nr].cid = id;
	if ((tr->changes[tr->nr].name = GDKstrdup(name)) == NULL)
		return LOG_ERR;
	tr->changes[tr->nr].b = NULL;
	tr->nr++;
	return LOG_OK;
}

static gdk_return
la_bat_use(logger *lg, logaction *la)
{
	log_bid bid = (log_bid) la->nr;
	BAT *b = BATdescriptor(bid);
	BUN p;

	assert(la->nr <= (lng) INT_MAX);
	if (b == NULL) {
		GDKerror("logger: could not use bat (%d) for %s\n", (int) bid, NAME(la->name, la->tpe, la->cid));
		return GDK_FAIL;
	}
	if (logger_add_bat(lg, b, la->name, la->tpe, la->cid) != GDK_SUCCEED)
		goto bailout;
#ifndef NDEBUG
	assert(b->batRole == PERSISTENT);
	assert(0 <= b->theap.farmid && b->theap.farmid < MAXFARMS);
	assert(BBPfarms[b->theap.farmid].roles & (1 << PERSISTENT));
	if (b->tvheap) {
		assert(0 <= b->tvheap->farmid && b->tvheap->farmid < MAXFARMS);
		assert(BBPfarms[b->tvheap->farmid].roles & (1 << PERSISTENT));
	}
#endif
	if ((p = log_find(lg->snapshots_bid, lg->dsnapshots, b->batCacheid)) != BUN_NONE &&
	    p >= lg->snapshots_bid->batInserted) {
		if (BUNinplace(lg->snapshots_tid, p, &lg->tid, false) != GDK_SUCCEED)
			goto bailout;
	} else {
		if (p != BUN_NONE) {
			oid pos = p;
			if (BUNappend(lg->dsnapshots, &pos, false) != GDK_SUCCEED)
				goto bailout;
		}
		/* move to the dirty new part of the snapshots list,
		 * new snapshots will get flushed to disk */
		if (BUNappend(lg->snapshots_bid, &b->batCacheid, false) != GDK_SUCCEED ||
		    BUNappend(lg->snapshots_tid, &lg->tid, false) != GDK_SUCCEED)
			goto bailout;
	}
	logbat_destroy(b);
	return GDK_SUCCEED;

  bailout:
	logbat_destroy(b);
	return GDK_FAIL;
}


#define TR_SIZE		1024

static trans *
tr_create(trans *tr, int tid)
{
	trans *ntr = GDKmalloc(sizeof(trans));

	if (ntr == NULL)
		return NULL;
	ntr->tid = tid;
	ntr->sz = TR_SIZE;
	ntr->nr = 0;
	ntr->changes = GDKmalloc(sizeof(logaction) * TR_SIZE);
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
	if (t == NULL)
		return NULL;	/* BAD missing transaction */
	if (t == tr)
		return tr;
	if (t->tr)		/* get this tid out of the list */
		p->tr = t->tr;
	t->tr = tr;		/* and move it to the front */
	return t;
}

static gdk_return
la_apply(logger *lg, logaction *c)
{
	gdk_return ret = GDK_FAIL;

	switch (c->type) {
	case LOG_INSERT:
	case LOG_UPDATE:
		ret = la_bat_updates(lg, c);
		break;
	case LOG_CREATE:
		ret = la_bat_create(lg, c);
		break;
	case LOG_USE:
		ret = la_bat_use(lg, c);
		break;
	case LOG_DESTROY:
		ret = la_bat_destroy(lg, c);
		break;
	case LOG_CLEAR:
		ret = la_bat_clear(lg, c);
		break;
	default:
		assert(0);
	}
	lg->changes += (ret == GDK_SUCCEED);
	return ret;
}

static void
la_destroy(logaction *c)
{
	if (c->name)
		GDKfree(c->name);
	if (c->b)
		logbat_destroy(c->b);
}

static gdk_return
tr_grow(trans *tr)
{
	if (tr->nr == tr->sz) {
		logaction *changes;
		tr->sz <<= 1;
		changes = GDKrealloc(tr->changes, tr->sz * sizeof(logaction));
		if (changes == NULL)
			return GDK_FAIL;
		tr->changes = changes;
	}
	/* cleanup the next */
	tr->changes[tr->nr].name = NULL;
	tr->changes[tr->nr].b = NULL;
	return GDK_SUCCEED;
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
tr_abort(logger *lg, trans *tr)
{
	int i;

	if (lg->debug & 1)
		fprintf(stderr, "#tr_abort\n");

	for (i = 0; i < tr->nr; i++)
		la_destroy(&tr->changes[i]);
	return tr_destroy(tr);
}

static trans *
tr_commit(logger *lg, trans *tr)
{
	int i;

	if (lg->debug & 1)
		fprintf(stderr, "#tr_commit\n");

	for (i = 0; i < tr->nr; i++) {
		if (la_apply(lg, &tr->changes[i]) != GDK_SUCCEED) {
			do {
				tr = tr_abort(lg, tr);
			} while (tr != NULL);
			return (trans *) -1;
		}
		la_destroy(&tr->changes[i]);
	}
	return tr_destroy(tr);
}

#ifdef _MSC_VER
#define access(file, mode)	_access(file, mode)
#endif

static gdk_return
logger_open(logger *lg)
{
	int len;
	char id[BUFSIZ];
	char *filename;

	if (LOG_DISABLED(lg)) {
		lg->end = 0;
		if (lg->id) /* go back to last used id */
			lg->id--;
		return GDK_SUCCEED;
	}
	len = snprintf(id, sizeof(id), LLFMT, lg->id);
	if (len == -1 || len >= BUFSIZ) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		return GDK_FAIL;
	}
	if (!(filename = GDKfilepath(BBPselectfarm(PERSISTENT, 0, offheap), lg->dir, LOGFILE, id))) {
		TRC_CRITICAL(GDK, "allocation failure\n");
		return GDK_FAIL;
	}

	lg->log = open_wstream(filename);
	if (lg->log) {
		short byteorder = 1234;
		mnstr_write(lg->log, &byteorder, sizeof(byteorder), 1);
	}
	lg->end = 0;

	if (lg->log == NULL || mnstr_errnr(lg->log)) {
		TRC_CRITICAL(GDK, "creating %s failed\n", filename);
		GDKfree(filename);
		return GDK_FAIL;
	}
	GDKfree(filename);
	return GDK_SUCCEED;
}

static inline void
logger_close(logger *lg)
{
	if (!LOG_DISABLED(lg))
		close_stream(lg->log);
	lg->log = NULL;
}

static gdk_return
logger_readlog(logger *lg, char *filename, bool *filemissing)
{
	trans *tr = NULL;
	logformat l;
	log_return err = LOG_OK;
	time_t t0, t1;
	struct stat sb;
	int dbg = GDKdebug;
	int fd;

	assert(!lg->inmemory);
	GDKdebug &= ~(CHECKMASK|PROPMASK);

	if (lg->debug & 1) {
		fprintf(stderr, "#logger_readlog opening %s\n", filename);
	}

	lg->log = open_rstream(filename);

	/* if the file doesn't exist, there is nothing to be read back */
	if (lg->log == NULL || mnstr_errnr(lg->log)) {
		logger_close(lg);
		GDKdebug = dbg;
		*filemissing = true;
		return GDK_SUCCEED;
	}
	short byteorder;
	switch (mnstr_read(lg->log, &byteorder, sizeof(byteorder), 1)) {
	case -1:
		logger_close(lg);
		GDKdebug = dbg;
		return GDK_FAIL;
	case 0:
		/* empty file is ok */
		logger_close(lg);
		GDKdebug = dbg;
		return GDK_SUCCEED;
	case 1:
		/* if not empty, must start with correct byte order mark */
		assert(byteorder == 1234);
		break;
	}
	if ((fd = getFileNo(lg->log)) < 0 || fstat(fd, &sb) < 0) {
		TRC_CRITICAL(GDK, "fstat on opened file %s failed\n", filename);
		logger_close(lg);
		GDKdebug = dbg;
		/* If the file could be opened, but fstat fails,
		 * something weird is going on */
		return GDK_FAIL;
	}
	t0 = time(NULL);
	if (lg->debug & 1) {
		printf("# Start reading the write-ahead log '%s'\n", filename);
		fflush(stdout);
	}
	while (err == LOG_OK && log_read_format(lg, &l)) {
		char *name = NULL;
		char tpe;
		oid id;

		t1 = time(NULL);
		if (t1 - t0 > 10) {
			lng fpos;
			t0 = t1;
			/* not more than once every 10 seconds */
			fpos = (lng) getfilepos(getFile(lg->log));
			if (fpos >= 0) {
				printf("# still reading write-ahead log \"%s\" (%d%% done)\n", filename, (int) ((fpos * 100 + 50) / sb.st_size));
				fflush(stdout);
			}
		}
		if ((l.flag >= LOG_INSERT && l.flag <= LOG_CLEAR) || l.flag == LOG_CREATE_ID || l.flag == LOG_USE_ID) {
			name = log_read_string(lg);

			if (name == NULL) {
				err = LOG_EOF;
				break;
			}
			if (name == (char *) -1) {
				err = LOG_ERR;
				break;
			}
		}
		if (lg->debug & 1) {
			fprintf(stderr, "#logger_readlog: ");
			if (l.flag > 0 &&
			    l.flag < (char) (sizeof(log_commands) / sizeof(log_commands[0])))
				fprintf(stderr, "%s", log_commands[(int) l.flag]);
			else
				fprintf(stderr, "%d", l.flag);
			fprintf(stderr, " %d " LLFMT, l.tid, l.nr);
			if (name)
				fprintf(stderr, " %s", name);
			fprintf(stderr, "\n");
		}
		/* find proper transaction record */
		if (l.flag != LOG_START)
			tr = tr_find(tr, l.tid);
		/* the functions we call here can succeed (LOG_OK),
		 * but they can also fail for two different reasons:
		 * they can run out of input (LOG_EOF -- this is not
		 * serious, we just abort the remaining transactions),
		 * or some malloc or BAT update fails (LOG_ERR -- this
		 * is serious, we must abort the complete process);
		 * the latter failure causes the current function to
		 * return GDK_FAIL */
		switch (l.flag) {
		case LOG_START:
			assert(l.nr <= (lng) INT_MAX);
			if (l.nr > lg->tid)
				lg->tid = (int)l.nr;
			if ((tr = tr_create(tr, (int)l.nr)) == NULL) {
				err = LOG_ERR;
				break;
			}
			if (lg->debug & 1)
				fprintf(stderr, "#logger tstart %d\n", tr->tid);
			break;
		case LOG_END:
			if (tr == NULL)
				err = LOG_EOF;
			else if (l.tid != l.nr)	/* abort record */
				tr = tr_abort(lg, tr);
			else
				tr = tr_commit(lg, tr);
			break;
		case LOG_SEQ:
			err = log_read_seq(lg, &l);
			break;
		case LOG_INSERT:
		case LOG_UPDATE:
			if (name == NULL || tr == NULL)
				err = LOG_EOF;
			else
				err = log_read_updates(lg, tr, &l, name, 0, 0, 0);
			break;
		case LOG_INSERT_ID:
		case LOG_UPDATE_ID:
		case LOG_UPDATE_PAX: {
			int pax = (l.flag == LOG_UPDATE_PAX);
			l.flag = (l.flag == LOG_INSERT_ID)?LOG_INSERT:LOG_UPDATE;
			if (log_read_id(lg, &tpe, &id) != LOG_OK)
				err = LOG_ERR;
			else
				err = log_read_updates(lg, tr, &l, name, tpe, id, pax);
		} 	break;
		case LOG_CREATE:
			if (name == NULL || tr == NULL)
				err = LOG_EOF;
			else
				err = log_read_create(lg, tr, name, 0, 0);
			break;
		case LOG_CREATE_ID:
			l.flag = LOG_CREATE;
			if (tr == NULL || log_read_id(lg, &tpe, &id) != LOG_OK)
				err = LOG_EOF;
			else
				err = log_read_create(lg, tr, name, tpe, id);
			break;
		case LOG_USE:
			if (name == NULL || tr == NULL)
				err = LOG_EOF;
			else
				err = log_read_use(lg, tr, &l, name, 0, 0);
			break;
		case LOG_USE_ID:
			l.flag = LOG_USE;
			if (tr == NULL || log_read_id(lg, &tpe, &id) != LOG_OK)
				err = LOG_EOF;
			else
				err = log_read_use(lg, tr, &l, name, tpe, id);
			break;
		case LOG_DESTROY:
			if (name == NULL || tr == NULL)
				err = LOG_EOF;
			else
				err = log_read_destroy(lg, tr, name, 0, 0);
			break;
		case LOG_DESTROY_ID:
			l.flag = LOG_DESTROY;
			if (tr == NULL || log_read_id(lg, &tpe, &id) != LOG_OK)
				err = LOG_EOF;
			else
				err = log_read_destroy(lg, tr, name, tpe, id);
			break;
		case LOG_CLEAR:
			if (name == NULL || tr == NULL)
				err = LOG_EOF;
			else
				err = log_read_clear(lg, tr, name, 0, 0);
			break;
		case LOG_CLEAR_ID:
			l.flag = LOG_CLEAR;
			if (tr == NULL || log_read_id(lg, &tpe, &id) != LOG_OK)
				err = LOG_EOF;
			else
				err = log_read_clear(lg, tr, name, tpe, id);
			break;
		case 0:
			break;
		default:
			err = LOG_ERR;
		}
		if (name)
			GDKfree(name);
		if (tr == (trans *) -1) {
			err = LOG_ERR;
			tr = NULL;
			break;
		}
	}
	logger_close(lg);

	/* remaining transactions are not committed, ie abort */
	while (tr)
		tr = tr_abort(lg, tr);
	if (lg->debug & 1) {
		printf("# Finished reading the write-ahead log '%s'\n", filename);
		fflush(stdout);
	}
	GDKdebug = dbg;
	/* we cannot distinguish errors from incomplete transactions
	 * (even if we would log aborts in the logs). So we simply
	 * abort and move to the next log file */
	return err == LOG_ERR ? GDK_FAIL : GDK_SUCCEED;
}

/*
 * The log files are incrementally numbered, starting from 2. They are
 * processed in the same sequence.
 */
static gdk_return
logger_readlogs(logger *lg, FILE *fp, char *filename)
{
	gdk_return res = GDK_SUCCEED;
	char id[BUFSIZ];
	int len;

	assert(!lg->inmemory);
	if (lg->debug & 1) {
		fprintf(stderr, "#logger_readlogs logger id is " LLFMT "\n", lg->id);
	}

	if (fgets(id, sizeof(id), fp) != NULL) {
		char log_filename[FILENAME_MAX];
		lng lid = strtoll(id, NULL, 10);

		if (lg->debug & 1) {
			fprintf(stderr, "#logger_readlogs last logger id written in %s is " LLFMT "\n", filename, lid);
		}

		if (lid >= lg->id) {
			bool filemissing = false;

			lg->id = lid;
			while (res == GDK_SUCCEED && !filemissing) {
				len = snprintf(log_filename, sizeof(log_filename), "%s." LLFMT, filename, lg->id);
				if (len == -1 || len >= FILENAME_MAX)
					GDKerror("Logger filename path is too large\n");
				res = logger_readlog(lg, log_filename, &filemissing);
				if (!filemissing)
					lg->id++;
			}
		} else {
			bool filemissing = false;
			while (lid >= lg->id && res == GDK_SUCCEED) {
				len = snprintf(log_filename, sizeof(log_filename), "%s." LLFMT, filename, lg->id);
				if (len == -1 || len >= FILENAME_MAX)
					GDKerror("Logger filename path is too large\n");
				res = logger_readlog(lg, log_filename, &filemissing);
				/* Increment the id only at the end,
				 * since we want to re-read the last
				 * file.  That is because last time we
				 * read it, it was empty, since the
				 * logger creates empty files and
				 * fills them in later. */
				lg->id++;
			}
			if (lid < lg->id) {
				lg->id = lid;
			}
		}
	}
	return res;
}

static gdk_return
logger_commit(logger *lg)
{
	if (lg->debug & 1)
		fprintf(stderr, "#logger_commit\n");

	/* cleanup old snapshots */
	if (BATcount(lg->snapshots_bid)) {
		if (BATclear(lg->snapshots_bid, true) != GDK_SUCCEED ||
		    BATclear(lg->snapshots_tid, true) != GDK_SUCCEED ||
		    BATclear(lg->dsnapshots, true) != GDK_SUCCEED)
			return GDK_FAIL;
		BATcommit(lg->snapshots_bid);
		BATcommit(lg->snapshots_tid);
		BATcommit(lg->dsnapshots);
	}
	return bm_commit(lg);
}

static gdk_return
check_version(logger *lg, FILE *fp)
{
	int version = 0;

	assert(!lg->inmemory);
	if (fscanf(fp, "%6d", &version) != 1) {
		GDKerror("Could not read the version number from the file '%s/log'.\n",
			 lg->dir);

		return GDK_FAIL;
	}
	if (version != lg->version) {
		if (lg->prefuncp == NULL ||
		    (*lg->prefuncp)(version, lg->version) != GDK_SUCCEED) {
			GDKerror("Incompatible database version %06d, "
				 "this server supports version %06d.\n%s",
				 version, lg->version,
				 version < lg->version ? "Maybe you need to upgrade to an intermediate release first.\n" : "");
			return GDK_FAIL;
		}
	} else {
		lg->postfuncp = NULL;	 /* don't call */
	}
	if (fgetc(fp) != '\n' ||	 /* skip \n */
	    fgetc(fp) != '\n') {	 /* skip \n */
		GDKerror("Badly formatted log file");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static BAT *
bm_tids(BAT *b, BAT *d)
{
	BUN sz = BATcount(b);
	BAT *tids = BATdense(0, 0, sz);

	if (tids == NULL)
		return NULL;

	if (BATcount(d)) {
		BAT *diff = BATdiff(tids, d, NULL, NULL, false, false, BUN_NONE);
		logbat_destroy(tids);
		tids = diff;
	}
	return tids;
}


static gdk_return
logger_switch_bat(BAT *old, BAT *new, const char *fn, const char *name)
{
	int len;
	char bak[BUFSIZ];

	if (BATmode(old, true) != GDK_SUCCEED) {
		GDKerror("Logger_new: cannot convert old %s to transient", name);
		return GDK_FAIL;
	}
	len = snprintf(bak, sizeof(bak), "tmp_%o", (unsigned) old->batCacheid);
	if (len == -1 || len >= BUFSIZ) {
		GDKerror("Logger_new: filename is too large");
		return GDK_FAIL;
	}
	if (BBPrename(old->batCacheid, bak) != 0) {
		return GDK_FAIL;
	}
	strconcat_len(bak, sizeof(bak), fn, "_", name, NULL);
	if (BBPrename(new->batCacheid, bak) != 0) {
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static gdk_return
bm_subcommit(logger *lg, BAT *list_bid, BAT *list_nme, BAT *catalog_bid, BAT *catalog_nme, BAT *catalog_tpe, BAT *catalog_oid, BAT *dcatalog, BAT *extra, int debug)
{
	BUN p, q;
	BUN nn = 13 + BATcount(list_bid) + (extra ? BATcount(extra) : 0);
	bat *n = GDKmalloc(sizeof(bat) * nn);
	int i = 0;
	BATiter iter = (list_nme)?bat_iterator(list_nme):bat_iterator(list_bid);
	gdk_return res;
	const log_bid *bids;

	if (n == NULL)
		return GDK_FAIL;

	n[i++] = 0;		/* n[0] is not used */
	bids = (const log_bid *) Tloc(list_bid, 0);
	BATloop(list_bid, p, q) {
		bat col = bids[p];
		oid pos = p;

		if (list_bid == catalog_bid && BUNfnd(dcatalog, &pos) != BUN_NONE)
			continue;
		if (debug & 1)
			fprintf(stderr, "#commit new %s (%d) %s\n",
				BBPname(col), col,
				(list_bid == catalog_bid) ? (char *) BUNtvar(iter, p) : "snapshot");
		assert(col);
		n[i++] = col;
	}
	if (extra) {
		iter = bat_iterator(extra);
		BATloop(extra, p, q) {
			str name = (str) BUNtvar(iter, p);

			if (debug & 1)
				fprintf(stderr, "#commit extra %s %s\n",
					name,
					(list_bid == catalog_bid) ? (char *) BUNtvar(iter, p) : "snapshot");
			assert(BBPindex(name));
			n[i++] = BBPindex(name);
		}
	}
	/* now commit catalog, so it's also up to date on disk */
	n[i++] = catalog_bid->batCacheid;
	n[i++] = catalog_nme->batCacheid;
	if (catalog_tpe) {
		n[i++] = catalog_tpe->batCacheid;
		n[i++] = catalog_oid->batCacheid;
	}
	n[i++] = dcatalog->batCacheid;

	if (BATcount(dcatalog) > 1024 &&
	    catalog_bid == list_bid &&
	    catalog_nme == list_nme &&
	    lg->catalog_bid == catalog_bid) {
		BAT *bids, *nmes, *tids, *tpes, *oids;

		tids = bm_tids(catalog_bid, dcatalog);
		if (tids == NULL) {
			GDKfree(n);
			return GDK_FAIL;
		}
		bids = logbat_new(TYPE_int, BATcount(tids), PERSISTENT);
		nmes = logbat_new(TYPE_str, BATcount(tids), PERSISTENT);
		tpes = logbat_new(TYPE_bte, BATcount(tids), PERSISTENT);
		oids = logbat_new(TYPE_lng, BATcount(tids), PERSISTENT);

		if (bids == NULL || nmes == NULL || tpes == NULL || oids == NULL) {
			logbat_destroy(tids);
			logbat_destroy(bids);
			logbat_destroy(nmes);
			logbat_destroy(tpes);
			logbat_destroy(oids);
			GDKfree(n);
			return GDK_FAIL;
		}

		if (BATappend(bids, catalog_bid, tids, true) != GDK_SUCCEED ||
		    BATappend(nmes, catalog_nme, tids, true) != GDK_SUCCEED ||
		    BATappend(tpes, catalog_tpe, tids, true) != GDK_SUCCEED ||
		    BATappend(oids, catalog_oid, tids, true) != GDK_SUCCEED) {
			logbat_destroy(tids);
			logbat_destroy(bids);
			logbat_destroy(nmes);
			logbat_destroy(tpes);
			logbat_destroy(oids);
			GDKfree(n);
			return GDK_FAIL;
		}
		logbat_destroy(tids);
		BATclear(dcatalog, true);

		if (logger_switch_bat(catalog_bid, bids, lg->fn, "catalog_bid") != GDK_SUCCEED ||
		    logger_switch_bat(catalog_nme, nmes, lg->fn, "catalog_nme") != GDK_SUCCEED ||
		    logger_switch_bat(catalog_tpe, tpes, lg->fn, "catalog_tpe") != GDK_SUCCEED ||
		    logger_switch_bat(catalog_oid, oids, lg->fn, "catalog_oid") != GDK_SUCCEED) {
			logbat_destroy(bids);
			logbat_destroy(nmes);
			GDKfree(n);
			return GDK_FAIL;
		}
		n[i++] = bids->batCacheid;
		n[i++] = nmes->batCacheid;
		n[i++] = tpes->batCacheid;
		n[i++] = oids->batCacheid;

		logbat_destroy(lg->catalog_bid);
		logbat_destroy(lg->catalog_nme);
		logbat_destroy(lg->catalog_tpe);
		logbat_destroy(lg->catalog_oid);

		lg->catalog_bid = catalog_bid = bids;
		lg->catalog_nme = catalog_nme = nmes;
		lg->catalog_tpe = catalog_tpe = tpes;
		lg->catalog_oid = catalog_oid = oids;
	}
	if (lg->seqs_id && list_nme) {
		n[i++] = lg->seqs_id->batCacheid;
		n[i++] = lg->seqs_val->batCacheid;
		n[i++] = lg->dseqs->batCacheid;
	}
	if (list_nme && lg->seqs_id && BATcount(lg->dseqs) > (BATcount(lg->seqs_id)/2)) {
		BAT *tids, *ids, *vals;

		tids = bm_tids(lg->seqs_id, lg->dseqs);
		if (tids == NULL) {
			GDKfree(n);
			return GDK_FAIL;
		}
		ids = logbat_new(TYPE_int, BATcount(tids), PERSISTENT);
		vals = logbat_new(TYPE_lng, BATcount(tids), PERSISTENT);

		if (ids == NULL || vals == NULL) {
			logbat_destroy(tids);
			logbat_destroy(ids);
			logbat_destroy(vals);
			GDKfree(n);
			return GDK_FAIL;
		}

		if (BATappend(ids, lg->seqs_id, tids, true) != GDK_SUCCEED ||
		    BATappend(vals, lg->seqs_val, tids, true) != GDK_SUCCEED) {
			logbat_destroy(tids);
			logbat_destroy(ids);
			logbat_destroy(vals);
			GDKfree(n);
			return GDK_FAIL;
		}
		logbat_destroy(tids);
		BATclear(lg->dseqs, true);

		if (logger_switch_bat(lg->seqs_id, ids, lg->fn, "seqs_id") != GDK_SUCCEED ||
		    logger_switch_bat(lg->seqs_val, vals, lg->fn, "seqs_val") != GDK_SUCCEED) {
			logbat_destroy(ids);
			logbat_destroy(vals);
			GDKfree(n);
			return GDK_FAIL;
		}
		n[i++] = ids->batCacheid;
		n[i++] = vals->batCacheid;
		n[i++] = lg->dseqs->batCacheid;

		logbat_destroy(lg->seqs_id);
		logbat_destroy(lg->seqs_val);

		lg->seqs_id = ids;
		lg->seqs_val = vals;
	}

	assert((BUN) i <= nn);
	BATcommit(catalog_bid);
	BATcommit(catalog_nme);
	if (catalog_tpe) {
		BATcommit(catalog_tpe);
		BATcommit(catalog_oid);
	}
	BATcommit(dcatalog);
	res = TMsubcommit_list(n, i);
	GDKfree(n);
	if (res != GDK_SUCCEED)
		TRC_CRITICAL(GDK, "commit failed\n");
	return res;
}

/* Load data from the logger logdir
 * Initialize new directories and catalog files if none are present,
 * unless running in read-only mode
 * Load data and persist it in the BATs */
static gdk_return
logger_load(int debug, const char *fn, char filename[FILENAME_MAX], logger *lg)
{
	int len;
	FILE *fp = NULL;
	char bak[FILENAME_MAX];
	str filenamestr = NULL;
	log_bid snapshots_bid = 0;
	bat catalog_bid, catalog_nme, catalog_tpe, catalog_oid, dcatalog, bid;
	int farmid = BBPselectfarm(PERSISTENT, 0, offheap);
	bool needcommit = false;
	int dbg = GDKdebug;

	if (!LOG_DISABLED(lg)) {
		if ((filenamestr = GDKfilepath(farmid, lg->dir, LOGFILE, NULL)) == NULL)
			goto error;
		len = snprintf(filename, FILENAME_MAX, "%s", filenamestr);
		if (len == -1 || len >= FILENAME_MAX) {
			GDKfree(filenamestr);
			GDKerror("Logger filename path is too large\n");
			goto error;
		}
		len = snprintf(bak, sizeof(bak), "%s.bak", filename);
		GDKfree(filenamestr);
		if (len == -1 || len >= FILENAME_MAX) {
			GDKerror("Logger filename path is too large\n");
			goto error;
		}
	}

	lg->catalog_bid = NULL;
	lg->catalog_nme = NULL;
	lg->catalog_tpe = NULL;
	lg->catalog_oid = NULL;
	lg->dcatalog = NULL;
	lg->snapshots_bid = NULL;
	lg->snapshots_tid = NULL;
	lg->dsnapshots = NULL;
	lg->freed = NULL;
	lg->seqs_id = NULL;
	lg->seqs_val = NULL;
	lg->dseqs = NULL;

	if (!LOG_DISABLED(lg)) {
		/* try to open logfile backup, or failing that, the file
		 * itself. we need to know whether this file exists when
		 * checking the database consistency later on */
		if ((fp = fopen(bak, "r")) != NULL) {
			fclose(fp);
			fp = NULL;
			if (GDKunlink(farmid, lg->dir, LOGFILE, NULL) != GDK_SUCCEED ||
			    GDKmove(farmid, lg->dir, LOGFILE, "bak", lg->dir, LOGFILE, NULL) != GDK_SUCCEED)
				goto error;
		} else if (errno != ENOENT) {
			GDKsyserror("open %s failed", bak);
			goto error;
		}
		fp = fopen(filename, "r");
		if (fp == NULL && errno != ENOENT) {
			GDKsyserror("open %s failed", filename);
			goto error;
		}
	}

	strconcat_len(bak, sizeof(bak), fn, "_catalog", NULL);
	bid = BBPindex(bak);

	strconcat_len(bak, sizeof(bak), fn, "_catalog_bid", NULL);
	catalog_bid = BBPindex(bak);

	if (bid != 0 && catalog_bid == 0) {
		GDKerror("ancient database, please upgrade "
			 "first to Jan2014 (11.17.X) release");
		goto error;
	}

	/* this is intentional - if catalog_bid is 0, force it to find
	 * the persistent catalog */
	if (catalog_bid == 0) {
		/* catalog does not exist, so the log file also
		 * shouldn't exist */
		if (fp != NULL) {
			GDKerror("there is no logger catalog, "
				 "but there is a log file. "
				 "Are you sure you are using the correct "
				 "combination of database "
				 "(--dbpath) and log directory "
				 "(--set %s_logdir)?\n", fn);
			goto error;
		}

		lg->catalog_bid = logbat_new(TYPE_int, BATSIZE, PERSISTENT);
		lg->catalog_nme = logbat_new(TYPE_str, BATSIZE, PERSISTENT);
		lg->catalog_tpe = logbat_new(TYPE_bte, BATSIZE, PERSISTENT);
		lg->catalog_oid = logbat_new(TYPE_lng, BATSIZE, PERSISTENT);
		lg->dcatalog = logbat_new(TYPE_oid, BATSIZE, PERSISTENT);
		if (lg->catalog_bid == NULL || lg->catalog_nme == NULL || lg->catalog_tpe == NULL || lg->catalog_oid == NULL || lg->dcatalog == NULL) {
			GDKerror("cannot create catalog bats");
			goto error;
		}
		if (debug & 1)
			fprintf(stderr, "#create %s catalog\n", fn);

		/* give the catalog bats names so we can find them
		 * next time */
		strconcat_len(bak, sizeof(bak), fn, "_catalog_bid", NULL);
		if (BBPrename(lg->catalog_bid->batCacheid, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_catalog_nme", NULL);
		if (BBPrename(lg->catalog_nme->batCacheid, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_catalog_tpe", NULL);
		if (BBPrename(lg->catalog_tpe->batCacheid, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_catalog_oid", NULL);
		if (BBPrename(lg->catalog_oid->batCacheid, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_dcatalog", NULL);
		if (BBPrename(lg->dcatalog->batCacheid, bak) < 0) {
			goto error;
		}

		if (!LOG_DISABLED(lg)) {
			if (GDKcreatedir(filename) != GDK_SUCCEED) {
				GDKerror("cannot create directory for log file %s\n",
					 filename);
				goto error;
			}
			if ((fp = fopen(filename, "w")) == NULL) {
				GDKsyserror("cannot create log file %s\n",
					    filename);
				goto error;
			}
			lg->id ++;
			if (fprintf(fp, "%06d\n\n" LLFMT "\n", lg->version, lg->id) < 0) {
				fclose(fp);
				remove(filename);
				GDKerror("writing log file %s failed",
					 filename);
				goto error;
			}
			if (fflush(fp) < 0 ||
			    (!(GDKdebug & NOSYNCMASK)
#if defined(_MSC_VER)
			     && _commit(_fileno(fp)) < 0
#elif defined(HAVE_FDATASYNC)
			     && fdatasync(fileno(fp)) < 0
#elif defined(HAVE_FSYNC)
			     && fsync(fileno(fp)) < 0
#endif
				    ) ||
			    fclose(fp) < 0) {
				remove(filename);
				GDKerror("closing log file %s failed",
					 filename);
				goto error;
			}
			fp = NULL;
		}

		BBPretain(lg->catalog_bid->batCacheid);
		BBPretain(lg->catalog_nme->batCacheid);
		BBPretain(lg->catalog_tpe->batCacheid);
		BBPretain(lg->catalog_oid->batCacheid);
		BBPretain(lg->dcatalog->batCacheid);

		if (bm_subcommit(lg, lg->catalog_bid, lg->catalog_nme, lg->catalog_bid, lg->catalog_nme, lg->catalog_tpe, lg->catalog_oid, lg->dcatalog, NULL, lg->debug) != GDK_SUCCEED) {
			/* cannot commit catalog, so remove log */
			remove(filename);
			BBPrelease(lg->catalog_bid->batCacheid);
			BBPrelease(lg->catalog_nme->batCacheid);
			BBPrelease(lg->catalog_tpe->batCacheid);
			BBPrelease(lg->catalog_oid->batCacheid);
			BBPrelease(lg->dcatalog->batCacheid);
			goto error;
		}
	} else {
		/* find the persistent catalog. As non persistent bats
		 * require a logical reference we also add a logical
		 * reference for the persistent bats */
		size_t i;
		BUN p, q;
		BAT *b = BATdescriptor(catalog_bid), *n, *t, *o, *d;

		assert(!lg->inmemory);
		if (b == NULL) {
			GDKerror("inconsistent database, catalog does not exist");
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_catalog_nme", NULL);
		catalog_nme = BBPindex(bak);
		n = BATdescriptor(catalog_nme);
		if (n == NULL) {
			BBPunfix(b->batCacheid);
			GDKerror("inconsistent database, catalog_nme does not exist");
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_catalog_tpe", NULL);
		catalog_tpe = BBPindex(bak);
		t = BATdescriptor(catalog_tpe);
		if (t == NULL) {
			t = logbat_new(TYPE_bte, BATSIZE, PERSISTENT);
			if (t == NULL
			    ||BBPrename(t->batCacheid, bak) < 0) {
				BBPunfix(b->batCacheid);
				BBPunfix(n->batCacheid);
				if (t)
					BBPunfix(t->batCacheid);
				GDKerror("inconsistent database, catalog_tpe does not exist");
				goto error;
			}
			for(i=0;i<BATcount(n); i++) {
				char zero = 0;
				if (BUNappend(t, &zero, false) != GDK_SUCCEED)
					goto error;
			}
			lg->with_ids = false;
		}

		strconcat_len(bak, sizeof(bak), fn, "_catalog_oid", NULL);
		catalog_oid = BBPindex(bak);
		o = BATdescriptor(catalog_oid);
		if (o == NULL) {
			o = logbat_new(TYPE_lng, BATSIZE, PERSISTENT);
			if (o == NULL
			    ||BBPrename(o->batCacheid, bak) < 0) {
				BBPunfix(b->batCacheid);
				BBPunfix(n->batCacheid);
				BBPunfix(t->batCacheid);
				if (o)
					BBPunfix(o->batCacheid);
				GDKerror("inconsistent database, catalog_oid does not exist");
				goto error;
			}
			for(i=0;i<BATcount(n); i++) {
				lng zero = 0;
				if (BUNappend(o, &zero, false) != GDK_SUCCEED)
					goto error;
			}
			lg->with_ids = false;
		}

		strconcat_len(bak, sizeof(bak), fn, "_dcatalog", NULL);
		dcatalog = BBPindex(bak);
		d = BATdescriptor(dcatalog);
		if (d == NULL) {
			/* older database: create dcatalog and convert
			 * catalog_bid and catalog_nme to
			 * dense-headed */
			d = logbat_new(TYPE_oid, BATSIZE, PERSISTENT);
			if (d == NULL) {
				GDKerror("Logger_new: cannot create dcatalog bat");
				BBPunfix(b->batCacheid);
				BBPunfix(n->batCacheid);
				BBPunfix(t->batCacheid);
				BBPunfix(o->batCacheid);
				goto error;
			}
			if (BBPrename(d->batCacheid, bak) < 0) {
				BBPunfix(b->batCacheid);
				BBPunfix(n->batCacheid);
				BBPunfix(t->batCacheid);
				BBPunfix(o->batCacheid);
				goto error;
			}
		}

		/* the catalog exists, and so should the log file */
		if (fp == NULL && !LOG_DISABLED(lg)) {
			GDKerror("there is a logger catalog, but no log file. "
				 "Are you sure you are using the correct combination of database "
				 "(--dbpath) and log directory (--set %s_logdir)? "
				 "If you have done a recent update of the server, it may be that your "
				 "logs are in an old location.  You should then either use "
				 "--set %s_logdir=<path to old log directory> or move the old log "
				 "directory to the new location (%s).\n",
				 fn, fn, lg->dir);
			BBPunfix(b->batCacheid);
			BBPunfix(n->batCacheid);
			BBPunfix(t->batCacheid);
			BBPunfix(o->batCacheid);
			BBPunfix(d->batCacheid);
			goto error;
		}
		lg->catalog_bid = b;
		lg->catalog_nme = n;
		lg->catalog_tpe = t;
		lg->catalog_oid = o;
		lg->dcatalog = d;
		BBPretain(lg->catalog_bid->batCacheid);
		BBPretain(lg->catalog_nme->batCacheid);
		BBPretain(lg->catalog_tpe->batCacheid);
		BBPretain(lg->catalog_oid->batCacheid);
		BBPretain(lg->dcatalog->batCacheid);
		const log_bid *bids = (const log_bid *) Tloc(b, 0);
		BATloop(b, p, q) {
			bat bid = bids[p];
			oid pos = p;

			if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE &&
			    BBPretain(bid) == 0 &&
			    BUNappend(lg->dcatalog, &pos, false) != GDK_SUCCEED)
				goto error;
		}
	}
	lg->freed = logbat_new(TYPE_int, 1, TRANSIENT);
	if (lg->freed == NULL) {
		GDKerror("Logger_new: failed to create freed bat");
		goto error;
	}
	strconcat_len(bak, sizeof(bak), fn, "_freed", NULL);
	if (BBPrename(lg->freed->batCacheid, bak) < 0) {
		goto error;
	}
	snapshots_bid = logger_find_bat(lg, "snapshots_bid", 0, 0);
	if (snapshots_bid == 0) {
		lg->snapshots_bid = logbat_new(TYPE_int, 1, PERSISTENT);
		lg->snapshots_tid = logbat_new(TYPE_int, 1, PERSISTENT);
		lg->dsnapshots = logbat_new(TYPE_oid, 1, PERSISTENT);
		if (lg->snapshots_bid == NULL ||
		    lg->snapshots_tid == NULL ||
		    lg->dsnapshots == NULL) {
			GDKerror("Logger_new: failed to create snapshots bats");
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_snapshots_bid", NULL);
		if (BBPrename(lg->snapshots_bid->batCacheid, bak) < 0) {
			goto error;
		}
		if (logger_add_bat(lg, lg->snapshots_bid, "snapshots_bid", 0, 0) != GDK_SUCCEED) {
			GDKerror("logger_add_bat for "
				 "%s failed", bak);
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_snapshots_tid", NULL);
		if (BBPrename(lg->snapshots_tid->batCacheid, bak) < 0) {
			goto error;
		}
		if (logger_add_bat(lg, lg->snapshots_tid, "snapshots_tid", 0, 0) != GDK_SUCCEED) {
			GDKerror("logger_add_bat for "
				 "%s failed", bak);
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_dsnapshots", NULL);
		if (BBPrename(lg->dsnapshots->batCacheid, bak) < 0) {
			goto error;
		}
		if (logger_add_bat(lg, lg->dsnapshots, "dsnapshots", 0, 0) != GDK_SUCCEED) {
			GDKerror("logger_add_bat for "
				 "%s failed", bak);
			goto error;
		}

		if (bm_subcommit(lg, lg->catalog_bid, lg->catalog_nme, lg->catalog_bid, lg->catalog_nme, lg->catalog_tpe, lg->catalog_oid, lg->dcatalog, NULL, lg->debug) != GDK_SUCCEED) {
			GDKerror("Logger_new: commit failed");
			goto error;
		}
	} else {
		bat snapshots_tid = logger_find_bat(lg, "snapshots_tid", 0, 0);
		bat dsnapshots = logger_find_bat(lg, "dsnapshots", 0, 0);

		GDKdebug &= ~CHECKMASK;
		lg->snapshots_bid = BATdescriptor(snapshots_bid);
		if (lg->snapshots_bid == NULL) {
			GDKerror("inconsistent database, snapshots_bid does not exist");
			goto error;
		}
		lg->snapshots_tid = BATdescriptor(snapshots_tid);
		if (lg->snapshots_tid == NULL) {
			GDKerror("inconsistent database, snapshots_tid does not exist");
			goto error;
		}
		GDKdebug = dbg;

		if (dsnapshots) {
			lg->dsnapshots = BATdescriptor(dsnapshots);
			if (lg->dsnapshots == NULL) {
				GDKerror("Logger_new: inconsistent database, snapshots_tid does not exist");
				goto error;
			}
		} else {
			lg->dsnapshots = logbat_new(TYPE_oid, 1, PERSISTENT);
			if (lg->dsnapshots == NULL) {
				GDKerror("Logger_new: cannot create dsnapshot bat");
				goto error;
			}
			strconcat_len(bak, sizeof(bak),
				      fn, "_dsnapshots", NULL);
			if (BBPrename(lg->dsnapshots->batCacheid, bak) < 0) {
				goto error;
			}
			if (logger_add_bat(lg, lg->dsnapshots, "dsnapshots", 0, 0) != GDK_SUCCEED) {
				GDKerror("logger_add_bat for "
					 "%s failed", bak);
				goto error;
			}
			needcommit = true;
		}
	}
	strconcat_len(bak, sizeof(bak), fn, "_seqs_id", NULL);
	if (BBPindex(bak)) {
		lg->seqs_id = BATdescriptor(BBPindex(bak));
		strconcat_len(bak, sizeof(bak), fn, "_seqs_val", NULL);
		lg->seqs_val = BATdescriptor(BBPindex(bak));
		strconcat_len(bak, sizeof(bak), fn, "_dseqs", NULL);
		lg->dseqs = BATdescriptor(BBPindex(bak));
	} else {
		lg->seqs_id = logbat_new(TYPE_int, 1, PERSISTENT);
		lg->seqs_val = logbat_new(TYPE_lng, 1, PERSISTENT);
		lg->dseqs = logbat_new(TYPE_oid, 1, PERSISTENT);
		if (lg->seqs_id == NULL ||
		    lg->seqs_val == NULL ||
		    lg->dseqs == NULL) {
			GDKerror("Logger_new: cannot create seqs bats");
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_seqs_id", NULL);
		if (BBPrename(lg->seqs_id->batCacheid, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_seqs_val", NULL);
		if (BBPrename(lg->seqs_val->batCacheid, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_dseqs", NULL);
		if (BBPrename(lg->dseqs->batCacheid, bak) < 0) {
			goto error;
		}
		needcommit = true;
	}
	GDKdebug &= ~CHECKMASK;
	if (needcommit && bm_commit(lg) != GDK_SUCCEED) {
		GDKerror("Logger_new: commit failed");
		goto error;
	}
	GDKdebug = dbg;

	if (fp != NULL) {
#ifdef GDKLIBRARY_OLDDATE
		char cvfile1[FILENAME_MAX];
#endif

		if (check_version(lg, fp) != GDK_SUCCEED) {
			goto error;
		}

#ifdef GDKLIBRARY_OLDDATE
		/* When a file *_date-convert exists in the
		 * database, it was left there by the BBP
		 * initialization code when it did a conversion of old
		 * style dates to new.  If the file exists, we first
		 * create a file called convert-date in the log
		 * directory and we write the current log ID into that
		 * file.  After this file is created, we delete the
		 * *_date-convert file in the database.  We then
		 * know that while reading the logs, we have to
		 * convert old style NILs to NaNs (this is indicated
		 * by setting the convert_date flag).  When we're
		 * done reading the logs, we remove the file and
		 * reset the flag.  If we get interrupted before we
		 * have written this file, the file in the database
		 * will still exist, so the next time we're started,
		 * BBPinit will not convert NILs (that was done before
		 * we got interrupted), but we will still know to
		 * convert the NILs ourselves.  If we get interrupted
		 * after we have deleted the file from the database,
		 * we check whether the file convert-date exists
		 * and if it contains the expected ID.  If it does, we
		 * again know that we have to convert.  If the ID is
		 * not what we expect, the conversion was apparently
		 * done already, and so we can delete the file. */

		{
			FILE *fp1;
			int len, curid;

			/* read the current log id without disturbing
			 * the file pointer */
#ifdef _MSC_VER
			/* work around bug in Visual Studio runtime:
			 * fgetpos may return incorrect value */
			if ((fp1 = fopen(filename, "r")) == NULL) {
				GDKsyserror("cannot open %s\n", filename);
				goto error;
			}
			if (fgets(bak, sizeof(bak), fp1) == NULL ||
			    fgets(bak, sizeof(bak), fp1) == NULL ||
			    fscanf(fp1, "%d", &curid) != 1) {
				fclose(fp1);
				goto error;
			}
			fclose(fp1);
#else
			fpos_t off;
			if (fgetpos(fp, &off) != 0)
				goto error; /* should never happen */
			if (fscanf(fp, "%d", &curid) != 1)
				curid = -1; /* shouldn't happen? */
			if (fsetpos(fp, &off) != 0)
				goto error; /* should never happen */
#endif
			len = snprintf(cvfile1, sizeof(cvfile1), "%sconvert-date",
				 lg->dir);
			if (len == -1 || len >= FILENAME_MAX) {
				GDKerror("Convert-date filename path is too large\n");
				goto error;
			}
			len = snprintf(bak, sizeof(bak), "%s_date-convert", fn);
			if (len == -1 || len >= FILENAME_MAX) {
				GDKerror("Convert-date filename path is too large\n");
				goto error;
			}

			if ((fp1 = GDKfileopen(0, NULL, bak, NULL, "r")) != NULL) {
				/* file indicating that we need to do
				 * an old to new date conversion exists;
				 * record the fact in case we get
				 * interrupted, and set the flag so
				 * that we actually do what's asked */
				fclose(fp1);
				/* first create a versioned file using
				 * the current log id */
				if ((fp1 = GDKfileopen(farmid, NULL, cvfile1, NULL, "w")) == NULL ||
				    fprintf(fp1, "%d\n", curid) < 2 ||
				    fflush(fp1) != 0 || /* make sure it's save on disk */
#if defined(_MSC_VER)
				    _commit(_fileno(fp1)) < 0 ||
#elif defined(HAVE_FDATASYNC)
				    fdatasync(fileno(fp1)) < 0 ||
#elif defined(HAVE_FSYNC)
				    fsync(fileno(fp1)) < 0 ||
#endif
				    fclose(fp1) != 0) {
					GDKsyserror("failed to write %s\n", cvfile1);
					goto error;
				}
				/* then remove the unversioned file
				 * that gdk_bbp created (in this
				 * order!) */
				if (GDKunlink(0, NULL, bak, NULL) != GDK_SUCCEED) {
					GDKerror("failed to unlink %s\n", bak);
					goto error;
				}
				/* set the flag that we need to convert */
				lg->convert_date = true;
			} else if (errno != ENOENT) {
				GDKsyserror("opening file %s failed\n", bak);
				goto error;
			} else if ((fp1 = GDKfileopen(farmid, NULL, cvfile1, NULL, "r")) != NULL) {
				/* the versioned conversion file
				 * exists: check version */
				int newid;

				if (fscanf(fp1, "%d", &newid) == 1 &&
				    newid == curid) {
					/* versions match, we need to
					 * convert */
					lg->convert_date = true;
				}
				fclose(fp1);
				if (!lg->convert_date) {
					/* no conversion, so we can
					 * remove the versioned
					 * file */
					GDKunlink(0, NULL, cvfile1, NULL);
				}
			} else if (errno != ENOENT) {
				GDKsyserror("opening file %s failed\n", cvfile1);
				goto error;
			}
		}
#endif
		if (logger_readlogs(lg, fp, filename) != GDK_SUCCEED) {
			goto error;
		}
		fclose(fp);
		fp = NULL;
#ifdef GDKLIBRARY_OLDDATE
		if (lg->convert_date) {
			/* we converted, remove versioned file and
			 * reset conversion flag */
			GDKunlink(0, NULL, cvfile1, NULL);
			lg->convert_date = false;
		}
#endif
		if (lg->postfuncp && (*lg->postfuncp)(lg) != GDK_SUCCEED)
			goto error;

		/* done reading the log, revert to "normal" behavior */
		geomisoldversion = 0;
	}

	return GDK_SUCCEED;
  error:
	if (fp)
		fclose(fp);
	logbat_destroy(lg->catalog_bid);
	logbat_destroy(lg->catalog_nme);
	logbat_destroy(lg->catalog_tpe);
	logbat_destroy(lg->catalog_oid);
	logbat_destroy(lg->dcatalog);
	logbat_destroy(lg->snapshots_bid);
	logbat_destroy(lg->snapshots_tid);
	logbat_destroy(lg->dsnapshots);
	logbat_destroy(lg->freed);
	logbat_destroy(lg->seqs_id);
	logbat_destroy(lg->seqs_val);
	logbat_destroy(lg->dseqs);
	GDKfree(lg->fn);
	GDKfree(lg->dir);
	GDKfree(lg->local_dir);
	GDKfree(lg->buf);
	GDKfree(lg);
	return GDK_FAIL;
}

/* Initialize a new logger
 * It will load any data in the logdir and persist it in the BATs*/
static logger *
logger_new(int debug, const char *fn, const char *logdir, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp)
{
	int len;
	logger *lg;
	char filename[FILENAME_MAX];

	if (!GDKinmemory() && MT_path_absolute(logdir)) {
		TRC_CRITICAL(GDK, "logdir must be relative path\n");
		return NULL;
	}

	lg = GDKmalloc(sizeof(struct logger));
	if (lg == NULL) {
		TRC_CRITICAL(GDK, "allocating logger structure failed\n");
		return NULL;
	}

	lg->inmemory = GDKinmemory();
	lg->debug = debug;

	lg->changes = 0;
	lg->version = version;
	lg->with_ids = true;
	lg->id = 1;

	lg->tid = 0;
#ifdef GDKLIBRARY_OLDDATE
	lg->convert_date = false;
#endif

	len = snprintf(filename, sizeof(filename), "%s%c%s%c", logdir, DIR_SEP, fn, DIR_SEP);
	if (len == -1 || len >= FILENAME_MAX) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		GDKfree(lg);
		return NULL;
	}
	lg->fn = GDKstrdup(fn);
	lg->dir = GDKstrdup(filename);
	lg->bufsize = 64*1024;
	lg->buf = GDKmalloc(lg->bufsize);
	if (lg->fn == NULL || lg->dir == NULL || lg->buf == NULL) {
		TRC_CRITICAL(GDK, "strdup failed\n");
		GDKfree(lg->fn);
		GDKfree(lg->dir);
		GDKfree(lg->buf);
		GDKfree(lg);
		return NULL;
	}
	if (lg->debug & 1) {
		fprintf(stderr, "#logger_new dir set to %s\n", lg->dir);
	}
	lg->local_dir = NULL;

	lg->prefuncp = prefuncp;
	lg->postfuncp = postfuncp;
	lg->log = NULL;
	lg->end = 0;
	lg->catalog_bid = NULL;
	lg->catalog_nme = NULL;
	lg->catalog_tpe = NULL;
	lg->catalog_oid = NULL;
	lg->dcatalog = NULL;
	lg->snapshots_bid = NULL;
	lg->snapshots_tid = NULL;
	lg->dsnapshots = NULL;
	lg->seqs_id = NULL;
	lg->seqs_val = NULL;
	lg->dseqs = NULL;

	if (logger_load(debug, fn, filename, lg) == GDK_SUCCEED) {
		return lg;
	}
	return NULL;
}

/* Create a new logger */
logger *
logger_create(int debug, const char *fn, const char *logdir, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp)
{
	logger *lg;
	lg = logger_new(debug, fn, logdir, version, prefuncp, postfuncp);
	if (lg == NULL)
		return NULL;
	if (lg->debug & 1) {
		printf("# Started processing logs %s/%s version %d\n",fn,logdir,version);
		fflush(stdout);
	}
	if (logger_open(lg) != GDK_SUCCEED) {
		logger_destroy(lg);
		return NULL;
	}
	if (lg->debug & 1) {
		printf("# Finished processing logs %s/%s\n",fn,logdir);
	}
	if (GDKsetenv("recovery", "finished") != GDK_SUCCEED) {
		logger_destroy(lg);
		return NULL;
	}
	fflush(stdout);
	if (lg->changes &&
	    (logger_restart(lg) != GDK_SUCCEED ||
	     logger_cleanup(lg) != GDK_SUCCEED)) {
		logger_destroy(lg);
		return NULL;
	}
	return lg;
}

void
logger_destroy(logger *lg)
{
	if (lg->catalog_bid) {
		BUN p, q;
		BAT *b = lg->catalog_bid;

		if (logger_cleanup(lg) != GDK_SUCCEED)
			TRC_CRITICAL(GDK, "logger_cleanup failed\n");

		/* free resources */
		const log_bid *bids = (const log_bid *) Tloc(b, 0);
		BATloop(b, p, q) {
			bat bid = bids[p];
			oid pos = p;

			if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE)
				BBPrelease(bid);
		}

		BBPrelease(lg->catalog_bid->batCacheid);
		BBPrelease(lg->catalog_nme->batCacheid);
		BBPrelease(lg->catalog_tpe->batCacheid);
		BBPrelease(lg->catalog_oid->batCacheid);
		BBPrelease(lg->dcatalog->batCacheid);
		logbat_destroy(lg->catalog_bid);
		logbat_destroy(lg->catalog_nme);
		logbat_destroy(lg->catalog_tpe);
		logbat_destroy(lg->catalog_oid);
		logbat_destroy(lg->dcatalog);
		logbat_destroy(lg->freed);
	}
	GDKfree(lg->fn);
	GDKfree(lg->dir);
	logger_close(lg);
	GDKfree(lg);
}

gdk_return
logger_exit(logger *lg)
{
	FILE *fp;
	char filename[FILENAME_MAX];
	int len, farmid;

	if (LOG_DISABLED(lg)) {
		logger_close(lg);
		if (logger_commit(lg) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "logger_commit failed\n");
			return GDK_FAIL;
		}
		lg->changes = 0;
		return GDK_SUCCEED;
	}

	farmid = BBPselectfarm(PERSISTENT, 0, offheap);
	logger_close(lg);
	if (GDKmove(farmid, lg->dir, LOGFILE, NULL, lg->dir, LOGFILE, "bak") != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "rename %s to %s.bak in %s failed\n",
			LOGFILE, LOGFILE, lg->dir);
		return GDK_FAIL;
	}

	len = snprintf(filename, sizeof(filename), "%s%s", lg->dir, LOGFILE);
	if (len == -1 || len >= FILENAME_MAX) {
		TRC_CRITICAL(GDK, "logger filename path is too large\n");
		return GDK_FAIL;
	}
	if ((fp = GDKfileopen(farmid, NULL, filename, NULL, "w")) != NULL) {
		char ext[FILENAME_MAX];

		if (fprintf(fp, "%06d\n\n", lg->version) < 0) {
			(void) fclose(fp);
			TRC_CRITICAL(GDK, "write to %s failed\n", filename);
			return GDK_FAIL;
		}
		lg->id ++;

		if (logger_commit(lg) != GDK_SUCCEED) {
			(void) fclose(fp);
			TRC_CRITICAL(GDK, "logger_commit failed\n");
			return GDK_FAIL;
		}

		if (fprintf(fp, LLFMT "\n", lg->id) < 0) {
			(void) fclose(fp);
			TRC_CRITICAL(GDK, "write to %s failed\n", filename);
			return GDK_FAIL;
		}

		if (fflush(fp) < 0 ||
		    (!(GDKdebug & NOSYNCMASK)
#if defined(NATIVE_WIN32)
		     && _commit(_fileno(fp)) < 0
#elif defined(HAVE_FDATASYNC)
		     && fdatasync(fileno(fp)) < 0
#elif defined(HAVE_FSYNC)
		     && fsync(fileno(fp)) < 0
#endif
			    )) {
			(void) fclose(fp);
			TRC_CRITICAL(GDK, "flush of %s failed\n", filename);
			return GDK_FAIL;
		}
		if (fclose(fp) < 0) {
			TRC_CRITICAL(GDK, "flush of %s failed\n", filename);
			return GDK_FAIL;
		}

		/* atomic action, switch to new log, keep old for
		 * later cleanup actions */
		len = snprintf(ext, sizeof(ext), "bak-" LLFMT, lg->id);
		if (len == -1 || len >= FILENAME_MAX) {
			TRC_CRITICAL(GDK, "new logger filename path is too large\n");
			return GDK_FAIL;
		}

		if (GDKmove(farmid, lg->dir, LOGFILE, "bak", lg->dir, LOGFILE, ext) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "rename %s.bak to %s.%s failed\n",
				LOGFILE, LOGFILE, ext);
			return GDK_FAIL;
		}

		lg->changes = 0;
	} else {
		GDKsyserror("could not create %s\n", filename);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

gdk_return
logger_restart(logger *lg)
{
	if (logger_exit(lg) == GDK_SUCCEED &&
	    logger_open(lg) == GDK_SUCCEED)
		return GDK_SUCCEED;
	return GDK_FAIL;
}

/* Clean-up write-ahead log files already persisted in the BATs.
 * Update the LOGFILE and delete all bak- files as well.
 */
gdk_return
logger_cleanup(logger *lg)
{
	int farmid, len;
	char buf[BUFSIZ];
	FILE *fp = NULL;

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	farmid = BBPselectfarm(PERSISTENT, 0, offheap);
	len = snprintf(buf, sizeof(buf), "%s%s.bak-" LLFMT, lg->dir, LOGFILE, lg->id);
	if (len == -1 || len >= BUFSIZ) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		return GDK_FAIL;
	}

	if (lg->debug & 1) {
		fprintf(stderr, "#logger_cleanup %s\n", buf);
	}

	lng lid = lg->id;
	// remove the last persisted WAL files as well to reduce the
	// work for the logger_cleanup_old()
	if ((fp = GDKfileopen(farmid, NULL, buf, NULL, "r")) == NULL) {
		GDKsyserror("cannot open file %s\n", buf);
		return GDK_FAIL;
	}

	while (lid-- > 0) {
		char log_id[FILENAME_MAX];

		len = snprintf(log_id, sizeof(log_id), LLFMT, lid);
		if (len == -1 || len >= FILENAME_MAX) {
			TRC_CRITICAL(GDK, "log_id filename is too large\n");
			fclose(fp);
			return GDK_FAIL;
		}
		if (GDKunlink(farmid, lg->dir, LOGFILE, log_id) != GDK_SUCCEED) {
			/* not a disaster (yet?) if unlink fails */
			TRC_ERROR(GDK, "failed to remove old WAL %s.%s\n", LOGFILE, buf);
			GDKclrerr();
		}
	}
	fclose(fp);

	len = snprintf(buf, sizeof(buf), "bak-" LLFMT, lg->id);
	if (len == -1 || len >= BUFSIZ) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		GDKclrerr();
	}

	if (GDKunlink(farmid, lg->dir, LOGFILE, buf) != GDK_SUCCEED) {
		/* not a disaster (yet?) if unlink fails */
		TRC_ERROR(GDK, "failed to remove old WAL %s.%s\n", LOGFILE, buf);
		GDKclrerr();
	}

	return GDK_SUCCEED;
}

void
logger_with_ids(logger *lg)
{
	lg->with_ids = true;
}

/* Clean-up write-ahead log files already persisted in the BATs, leaving only the most recent one.
 * Only the bak- files are deleted for the preserved WAL files.
 */
lng
logger_changes(logger *lg)
{
	return lg->changes;
}

int
logger_sequence(logger *lg, int seq, lng *id)
{
	BUN p = log_find(lg->seqs_id, lg->dseqs, seq);

	if (p != BUN_NONE) {
		*id = *(lng *) Tloc(lg->seqs_val, p);

		return 1;
	}
	return 0;
}

/*
 * Changes made to the BAT descriptor should be stored in the log
 * files.  Actually, we need to save the descriptor file, perhaps we
 * should simply introduce a versioning scheme.
 */
gdk_return
log_bat_persists(logger *lg, BAT *b, const char *name, char tpe, oid id)
{
	char *ha, *ta;
	int len;
	char buf[BUFSIZ];
	logformat l;
	int flag = b->batTransient ? LOG_CREATE : LOG_USE;
	BUN p;

	l.nr = 0;
	if (flag == LOG_USE) {
#ifndef NDEBUG
		assert(b->batRole == PERSISTENT);
		assert(0 <= b->theap.farmid && b->theap.farmid < MAXFARMS);
		assert(BBPfarms[b->theap.farmid].roles & (1 << PERSISTENT));
		if (b->tvheap) {
			assert(0 <= b->tvheap->farmid && b->tvheap->farmid < MAXFARMS);
			assert(BBPfarms[b->tvheap->farmid].roles & (1 << PERSISTENT));
		}
#endif
		l.nr = b->batCacheid;
	}
	l.flag = flag;
	if (tpe)
		l.flag = (l.flag == LOG_USE)?LOG_USE_ID:LOG_CREATE_ID;
	l.tid = lg->tid;
	lg->changes++;
	if (!LOG_DISABLED(lg)) {
		if (log_write_format(lg, &l) != GDK_SUCCEED ||
		    log_write_string(lg, name) != GDK_SUCCEED ||
		    (tpe && log_write_id(lg, tpe, id) != GDK_SUCCEED))
			return GDK_FAIL;
	}

	if (lg->debug & 1)
		fprintf(stderr, "#persists bat %s (%d) %s\n",
			name, b->batCacheid,
			(flag == LOG_USE) ? "use" : "create");

	if (flag == LOG_USE) {
		assert(b->batRole == PERSISTENT);
		assert(b->theap.farmid == 0);
		assert(b->tvheap == NULL ||
		       BBPfarms[b->tvheap->farmid].roles & (1 << PERSISTENT));
		if ((p = log_find(lg->snapshots_bid, lg->dsnapshots, b->batCacheid)) != BUN_NONE &&
		    p >= lg->snapshots_tid->batInserted) {
			if (BUNinplace(lg->snapshots_tid, p, &lg->tid, false) != GDK_SUCCEED)
				return GDK_FAIL;
		} else {
			if (p != BUN_NONE) {
				oid pos = p;
				if (BUNappend(lg->dsnapshots, &pos, false) != GDK_SUCCEED)
					return GDK_FAIL;
			}
			if (BUNappend(lg->snapshots_bid, &b->batCacheid, false) != GDK_SUCCEED ||
			    BUNappend(lg->snapshots_tid, &lg->tid, false) != GDK_SUCCEED)
				return GDK_FAIL;
		}
		return GDK_SUCCEED;
	}
	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	ha = "vid";
	ta = ATOMname(b->ttype);
	len = (int) strconcat_len(buf, sizeof(buf), ha, ",", ta, NULL);
	len++;			/* include EOS */
	if (!mnstr_writeInt(lg->log, len) ||
	    mnstr_write(lg->log, buf, 1, len) != (ssize_t) len) {
		TRC_CRITICAL(GDK, "write failed\n");
		return GDK_FAIL;
	}

	if (lg->debug & 1)
		fprintf(stderr, "#Logged new bat [%s,%s] %s " BUNFMT " (%d)\n",
			ha, ta, name, BATcount(b), b->batCacheid);
	return log_bat(lg, b, name, tpe, id);
}

gdk_return
log_bat_transient(logger *lg, const char *name, char tpe, oid id)
{
	log_bid bid = logger_find_bat(lg, name, tpe, id);
	logformat l;
	BUN p;

	l.flag = (tpe)?LOG_DESTROY_ID:LOG_DESTROY;
	l.tid = lg->tid;
	l.nr = 0;
	lg->changes++;

	/* if this is a snapshot bat, we need to skip all changes */
	if ((p = log_find(lg->snapshots_bid, lg->dsnapshots, bid)) != BUN_NONE) {
		//	int tid = *(int*)Tloc(lg->snapshots_tid, p);
#ifndef NDEBUG
		assert(BBP_desc(bid)->batRole == PERSISTENT);
		assert(0 <= BBP_desc(bid)->theap.farmid && BBP_desc(bid)->theap.farmid < MAXFARMS);
		assert(BBPfarms[BBP_desc(bid)->theap.farmid].roles & (1 << PERSISTENT));
		if (BBP_desc(bid)->tvheap) {
			assert(0 <= BBP_desc(bid)->tvheap->farmid && BBP_desc(bid)->tvheap->farmid < MAXFARMS);
			assert(BBPfarms[BBP_desc(bid)->tvheap->farmid].roles & (1 << PERSISTENT));
		}
#endif
		//	if (lg->tid == tid)
		if (p >= lg->snapshots_tid->batInserted) {
			if (BUNinplace(lg->snapshots_tid, p, &lg->tid, false) != GDK_SUCCEED)
				return GDK_FAIL;
		} else {
			oid pos = p;
			if (BUNappend(lg->dsnapshots, &pos, false) != GDK_SUCCEED ||
			    BUNappend(lg->snapshots_tid, &lg->tid, false) != GDK_SUCCEED ||
			    BUNappend(lg->snapshots_bid, &bid, false) != GDK_SUCCEED)
				return GDK_FAIL;
		}
		//	else
		//		printf("%d != %d\n", lg->tid, tid);
		//	assert(lg->tid == tid);
	}

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	if (log_write_format(lg, &l) != GDK_SUCCEED ||
	    (tpe ? log_write_id(lg, tpe, id) : log_write_string(lg, name)) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "write failed\n");
		return GDK_FAIL;
	}

	if (lg->debug & 1)
		fprintf(stderr, "#Logged destroyed bat %s\n", NAME(name, tpe, id));
	return GDK_SUCCEED;
}

gdk_return
log_delta(logger *lg, BAT *uid, BAT *uval, const char *name, char tpe, oid id)
{
	gdk_return ok = GDK_SUCCEED;
	logformat l;
	BUN p;

	assert(uid->ttype == TYPE_oid || uid->ttype == TYPE_void);

	l.tid = lg->tid;
	l.nr = (BUNlast(uval));
	lg->changes += l.nr;

	if (LOG_DISABLED(lg)) {
		/* logging is switched off */
		return GDK_SUCCEED;
	}

	if (l.nr) {
		BATiter vi = bat_iterator(uval);
		gdk_return (*wh) (const void *, stream *, size_t) = BATatoms[TYPE_oid].atomWrite;
		gdk_return (*wt) (const void *, stream *, size_t) = BATatoms[uval->ttype].atomWrite;
		char compress = (tpe && BATtdense(uid)?1:0);

		l.flag = (tpe)?LOG_UPDATE_PAX:LOG_UPDATE;
		if (log_write_format(lg, &l) != GDK_SUCCEED ||
		    (tpe ? log_write_id(lg, tpe, id) : log_write_string(lg, name)) != GDK_SUCCEED)
			return GDK_FAIL;
		if (l.flag == LOG_UPDATE) { /* old style */
			for (p = 0; p < BUNlast(uid) && ok == GDK_SUCCEED; p++) {
				const oid id = BUNtoid(uid, p);
				const void *val = BUNtail(vi, p);

				ok = wh(&id, lg->log, 1);
				if (ok == GDK_SUCCEED)
					ok = wt(val, lg->log, 1);
			}
		} else {
			BATiter ui = bat_iterator(uid);
			const oid *id = BUNtail(ui, 0);

			if (mnstr_write(lg->log, &compress, 1, 1) != 1)
				return GDK_FAIL;
			if (compress) {
				oid seq = uid->tseqbase;
				ok = wh(&seq, lg->log, 1);
			} else {
				ok = wh(id, lg->log, (size_t)l.nr);
			}

			if (ok == GDK_SUCCEED) {
				if (uval->ttype > TYPE_void && uval->ttype < TYPE_str && !isVIEW(uval)) {
					const void *val = BUNtail(vi, 0);
					ok = wt(val, lg->log, (size_t)l.nr);
				} else {
					for (p = 0; p < BUNlast(uval) && ok == GDK_SUCCEED; p++) {
						const void *val = BUNtail(vi, p);
						ok = wt(val, lg->log, 1);
					}
				}
			}
		}

		if (lg->debug & 1)
			fprintf(stderr, "#Logged %s " LLFMT " inserts\n", name, l.nr);
	}
	if (ok != GDK_SUCCEED)
		TRC_CRITICAL(GDK, "write failed for %s\n", name);
	return ok;
}

gdk_return
log_bat(logger *lg, BAT *b, const char *name, char tpe, oid id)
{
	gdk_return ok = GDK_SUCCEED;
	logformat l;
	BUN p;

	l.tid = lg->tid;
	l.nr = (BUNlast(b) - b->batInserted);
	lg->changes += (b->batInserted)?l.nr:1; /* initial large inserts is counted as 1 change */

	if (LOG_DISABLED(lg)) {
		/* logging is switched off */
		return GDK_SUCCEED;
	}

	if (l.nr) {
		BATiter bi = bat_iterator(b);
		gdk_return (*wt) (const void *, stream *, size_t) = BATatoms[b->ttype].atomWrite;

		l.flag = tpe?LOG_INSERT_ID:LOG_INSERT;
		if (log_write_format(lg, &l) != GDK_SUCCEED ||
		    (tpe ? log_write_id(lg, tpe, id) : log_write_string(lg, name)) != GDK_SUCCEED)
			return GDK_FAIL;

		if (b->ttype > TYPE_void &&
		    b->ttype < TYPE_str &&
		    !isVIEW(b)) {
			const void *t = BUNtail(bi, b->batInserted);

			ok = wt(t, lg->log, (size_t)l.nr);
		} else {
			for (p = b->batInserted; p < BUNlast(b) && ok == GDK_SUCCEED; p++) {
				const void *t = BUNtail(bi, p);

				ok = wt(t, lg->log, 1);
			}
		}

		if (lg->debug & 1)
			fprintf(stderr, "#Logged %s " LLFMT " inserts\n", name, l.nr);
	}

	if (ok != GDK_SUCCEED)
		TRC_CRITICAL(GDK, "write failed for %s\n", name);
	return ok;
}

gdk_return
log_bat_clear(logger *lg, const char *name, char tpe, oid id)
{
	logformat l;

	l.nr = 1;
	l.tid = lg->tid;
	lg->changes += l.nr;

	if (LOG_DISABLED(lg)) {
		/* logging is switched off */
		return GDK_SUCCEED;
	}

	l.flag = (tpe)?LOG_CLEAR_ID:LOG_CLEAR;
	if (log_write_format(lg, &l) != GDK_SUCCEED ||
	    (tpe ? log_write_id(lg, tpe, id) : log_write_string(lg, name)) != GDK_SUCCEED)
		return GDK_FAIL;

	if (lg->debug & 1)
		fprintf(stderr, "#Logged clear %s\n", NAME(name, tpe, id));

	return GDK_SUCCEED;
}

gdk_return
log_tstart(logger *lg)
{
	logformat l;

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	l.flag = LOG_START;
	l.tid = ++lg->tid;
	l.nr = lg->tid;

	if (lg->debug & 1)
		fprintf(stderr, "#log_tstart %d\n", lg->tid);

	return log_write_format(lg, &l);
}

#define DBLKSZ		8192
#define SEGSZ		(64*DBLKSZ)

#define LOG_LARGE	(LL_CONSTANT(2)*1024*1024*1024)

static gdk_return
pre_allocate(logger *lg)
{
	// FIXME: this causes serious issues on Windows at least with MinGW
	assert(!LOG_DISABLED(lg));
#ifndef WIN32
	lng p;
	p = (lng) getfilepos(getFile(lg->log));
	if (p == -1)
		return GDK_FAIL;
	if (p > LOG_LARGE) {
		logger_close(lg);
		lg->id++;
		return logger_open(lg);
	}
	if (p + DBLKSZ > lg->end) {
		p &= ~(DBLKSZ - 1);
		p += SEGSZ;
		if (GDKextendf(getFileNo(lg->log), (size_t) p, "WAL file") != GDK_SUCCEED)
			return GDK_FAIL;
		lg->end = p;
	}
#else
	(void) lg;
#endif
	return GDK_SUCCEED;
}

gdk_return
log_tend(logger *lg)
{
	logformat l;
	gdk_return res = GDK_SUCCEED;

	if (lg->debug & 1)
		fprintf(stderr, "#log_tend %d\n", lg->tid);

	if (DELTAdirty(lg->snapshots_bid)) {
		/* sub commit all new snapshots */
		BAT *cands, *tids, *bids;

		tids = bm_tids(lg->snapshots_tid, lg->dsnapshots);
		if (tids == NULL) {
			TRC_CRITICAL(GDK, "bm_tids failed\n");
			return GDK_FAIL;
		}
		cands = BATselect(lg->snapshots_tid, tids, &lg->tid, &lg->tid,
				     true, true, false);
		if (cands == NULL) {
			TRC_CRITICAL(GDK, "select failed\n");
			return GDK_FAIL;
		}
		bids = BATproject(cands, lg->snapshots_bid);
		BBPunfix(cands->batCacheid);
		BBPunfix(tids->batCacheid);
		if (bids == NULL) {
			TRC_CRITICAL(GDK, "project failed\n");
			return GDK_FAIL;
		}
		res = bm_subcommit(lg, bids, NULL, lg->snapshots_bid,
				   lg->snapshots_tid, NULL, NULL, lg->dsnapshots, NULL, lg->debug);
		BBPunfix(bids->batCacheid);
	}
	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;
	l.flag = LOG_END;
	l.tid = lg->tid;
	l.nr = lg->tid;

	if (res != GDK_SUCCEED ||
	    log_write_format(lg, &l) != GDK_SUCCEED ||
	    mnstr_flush(lg->log) ||
	    (!(GDKdebug & NOSYNCMASK) && mnstr_fsync(lg->log)) ||
	    pre_allocate(lg) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "write failed\n");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

gdk_return
log_abort(logger *lg)
{
	logformat l;

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;
	if (lg->debug & 1)
		fprintf(stderr, "#log_abort %d\n", lg->tid);

	l.flag = LOG_END;
	l.tid = lg->tid;
	l.nr = -1;

	if (log_write_format(lg, &l) != GDK_SUCCEED)
		return GDK_FAIL;

	return GDK_SUCCEED;
}

static gdk_return
log_sequence_(logger *lg, int seq, lng val, int flush)
{
	logformat l;

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;
	l.flag = LOG_SEQ;
	l.tid = lg->tid;
	l.nr = seq;

	if (lg->debug & 1)
		fprintf(stderr, "#log_sequence_ (%d," LLFMT ")\n", seq, val);

	if (log_write_format(lg, &l) != GDK_SUCCEED ||
	    !mnstr_writeLng(lg->log, val) ||
	    (flush && mnstr_flush(lg->log)) ||
	    (flush && !(GDKdebug & NOSYNCMASK) && mnstr_fsync(lg->log))) {
		TRC_CRITICAL(GDK, "write failed\n");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* a transaction in it self */
gdk_return
log_sequence(logger *lg, int seq, lng val)
{
	BUN p;

	if (lg->debug & 1)
		fprintf(stderr, "#log_sequence (%d," LLFMT ")\n", seq, val);

	if ((p = log_find(lg->seqs_id, lg->dseqs, seq)) != BUN_NONE &&
	    p >= lg->seqs_id->batInserted) {
		if (BUNinplace(lg->seqs_val, p, &val, false) != GDK_SUCCEED)
			return GDK_FAIL;
	} else {
		if (p != BUN_NONE) {
			oid pos = p;
			if (BUNappend(lg->dseqs, &pos, false) != GDK_SUCCEED)
				return GDK_FAIL;
		}
		if (BUNappend(lg->seqs_id, &seq, false) != GDK_SUCCEED ||
		    BUNappend(lg->seqs_val, &val, false) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	return log_sequence_(lg, seq, val, 1);
}

static gdk_return
bm_commit(logger *lg)
{
	BUN p, q;
	BAT *b = lg->catalog_bid;
	BAT *n = logbat_new(TYPE_str, BATcount(lg->freed), TRANSIENT);
	gdk_return res;
	const log_bid *bids;

	if (n == NULL)
		return GDK_FAIL;

	/* subcommit the freed bats */
	bids = (const log_bid *) Tloc(lg->freed, 0);
	BATloop(lg->freed, p, q) {
		bat bid = bids[p];
		BAT *lb = BATdescriptor(bid);
		str name = BBPname(bid);

		if (lb == NULL ||
		    BATmode(lb, true) != GDK_SUCCEED) {
			logbat_destroy(lb);
			logbat_destroy(n);
			return GDK_FAIL;
		}
		logbat_destroy(lb);
		if (lg->debug & 1)
			fprintf(stderr,
				"#commit deleted (snapshot) %s (%d)\n",
				name, bid);
		if (BUNappend(n, name, false) != GDK_SUCCEED) {
			logbat_destroy(lb);
			logbat_destroy(n);
			return GDK_FAIL;
		}
		BBPrelease(bid);
	}

	bids = (log_bid *) Tloc(b, 0);
	for (p = b->batInserted; p < BUNlast(b); p++) {
		log_bid bid = bids[p];
		BAT *lb;
		oid pos = p;

		if (BUNfnd(lg->dcatalog, &pos) != BUN_NONE)
			continue;

		if (bid == lg->dsnapshots->batCacheid)
			continue;

		if ((lb = BATdescriptor(bid)) == NULL ||
		    BATmode(lb, false) != GDK_SUCCEED) {
			logbat_destroy(lb);
			logbat_destroy(n);
			return GDK_FAIL;
		}

		assert(lb->batRestricted != BAT_WRITE);
		logbat_destroy(lb);

		if (lg->debug & 1)
			fprintf(stderr, "#bm_commit: create %d (%d)\n",
				bid, BBP_lrefs(bid));
	}
	res = bm_subcommit(lg, lg->catalog_bid, lg->catalog_nme, lg->catalog_bid, lg->catalog_nme, lg->catalog_tpe, lg->catalog_oid, lg->dcatalog, n, lg->debug);
	BBPreclaim(n);
	if (res == GDK_SUCCEED) {
		BATclear(lg->freed, false);
		BATcommit(lg->freed);
		return GDK_SUCCEED;
	}
	return GDK_FAIL;
}

gdk_return
logger_add_bat(logger *lg, BAT *b, const char *name, char tpe, oid id)
{
	log_bid bid = logger_find_bat(lg, name, tpe, id);
	lng lid = tpe ? (lng) id : 0;

	assert(b->batRestricted != BAT_WRITE ||
	       b == lg->snapshots_bid ||
	       b == lg->snapshots_tid ||
	       b == lg->dsnapshots ||
	       b == lg->catalog_bid ||
	       b == lg->catalog_nme ||
	       b == lg->catalog_tpe ||
	       b == lg->catalog_oid ||
	       b == lg->dcatalog ||
	       b == lg->seqs_id ||
	       b == lg->seqs_val ||
	       b == lg->dseqs);
	assert(b->batRole == PERSISTENT);
	if (bid) {
		if (bid != b->batCacheid) {
			if (logger_del_bat(lg, bid) != GDK_SUCCEED)
				return GDK_FAIL;
		} else {
			return GDK_SUCCEED;
		}
	}
	bid = b->batCacheid;
	if (lg->debug & 1)
		fprintf(stderr, "#create %s\n", NAME(name, tpe, id));
	assert(log_find(lg->catalog_bid, lg->dcatalog, bid) == BUN_NONE);
	lg->changes += BATcount(b) + 1000;
	if (BUNappend(lg->catalog_bid, &bid, false) != GDK_SUCCEED ||
	    BUNappend(lg->catalog_nme, name, false) != GDK_SUCCEED ||
	    BUNappend(lg->catalog_tpe, &tpe, false) != GDK_SUCCEED ||
	    BUNappend(lg->catalog_oid, &lid, false) != GDK_SUCCEED)
		return GDK_FAIL;
	BBPretain(bid);
	return GDK_SUCCEED;
}

gdk_return
logger_upgrade_bat(logger *lg, const char *name, char tpe, oid id)
{
	log_bid bid = logger_find_bat(lg, name, tpe, id);

	if (bid) {
		oid p = (oid) log_find(lg->catalog_bid, lg->dcatalog, bid);
		lng lid = tpe ? (lng) id : 0;

		if (BUNappend(lg->dcatalog, &p, false) != GDK_SUCCEED ||
		    BUNappend(lg->catalog_bid, &bid, false) != GDK_SUCCEED ||
		    BUNappend(lg->catalog_nme, name, false) != GDK_SUCCEED ||
		    BUNappend(lg->catalog_tpe, &tpe, false) != GDK_SUCCEED ||
		    BUNappend(lg->catalog_oid, &lid, false) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

gdk_return
logger_del_bat(logger *lg, log_bid bid)
{
	BAT *b = BATdescriptor(bid);
	BUN p = log_find(lg->catalog_bid, lg->dcatalog, bid), q;
	oid pos;

	assert(p != BUN_NONE);
	if (p == BUN_NONE) {
		logbat_destroy(b);
		GDKerror("cannot find BAT\n");
		return GDK_FAIL;
	}

	/* if this is a not logger commited snapshot bat, make it
	 * transient */
	if (p >= lg->catalog_bid->batInserted &&
	    (q = log_find(lg->snapshots_bid, lg->dsnapshots, bid)) != BUN_NONE) {
		pos = (oid) q;
		if (BUNappend(lg->dsnapshots, &pos, false) != GDK_SUCCEED) {
			logbat_destroy(b);
			return GDK_FAIL;
		}
		if (lg->debug & 1)
			fprintf(stderr,
				"#logger_del_bat release snapshot %d (%d)\n",
				bid, BBP_lrefs(bid));
		if (BUNappend(lg->freed, &bid, false) != GDK_SUCCEED) {
			logbat_destroy(b);
			return GDK_FAIL;
		}
	} else if (p >= lg->catalog_bid->batInserted) {
		BBPrelease(bid);
	} else {
		if (BUNappend(lg->freed, &bid, false) != GDK_SUCCEED) {
			logbat_destroy(b);
			return GDK_FAIL;
		}
	}
	if (b) {
		lg->changes += BATcount(b) + 1;
		BBPunfix(b->batCacheid);
	}
	pos = (oid) p;
	return BUNappend(lg->dcatalog, &pos, false);
/*assert(BBP_lrefs(bid) == 0);*/
}

log_bid
logger_find_bat(logger *lg, const char *name, char tpe, oid id)
{
	if (!tpe || !lg->with_ids) {
		BATiter cni = bat_iterator(lg->catalog_nme);
		BUN p;

		if (BAThash(lg->catalog_nme) == GDK_SUCCEED) {
			HASHloop_str(cni, cni.b->thash, p, name) {
				oid pos = p;
				if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE) {
					oid lid = *(oid*) Tloc(lg->catalog_oid, p);
					if (!lid)
						return *(log_bid *) Tloc(lg->catalog_bid, p);
				}
			}
		}
	} else {
		BATiter cni = bat_iterator(lg->catalog_oid);
		BUN p;

		if (BAThash(lg->catalog_oid) == GDK_SUCCEED) {
			lng lid = (lng) id;
			HASHloop_lng(cni, cni.b->thash, p, &lid) {
				oid pos = p;
				if (*(char*)Tloc(lg->catalog_tpe, p) == tpe) {
					if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE)
						return *(log_bid *) Tloc(lg->catalog_bid, p);
				}
			}
		}
	}
	return 0;
}

static geomcatalogfix_fptr geomcatalogfix = NULL;
static geomsqlfix_fptr geomsqlfix = NULL;

void
geomcatalogfix_set(geomcatalogfix_fptr f)
{
	geomcatalogfix = f;
}

geomcatalogfix_fptr
geomcatalogfix_get(void)
{
	return geomcatalogfix;
}

void
geomsqlfix_set(geomsqlfix_fptr f)
{
	geomsqlfix = f;
}

geomsqlfix_fptr
geomsqlfix_get(void)
{
	return geomsqlfix;
}

void
geomversion_set(void)
{
	geomisoldversion = 1;
}
int geomversion_get(void)
{
	return geomisoldversion;
}
