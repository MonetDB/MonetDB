/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "gdk_logger_internals.h"
#include "mutils.h"
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

#define BATSIZE 0

#define NAME(name,tpe,id) (name?name:"tpe id")

#define LOG_DISABLED(lg) ((lg)->lg->debug&128)

static gdk_return logger_cleanup(old_logger *lg);
static gdk_return logger_add_bat(old_logger *lg, BAT *b, const char *name, char tpe, oid id);
static gdk_return logger_del_bat(old_logger *lg, log_bid bid);

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

typedef struct logformat_t {
	char flag;
	int tid;
	lng nr;
} logformat;

typedef enum {LOG_OK, LOG_EOF, LOG_ERR} log_return;

#include "gdk_geomlogger.h"

/* When reading an old format database, we may need to read the geom
 * Well-known Binary (WKB) type differently.  This variable is used to
 * indicate that to the function wkbREAD during reading of the log. */
static bool geomisoldversion;

static gdk_return tr_grow(trans *tr);

static BUN
log_find(BAT *b, BAT *d, int val)
{
	BATiter cni = bat_iterator(b);
	BUN p;

	assert(b->ttype == TYPE_int);
	assert(d->ttype == TYPE_oid);
	if (BAThash(b) == GDK_SUCCEED) {
		MT_rwlock_rdlock(&cni.b->thashlock);
		HASHloop_int(cni, cni.b->thash, p, &val) {
			oid pos = p;
			if (BUNfnd(d, &pos) == BUN_NONE) {
				MT_rwlock_rdunlock(&cni.b->thashlock);
				return p;
			}
		}
		MT_rwlock_rdunlock(&cni.b->thashlock);
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
log_read_format(old_logger *l, logformat *data)
{
	return mnstr_read(l->log, &data->flag, 1, 1) == 1 &&
		mnstr_readLng(l->log, &data->nr) == 1 &&
		mnstr_readInt(l->log, &data->tid) == 1;
}

static char *
log_read_string(old_logger *l)
{
	int len;
	ssize_t nr;
	char *buf;

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

static log_return
log_read_clear(old_logger *lg, trans *tr, char *name, char tpe, oid id)
{
	if (lg->lg->debug & 1)
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
avoid_snapshot(old_logger *lg, log_bid bid)
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

log_bid
old_logger_find_bat(old_logger *lg, const char *name, char tpe, oid id)
{
	if (!tpe || !lg->with_ids) {
		BATiter cni = bat_iterator(lg->catalog_nme);
		BUN p;

		if (BAThash(lg->catalog_nme) == GDK_SUCCEED) {
			MT_rwlock_rdlock(&cni.b->thashlock);
			HASHloop_str(cni, cni.b->thash, p, name) {
				oid pos = p;
				if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE) {
					oid lid = *(oid*) Tloc(lg->catalog_oid, p);
					if (!lid) {
						MT_rwlock_rdunlock(&cni.b->thashlock);
						return *(log_bid *) Tloc(lg->catalog_bid, p);
					}
				}
			}
			MT_rwlock_rdunlock(&cni.b->thashlock);
		}
	} else {
		BATiter cni = bat_iterator(lg->catalog_oid);
		BUN p;

		if (BAThash(lg->catalog_oid) == GDK_SUCCEED) {
			lng lid = (lng) id;
			MT_rwlock_rdlock(&cni.b->thashlock);
			HASHloop_lng(cni, cni.b->thash, p, &lid) {
				oid pos = p;
				if (*(char*)Tloc(lg->catalog_tpe, p) == tpe) {
					if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE) {
						MT_rwlock_rdunlock(&cni.b->thashlock);
						return *(log_bid *) Tloc(lg->catalog_bid, p);
					}
				}
			}
			MT_rwlock_rdunlock(&cni.b->thashlock);
		}
	}
	return 0;
}

static gdk_return
la_bat_clear(old_logger *lg, logaction *la)
{
	log_bid bid = old_logger_find_bat(lg, la->name, la->tpe, la->cid);
	BAT *b;

	if (lg->lg->debug & 1)
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
log_read_seq(old_logger *lg, logformat *l)
{
	int seq = (int) l->nr;
	lng val;
	BUN p;

	assert(l->nr <= (lng) INT_MAX);
	if (mnstr_readLng(lg->log, &val) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}

	if ((p = log_find(lg->seqs_id, lg->dseqs, seq)) != BUN_NONE &&
	    p >= lg->seqs_id->batInserted) {
		assert(lg->seqs_val->hseqbase == 0);
		if (BUNreplace(lg->seqs_val, p, &val, false) != GDK_SUCCEED)
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

static int
log_read_id(old_logger *lg, char *tpe, oid *id)
{
	lng lid;

	if (mnstr_readChr(lg->log, tpe) != 1 ||
	    mnstr_readLng(lg->log, &lid) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}
	*id = (oid)lid;
	return LOG_OK;
}

static log_return
log_read_updates(old_logger *lg, trans *tr, logformat *l, char *name, int tpe, oid id, int pax)
{
	log_bid bid = old_logger_find_bat(lg, name, tpe, id);
	BAT *b = BATdescriptor(bid);
	log_return res = LOG_OK;
	int ht = -1, tt = -1, tseq = 0;

	if (lg->lg->debug & 1) {
		if (name)
			fprintf(stderr, "#logger found log_read_updates %s %s " LLFMT "\n", name, l->flag == LOG_INSERT ? "insert" : "update", l->nr);
		else
			fprintf(stderr, "#logger found log_read_updates " OIDFMT " %s " LLFMT "\n", id, l->flag == LOG_INSERT ? "insert" : "update", l->nr);
	}

	if (b) {
		ht = TYPE_void;
		tt = b->ttype;
		if (tt == TYPE_void && BATtdense(b))
			tseq = 1;
	} else {		/* search trans action for create statement */
		int i;

		for (i = 0; i < tr->nr; i++) {
			if (tr->changes[i].type == LOG_CREATE &&
			    (tpe == 0 && name != NULL
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
				   (tpe == 0 && name != NULL
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
		void *(*rt) (ptr, size_t *, stream *, size_t) = BATatoms[tt].atomRead;

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
				size_t tlen = lg->lg->bufsize;
				void *t = rt(lg->lg->buf, &tlen, lg->log, 1);

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
				} else {
					lg->lg->buf = t;
					lg->lg->bufsize = tlen;
				}
				if (BUNappend(r, t, true) != GDK_SUCCEED)
					res = LOG_ERR;
			}
		} else {
			void *(*rh) (ptr, size_t *, stream *, size_t) = ht == TYPE_void ? BATatoms[TYPE_oid].atomRead : BATatoms[ht].atomRead;
			void *hv = ATOMnil(ht);
			size_t hlen = ATOMsize(ht);

			if (hv == NULL)
				res = LOG_ERR;

			if (!pax) {
				lng nr = l->nr;
				for (; res == LOG_OK && nr > 0; nr--) {
					size_t tlen = lg->lg->bufsize;
					void *h = rh(hv, &hlen, lg->log, 1);
					void *t = rt(lg->lg->buf, &tlen, lg->log, 1);

					if (t != NULL) {
						lg->lg->buf = t;
						lg->lg->bufsize = tlen;
					}
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
				}
			} else {
				char compressed = 0;
				lng nr = l->nr;

				if (mnstr_read(lg->log, &compressed, 1, 1) != 1)
					return LOG_ERR;

				if (compressed) {
					void *h = rh(hv, &hlen, lg->log, 1);

					assert(uid->ttype == TYPE_void);
					if (h == NULL)
						res = LOG_EOF;
					else {
						BATtseqbase(uid, *(oid*)h);
						BATsetcount(uid, (BUN) l->nr);
					}
				} else {
					for (; res == LOG_OK && nr > 0; nr--) {
						void *h = rh(hv, &hlen, lg->log, 1);

						if (h == NULL)
							res = LOG_EOF;
						else if (BUNappend(uid, h, true) != GDK_SUCCEED)
							res = LOG_ERR;
					}
				}
				nr = l->nr;
				for (; res == LOG_OK && nr > 0; nr--) {
					size_t tlen = lg->lg->bufsize;
					void *t = rt(lg->lg->buf, &tlen, lg->log, 1);

					if (t == NULL) {
						if (strstr(GDKerrbuf, "malloc") == NULL)
							res = LOG_EOF;
						else
							res = LOG_ERR;
					} else {
						lg->lg->buf = t;
						lg->lg->bufsize = tlen;
						if (BUNappend(r, t, true) != GDK_SUCCEED)
							res = LOG_ERR;
					}
				}
			}
			GDKfree(hv);
		}

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
la_bat_updates(old_logger *lg, logaction *la)
{
	log_bid bid = old_logger_find_bat(lg, la->name, la->tpe, la->cid);
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
log_read_destroy(old_logger *lg, trans *tr, char *name, char tpe, oid id)
{
	(void) lg;
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
la_bat_destroy(old_logger *lg, logaction *la)
{
	log_bid bid = old_logger_find_bat(lg, la->name, la->tpe, la->cid);

	if (bid) {
		BUN p;

		if (logger_del_bat(lg, bid) != GDK_SUCCEED)
			return GDK_FAIL;

		if ((p = log_find(lg->snapshots_bid, lg->dsnapshots, bid)) != BUN_NONE) {
			oid pos = (oid) p;
#ifndef NDEBUG
			assert(BBP_desc(bid)->batRole == PERSISTENT);
			assert(0 <= BBP_desc(bid)->theap->farmid && BBP_desc(bid)->theap->farmid < MAXFARMS);
			assert(BBPfarms[BBP_desc(bid)->theap->farmid].roles & (1 << PERSISTENT));
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
log_read_create(old_logger *lg, trans *tr, char *name, char tpe, oid id)
{
	char *buf = log_read_string(lg);
	int ht, tt;
	char *ha, *ta;

	if (lg->lg->debug & 1)
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
la_bat_create(old_logger *lg, logaction *la)
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
log_read_use(old_logger *lg, trans *tr, logformat *l, char *name, char tpe, oid id)
{
	(void) lg;

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
la_bat_use(old_logger *lg, logaction *la)
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
	assert(0 <= b->theap->farmid && b->theap->farmid < MAXFARMS);
	assert(BBPfarms[b->theap->farmid].roles & (1 << PERSISTENT));
	if (b->tvheap) {
		assert(0 <= b->tvheap->farmid && b->tvheap->farmid < MAXFARMS);
		assert(BBPfarms[b->tvheap->farmid].roles & (1 << PERSISTENT));
	}
#endif
	if ((p = log_find(lg->snapshots_bid, lg->dsnapshots, b->batCacheid)) != BUN_NONE &&
	    p >= lg->snapshots_bid->batInserted) {
		assert(lg->snapshots_tid->hseqbase == 0);
		if (BUNreplace(lg->snapshots_tid, p, &lg->tid, false) != GDK_SUCCEED)
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
la_apply(old_logger *lg, logaction *c)
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
tr_abort(old_logger *lg, trans *tr)
{
	int i;

	if (lg->lg->debug & 1)
		fprintf(stderr, "#tr_abort\n");

	for (i = 0; i < tr->nr; i++)
		la_destroy(&tr->changes[i]);
	return tr_destroy(tr);
}

static trans *
tr_commit(old_logger *lg, trans *tr)
{
	int i;

	if (lg->lg->debug & 1)
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

static inline void
logger_close(old_logger *lg)
{
	if (!LOG_DISABLED(lg))
		close_stream(lg->log);
	lg->log = NULL;
}

static gdk_return
logger_readlog(old_logger *lg, char *filename, bool *filemissing)
{
	trans *tr = NULL;
	logformat l;
	log_return err = LOG_OK;
	time_t t0, t1;
	struct stat sb;
	int dbg = GDKdebug;
	int fd;

	GDKdebug &= ~(CHECKMASK|PROPMASK);

	if (lg->lg->debug & 1) {
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
		if (byteorder != 1234) {
			TRC_CRITICAL(GDK, "incorrect byte order word in file %s\n", filename);
			logger_close(lg);
			GDKdebug = dbg;
			return GDK_FAIL;
		}
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
	if (lg->lg->debug & 1) {
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
		if (lg->lg->debug & 1) {
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
			if (lg->lg->debug & 1)
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
	if (lg->lg->debug & 1) {
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
logger_readlogs(old_logger *lg, FILE *fp, char *filename)
{
	gdk_return res = GDK_SUCCEED;
	char id[BUFSIZ];

	if (lg->lg->debug & 1) {
		fprintf(stderr, "#logger_readlogs logger id is " LLFMT "\n", lg->id);
	}

	if (fgets(id, sizeof(id), fp) != NULL) {
		char log_filename[FILENAME_MAX];
		lng lid = strtoll(id, NULL, 10);

		if (lg->lg->debug & 1) {
			fprintf(stderr, "#logger_readlogs last logger id written in %s is " LLFMT "\n", filename, lid);
		}

		if (lid >= lg->id) {
			bool filemissing = false;

			lg->id = lid;
			while (res == GDK_SUCCEED && !filemissing) {
				if (snprintf(log_filename, sizeof(log_filename), "%s." LLFMT, filename, lg->id) >= FILENAME_MAX) {
					GDKerror("Logger filename path is too large\n");
					return GDK_FAIL;
				}
				res = logger_readlog(lg, log_filename, &filemissing);
				if (!filemissing)
					lg->id++;
			}
		} else {
			bool filemissing = false;
			while (lid >= lg->id && res == GDK_SUCCEED) {
				if (snprintf(log_filename, sizeof(log_filename), "%s." LLFMT, filename, lg->id) >= FILENAME_MAX) {
					GDKerror("Logger filename path is too large\n");
					return GDK_FAIL;
				}
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
check_version(old_logger *lg, FILE *fp, int version)
{
	/* if these were equal we wouldn't have gotten here */
	assert(version != lg->lg->version);

	if (lg->lg->prefuncp == NULL ||
	    (*lg->lg->prefuncp)(lg->lg->funcdata, version, lg->lg->version) != GDK_SUCCEED) {
		GDKerror("Incompatible database version %06d, "
			 "this server supports version %06d.\n%s",
			 version, lg->lg->version,
			 version < lg->lg->version ? "Maybe you need to upgrade to an intermediate release first.\n" : "");
		return GDK_FAIL;
	}

	if (fgetc(fp) != '\n' ||	 /* skip \n */
	    fgetc(fp) != '\n') {	 /* skip \n */
		GDKerror("Badly formatted log file");
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* Load data from the logger logdir
 * Initialize new directories and catalog files if none are present,
 * unless running in read-only mode
 * Load data and persist it in the BATs */
static gdk_return
logger_load(const char *fn, char filename[FILENAME_MAX], old_logger *lg, FILE *fp, int version)
{
	size_t len;
	char bak[FILENAME_MAX];
	str filenamestr = NULL;
	log_bid snapshots_bid = 0;
	bat catalog_bid, catalog_nme, catalog_tpe, catalog_oid, dcatalog;
	int dbg = GDKdebug;

	assert(!LOG_DISABLED(lg));

	if ((filenamestr = GDKfilepath(0, lg->lg->dir, LOGFILE, NULL)) == NULL)
		goto error;
	len = strcpy_len(filename, filenamestr, FILENAME_MAX);
	GDKfree(filenamestr);
	if (len >= FILENAME_MAX) {
		GDKerror("Logger filename path is too large\n");
		goto error;
	}

	strconcat_len(bak, sizeof(bak), fn, "_catalog_bid", NULL);
	catalog_bid = BBPindex(bak);

	assert(catalog_bid != 0); /* has been checked by new logger */

	/* find the persistent catalog. As non persistent bats
	 * require a logical reference we also add a logical
	 * reference for the persistent bats */
	size_t i;
	BUN p, q;
	BAT *b, *n, *t, *o, *d;

	b = BATdescriptor(catalog_bid);
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
		t = logbat_new(TYPE_bte, BATSIZE, TRANSIENT);
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
	} else {
		if (BUNappend(lg->del, &t->batCacheid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(t->batCacheid);
	}

	strconcat_len(bak, sizeof(bak), fn, "_catalog_oid", NULL);
	catalog_oid = BBPindex(bak);
	o = BATdescriptor(catalog_oid);
	if (o == NULL) {
		o = logbat_new(TYPE_lng, BATSIZE, TRANSIENT);
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
	} else {
		if (BUNappend(lg->del, &o->batCacheid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(o->batCacheid);
	}

	strconcat_len(bak, sizeof(bak), fn, "_dcatalog", NULL);
	dcatalog = BBPindex(bak);
	d = BATdescriptor(dcatalog);
	if (d == NULL) {
		/* older database: create dcatalog and convert
		 * catalog_bid and catalog_nme to
		 * dense-headed */
		d = logbat_new(TYPE_oid, BATSIZE, TRANSIENT);
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
	} else {
		if (BUNappend(lg->del, &d->batCacheid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(d->batCacheid);
	}

	lg->catalog_bid = b;
	lg->catalog_nme = n;
	lg->catalog_tpe = t;
	lg->catalog_oid = o;
	lg->dcatalog = d;
	if (BUNappend(lg->del, &b->batCacheid, false) != GDK_SUCCEED)
		goto error;
	BBPretain(b->batCacheid);
	if (BUNappend(lg->del, &n->batCacheid, false) != GDK_SUCCEED)
		goto error;
	BBPretain(n->batCacheid);

	const log_bid *bids;
	bids = (const log_bid *) Tloc(b, 0);
	BATloop(b, p, q) {
		bat bid = bids[p];
		oid pos = p;

		if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE &&
		    BBPretain(bid) == 0 &&
		    BUNappend(lg->dcatalog, &pos, false) != GDK_SUCCEED)
			goto error;
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
	snapshots_bid = old_logger_find_bat(lg, "snapshots_bid", 0, 0);
	if (snapshots_bid == 0) {
		lg->snapshots_bid = logbat_new(TYPE_int, 1, TRANSIENT);
		lg->snapshots_tid = logbat_new(TYPE_int, 1, TRANSIENT);
		lg->dsnapshots = logbat_new(TYPE_oid, 1, TRANSIENT);
		if (lg->snapshots_bid == NULL ||
		    lg->snapshots_tid == NULL ||
		    lg->dsnapshots == NULL) {
			GDKerror("Logger_new: failed to create snapshots bats");
			goto error;
		}
	} else {
		bat snapshots_tid = old_logger_find_bat(lg, "snapshots_tid", 0, 0);
		bat dsnapshots = old_logger_find_bat(lg, "dsnapshots", 0, 0);

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
			if (BUNappend(lg->del, &dsnapshots, false) != GDK_SUCCEED)
				goto error;
			BBPretain(dsnapshots);
		} else {
			lg->dsnapshots = logbat_new(TYPE_oid, 1, TRANSIENT);
			if (lg->dsnapshots == NULL) {
				GDKerror("Logger_new: cannot create dsnapshot bat");
				goto error;
			}
		}
		if (BUNappend(lg->del, &snapshots_bid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(snapshots_bid);
		if (BUNappend(lg->del, &snapshots_tid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(snapshots_tid);
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
		if (BUNappend(lg->add, &lg->seqs_id->batCacheid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(lg->seqs_id->batCacheid);
		if (BUNappend(lg->add, &lg->seqs_val->batCacheid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(lg->seqs_val->batCacheid);
		if (BUNappend(lg->add, &lg->dseqs->batCacheid, false) != GDK_SUCCEED)
			goto error;
		BBPretain(lg->dseqs->batCacheid);
	}

	if (check_version(lg, fp, version) != GDK_SUCCEED) {
		goto error;
	}

	if (logger_readlogs(lg, fp, filename) != GDK_SUCCEED) {
		goto error;
	}
	fclose(fp);
	fp = NULL;

	if (lg->lg->postfuncp &&
	    (*lg->lg->postfuncp)(lg->lg->funcdata, lg) != GDK_SUCCEED)
		goto error;
	lg->lg->postfuncp = NULL; /* not again */

	if (BUNappend(lg->add, &lg->lg->catalog_bid->batCacheid, false) != GDK_SUCCEED)
		goto error;
	BBPretain(lg->lg->catalog_bid->batCacheid);
	if (BUNappend(lg->add, &lg->lg->catalog_id->batCacheid, false) != GDK_SUCCEED)
		goto error;
	BBPretain(lg->lg->catalog_id->batCacheid);
	if (BUNappend(lg->add, &lg->lg->dcatalog->batCacheid, false) != GDK_SUCCEED)
		goto error;
	BBPretain(lg->lg->dcatalog->batCacheid);

	/* done reading the log, revert to "normal" behavior */
	geomisoldversion = false;

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
	bids = (const log_bid *) Tloc(lg->add, 0);
	BATloop(lg->add, p, q) {
		BBPrelease(bids[p]);
	}
	logbat_destroy(lg->add);
	bids = (const log_bid *) Tloc(lg->del, 0);
	BATloop(lg->del, p, q) {
		BBPrelease(bids[p]);
	}
	logbat_destroy(lg->del);
	GDKfree(lg->local_dir);
	GDKfree(lg);
	return GDK_FAIL;
}

/* Initialize a new logger
 * It will load any data in the logdir and persist it in the BATs*/
static old_logger *
logger_new(logger *lg, const char *fn, const char *logdir, FILE *fp, int version, const char *logfile)
{
	old_logger *old_lg;
	char filename[FILENAME_MAX];

	assert(!GDKinmemory(0));
	assert(lg != NULL);
	if (GDKinmemory(0)) {
		TRC_CRITICAL(GDK, "old logger can only be used with a disk-based database\n");
		return NULL;
	}
	if (MT_path_absolute(logdir)) {
		TRC_CRITICAL(GDK, "logdir must be relative path\n");
		return NULL;
	}

	old_lg = GDKmalloc(sizeof(struct old_logger));
	if (old_lg == NULL) {
		TRC_CRITICAL(GDK, "allocating logger structure failed\n");
		return NULL;
	}

	*old_lg = (struct old_logger) {
		.lg = lg,
		.filename = logfile,
		.with_ids = true,
		.id = 1,
	};

	old_lg->add = COLnew(0, TYPE_int, 0, TRANSIENT);
	old_lg->del = COLnew(0, TYPE_int, 0, TRANSIENT);
	if (old_lg->add == NULL || old_lg->del == NULL) {
		TRC_CRITICAL(GDK, "cannot allocate temporary bats\n");
		goto bailout;
	}

	if (snprintf(filename, sizeof(filename), "%s%c%s%c", logdir, DIR_SEP, fn, DIR_SEP) >= FILENAME_MAX) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		goto bailout;
	}
	if (old_lg->lg->debug & 1) {
		fprintf(stderr, "#logger_new dir set to %s\n", old_lg->lg->dir);
	}

	if (logger_load(fn, filename, old_lg, fp, version) == GDK_SUCCEED) {
		return old_lg;
	}
	return NULL;

  bailout:
	logbat_destroy(old_lg->add);
	logbat_destroy(old_lg->del);
	GDKfree(old_lg);
	return NULL;
}

static gdk_return
old_logger_destroy(old_logger *lg)
{
	BUN p, q;
	BAT *b = NULL;
	const log_bid *bids;

	bat *subcommit = GDKmalloc(sizeof(log_bid) * (BATcount(lg->add) + BATcount(lg->del) + 1));
	if (subcommit == NULL) {
		TRC_CRITICAL(GDK, "logger_destroy failed\n");
		return GDK_FAIL;
	}
	int i = 0;
	subcommit[i++] = 0;

	bids = (const log_bid *) Tloc(lg->add, 0);
	BATloop(lg->add, p, q) {
		b = BATdescriptor(bids[p]);
		if (b) {
			BATmode(b, false);
			BBPunfix(bids[p]);
		}
		subcommit[i++] = bids[p];
	}
	bids = (const log_bid *) Tloc(lg->del, 0);
	BATloop(lg->del, p, q) {
		b = BATdescriptor(bids[p]);
		if (b) {
			BATmode(b, true);
			BBPunfix(bids[p]);
		}
		subcommit[i++] = bids[p];
	}
	/* give the catalog bats names so we can find them
	 * next time */
	char bak[IDLENGTH];
	if (BBPrename(lg->catalog_bid->batCacheid, NULL) < 0 ||
	    BBPrename(lg->catalog_nme->batCacheid, NULL) < 0 ||
	    BBPrename(lg->catalog_tpe->batCacheid, NULL) < 0 ||
	    BBPrename(lg->catalog_oid->batCacheid, NULL) < 0 ||
	    BBPrename(lg->dcatalog->batCacheid, NULL) < 0 ||
	    BBPrename(lg->snapshots_bid->batCacheid, NULL) < 0 ||
	    BBPrename(lg->snapshots_tid->batCacheid, NULL) < 0 ||
	    BBPrename(lg->dsnapshots->batCacheid, NULL) < 0 ||
	    strconcat_len(bak, sizeof(bak), lg->lg->fn, "_catalog_bid", NULL) >= sizeof(bak) ||
	    BBPrename(lg->lg->catalog_bid->batCacheid, bak) < 0 ||
	    strconcat_len(bak, sizeof(bak), lg->lg->fn, "_catalog_id", NULL) >= sizeof(bak) ||
	    BBPrename(lg->lg->catalog_id->batCacheid, bak) < 0 ||
	    strconcat_len(bak, sizeof(bak), lg->lg->fn, "_dcatalog", NULL) >= sizeof(bak) ||
	    BBPrename(lg->lg->dcatalog->batCacheid, bak) < 0) {
		return GDK_FAIL;
	}
	if (GDKmove(0, lg->lg->dir, LOGFILE, NULL, lg->lg->dir, LOGFILE, "bak") != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "logger_destroy failed\n");
		return GDK_FAIL;
	}
	if (logger_create_types_file(lg->lg, lg->filename) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "logger_destroy failed\n");
		return GDK_FAIL;
	}
	lg->lg->id = (ulng) lg->id;
	lg->lg->saved_id = lg->lg->id;
	if (TMsubcommit_list(subcommit, NULL, i, lg->lg->saved_id, lg->lg->saved_tid) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "logger_destroy failed\n");
		return GDK_FAIL;
	}
	snprintf(bak, sizeof(bak), "bak-" LLFMT, lg->id);
	if (GDKmove(0, lg->lg->dir, LOGFILE, "bak", lg->lg->dir, LOGFILE, bak) != GDK_SUCCEED) {
		TRC_CRITICAL(GDK, "logger_destroy failed\n");
		return GDK_FAIL;
	}

	if (logger_cleanup(lg) != GDK_SUCCEED)
		TRC_CRITICAL(GDK, "logger_cleanup failed\n");

	/* free resources */
	bids = (const log_bid *) Tloc(lg->catalog_bid, 0);
	BATloop(lg->catalog_bid, p, q) {
		bat bid = bids[p];
		oid pos = p;

		if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE)
			BBPrelease(bid);
	}
	bids = (const log_bid *) Tloc(lg->add, 0);
	BATloop(lg->add, p, q) {
		BBPrelease(bids[p]);
	}
	logbat_destroy(lg->add);
	bids = (const log_bid *) Tloc(lg->del, 0);
	BATloop(lg->del, p, q) {
		BBPrelease(bids[p]);
	}
	logbat_destroy(lg->del);

	logbat_destroy(lg->catalog_bid);
	logbat_destroy(lg->catalog_nme);
	logbat_destroy(lg->catalog_tpe);
	logbat_destroy(lg->catalog_oid);
	logbat_destroy(lg->dcatalog);
	logbat_destroy(lg->freed);
	logbat_destroy(lg->seqs_id);
	logbat_destroy(lg->seqs_val);
	logbat_destroy(lg->dseqs);
	logbat_destroy(lg->snapshots_bid);
	logbat_destroy(lg->snapshots_tid);
	logbat_destroy(lg->dsnapshots);

	GDKfree(lg);
	return GDK_SUCCEED;
}

/* Create a new logger */
gdk_return
old_logger_load(logger *lg, const char *fn, const char *logdir, FILE *fp, int version, const char *filename)
{
	old_logger *old_lg;
	old_lg = logger_new(lg, fn, logdir, fp, version, filename);
	if (old_lg == NULL)
		return GDK_FAIL;
	old_logger_destroy(old_lg);
	return GDK_SUCCEED;
}

/* Clean-up write-ahead log files already persisted in the BATs.
 * Update the LOGFILE and delete all bak- files as well.
 */
static gdk_return
logger_cleanup(old_logger *lg)
{
	char buf[BUFSIZ];
	FILE *fp = NULL;

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	if (snprintf(buf, sizeof(buf), "%s%s.bak-" LLFMT, lg->lg->dir, LOGFILE, lg->id) >= (int) sizeof(buf)) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		return GDK_FAIL;
	}

	if (lg->lg->debug & 1) {
		fprintf(stderr, "#logger_cleanup %s\n", buf);
	}

	lng lid = lg->id;
	// remove the last persisted WAL files as well to reduce the
	// work for the logger_cleanup_old()
	if ((fp = GDKfileopen(0, NULL, buf, NULL, "r")) == NULL) {
		GDKsyserror("cannot open file %s\n", buf);
		return GDK_FAIL;
	}

	while (lid-- > 0) {
		char log_id[FILENAME_MAX];

		if (snprintf(log_id, sizeof(log_id), LLFMT, lid) >= (int) sizeof(log_id)) {
			TRC_CRITICAL(GDK, "log_id filename is too large\n");
			fclose(fp);
			return GDK_FAIL;
		}
		if (GDKunlink(0, lg->lg->dir, LOGFILE, log_id) != GDK_SUCCEED) {
			/* not a disaster (yet?) if unlink fails */
			TRC_ERROR(GDK, "failed to remove old WAL %s.%s\n", LOGFILE, buf);
			GDKclrerr();
		}
	}
	fclose(fp);

	if (snprintf(buf, sizeof(buf), "bak-" LLFMT, lg->id) >= (int) sizeof(buf)) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		GDKclrerr();
	}

	if (GDKunlink(0, lg->lg->dir, LOGFILE, buf) != GDK_SUCCEED) {
		/* not a disaster (yet?) if unlink fails */
		TRC_ERROR(GDK, "failed to remove old WAL %s.%s\n", LOGFILE, buf);
		GDKclrerr();
	}

	return GDK_SUCCEED;
}

static gdk_return
logger_add_bat(old_logger *lg, BAT *b, const char *name, char tpe, oid id)
{
	log_bid bid = old_logger_find_bat(lg, name, tpe, id);
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
	if (lg->lg->debug & 1)
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


static gdk_return
logger_del_bat(old_logger *lg, log_bid bid)
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
		if (lg->lg->debug & 1)
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
	geomisoldversion = true;
}

bool
geomversion_get(void)
{
	return geomisoldversion;
}
