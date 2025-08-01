/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_logger.h"
#include "gdk_logger_internals.h"
#include "mutils.h"
#include <string.h>

static gdk_return log_add_bat(logger *lg, BAT *b, log_id id, int tid);
static gdk_return log_del_bat(logger *lg, log_bid bid);
/*
 * The logger uses a directory to store its log files. One master log
 * file stores information about the version of the logger and the
 * type mapping it uses. This file is a simple ascii file with the
 * following format:
 *  {6DIGIT-VERSION\n[id,type_name\n]*}
 * The transaction log files have a binary format.
 */

#define LOG_START	0
#define LOG_END		1
#define LOG_UPDATE_CONST	2
#define LOG_UPDATE_BULK	3
#define LOG_UPDATE	4
#define LOG_CREATE	5
#define LOG_DESTROY	6
#define LOG_SEQ		7
#define LOG_CLEAR	8	/* DEPRECATED */
#define LOG_BAT_GROUP	9
#define LOG_UPDATE_CB  10

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

#define LOG_DISABLED(lg) ((lg)->debug&128 || (lg)->inmemory || (lg)->flushnow)

static const char *log_commands[] = {
	"LOG_START",
	"LOG_END",
	"LOG_UPDATE_CONST",
	"LOG_UPDATE_BULK",
	"LOG_UPDATE",
	"LOG_CREATE",
	"LOG_DESTROY",
	"LOG_SEQ",
	"",			/* LOG_CLEAR IS DEPRECATED */
	"LOG_BAT_GROUP",
	"LOG_UPDATE_CB",
};

typedef struct logaction {
	int type;		/* type of change */
	lng nr;
	int tt;
	lng id;
	lng offset;
	log_id cid;		/* id of object */
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
	bte flag;
	int id;
} logformat;

typedef enum { LOG_OK, LOG_EOF, LOG_ERR } log_return;

static gdk_return bm_commit(logger *lg, logged_range *pending, uint32_t *updated, BUN maxupdated);
static gdk_return tr_grow(trans *tr);

#define log_lock(lg)	MT_lock_set(&(lg)->lock)
#define log_unlock(lg)	MT_lock_unset(&(lg)->lock)

static inline bte
find_type(logger *lg, int tpe)
{
	assert(tpe >= 0 && tpe < MAXATOMS);
	return lg->type_id[tpe];
}

static inline int
find_type_nr(logger *lg, bte tpe)
{
	int nr = lg->type_nr[tpe < 0 ? 256 + tpe : tpe];
	if (nr == 255)
		return -1;
	return nr;
}

static BUN
log_find(BAT *b, BAT *d, int val)
{
	BUN p;

	assert(b->ttype == TYPE_int);
	assert(d->ttype == TYPE_oid);
	BATiter bi = bat_iterator(b);
	if (BAThash(b) == GDK_SUCCEED) {
		MT_rwlock_rdlock(&b->thashlock);
		HASHloop_int(bi, b->thash, p, &val) {
			oid pos = p;
			if (BUNfnd(d, &pos) == BUN_NONE) {
				MT_rwlock_rdunlock(&b->thashlock);
				bat_iterator_end(&bi);
				return p;
			}
		}
		MT_rwlock_rdunlock(&b->thashlock);
	} else {		/* unlikely: BAThash failed */
		int *t = (int *) bi.base;

		for (p = 0; p < bi.count; p++) {
			if (t[p] == val) {
				oid pos = p;
				if (BUNfnd(d, &pos) == BUN_NONE) {
					bat_iterator_end(&bi);
					return p;
				}
			}
		}
	}
	bat_iterator_end(&bi);
	return BUN_NONE;
}

static log_bid
internal_find_bat(logger *lg, log_id id, int tid)
{
	BUN p;

	if (BAThash(lg->catalog_id) == GDK_SUCCEED) {
		BATiter cni = bat_iterator(lg->catalog_id);
		MT_rwlock_rdlock(&cni.b->thashlock);
		if (tid < 0) {
			HASHloop_int(cni, cni.b->thash, p, &id) {
				oid pos = p;
				if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE) {
					MT_rwlock_rdunlock(&cni.b->thashlock);
					bat_iterator_end(&cni);
					return *(log_bid *) Tloc(lg->catalog_bid, p);
				}
			}
		} else {
			BUN cp = BUN_NONE;
			HASHloop_int(cni, cni.b->thash, p, &id) {
				lng lid = *(lng *) Tloc(lg->catalog_lid, p);
				if (lid != lng_nil && lid <= tid) {
					break;
				}
				cp = p;
			}
			if (cp != BUN_NONE) {
				MT_rwlock_rdunlock(&cni.b->thashlock);
				bat_iterator_end(&cni);
				return *(log_bid *) Tloc(lg->catalog_bid, cp);
			}
		}
		MT_rwlock_rdunlock(&cni.b->thashlock);
		bat_iterator_end(&cni);
		return 0;	/* not found */
	}
	return -1;		/* error creating hash */
}

static inline void
logbat_destroy(BAT *b)
{
	BBPreclaim(b);
}

static BAT *
logbat_new(int tt, BUN size, role_t role)
{
	BAT *nb = COLnew(0, tt, size, role);

	if (nb) {
		BBP_pid(nb->batCacheid) = 0;
		if (role == PERSISTENT) {
			BATmode(nb, false);
			nb = BATsetaccess(nb, BAT_READ);
		}
	} else {
		TRC_CRITICAL(GDK, "creating new BAT[%s]#" BUNFMT " failed\n", ATOMname(tt), size);
	}
	return nb;
}

static bool
log_read_format(logger *lg, logformat *data)
{
	assert(!lg->inmemory);
	if (mnstr_read(lg->input_log, &data->flag, 1, 1) == 1) {
		if (mnstr_readInt(lg->input_log, &data->id) == 1)
			return true;
		/* could only read part, so complain */
		TRC_CRITICAL(GDK, "read failed\n");
	}
	return false;
}

static gdk_return
log_write_format(logger *lg, logformat *data)
{
	assert(data->id || data->flag);
	assert(!lg->inmemory);
	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR &&
	    mnstr_write(lg->current->output_log, &data->flag, 1, 1) == 1 &&
	    mnstr_writeInt(lg->current->output_log, data->id))
		return GDK_SUCCEED;
	/* error message is generated by caller */
	return GDK_FAIL;
}

static log_return
log_read_seq(logger *lg, logformat *l)
{
	int seq = l->id;
	lng val;
	BUN p;

	assert(!lg->inmemory);
	if (mnstr_readLng(lg->input_log, &val) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}
	if (lg->flushing)
		return LOG_OK;

	if ((p = log_find(lg->seqs_id, lg->dseqs, seq)) != BUN_NONE &&
	    p >= lg->seqs_id->batInserted) {
		assert(lg->seqs_val->hseqbase == 0);
		if (BUNreplace(lg->seqs_val, p, &val, true) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "replace of %s_seqs_val failed\n", lg->fn);
			return LOG_ERR;
		}
	} else {
		if (p != BUN_NONE) {
			oid pos = p;
			if (BUNappend(lg->dseqs, &pos, true) != GDK_SUCCEED) {
				TRC_CRITICAL(GDK, "append to %s_dseqs failed\n", lg->fn);
				return LOG_ERR;
			}
		}
		if (BUNappend(lg->seqs_id, &seq, true) != GDK_SUCCEED ||
		    BUNappend(lg->seqs_val, &val, true) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "append to %s_seqs_val/id failed\n", lg->fn);
			return LOG_ERR;
		}
	}
	return LOG_OK;
}

#if 0
static gdk_return
log_write_id(logger *lg, int id)
{
	assert(!lg->inmemory);
	assert(id >= 0);
	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR &&
	    mnstr_writeInt(lg->current->output_log, id))
		return GDK_SUCCEED;
	const char *err = mnstr_peek_error(lg->current->output_log);
	TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
	return GDK_FAIL;
}

static log_return
log_read_id(logger *lg, log_id *id)
{
	assert(!lg->inmemory);
	if (mnstr_readInt(lg->input_log, id) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}
	return LOG_OK;
}
#endif

static log_return
string_reader(logger *lg, BAT *b, lng nr)
{
	size_t sz = 0;
	lng SZ = 0;
	log_return res = LOG_OK;

	while (nr && res == LOG_OK) {
		if (mnstr_readLng(lg->input_log, &SZ) != 1) {
			TRC_CRITICAL(GDK, "read failed\n");
			return LOG_EOF;
		}
		sz = (size_t) SZ;
		char *buf = lg->rbuf;
		if (lg->rbufsize < sz) {
			if (!(buf = GDKrealloc(lg->rbuf, sz))) {
				TRC_CRITICAL(GDK, "couldn't grow string buffer\n");
				return LOG_ERR;
			}
			lg->rbuf = buf;
			lg->rbufsize = sz;
		}

		if (mnstr_read(lg->input_log, buf, sz, 1) != 1) {
			TRC_CRITICAL(GDK, "read failed\n");
			return LOG_EOF;
		}
		/* handle strings */
		char *t = buf;
		/* chunked */
#define CHUNK_SIZE 1024
		char *strings[CHUNK_SIZE];
		int cur = 0;

		for (; nr > 0 && res == LOG_OK && t < (buf + sz); nr--) {
			strings[cur++] = t;
			if (cur == CHUNK_SIZE &&
			    b &&
			    BUNappendmulti(b, strings, cur, true) != GDK_SUCCEED) {
				TRC_CRITICAL(GDK, "append to string bat failed\n");
				res = LOG_ERR;
			}
			if (cur == CHUNK_SIZE)
				cur = 0;
			/* find next */
			while (*t)
				t++;
			t++;
		}
		if (cur &&
		    b &&
		    BUNappendmulti(b, strings, cur, true) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "append to string bat failed\n");
			res = LOG_ERR;
		}
	}
	return res;
}


struct offset {
	lng os;		  /* offset within source BAT in logfile */
	lng nr;		  /* number of values to be copied */
	lng od;		  /* offset within destination BAT in database */
};

static log_return
log_read_updates(logger *lg, trans *tr, logformat *l, log_id id, BAT **cands, bool skip_entry)
{
	log_return res = LOG_OK;
	lng nr, pnr;
	bte type_id = -1;
	int tpe;

	assert(!lg->inmemory);
	TRC_DEBUG(WAL, "found %d %s", id, l->flag == LOG_UPDATE ? "update" : "update_buld");

	if (mnstr_readLng(lg->input_log, &nr) != 1 ||
	    mnstr_read(lg->input_log, &type_id, 1, 1) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}

	pnr = nr;
	tpe = find_type_nr(lg, type_id);
	if (tpe >= 0) {
		BAT *uid = NULL;
		BAT *r = NULL;
		void *(*rt)(ptr, size_t *, stream *, size_t) = BATatoms[tpe].atomRead;
		lng offset;

		assert(nr <= (lng) BUN_MAX);
		if (!lg->flushing && !skip_entry && l->flag == LOG_UPDATE) {
			uid = COLnew(0, TYPE_oid, (BUN) nr, PERSISTENT);
			if (uid == NULL) {
				TRC_CRITICAL(GDK, "creating bat failed\n");
				return LOG_ERR;
			}
		}

		if (l->flag == LOG_UPDATE_CONST) {
			if (mnstr_readLng(lg->input_log, &offset) != 1) {
				TRC_CRITICAL(GDK, "read failed\n");
				return LOG_EOF;
			}
			if (cands) {
				/* This const range actually represents a segment of candidates corresponding to updated bat entries */

				if (BATcount(*cands) == 0 || lg->flushing || skip_entry) {
					/* when flushing, we only need the offset and count of the last segment of inserts. */
					assert((*cands)->ttype == TYPE_void);
					BATtseqbase(*cands, (oid) offset);
					BATsetcount(*cands, (BUN) nr);
				} else if (!lg->flushing && !skip_entry) {
					assert(BATcount(*cands) > 0);
					BAT *dense = BATdense(0, (oid) offset, (BUN) nr);
					BAT *newcands = NULL;
					if (!dense) {
						TRC_CRITICAL(GDK, "creating bat failed\n");
						res = LOG_ERR;
					} else if ((*cands)->ttype == TYPE_void) {
						if ((newcands = BATmergecand(*cands, dense))) {
							BBPreclaim(*cands);
							*cands = newcands;
						} else {
							TRC_CRITICAL(GDK, "creating bat failed\n");
							res = LOG_ERR;
						}
					} else {
						assert((*cands)->ttype == TYPE_oid);
						assert(BATcount(*cands) > 0);
						if (BATappend(*cands, dense, NULL, true) != GDK_SUCCEED) {
							TRC_CRITICAL(GDK, "appending to bat failed\n");
							res = LOG_ERR;
						}
					}
					BBPreclaim(dense);
				}

				/* We have to read the value to update the read cursor */
				size_t tlen = lg->rbufsize;
				void *t = rt(lg->rbuf, &tlen, lg->input_log, 1);
				if (t == NULL) {
					TRC_CRITICAL(GDK, "read failed\n");
					res = LOG_EOF;
				}
				return res;
			}
		} else if (l->flag == LOG_UPDATE_CB) {
			if (cands) {
				bool append = (!lg->flushing && !skip_entry);
				BUN snr = (BUN) nr;
				BUN total = snr;

				if (append && (*cands)->ttype == TYPE_void) {
					BBPreclaim(*cands);
					*cands = COLnew(0, TYPE_oid, (BUN) nr, TRANSIENT);
				}
				oid *c = append?Tloc((*cands), 0):NULL;
				while (snr) {
					if (mnstr_readLng(lg->input_log, &nr) != 1 ||
					    mnstr_readLng(lg->input_log, &offset) != 1) {
						TRC_CRITICAL(GDK, "read failed\n");
						return LOG_EOF;
					}
					size_t tlen = lg->rbufsize;
					void *t = rt(lg->rbuf, &tlen, lg->input_log, 1);
					if (t == NULL) {
						TRC_CRITICAL(GDK, "read failed\n");
						return LOG_EOF;
					} else if (append) {
						lg->rbuf = t;
						lg->rbufsize = tlen;
						for (BUN p = 0; p < (BUN) nr; p++)
							*c++ = (oid) offset++;
					} else
						offset += nr;
					snr -= (BUN) nr;
				}
				if (append) {
					BATsetcount( *cands, total );
					(*cands)->tnonil = true;
					(*cands)->tnil = false;
					(*cands)->tseqbase = oid_nil;
					(*cands)->tkey = true;
					(*cands)->tsorted = true;
					(*cands)->trevsorted = false;
					(*cands)->tnorevsorted = 0;
					(*cands)->tmaxpos = (*cands)->tminpos = BUN_NONE;
				} else {
					assert((*cands)->ttype == TYPE_void);
					BATtseqbase(*cands, (oid) (offset - total));
					BATsetcount(*cands, total);
				}
				return res;
			}
		}

		if (!lg->flushing && !skip_entry) {
			r = COLnew(0, tpe, (BUN) nr, PERSISTENT);
			if (r == NULL) {
				if (uid)
					BBPreclaim(uid);
				return LOG_ERR;
			}
		}

		if (l->flag == LOG_UPDATE_CONST) {
			size_t tlen = lg->rbufsize;
			void *t = rt(lg->rbuf, &tlen, lg->input_log, 1);
			if (t == NULL) {
				TRC_CRITICAL(GDK, "read failed\n");
				res = LOG_EOF;
			} else {
				lg->rbuf = t;
				lg->rbufsize = tlen;
				if (r) {
					for (BUN p = 0; p < (BUN) nr; p++) {
						if (BUNappend(r, t, true) != GDK_SUCCEED) {
							TRC_CRITICAL(GDK, "append to bat failed\n");
							res = LOG_ERR;
						}
					}
				}
			}
		} else if (l->flag == LOG_UPDATE_CB) {
			BUN snr = (BUN) nr;

			if (r) {
				uid = COLnew(0, TYPE_oid, (BUN) nr, TRANSIENT);
				if (uid == NULL) {
					BBPreclaim(r);
					return LOG_ERR;
				}
			}
			oid *c = uid ? Tloc(uid, 0) : NULL;
			BUN total = snr;
			while (snr) {
				if (mnstr_readLng(lg->input_log, &nr) != 1 ||
				    mnstr_readLng(lg->input_log, &offset) != 1) {
					if (r)
						BBPreclaim(r);
					TRC_CRITICAL(GDK, "read failed\n");
					return LOG_EOF;
				}
				size_t tlen = lg->rbufsize;
				void *t = rt(lg->rbuf, &tlen, lg->input_log, 1);
				if (t == NULL) {
					TRC_CRITICAL(GDK, "read failed\n");
					res = LOG_EOF;
				} else {
					lg->rbuf = t;
					lg->rbufsize = tlen;
					if (r) {
						for (BUN p = 0; p < (BUN) nr; p++) {
							if (BUNappend(r, t, true) != GDK_SUCCEED) {
								TRC_CRITICAL(GDK, "append to bat failed\n");
								res = LOG_ERR;
							}
							*c++ = (oid) offset++;
						}
					} else
						offset += nr;
				}
				snr -= (BUN) nr;
			}
			if (uid) {
				BATsetcount( uid, total );
				uid->tnonil = true;
				uid->tnil = false;
				uid->tseqbase = oid_nil;
				uid->tkey = true;
				uid->tsorted = true;
				uid->trevsorted = false;
				uid->tnorevsorted = 0;
				uid->tmaxpos = uid->tminpos = BUN_NONE;
			}
			offset -= pnr;
			/* change into */
		} else if (l->flag == LOG_UPDATE_BULK) {
			if (mnstr_readLng(lg->input_log, &offset) != 1) {
				if (r)
					BBPreclaim(r);
				TRC_CRITICAL(GDK, "read failed\n");
				return LOG_EOF;
			}
			if (tpe == TYPE_msk) {
				if (r) {
					if (mnstr_readIntArray(lg->input_log, Tloc(r, 0), (size_t) ((nr + 31) / 32)))
						BATsetcount(r, (BUN) nr);
					else {
						TRC_CRITICAL(GDK, "read failed\n");
						res = LOG_EOF;
					}
				} else {
					size_t tlen = lg->rbufsize / sizeof(int);
					size_t cnt = 0, snr = (size_t) nr;
					snr = (snr + 31) / 32;
					assert(tlen);
					for (; res == LOG_OK && snr > 0; snr -= cnt) {
						cnt = snr > tlen ? tlen : snr;
						if (!mnstr_readIntArray(lg->input_log, lg->rbuf, cnt)) {
							TRC_CRITICAL(GDK, "read failed\n");
							res = LOG_EOF;
						}
					}
				}
			} else {
				if (!ATOMvarsized(tpe)) {
					size_t cnt = 0, snr = (size_t) nr;
					size_t tlen = lg->rbufsize / ATOMsize(tpe), ntlen = lg->rbufsize;
					assert(tlen);
					/* read in chunks of max
					 * BUFSIZE/width rows */
					for (; res == LOG_OK && snr > 0; snr -= cnt) {
						cnt = snr > tlen ? tlen : snr;
						void *t = rt(lg->rbuf, &ntlen, lg->input_log, cnt);

						if (t == NULL) {
							res = LOG_EOF;
							break;
						}
						assert(t == lg->rbuf);
						if (r && BUNappendmulti(r, t, cnt, true) != GDK_SUCCEED) {
							TRC_CRITICAL(GDK, "append to bat failed\n");
							res = LOG_ERR;
						}
					}
				} else if (tpe == TYPE_str) {
					/* efficient string */
					res = string_reader(lg, r, nr);
				} else {
					for (; res == LOG_OK && nr > 0; nr--) {
						size_t tlen = lg->rbufsize;
						void *t = rt(lg->rbuf, &tlen, lg->input_log, 1);

						if (t == NULL) {
							/* see if failure was due to
							 * malloc or something less
							 * serious (in the current
							 * context) */
							if (strstr(GDKerrbuf, "alloc") == NULL)
								res = LOG_EOF;
							else
								res = LOG_ERR;
							TRC_CRITICAL(GDK, "read failed\n");
						} else {
							lg->rbuf = t;
							lg->rbufsize = tlen;
							if (r && BUNappend(r, t, true) != GDK_SUCCEED) {
								TRC_CRITICAL(GDK, "append to bat failed\n");
								res = LOG_ERR;
							}
						}
					}
				}
			}
		} else {
			void *(*rh)(ptr, size_t *, stream *, size_t) = BATatoms[TYPE_oid].atomRead;
			void *hv = ATOMnil(TYPE_oid);
			offset = 0;

			if (hv == NULL) {
				TRC_CRITICAL(GDK, "read failed\n");
				res = LOG_EOF;
			}
			for (; res == LOG_OK && nr > 0; nr--) {
				size_t hlen = sizeof(oid);
				void *h = rh(hv, &hlen, lg->input_log, 1);
				if (h == NULL) {
					res = LOG_EOF;
					TRC_CRITICAL(GDK, "read failed\n");
					break;
				}
				assert(hlen == sizeof(oid));
				assert(h == hv);
				if ((uid && BUNappend(uid, h, true) != GDK_SUCCEED)) {
					TRC_CRITICAL(GDK, "append to bat failed\n");
					res = LOG_ERR;
				}
			}
			if (res == LOG_OK) {
				nr = pnr;
				if (tpe == TYPE_msk) {
					if (r) {
						if (mnstr_readIntArray(lg->input_log, Tloc(r, 0), (size_t) ((nr + 31) / 32)))
							BATsetcount(r, (BUN) nr);
						else {
							TRC_CRITICAL(GDK, "read failed\n");
							res = LOG_EOF;
						}
					} else {
						for (lng i = 0; i < nr; i += 32) {
							int v;
							switch (mnstr_readInt(lg->input_log, &v)) {
							case 1:
								continue;
							case 0:
								res = LOG_EOF;
								break;
							default:
								res = LOG_ERR;
								break;
							}
							TRC_CRITICAL(GDK, "read failed\n");
							break;
						}
					}
				} else if (tpe == TYPE_str) {
					/* efficient string */
					res = string_reader(lg, r, nr);
				} else {
					for (; res == LOG_OK && nr > 0; nr--) {
						size_t tlen = lg->rbufsize;
						void *t = rt(lg->rbuf, &tlen, lg->input_log, 1);

						if (t == NULL) {
							if (strstr(GDKerrbuf, "malloc") == NULL)
								res = LOG_EOF;
							else
								res = LOG_ERR;
							TRC_CRITICAL(GDK, "read failed\n");
						} else {
							lg->rbuf = t;
							lg->rbufsize = tlen;
							if ((r && BUNappend(r, t, true) != GDK_SUCCEED)) {
								TRC_CRITICAL(GDK, "append to bat failed\n");
								res = LOG_ERR;
							}
						}
					}
				}
			}
			GDKfree(hv);
		}

		if (res == LOG_OK && !skip_entry) {
			if (tr_grow(tr) == GDK_SUCCEED) {
				tr->changes[tr->nr].type = l->flag;
				if (l->flag == LOG_UPDATE_BULK && offset == -1) {
					assert(cands);	/* bat r is part of a group of bats logged together. */
					struct canditer ci;
					canditer_init(&ci, NULL, *cands);
					const oid first = canditer_peek(&ci);
					const oid last = canditer_last(&ci);
					offset = (lng) first;
					pnr = (lng) (last - first) + 1;
					if (!lg->flushing && !skip_entry) {
						assert(uid == NULL);
						uid = *cands;
						BBPfix((*cands)->batCacheid);
						tr->changes[tr->nr].type = LOG_UPDATE;
					}
				}
				if (uid && l->flag == LOG_UPDATE_CB) {
					assert(!cands);	/* TODO: This might change in the future. */
					tr->changes[tr->nr].type = LOG_UPDATE;
				} else if (l->flag == LOG_UPDATE_CONST || l->flag == LOG_UPDATE_CB) {
					assert(!cands);	/* TODO: This might change in the future. */
					tr->changes[tr->nr].type = LOG_UPDATE_BULK;
				}
				tr->changes[tr->nr].nr = pnr;
				tr->changes[tr->nr].tt = tpe;
				tr->changes[tr->nr].cid = id;
				tr->changes[tr->nr].offset = offset;
				tr->changes[tr->nr].b = r;
				tr->changes[tr->nr].uid = uid;
				tr->nr++;
			} else {
				TRC_CRITICAL(GDK, "memory allocation failed\n");
				res = LOG_ERR;
			}
		}
		if (res != LOG_OK) {
			if (r)
				BBPreclaim(r);
			if (cands && uid)
				BBPunfix((*cands)->batCacheid);
			else if (uid)
				BBPreclaim(uid);
		}
	} else {
		/* bat missing ERROR or ignore ? currently error. */
		TRC_CRITICAL(GDK, "unknown type\n");
		res = LOG_ERR;
	}
	return res;
}


static gdk_return
la_bat_update_count(logger *lg, log_id id, lng cnt, int tid)
{
	BATiter cni = bat_iterator_nolock(lg->catalog_id);

	if (BAThash(lg->catalog_id) == GDK_SUCCEED) {
		MT_rwlock_rdlock(&cni.b->thashlock);
		BUN p, cp = BUN_NONE;

		HASHloop_int(cni, cni.b->thash, p, &id) {
			lng lid = *(lng *) Tloc(lg->catalog_lid, p);

			if (lid != lng_nil && lid <= tid)
				break;
			cp = p;
		}
		if (cp != BUN_NONE) {
			lng ocnt = *(lng *) Tloc(lg->catalog_cnt, cp);
			assert(lg->catalog_cnt->hseqbase == 0);
			if (ocnt < cnt && BUNreplace(lg->catalog_cnt, cp, &cnt, false) != GDK_SUCCEED) {
				MT_rwlock_rdunlock(&cni.b->thashlock);
				return GDK_FAIL;
			}
		}
		MT_rwlock_rdunlock(&cni.b->thashlock);
		return GDK_SUCCEED;
	}
	return GDK_FAIL;
}

static gdk_return
la_bat_updates(logger *lg, logaction *la, int tid)
{
	log_bid bid = internal_find_bat(lg, la->cid, tid);
	BAT *b = NULL;

	if (bid < 0)
		return GDK_FAIL;
	if (!bid) {
		/* object already gone, nothing needed */
		return GDK_SUCCEED;
	}

	if (!lg->flushing) {
		b = BATdescriptor(bid);
		if (b == NULL)
			return GDK_FAIL;
	}
	BUN cnt = 0;
	if (la->type == LOG_UPDATE_BULK) {
		if (!lg->flushing) {
			cnt = BATcount(b);
			int is_msk = (b->ttype == TYPE_msk);
			/* handle offset 0 ie clear */
			if ( /* DISABLES CODE */ (0) && la->offset == 0 && cnt)
				BATclear(b, true);
			/* handle offset */
			if (cnt <= (BUN) la->offset) {
				msk t = 1;
				if (cnt < (BUN) la->offset) {	/* insert nils */
					const void *tv = (is_msk) ? &t : ATOMnilptr(b->ttype);
					lng i, d = la->offset - BATcount(b);
					for (i = 0; i < d; i++) {
						if (BUNappend(b, tv, true) != GDK_SUCCEED) {
							logbat_destroy(b);
							return GDK_FAIL;
						}
					}
				}
				if (BATcount(b) == (BUN) la->offset && BATappend(b, la->b, NULL, true) != GDK_SUCCEED) {
					logbat_destroy(b);
					return GDK_FAIL;
				}
			} else {
				BATiter vi = bat_iterator(la->b);
				BUN p, q;

				for (p = 0, q = (BUN) la->offset; p < (BUN) la->nr; p++, q++) {
					const void *t = BUNtail(vi, p);

					if (q < cnt) {
						if (b->tnosorted == q ||
						    b->tnosorted == q + 1)
							b->tnosorted = 0;
						if (b->tnorevsorted == q ||
						    b->tnorevsorted == q + 1)
							b->tnorevsorted = 0;
						if (b->tnokey[0] == q ||
						    b->tnokey[1] == q) {
							b->tnokey[0] = 0;
							b->tnokey[1] = 0;
						}
						if (b->tminpos == q)
							b->tminpos = BUN_NONE;
						if (b->tmaxpos == q)
							b->tmaxpos = BUN_NONE;
						b->tkey = false;
						b->tsorted = false;
						if (BUNreplace(b, q, t, true) != GDK_SUCCEED) {
							logbat_destroy(b);
							bat_iterator_end(&vi);
							return GDK_FAIL;
						}
					} else {
						if (BUNappend(b, t, true) != GDK_SUCCEED) {
							logbat_destroy(b);
							bat_iterator_end(&vi);
							return GDK_FAIL;
						}
					}
				}
				bat_iterator_end(&vi);
			}
		}
	} else if (la->type == LOG_UPDATE) {
		if (!lg->flushing && BATupdate(b, la->uid, la->b, true) != GDK_SUCCEED) {
			return GDK_FAIL;
		}
	}
	cnt = (BUN) (la->offset + la->nr);
	if (la_bat_update_count(lg, la->cid, cnt, tid) != GDK_SUCCEED) {
		if (b)
			logbat_destroy(b);
		return GDK_FAIL;
	}
	if (b)
		logbat_destroy(b);
	return GDK_SUCCEED;
}

static log_return
log_read_destroy(logger *lg, trans *tr, log_id id)
{
	(void) lg;
	assert(!lg->inmemory);
	if (tr_grow(tr) == GDK_SUCCEED) {
		tr->changes[tr->nr].type = LOG_DESTROY;
		tr->changes[tr->nr].cid = id;
		tr->nr++;
		return LOG_OK;
	}
	TRC_CRITICAL(GDK, "memory allocation failed\n");
	return LOG_ERR;
}

static gdk_return
la_bat_destroy(logger *lg, logaction *la, int tid)
{
	log_bid bid = internal_find_bat(lg, la->cid, tid);

	if (bid < 0)
		return GDK_FAIL;
	if (!bid) {
#ifndef NDEBUG
		GDKwarning("failed to find bid for object %d\n", la->cid);
#endif
		return GDK_SUCCEED;
	}
	if (bid && log_del_bat(lg, bid) != GDK_SUCCEED)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

static log_return
log_read_create(logger *lg, trans *tr, log_id id)
{
	bte tt;
	int tpe;

	assert(!lg->inmemory);
	TRC_DEBUG(WAL, "create %d", id);

	if (mnstr_read(lg->input_log, &tt, 1, 1) != 1) {
		TRC_CRITICAL(GDK, "read failed\n");
		return LOG_EOF;
	}

	tpe = find_type_nr(lg, tt);
	/* read create */
	if (tr_grow(tr) == GDK_SUCCEED) {
		tr->changes[tr->nr].type = LOG_CREATE;
		tr->changes[tr->nr].tt = tpe;
		tr->changes[tr->nr].cid = id;
		tr->nr++;
		return LOG_OK;
	}
	TRC_CRITICAL(GDK, "memory allocation failed\n");
	return LOG_ERR;
}

static gdk_return
la_bat_create(logger *lg, logaction *la, int tid)
{
	BAT *b;

	/* formerly head column type, should be void */
	if ((b = COLnew(0, la->tt, BATSIZE, PERSISTENT)) == NULL)
		return GDK_FAIL;

	if (la->tt < 0)
		BATtseqbase(b, 0);

	if ((b = BATsetaccess(b, BAT_READ)) == NULL ||
	    log_add_bat(lg, b, la->cid, tid) != GDK_SUCCEED) {
		logbat_destroy(b);
		return GDK_FAIL;
	}
	logbat_destroy(b);
	return GDK_SUCCEED;
}

static gdk_return
log_write_new_types(logger *lg, FILE *fp)
{
	bte id = 0;

	/* write types and insert into bats */
	memset(lg->type_id, -1, sizeof(lg->type_id));
	memset(lg->type_nr, 255, sizeof(lg->type_nr));
	/* first the fixed sized types */
	for (int i = 0; i < GDKatomcnt; i++) {
		if (ATOMvarsized(i))
			continue;
		lg->type_id[i] = id;
		lg->type_nr[id] = i;
		if (fprintf(fp, "%d,%s\n", id, BATatoms[i].name) < 0)
			return GDK_FAIL;
		id++;
	}
	/* second the var sized types */
	id = -127;		/* start after nil */
	for (int i = 0; i < GDKatomcnt; i++) {
		if (!ATOMvarsized(i))
			continue;
		lg->type_id[i] = id;
		lg->type_nr[256 + id] = i;
		if (fprintf(fp, "%d,%s\n", id, BATatoms[i].name) < 0)
			return GDK_FAIL;
		id++;
	}
	return GDK_SUCCEED;
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

static gdk_return
la_apply(logger *lg, logaction *c, int tid)
{
	gdk_return ret = GDK_SUCCEED;

	switch (c->type) {
	case LOG_UPDATE_BULK:
	case LOG_UPDATE:
		ret = la_bat_updates(lg, c, tid);
		break;
	case LOG_CREATE:
		if (!lg->flushing)
			ret = la_bat_create(lg, c, tid);
		break;
	case LOG_DESTROY:
		if (!lg->flushing)
			ret = la_bat_destroy(lg, c, tid);
		break;
	default:
		MT_UNREACHABLE();
	}
	return ret;
}

static void
la_destroy(logaction *c)
{
	if ((c->type == LOG_UPDATE || c->type == LOG_UPDATE_BULK) && c->b)
		logbat_destroy(c->b);
	if (c->type == LOG_UPDATE && c->uid)
		logbat_destroy(c->uid);
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
tr_abort_(logger *lg, trans *tr, int s)
{
	int i;

	(void) lg;

	TRC_DEBUG(WAL, "abort");

	for (i = s; i < tr->nr; i++)
		la_destroy(&tr->changes[i]);
	return tr_destroy(tr);
}

static trans *
tr_abort(logger *lg, trans *tr)
{
	return tr_abort_(lg, tr, 0);
}

static trans *
tr_commit(logger *lg, trans *tr, time_t *t)
{
	int i;

	TRC_INFO(WAL, "apply %d changes\n", tr->nr);
	time_t t0 = *t;

	for (i = 0; i < tr->nr; i++) {
		if (t0) {
			TRC_INFO_IF(WAL) {
				time_t t1 = time(NULL);
				/* not more than once every 10 seconds */
				if (t1 - t0 > 10) {
					t0 = t1;
					lng fpos = (lng) getfilepos(getFile(lg->input_log));
					if (fpos >= 0) {
						TRC_INFO_ENDIF(WAL, "still applying changes (%d%% done)\n",
							       i * 100 / tr->nr);
						GDKtracer_flush_buffer();
					}
				}
			}
		}
		if (la_apply(lg, &tr->changes[i], tr->tid) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "aborting transaction\n");
			do {
				tr = tr_abort_(lg, tr, i);
			} while (tr != NULL);
			return (trans *) -1;
		}
		la_destroy(&tr->changes[i]);
	}
	*t = t0;
	lg->saved_tid = tr->tid;
	return tr_destroy(tr);
}

static gdk_return
log_read_types_file(logger *lg, FILE *fp, int version, bool *needsnew)
{
	int id = 0;
	char atom_name[IDLENGTH];
	bool seen_geom = false;

	/* scanf should use IDLENGTH somehow */
	while (fscanf(fp, "%d,%63s\n", &id, atom_name) == 2) {
		if (version < 52303 && strcmp(atom_name, "BAT") == 0) {
			*needsnew = true;
			continue;
		}
		if (version < 52304 && strcmp(atom_name, "color") == 0) {
			*needsnew = true;
			continue;
		}
		if (version < 52304 && strcmp(atom_name, "identifier") == 0) {
			*needsnew = true;
			continue;
		}
		if (version < 52304 && strcmp(atom_name, "wkba") == 0) {
			*needsnew = true;
			continue;
		}
		int i = ATOMindex(atom_name);

		if (id < -127 || id > 127 || i < 0) {
			GDKerror("unknown type in log file '%s'\n", atom_name);
			return GDK_FAIL;
		}
		seen_geom |= strcmp(atom_name, "mbr") == 0 || strcmp(atom_name, "wkb") == 0;
		lg->type_id[i] = (int8_t) id;
		lg->type_nr[id < 0 ? 256 + id : id] = i;
	}
#ifdef HAVE_GEOM
	if (!seen_geom && ATOMindex("mbr") > 0) {
		GDKerror("incompatible database: server supports GEOM, but database does not\n");
		return GDK_FAIL;
	}
#endif
	(void) seen_geom;
	return GDK_SUCCEED;
}


static gdk_return
log_create_types_file(logger *lg, const char *filename)
{
	FILE *fp;

	if ((fp = MT_fopen(filename, "w")) == NULL) {
		GDKerror("cannot create log file %s\n", filename);
		return GDK_FAIL;
	}
	if (fprintf(fp, "%06d\n\n", lg->version) < 0) {
		fclose(fp);
		GDKerror("writing log file %s failed", filename);
		if (MT_remove(filename) < 0)
			GDKsyserror("remove %s failed\n", filename);
		return GDK_FAIL;
	}

	if (log_write_new_types(lg, fp) != GDK_SUCCEED) {
		fclose(fp);
		GDKerror("writing log file %s failed", filename);
		if (MT_remove(filename) < 0)
			GDKsyserror("remove %s failed\n", filename);
		return GDK_FAIL;
	}
	if (fflush(fp) < 0 || (!(ATOMIC_GET(&GDKdebug) & NOSYNCMASK)
#if defined(_MSC_VER)
			       && _commit(_fileno(fp)) < 0
#elif defined(HAVE_FDATASYNC)
			       && fdatasync(fileno(fp)) < 0
#elif defined(HAVE_FSYNC)
			       && fsync(fileno(fp)) < 0
#endif
	    )) {
		GDKsyserror("flushing log file %s failed", filename);
		fclose(fp);
		if (MT_remove(filename) < 0)
			GDKsyserror("remove %s failed\n", filename);
		return GDK_FAIL;
	}
	if (fclose(fp) < 0) {
		GDKsyserror("closing log file %s failed", filename);
		if (MT_remove(filename) < 0)
			GDKsyserror("remove %s failed\n", filename);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

#define rotation_lock(lg)	MT_lock_set(&(lg)->rotation_lock)
#define rotation_unlock(lg)	MT_lock_unset(&(lg)->rotation_lock)
#define rotation_trylock(lg, ms) MT_lock_trytime(&(lg)->rotation_lock, ms)

static gdk_return
log_open_output(logger *lg)
{
	logged_range *new_range = (logged_range *) GDKmalloc(sizeof(logged_range));

	if (!new_range) {
		TRC_CRITICAL(GDK, "allocation failure\n");
		return GDK_FAIL;
	}
	if (!LOG_DISABLED(lg)) {
		char id[32];
		char filename[MAXPATH];

		if (snprintf(id, sizeof(id), LLFMT, lg->id) >= (int) sizeof(id)) {
			TRC_CRITICAL(GDK, "filename is too large\n");
			GDKfree(new_range);
			return GDK_FAIL;
		}
		if (GDKfilepath(filename, sizeof(filename), BBPselectfarm(PERSISTENT, 0, offheap), lg->dir, LOGFILE, id) != GDK_SUCCEED) {
			GDKfree(new_range);
			return GDK_FAIL;
		}

		TRC_INFO(WAL, "opening %s.%s", LOGFILE, id);
		new_range->output_log = open_wstream(filename);
		if (new_range->output_log) {
			short byteorder = 1234;
			mnstr_write(new_range->output_log, &byteorder, sizeof(byteorder), 1);
		}

		if (new_range->output_log == NULL || mnstr_errnr(new_range->output_log) != MNSTR_NO__ERROR) {
			TRC_CRITICAL(GDK, "creating %s failed: %s\n", filename, mnstr_peek_error(NULL));
			close_stream(new_range->output_log);
			GDKfree(new_range);
			return GDK_FAIL;
		}
	} else {
		new_range->output_log = NULL;
	}
	ATOMIC_INIT(&new_range->refcount, 1);
	ATOMIC_INIT(&new_range->last_ts, 0);
	ATOMIC_INIT(&new_range->flushed_ts, 0);
	ATOMIC_INIT(&new_range->drops, 0);
	new_range->id = lg->id;
	new_range->next = NULL;
	logged_range *current = lg->current;
	assert(current && current->next == NULL);
	new_range->cnt = current->cnt;
	current->next = new_range;
	lg->file_age = GDKusec();
	return GDK_SUCCEED;
}

static inline void
log_close_input(logger *lg)
{
	if (!lg->inmemory && lg->input_log) {
		TRC_DEBUG(WAL, "closing input log %s", mnstr_name(lg->input_log));
		close_stream(lg->input_log);
	}
	lg->input_log = NULL;
}

static inline void
log_close_output(logger *lg)
{
	if (!LOG_DISABLED(lg) && lg->current->output_log) {
		TRC_INFO(WAL, "closing output log %s", mnstr_name(lg->current->output_log));
		close_stream(lg->current->output_log);
	}
	lg->current->output_log = NULL;
}

static gdk_return
log_open_input(logger *lg, const char *filename, bool *filemissing)
{
	TRC_DEBUG(WAL, "opening input log %s", filename);
	lg->input_log = open_rstream(filename);

	/* if the file doesn't exist, there is nothing to be read back */
	if (lg->input_log == NULL || mnstr_errnr(lg->input_log) != MNSTR_NO__ERROR) {
		log_close_input(lg);
		*filemissing = true;
		return GDK_SUCCEED;
	}
	short byteorder;
	switch (mnstr_read(lg->input_log, &byteorder, sizeof(byteorder), 1)) {
	case -1:
		log_close_input(lg);
		TRC_CRITICAL(GDK, "read failed\n");
		return GDK_FAIL;
	case 0:
		/* empty file is ok */
		log_close_input(lg);
		return GDK_SUCCEED;
	case 1:
		/* if not empty, must start with correct byte order mark */
		if (byteorder != 1234) {
			TRC_CRITICAL(GDK, "incorrect byte order word in file %s\n", filename);
			log_close_input(lg);
			return GDK_FAIL;
		}
		break;
	}
	return GDK_SUCCEED;
}

static log_return
log_read_transaction(logger *lg, BAT *ids_to_omit, uint32_t *updated, BUN maxupdated, time_t *t)
{
	logformat l;
	trans *tr = NULL;
	log_return err = LOG_OK;
	bool ok = true;
	bool skip_entry = false;
	ATOMIC_BASE_TYPE dbg = ATOMIC_GET(&GDKdebug);
	time_t t0 = 0;
	size_t fs = 0;

	(void) maxupdated;	/* only used inside assert() */

	if (!lg->flushing)
		ATOMIC_AND(&GDKdebug, ~CHECKMASK);

	BAT *cands = NULL;	/* used in case of LOG_BAT_GROUP */

	if (t) {
		t0 = *t;
		fs = getFileSize(lg->input_log);
	}

	while (err == LOG_OK && (ok = log_read_format(lg, &l))) {
		if (t) {
			TRC_INFO_IF(WAL) {
				time_t t1 = time(NULL);
				/* not more than once every 10 seconds */
				if (t1 - t0 > 10) {
					t0 = t1;
					lng fpos = (lng) getfilepos(getFile(lg->input_log));
					if (fpos >= 0) {
						TRC_INFO_ENDIF(WAL, "still reading write-ahead log \"%s\" (%d%% done)\n",
							       mnstr_name(lg->input_log), (int) ((fpos * 100 + 50) / fs));
						GDKtracer_flush_buffer();
					}
				}
			}
		}
		if (l.flag == 0 && l.id == 0) {
			err = LOG_EOF;
			break;
		}

		TRC_DEBUG_IF(WAL) {
			if (l.flag > 0 && l.flag != LOG_CLEAR &&
			    l.flag < (bte) (sizeof(log_commands) / sizeof(log_commands[0])))
				TRC_DEBUG_ENDIF(WAL, "%s %d", log_commands[(int) l.flag], l.id);
			else
				TRC_DEBUG_ENDIF(WAL, "%d %d", l.flag, l.id);
		}
		skip_entry = (ids_to_omit && BUNfnd(ids_to_omit, &l.id) != BUN_NONE);
		switch (l.flag) {
		case LOG_UPDATE_CB:
		case LOG_UPDATE_CONST:
		case LOG_UPDATE_BULK:
		case LOG_UPDATE:
			if (skip_entry)
				break;
			/* fall through */
		case LOG_CREATE:
		case LOG_DESTROY:
			if (tr != NULL && updated && BAThash(lg->catalog_id) == GDK_SUCCEED) {
				BATiter cni = bat_iterator(lg->catalog_id);
				BUN p;
				BUN posnew = BUN_NONE;
				BUN posold = BUN_NONE;
				MT_rwlock_rdlock(&cni.b->thashlock);
				HASHloop_int(cni, cni.b->thash, p, &l.id) {
					lng lid = *(lng *) Tloc(lg->catalog_lid, p);
					if (lid == lng_nil || lid > tr->tid)
						posnew = p;
					else if (lid == tr->tid)
						posold = p;
				}
				MT_rwlock_rdunlock(&cni.b->thashlock);
				bat_iterator_end(&cni);
				/* Normally at this point, posnew is the
				 * location of the bat that this
				 * transaction is working on, and posold
				 * is the location of the previous
				 * version of the bat.  If LOG_CREATE,
				 * both are relevant, since the latter
				 * is the new bat, and the former is the
				 * to-be-destroyed bat.  For
				 * LOG_DESTROY, only posnew should be
				 * relevant, but for the other types, if
				 * the table is destroyed later in the
				 * same transaction, we need posold, and
				 * else (the normal case) we need
				 * posnew. */
				if (posnew != BUN_NONE) {
					assert(posnew < maxupdated);
					updated[posnew / 32] |= 1U << (posnew % 32);
				}
				if ((l.flag == LOG_CREATE || posnew == BUN_NONE) && posold != BUN_NONE) {
					assert(posold < maxupdated);
					updated[posold / 32] |= 1U << (posold % 32);
				}
			}
			break;
		default:
			/* do nothing */
			break;
		}
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
			assert(!lg->flushing || l.id <= lg->tid);
			if (!lg->flushing && l.id > lg->tid)
				lg->tid = l.id;	/* should only happen during initialization */
			if ((tr = tr_create(tr, l.id)) == NULL) {
				TRC_CRITICAL(GDK, "memory allocation failed\n");
				err = LOG_ERR;
				break;
			}
			TRC_DEBUG(WAL, "tstart %d\n", tr->tid);
			break;
		case LOG_END:
			if (tr == NULL)
				err = LOG_EOF;
			else if (tr->tid != l.id)	/* abort record */
				tr = tr_abort(lg, tr);
			else
				tr = tr_commit(lg, tr, &t0);
			break;
		case LOG_SEQ:
			err = log_read_seq(lg, &l);
			break;
		case LOG_UPDATE_CB:
		case LOG_UPDATE_CONST:
		case LOG_UPDATE_BULK:
		case LOG_UPDATE:
			if (tr == NULL)
				err = LOG_EOF;
			else {
				err = log_read_updates(lg, tr, &l, l.id, cands ? &cands : NULL, skip_entry);
			}
			break;
		case LOG_CREATE:
			if (tr == NULL)
				err = LOG_EOF;
			else
				err = log_read_create(lg, tr, l.id);
			break;
		case LOG_DESTROY:
			if (tr == NULL)
				err = LOG_EOF;
			else
				err = log_read_destroy(lg, tr, l.id);
			break;
		case LOG_BAT_GROUP:
			if (tr == NULL)
				err = LOG_EOF;
			else {
				if (l.id > 0) {
					/* START OF LOG_BAT_GROUP */
					cands = COLnew(0, TYPE_void, 0, SYSTRANS);
					if (!cands) {
						TRC_CRITICAL(GDK, "creating bat failed\n");
						err = LOG_ERR;
					}
				} else if (cands == NULL) {
					/* should have gone through the
					 * above option earlier */
					TRC_CRITICAL(GDK, "unexpected error\n");
					err = LOG_ERR;
				} else {
					/* END OF LOG_BAT_GROUP */
					BBPunfix(cands->batCacheid);
					cands = NULL;
				}
			}
			break;
		default:
			TRC_CRITICAL(GDK, "unrecognized log entry %d", l.flag);
			err = LOG_ERR;
		}
		if (tr == (trans *) -1) {
			/* message already generated by tr_commit */
			err = LOG_ERR;
			tr = NULL;
			break;
		}
	}
	while (tr) {
		TRC_WARNING(GDK, "aborting transaction\n");
		tr = tr_abort(lg, tr);
	}
	if (!lg->flushing)
		ATOMIC_SET(&GDKdebug, dbg);

	BBPreclaim(cands);
	if (t)
		*t = t0;
	if (!ok)
		return LOG_EOF;
	return err;
}

static gdk_return
log_readlog(logger *lg, const char *filename, BAT *ids_to_omit, bool *filemissing)
{
	log_return err = LOG_OK;
	time_t t0;

	assert(!lg->inmemory);

	gdk_return res = log_open_input(lg, filename, filemissing);
	if (!lg->input_log || res != GDK_SUCCEED)
		return res;
	t0 = time(NULL);
	TRC_INFO_IF(WAL) {
		TRC_INFO_ENDIF(WAL, "Start reading the write-ahead log '%s'\n", filename);
		GDKtracer_flush_buffer();
	}
	while (err != LOG_EOF && err != LOG_ERR) {
		err = log_read_transaction(lg, ids_to_omit, NULL, 0, &t0);
	}
	log_close_input(lg);
	lg->input_log = NULL;

	/* remaining transactions are not committed, ie abort */
	TRC_INFO_IF(WAL) {
		TRC_INFO_ENDIF(WAL, "Finished reading the write-ahead log '%s'\n", filename);
		GDKtracer_flush_buffer();
	}
	/* we cannot distinguish errors from incomplete transactions
	 * (even if we would log aborts in the logs). So we simply
	 * abort and move to the next log file */
	return err == LOG_ERR ? GDK_FAIL : GDK_SUCCEED;
}

static gdk_return
read_omitted_ids(const char *filename, BAT **ids_to_omit)
{
	gdk_return ret = GDK_FAIL;
	BAT *ids_bat = NULL;
	FILE *f = fopen(filename, "r");
	if (!f) {
		if (errno != ENOENT) {
			GDKsyserror("fopen %s failed\n", filename);
			goto end;
		}
		/* file does not exist, return NULL. */
		ids_bat = NULL;
		ret = GDK_SUCCEED;
		goto end;
	}

	ids_bat = COLnew(0, TYPE_int, 0, TRANSIENT);
	if (!ids_bat) {
		GDKerror("read_omitted_ids: cannot create bat");
		goto end;
	}
	while (1) {
		int id;
		if (fscanf(f, "%d", &id) == 1) {
			if (BUNappend(ids_bat, &id, true) != GDK_SUCCEED) {
				GDKerror("read_omitted_ids: cannot append to bat");
				goto end;
			}
		} else {
			break;
		}
	}
	if (ferror(f)) {
		GDKsyserror("fscanf %s failed\n", filename);
		goto end;
	}

	ret = GDK_SUCCEED;
end:
	if (f)
		fclose(f);
	if (ret == GDK_SUCCEED)
		*ids_to_omit = ids_bat;
	else if (ids_bat)
		BBPunfix(ids_bat->batCacheid);
	return ret;
}

/*
 * The log files are incrementally numbered, starting from 2. They are
 * processed in the same sequence.
 */
static gdk_return
log_readlogs(logger *lg, const char *filename)
{
	gdk_return ret = GDK_FAIL;
	char log_filename[FILENAME_MAX];
	BAT *ids_to_omit = NULL;

	assert(!lg->inmemory);
	TRC_DEBUG(WAL, "logger id is " LLFMT " last logger id is " LLFMT "\n", lg->id, lg->saved_id);

	if (snprintf(log_filename, sizeof(log_filename), "%s.omitted", filename) >= FILENAME_MAX) {
		GDKerror("Logger filename path is too large\n");
		goto end;
	}
	if (read_omitted_ids(log_filename, &ids_to_omit) != GDK_SUCCEED)
		goto end;

	if (lg->saved_id >= lg->id) {
		bool filemissing = false;

		lg->id = lg->saved_id + 1;
		gdk_return res = GDK_SUCCEED;
		while (res == GDK_SUCCEED && !filemissing) {
			if (snprintf(log_filename, sizeof(log_filename), "%s." LLFMT, filename, lg->id) >= FILENAME_MAX) {
				GDKerror("Logger filename path is too large\n");
				goto end;
			}
			res = log_readlog(lg, log_filename, ids_to_omit, &filemissing);
			if (!filemissing) {
				lg->saved_id++;
				lg->id++;
			}
		}
	}

	ret = GDK_SUCCEED;
end:
	if (ids_to_omit)
		BBPunfix(ids_to_omit->batCacheid);
	return ret;
}

static gdk_return
log_commit(logger *lg, logged_range *pending, uint32_t *updated, BUN maxupdated)
{
	TRC_DEBUG(WAL, "commit");

	return bm_commit(lg, pending, updated, maxupdated);
}

static gdk_return
check_version(logger *lg, FILE *fp, bool *needsnew)
{
	int version = 0;

	assert(!lg->inmemory);
	if (fscanf(fp, "%6d", &version) != 1) {
		GDKerror("Could not read the version number from the file '%s/log'.\n", lg->dir);
		fclose(fp);
		return GDK_FAIL;
	}
	if (version != lg->version) {
		if (lg->prefuncp == NULL ||
		    (*lg->prefuncp) (lg->funcdata, version, lg->version) != GDK_SUCCEED) {
			GDKerror("Incompatible database version %06d, "
				 "this server supports version %06d.\n%s",
				 version, lg->version,
				 version < lg->version ? "Maybe you need to upgrade to an intermediate release first.\n" : "");
			fclose(fp);
			return GDK_FAIL;
		}
		*needsnew = true;	/* we need to write a new log file */
	} else {
		lg->postfuncp = NULL;	/* don't call */
		*needsnew = false;	/* log file already up-to-date */
	}
	if (fgetc(fp) != '\n' ||	/* skip \n */
	    fgetc(fp) != '\n') {	/* skip \n */
		GDKerror("Badly formatted log file");
		fclose(fp);
		return GDK_FAIL;
	}
	if (log_read_types_file(lg, fp, version, needsnew) != GDK_SUCCEED) {
		fclose(fp);
		return GDK_FAIL;
	}
	fclose(fp);
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
log_switch_bat(BAT *old, BAT *new, const char *fn, const char *name)
{
	char bak[IDLENGTH];

	if (BATmode(old, true) != GDK_SUCCEED) {
		GDKerror("cannot convert old %s to transient", name);
		return GDK_FAIL;
	}
	if (strconcat_len(bak, sizeof(bak), fn, "_", name, NULL) >= sizeof(bak)) {
		GDKerror("name %s_%s too long\n", fn, name);
		return GDK_FAIL;
	}
	if (BBPrename(old, NULL) != 0 || BBPrename(new, bak) != 0) {
		GDKerror("rename (%s) failed\n", bak);
		return GDK_FAIL;
	}
	BBPretain(new->batCacheid);
	return GDK_SUCCEED;
}

static gdk_return
bm_get_counts(logger *lg)
{
	BUN p, q;
	const log_bid *bids = (const log_bid *) Tloc(lg->catalog_bid, 0);

	BATloop(lg->catalog_bid, p, q) {
		oid pos = p;
		lng cnt = 0;
		lng lid = lng_nil;

		if (BUNfnd(lg->dcatalog, &pos) == BUN_NONE) {
			BAT *b = BBPquickdesc(bids[p]);
			assert(b);
			cnt = BATcount(b);
		} else {
			lid = BBP_desc(bids[p])->batCacheid != 0 && log_find(lg->catalog_bid, lg->dcatalog, bids[p]) == BUN_NONE ? 1 : -1;
		}
		if (BUNappend(lg->catalog_cnt, &cnt, false) != GDK_SUCCEED)
			return GDK_FAIL;
		if (BUNappend(lg->catalog_lid, &lid, false) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

static int
subcommit_list_add(int next, bat *n, BUN *sizes, bat bid, BUN sz)
{
	assert(sz <= BBP_desc(bid)->batCount || sz == BUN_NONE);
	for (int i = 0; i < next; i++) {
		if (n[i] == bid) {
			sizes[i] = sz;
			return next;
		}
	}
	n[next] = bid;
	sizes[next++] = sz;
	return next;
}

static int
cleanup_and_swap(logger *lg, int *r, const log_bid *bids, lng *lids, lng *cnts,
		 BAT *catalog_bid, BAT *catalog_id, BAT *dcatalog, BUN cleanup)
{
	BAT *nbids, *noids, *ncnts, *nlids, *ndels;
	BUN p, q;
	int err = 0, rcnt = 0;

	BUN ocnt = BATcount(catalog_bid);
	nbids = logbat_new(TYPE_int, ocnt - cleanup, PERSISTENT);
	noids = logbat_new(TYPE_int, ocnt - cleanup, PERSISTENT);
	ncnts = logbat_new(TYPE_lng, ocnt - cleanup, SYSTRANS);
	nlids = logbat_new(TYPE_lng, ocnt - cleanup, SYSTRANS);
	ndels = logbat_new(TYPE_oid, BATcount(dcatalog) - cleanup, PERSISTENT);

	if (nbids == NULL || noids == NULL || ncnts == NULL || nlids == NULL || ndels == NULL) {
		logbat_destroy(nbids);
		logbat_destroy(noids);
		logbat_destroy(ncnts);
		logbat_destroy(nlids);
		logbat_destroy(ndels);
		return 0;
	}

	oid *poss = Tloc(dcatalog, 0);
	BATloop(dcatalog, p, q) {
		oid pos = poss[p];

		if (lids[pos] == lng_nil || lids[pos] > lg->saved_tid)
			continue;

		if (lids[pos] >= 0) {
			bat bid = bids[pos];
			BAT *lb = BBP_desc(bid);

			if (lb->batCacheid == 0 || BATmode(lb, true /*transient */ ) != GDK_SUCCEED) {
				GDKwarning("Failed to set bat(%d) transient\n", bid);
			} else {
				lids[pos] = -1;	/* mark as transient */
				r[rcnt++] = bid;
			}
		}
	}

	int *oids = (int *) Tloc(catalog_id, 0);
	q = BATcount(catalog_bid);
	for (p = 0; p < q && !err; p++) {
		bat col = bids[p];
		int nid = oids[p];
		lng lid = lids[p];
		lng cnt = cnts[p];
		oid pos = p;

		/* only project out the deleted with lid == -1
		 * update dcatalog */
		if (lid == -1)
			continue;	/* remove */

		if (BUNappend(nbids, &col, true) != GDK_SUCCEED ||
		    BUNappend(noids, &nid, true) != GDK_SUCCEED ||
		    BUNappend(nlids, &lid, false) != GDK_SUCCEED ||
		    BUNappend(ncnts, &cnt, false) != GDK_SUCCEED)
			err = 1;
		if (BUNfnd(lg->dcatalog, &pos) != BUN_NONE) {
			pos = (oid) (BATcount(nbids) - 1);
			if (BUNappend(ndels, &pos, true) != GDK_SUCCEED)
				err = 1;
		}
	}

	if (err) {
		logbat_destroy(nbids);
		logbat_destroy(noids);
		logbat_destroy(ndels);
		logbat_destroy(ncnts);
		logbat_destroy(nlids);
		return 0;
	}
	/* point of no return */
	if (log_switch_bat(catalog_bid, nbids, lg->fn, "catalog_bid") != GDK_SUCCEED ||
	    log_switch_bat(catalog_id, noids, lg->fn, "catalog_id") != GDK_SUCCEED ||
	    log_switch_bat(dcatalog, ndels, lg->fn, "dcatalog") != GDK_SUCCEED) {
		logbat_destroy(nbids);
		logbat_destroy(noids);
		logbat_destroy(ndels);
		logbat_destroy(ncnts);
		logbat_destroy(nlids);
		return -1;
	}
	r[rcnt++] = lg->catalog_bid->batCacheid;
	r[rcnt++] = lg->catalog_id->batCacheid;
	r[rcnt++] = lg->dcatalog->batCacheid;

	assert(BATcount(lg->dcatalog) - cleanup == BATcount(ndels));

	logbat_destroy(lg->catalog_bid);
	logbat_destroy(lg->catalog_id);
	logbat_destroy(lg->dcatalog);

	lg->catalog_bid = nbids;
	lg->catalog_id = noids;
	lg->dcatalog = ndels;

	/* failing to rename these two bats is not fatal */
	if (BBPrename(lg->catalog_cnt, NULL) != GDK_SUCCEED)
		GDKclrerr();
	if (BBPrename(lg->catalog_lid, NULL) != GDK_SUCCEED)
		GDKclrerr();
	BBPunfix(lg->catalog_cnt->batCacheid);
	BBPunfix(lg->catalog_lid->batCacheid);

	lg->catalog_cnt = ncnts;
	lg->catalog_lid = nlids;
	char bak[FILENAME_MAX];
	strconcat_len(bak, sizeof(bak), lg->fn, "_catalog_cnt", NULL);
	if (BBPrename(lg->catalog_cnt, bak) < 0)
		GDKclrerr();
	strconcat_len(bak, sizeof(bak), lg->fn, "_catalog_lid", NULL);
	if (BBPrename(lg->catalog_lid, bak) < 0)
		GDKclrerr();
	rotation_lock(lg);
	for (logged_range *p = lg->pending; p; p = p->next) {
		p->cnt -= cleanup;
	}
	rotation_unlock(lg);
	return rcnt;
}

/* this function is called with log_lock() held; it releases the lock
 * before returning */
static gdk_return
bm_subcommit(logger *lg, logged_range *pending, uint32_t *updated, BUN maxupdated)
{
	BUN cnt = pending ? pending->cnt : BATcount(lg->catalog_bid);
	BUN dcnt = BATcount(lg->dcatalog);
	BUN p, q;
	BAT *catalog_bid = lg->catalog_bid;
	BAT *catalog_id = lg->catalog_id;
	BAT *dcatalog = lg->dcatalog;
	BUN nn = 13 + cnt;
	bat *n = GDKmalloc(sizeof(bat) * nn);
	bat *r = GDKmalloc(sizeof(bat) * nn);
	BUN *sizes = GDKmalloc(sizeof(BUN) * nn);
	int i = 0, rcnt = 0;
	gdk_return res;
	const log_bid *bids;
	lng *cnts = NULL, *lids = NULL;
	BUN cleanup = 0;
	lng t0 = 0;

	if (n == NULL || r == NULL || sizes == NULL) {
		GDKfree(n);
		GDKfree(r);
		GDKfree(sizes);
		log_unlock(lg);
		return GDK_FAIL;
	}

	sizes[i] = 0;
	n[i++] = 0;		/* n[0] is not used */
	bids = (const log_bid *) Tloc(catalog_bid, 0);
	if (lg->catalog_cnt)
		cnts = (lng *) Tloc(lg->catalog_cnt, 0);
	if (lg->catalog_lid)
		lids = (lng *) Tloc(lg->catalog_lid, 0);
	BATloop(catalog_bid, p, q) {
		if (lids && lids[p] != lng_nil && lids[p] <= lg->saved_tid) {
			cleanup++;
			if (lids[p] == -1)
				continue;
			if (BUNfnd(dcatalog, &(oid){p}) == BUN_NONE &&
			    BUNappend(dcatalog, &(oid){p}, true) != GDK_SUCCEED) {
				while (BATcount(dcatalog) > dcnt) {
					if (BUNdelete(dcatalog, BATcount(dcatalog) - 1) != GDK_SUCCEED) {
						TRC_CRITICAL(WAL, "delete after failed append failed\n");
						break;
					}
				}
				GDKfree(n);
				GDKfree(r);
				GDKfree(sizes);
				log_unlock(lg);
				return GDK_FAIL;
			}
		}
		if (updated && p < maxupdated && (updated[p / 32] & (1U << (p % 32))) == 0) {
			continue;
		}
		bat col = bids[p];

		TRC_DEBUG(WAL, "new %s (%d)\n", BBP_logical(col), col);
		assert(col);
		sizes[i] = cnts ? (BUN) cnts[p] : 0;
		n[i++] = col;
	}
	/* now commit catalog, so it's also up to date on disk */
	sizes[i] = cnt;
	n[i++] = catalog_bid->batCacheid;
	sizes[i] = cnt;
	n[i++] = catalog_id->batCacheid;
	sizes[i] = BATcount(dcatalog);
	n[i++] = dcatalog->batCacheid;

	if (cleanup) {
		if ((rcnt = cleanup_and_swap(lg, r, bids, lids, cnts,
					     catalog_bid, catalog_id, dcatalog,
					     cleanup)) < 0) {
			GDKfree(n);
			GDKfree(r);
			GDKfree(sizes);
			log_unlock(lg);
			return GDK_FAIL;
		}
		cnt -= cleanup;
	}
	if (dcatalog != lg->dcatalog) {
		i = subcommit_list_add(i, n, sizes, lg->catalog_bid->batCacheid, cnt);
		i = subcommit_list_add(i, n, sizes, lg->catalog_id->batCacheid, cnt);
		i = subcommit_list_add(i, n, sizes, lg->dcatalog->batCacheid, BATcount(lg->dcatalog));
	}
	if (lg->seqs_id) {
		sizes[i] = BATcount(lg->seqs_id);
		n[i++] = lg->seqs_id->batCacheid;
		sizes[i] = BATcount(lg->seqs_id);
		n[i++] = lg->seqs_val->batCacheid;
	}
	if (!cleanup && lg->seqs_id && BATcount(lg->dseqs) > (BATcount(lg->seqs_id) / 2) && BATcount(lg->dseqs) > 10) {
		BAT *tids, *ids, *vals;

		tids = bm_tids(lg->seqs_id, lg->dseqs);
		if (tids == NULL) {
			GDKfree(n);
			GDKfree(r);
			GDKfree(sizes);
			log_unlock(lg);
			return GDK_FAIL;
		}
		ids = logbat_new(TYPE_int, BATcount(tids), PERSISTENT);
		vals = logbat_new(TYPE_lng, BATcount(tids), PERSISTENT);

		if (ids == NULL || vals == NULL) {
			logbat_destroy(tids);
			logbat_destroy(ids);
			logbat_destroy(vals);
			GDKfree(n);
			GDKfree(r);
			GDKfree(sizes);
			log_unlock(lg);
			return GDK_FAIL;
		}

		if (BATappend(ids, lg->seqs_id, tids, true) != GDK_SUCCEED ||
		    BATappend(vals, lg->seqs_val, tids, true) != GDK_SUCCEED) {
			logbat_destroy(tids);
			logbat_destroy(ids);
			logbat_destroy(vals);
			GDKfree(n);
			GDKfree(r);
			GDKfree(sizes);
			log_unlock(lg);
			return GDK_FAIL;
		}
		logbat_destroy(tids);
		BATclear(lg->dseqs, true);

		if (log_switch_bat(lg->seqs_id, ids, lg->fn, "seqs_id") != GDK_SUCCEED ||
		    log_switch_bat(lg->seqs_val, vals, lg->fn, "seqs_val") != GDK_SUCCEED) {
			logbat_destroy(ids);
			logbat_destroy(vals);
			GDKfree(n);
			GDKfree(r);
			GDKfree(sizes);
			log_unlock(lg);
			return GDK_FAIL;
		}
		i = subcommit_list_add(i, n, sizes, ids->batCacheid, BATcount(ids));
		i = subcommit_list_add(i, n, sizes, vals->batCacheid, BATcount(ids));

		if (BBP_lrefs(lg->seqs_id->batCacheid) > 0)
			r[rcnt++] = lg->seqs_id->batCacheid;
		if (BBP_lrefs(lg->seqs_val->batCacheid) > 0)
			r[rcnt++] = lg->seqs_val->batCacheid;

		logbat_destroy(lg->seqs_id);
		logbat_destroy(lg->seqs_val);

		lg->seqs_id = ids;
		lg->seqs_val = vals;
	}
	if (lg->seqs_id) {
		sizes[i] = BATcount(lg->dseqs);
		n[i++] = lg->dseqs->batCacheid;
	}

	assert((BUN) i <= nn);
	log_unlock(lg);
	TRC_DEBUG_IF(WAL)
		t0 = GDKusec();
	res = TMsubcommit_list(n, cnts ? sizes : NULL, i, lg->saved_id);
	TRC_DEBUG(WAL, "subcommit " LLFMT "usec\n", GDKusec() - t0);
	if (res == GDK_SUCCEED) {	/* now cleanup */
		for (i = 0; i < rcnt; i++) {
			TRC_DEBUG_IF(WAL) {
				TRC_DEBUG_ENDIF(WAL, "release %d\n", r[i]);
				if (BBP_lrefs(r[i]) != 2)
					TRC_DEBUG_ENDIF(WAL, "release %d %d\n", r[i], BBP_lrefs(r[i]));
			}
			BBPrelease(r[i]);
		}
	}
	GDKfree(n);
	GDKfree(r);
	GDKfree(sizes);
	if (res != GDK_SUCCEED)
		TRC_CRITICAL(GDK, "commit failed\n");
	return res;
}

static gdk_return
log_filename(logger *lg, char bak[FILENAME_MAX], char filename[FILENAME_MAX])
{
	if (GDKfilepath(filename, FILENAME_MAX, 0, lg->dir, LOGFILE, NULL) != GDK_SUCCEED) {
		return GDK_FAIL;
	}
	if (bak) {
		if (strconcat_len(bak, FILENAME_MAX, filename, ".bak", NULL) >= FILENAME_MAX) {
			GDKerror("Logger filename path is too large\n");
			return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
}

static gdk_return
log_cleanup(logger *lg, lng id)
{
	char log_id[FILENAME_MAX];

	if (snprintf(log_id, sizeof(log_id), LLFMT, id) >= FILENAME_MAX) {
		GDKerror("log_id filename is too large\n");
		return GDK_FAIL;
	}
	if (GDKunlink(0, lg->dir, LOGFILE, log_id) != GDK_SUCCEED) {
		GDKwarning("failed to remove old WAL %s.%s\n", LOGFILE, log_id);
		GDKclrerr();	/* clear error from unlink */
	}
	return GDK_SUCCEED;
}

#ifdef GDKLIBRARY_JSON
static gdk_return
log_json_upgrade_finalize(void)
{
	int json_tpe = ATOMindex("json");
	if (!GDKinmemory(0) &&
	    GDKunlink(0, BATDIR, "jsonupgradeneeded", NULL) == GDK_FAIL) {
		TRC_CRITICAL(GDK, "Failed to remove json upgrade signal file");
		return GDK_FAIL;
	}
	BATatoms[json_tpe].atomRead = (void *(*)(void *, size_t *, stream *, size_t))strRead;

	return GDK_SUCCEED;
}
#endif

/* clean up old junk left over from old upgrades: bats that are
 * persistent but not in the SQL catalog and that have no name, and bats
 * that do have a name that starts with "stat_opt_" (from the statistics
 * optimizer that was removed in 2017) are removed here
 *
 * this function ignores any errors */
static void
clean_bbp(logger *lg)
{
	BAT *b = COLnew(0, TYPE_int, 256, TRANSIENT);
	if (b == NULL)
		return;
	if (BUNappend(b, &(int){0}, false) != GDK_SUCCEED) {
		BBPreclaim(b);
		return;
	}
	/* mark persistent bats that have no name or have a name
	 * starting with "stat_opt_" */
	for (bat bid = 1, bsz = getBBPsize(); bid < bsz; bid++)
		if (BBP_status(bid) & BBPEXISTING &&
		    (BBP_logical(bid) == NULL ||
		     strncmp(BBP_logical(bid), "tmp_", 4) == 0 ||
		     strncmp(BBP_logical(bid), "stat_opt_", 9) == 0))
			BBP_status_on(bid, 1U << 31);
	/* remove mark from bats that are in the SQL catalog */
	for (BUN i = 0, n = BATcount(lg->catalog_bid); i < n; i++)
		BBP_status_off(((int *) lg->catalog_bid->theap->base)[i], 1U << 31);
	/* what's left over are junk bats */
	for (bat bid = 1, bsz = getBBPsize(); bid < bsz; bid++)
		if (BBP_status(bid) & (1U << 31)) {
			BBP_status_off(bid, 1U << 31);
			if (BATmode(BBP_desc(bid), true) != GDK_SUCCEED ||
			    BUNappend(b, &bid, false) != GDK_SUCCEED) {
				BBPreclaim(b);
				return;
			}
			printf("# removing bat %d (tmp_%o)\n", bid, bid);
		}
	/* if there were any junk bats, commit their removal */
	if (b->batCount > 1 &&
	    TMsubcommit_list(Tloc(b, 0), NULL, (int) b->batCount, -1) != GDK_SUCCEED)
		printf("clean_bbp transaction failed\n");
	BBPreclaim(b);
}

/* Load data from the logger logdir
 * Initialize new directories and catalog files if none are present,
 * unless running in read-only mode
 * Load data and persist it in the BATs */
static gdk_return
log_load(const char *fn, logger *lg, char filename[FILENAME_MAX])
{
	FILE *fp = NULL;
	char bak[FILENAME_MAX];
	bat catalog_bid, catalog_id, dcatalog;
	bool needcommit = false;
	ATOMIC_BASE_TYPE dbg = ATOMIC_GET(&GDKdebug);
	bool readlogs = false;
	bool needsnew = false;	/* need to write new log file? */

	/* refactor */
	if (!LOG_DISABLED(lg)) {
		if (log_filename(lg, bak, filename) != GDK_SUCCEED)
			goto error;
	}

	lg->catalog_bid = NULL;
	lg->catalog_id = NULL;
	lg->catalog_cnt = NULL;
	lg->catalog_lid = NULL;
	lg->dcatalog = NULL;

	lg->seqs_id = NULL;
	lg->seqs_val = NULL;
	lg->dseqs = NULL;

	if (!LOG_DISABLED(lg)) {
		/* try to open logfile backup, or failing that, the file
		 * itself. we need to know whether this file exists when
		 * checking the database consistency later on */
		if ((fp = MT_fopen(bak, "r")) != NULL) {
			fclose(fp);
			fp = NULL;
			if (GDKunlink(0, lg->dir, LOGFILE, NULL) != GDK_SUCCEED ||
			    GDKmove(0, lg->dir, LOGFILE, "bak", lg->dir, LOGFILE, NULL, true) != GDK_SUCCEED)
				goto error;
		} else if (errno != ENOENT) {
			GDKsyserror("open %s failed", bak);
			goto error;
		}
		fp = MT_fopen(filename, "r");
		if (fp == NULL && errno != ENOENT) {
			GDKsyserror("open %s failed", filename);
			goto error;
		}
	}

	strconcat_len(bak, sizeof(bak), fn, "_catalog_bid", NULL);
	catalog_bid = BBPindex(bak);

	/* initialize arrays for type mapping, to be read from disk */
	memset(lg->type_id, -1, sizeof(lg->type_id));
	memset(lg->type_nr, 255, sizeof(lg->type_nr));

	/* this is intentional - if catalog_bid is 0, force it to find
	 * the persistent catalog */
	if (catalog_bid == 0) {
		/* catalog does not exist, so the log file also
		 * shouldn't exist */
		if (fp != NULL) {
			GDKerror("there is no logger catalog, "
				 "but there is a log file.\n");
			goto error;
		}

		lg->catalog_bid = logbat_new(TYPE_int, BATSIZE, PERSISTENT);
		lg->catalog_id = logbat_new(TYPE_int, BATSIZE, PERSISTENT);
		lg->dcatalog = logbat_new(TYPE_oid, BATSIZE, PERSISTENT);

		if (lg->catalog_bid == NULL || lg->catalog_id == NULL || lg->dcatalog == NULL) {
			GDKerror("cannot create catalog bats");
			goto error;
		}
		TRC_INFO(WAL, "create %s catalog\n", fn);

		/* give the catalog bats names so we can find them
		 * next time */
		strconcat_len(bak, sizeof(bak), fn, "_catalog_bid", NULL);
		if (BBPrename(lg->catalog_bid, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_catalog_id", NULL);
		if (BBPrename(lg->catalog_id, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_dcatalog", NULL);
		if (BBPrename(lg->dcatalog, bak) < 0) {
			goto error;
		}

		if (!LOG_DISABLED(lg)) {
			if (GDKcreatedir(filename) != GDK_SUCCEED) {
				GDKerror("cannot create directory for log file %s\n", filename);
				goto error;
			}
			if (log_create_types_file(lg, filename) != GDK_SUCCEED)
				goto error;
		}

		BBPretain(lg->catalog_bid->batCacheid);
		BBPretain(lg->catalog_id->batCacheid);
		BBPretain(lg->dcatalog->batCacheid);

		log_lock(lg);
		/* bm_subcommit releases the lock */
		if (bm_subcommit(lg, NULL, NULL, 0) != GDK_SUCCEED) {
			/* cannot commit catalog, so remove log */
			if (MT_remove(filename) < 0)
				GDKsyserror("remove %s failed\n", filename);
			BBPrelease(lg->catalog_bid->batCacheid);
			BBPrelease(lg->catalog_id->batCacheid);
			BBPrelease(lg->dcatalog->batCacheid);
			goto error;
		}
	} else {
		/* find the persistent catalog. As non persistent bats
		 * require a logical reference we also add a logical
		 * reference for the persistent bats */
		BUN p, q;
		BAT *b, *o, *d;

		assert(!lg->inmemory);

		/* the catalog exists, and so should the log file */
		if (fp == NULL && !LOG_DISABLED(lg)) {
			GDKerror("There is a logger catalog, but no log file.\n");
			goto error;
		}
		if (fp != NULL) {
			/* check_version always closes fp */
			if (check_version(lg, fp, &needsnew) != GDK_SUCCEED) {
				fp = NULL;
				goto error;
			}
			readlogs = true;
			fp = NULL;
		}

		if (lg->catalog_bid == NULL && lg->catalog_id == NULL && lg->dcatalog == NULL) {
			b = BATdescriptor(catalog_bid);
			if (b == NULL) {
				GDKerror("inconsistent database, catalog does not exist");
				goto error;
			}

			strconcat_len(bak, sizeof(bak), fn, "_catalog_id", NULL);
			catalog_id = BBPindex(bak);
			o = BATdescriptor(catalog_id);
			if (o == NULL) {
				BBPunfix(b->batCacheid);
				GDKerror("inconsistent database, catalog_id does not exist");
				goto error;
			}

			strconcat_len(bak, sizeof(bak), fn, "_dcatalog", NULL);
			dcatalog = BBPindex(bak);
			d = BATdescriptor(dcatalog);
			if (d == NULL) {
				GDKerror("cannot create dcatalog bat");
				BBPunfix(b->batCacheid);
				BBPunfix(o->batCacheid);
				goto error;
			}

			lg->catalog_bid = b;
			lg->catalog_id = o;
			lg->dcatalog = d;
			const log_bid *bids = (const log_bid *) Tloc(lg->catalog_bid, 0);
			BATloop(lg->catalog_bid, p, q) {
				bat bid = bids[p];
				oid pos = p;

				if (BBPretain(bid) == 0 &&	/* any bid in the catalog_bid, needs one logical ref */
				    BUNfnd(lg->dcatalog, &pos) == BUN_NONE &&
				    BUNappend(lg->dcatalog, &pos, true) != GDK_SUCCEED)
					goto error;
			}
		}
		if ((lg->catalog_bid = BATsetaccess(lg->catalog_bid, BAT_READ)) == NULL ||
		    (lg->catalog_id = BATsetaccess(lg->catalog_id, BAT_READ)) == NULL ||
		    (lg->dcatalog = BATsetaccess(lg->dcatalog, BAT_READ)) == NULL) {
			goto error;
		}
		BBPretain(lg->catalog_bid->batCacheid);
		BBPretain(lg->catalog_id->batCacheid);
		BBPretain(lg->dcatalog->batCacheid);
	}
	/* failing to rename the catalog_cnt and catalog_lid bats is not
	 * fatal */
	lg->catalog_cnt = logbat_new(TYPE_lng, 1, SYSTRANS);
	if (lg->catalog_cnt == NULL) {
		GDKerror("failed to create catalog_cnt bat");
		goto error;
	}
	strconcat_len(bak, sizeof(bak), fn, "_catalog_cnt", NULL);
	if (BBPrename(lg->catalog_cnt, bak) < 0)
		GDKclrerr();
	lg->catalog_lid = logbat_new(TYPE_lng, 1, SYSTRANS);
	if (lg->catalog_lid == NULL) {
		GDKerror("failed to create catalog_lid bat");
		goto error;
	}
	strconcat_len(bak, sizeof(bak), fn, "_catalog_lid", NULL);
	if (BBPrename(lg->catalog_lid, bak) < 0)
		GDKclrerr();
	if (bm_get_counts(lg) != GDK_SUCCEED)
		goto error;

	strconcat_len(bak, sizeof(bak), fn, "_seqs_id", NULL);
	if (BBPindex(bak)) {
		lg->seqs_id = BATdescriptor(BBPindex(bak));
		strconcat_len(bak, sizeof(bak), fn, "_seqs_val", NULL);
		lg->seqs_val = BATdescriptor(BBPindex(bak));
		strconcat_len(bak, sizeof(bak), fn, "_dseqs", NULL);
		lg->dseqs = BATdescriptor(BBPindex(bak));
		if (lg->seqs_id == NULL ||
		    lg->seqs_val == NULL ||
		    lg->dseqs == NULL) {
			GDKerror("Logger_new: cannot load seqs bats");
			goto error;
		}
		if ((lg->seqs_val = BATsetaccess(lg->seqs_val, BAT_READ)) == NULL ||
		    (lg->seqs_id = BATsetaccess(lg->seqs_id, BAT_READ)) == NULL ||
		    (lg->dseqs = BATsetaccess(lg->dseqs, BAT_READ)) == NULL) {
			goto error;
		}
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
		if (BBPrename(lg->seqs_id, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_seqs_val", NULL);
		if (BBPrename(lg->seqs_val, bak) < 0) {
			goto error;
		}

		strconcat_len(bak, sizeof(bak), fn, "_dseqs", NULL);
		if (BBPrename(lg->dseqs, bak) < 0) {
			goto error;
		}
		needcommit = true;
	}
	dbg = ATOMIC_GET(&GDKdebug);
	ATOMIC_AND(&GDKdebug, ~CHECKMASK);
	if (needcommit && bm_commit(lg, NULL, NULL, 0) != GDK_SUCCEED) {
		GDKerror("Logger_new: commit failed");
		goto error;
	}
	ATOMIC_SET(&GDKdebug, dbg);

	if (readlogs) {
		ulng log_id = lg->saved_id + 1;
		bool earlyexit = GDKgetenv_isyes("process-wal-and-exit");
		if (log_readlogs(lg, filename) != GDK_SUCCEED) {
			goto error;
		}
		if (!earlyexit) {
			if (lg->postfuncp && (*lg->postfuncp) (lg->funcdata, lg) != GDK_SUCCEED)
				goto error;
			if (needsnew) {
				if (GDKmove(0, lg->dir, LOGFILE, NULL, lg->dir, LOGFILE, "bak", true) != GDK_SUCCEED) {
					TRC_CRITICAL(GDK, "couldn't move log to log.bak\n");
					return GDK_FAIL;
				}
				if (log_create_types_file(lg, filename) != GDK_SUCCEED) {
					TRC_CRITICAL(GDK, "couldn't write new log\n");
					return GDK_FAIL;
				}
			}
		}
		dbg = ATOMIC_GET(&GDKdebug);
		ATOMIC_AND(&GDKdebug, ~CHECKMASK);
		if (log_commit(lg, NULL, NULL, 0) != GDK_SUCCEED) {
			goto error;
		}
		// We can unconditionally GDKunlink because it's a no-op if the file does not exist
		if (GDKunlink(0, lg->dir, LOGFILE ".omitted", NULL) != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "couldn't remove file " LOGFILE ".omitted\n");
			return GDK_FAIL;
		}
		ATOMIC_SET(&GDKdebug, dbg);
		for (; log_id <= lg->saved_id; log_id++)
			(void) log_cleanup(lg, log_id);	/* ignore error of removing file */
		if (earlyexit) {
			printf("# mserver5 exiting\n");
			exit(0);
		}
		if (needsnew &&
		    GDKunlink(0, lg->dir, LOGFILE, "bak") != GDK_SUCCEED) {
			TRC_CRITICAL(GDK, "couldn't remove old log.bak file\n");
			return GDK_FAIL;
		}
	} else {
		lg->id = lg->saved_id + 1;
		if (GDKgetenv_isyes("process-wal-and-exit")) {
			printf("# mserver5 exiting\n");
			exit(0);
		}
	}
#ifdef GDKLIBRARY_JSON
	if (log_json_upgrade_finalize() == GDK_FAIL)
		goto error;
#endif
	if (GDKgetenv_isyes("clean-BBP"))
		clean_bbp(lg);
	return GDK_SUCCEED;
  error:
	if (fp)
		fclose(fp);
	logbat_destroy(lg->catalog_bid);
	logbat_destroy(lg->catalog_id);
	logbat_destroy(lg->dcatalog);
	logbat_destroy(lg->seqs_id);
	logbat_destroy(lg->seqs_val);
	logbat_destroy(lg->dseqs);
	MT_lock_destroy(&lg->lock);
	MT_lock_destroy(&lg->rotation_lock);
	GDKfree(lg->fn);
	GDKfree(lg->dir);
	GDKfree(lg->rbuf);
	GDKfree(lg->wbuf);
	GDKfree(lg);
	ATOMIC_SET(&GDKdebug, dbg);
	/* We do not call log_json_upgrade_finalize here because we want
	 * the upgrade to run again next time we try, so we do not want
	 * to remove the signal file just yet.
	 */
	return GDK_FAIL;
}

/* Initialize a new logger
 * It will load any data in the logdir and persist it in the BATs*/
static logger *
log_new(int debug, const char *fn, const char *logdir, int version, preversionfix_fptr prefuncp,
	postversionfix_fptr postfuncp, void *funcdata)
{
	logger *lg;
	char filename[FILENAME_MAX];

	lng max_dropped = GDKgetenv_int("wal_max_dropped", 100000);
	lng max_file_age = GDKgetenv_int("wal_max_file_age", 600);
	int max_pending = GDKgetenv_int("wal_max_pending", 5);
	lng max_file_size = 0;

	if (GDKdebug & TESTINGMASK) {
		max_file_size = 2048; /* 2 KiB */
	} else {
		const char *max_file_size_str = GDKgetenv("wal_max_file_size");
		max_file_size = max_file_size_str ? strtoul(max_file_size_str, NULL, 10) : 2147483648;
	}

	if (!GDKinmemory(0) && MT_path_absolute(logdir)) {
		TRC_CRITICAL(GDK, "logdir must be relative path\n");
		return NULL;
	}

	if (snprintf(filename, sizeof(filename), "%s%c%s%c", logdir, DIR_SEP, fn, DIR_SEP) >= FILENAME_MAX) {
		TRC_CRITICAL(GDK, "filename is too large\n");
		return NULL;
	}

	lg = GDKmalloc(sizeof(struct logger));
	if (lg == NULL) {
		TRC_CRITICAL(GDK, "allocating logger structure failed\n");
		return NULL;
	}

	*lg = (logger) {
		.inmemory = GDKinmemory(0),
		.debug = debug,
		.version = version,
		.prefuncp = prefuncp,
		.postfuncp = postfuncp,
		.funcdata = funcdata,

		.max_dropped = max_dropped >= 0 ? max_dropped : 100000,
		.file_age = 0,
		.max_file_age = max_file_age >= 0 ? max_file_age * 1000000 : 600000000,
		.max_file_size = max_file_size >= 0 ? max_file_size : 2147483648,
		.max_pending = max_pending,
		.cur_max_pending = max_pending,

		.id = 0,
		.saved_id = getBBPlogno(),	/* get saved log number from bbp */
		.nr_flushers = ATOMIC_VAR_INIT(0),
		.fn = GDKstrdup(fn),
		.dir = GDKstrdup(filename),
		.rbufsize = 64 * 1024,
		.rbuf = GDKmalloc(64 * 1024),
		.wbufsize = 64 * 1024,
		.wbuf = GDKmalloc(64 * 1024),
	};

	/* probably open file and check version first, then call call old logger code */
	if (lg->fn == NULL ||
	    lg->dir == NULL ||
	    lg->rbuf == NULL ||
	    lg->wbuf == NULL) {
		TRC_CRITICAL(GDK, "allocating for logger structure failed\n");
		GDKfree(lg->fn);
		GDKfree(lg->dir);
		GDKfree(lg->rbuf);
		GDKfree(lg->wbuf);
		GDKfree(lg);
		return NULL;
	}
	TRC_DEBUG(WAL, "dir set to %s\n", lg->dir);

	MT_lock_init(&lg->lock, fn);
	MT_lock_init(&lg->rotation_lock, "rotation_lock");
	MT_lock_init(&lg->flush_lock, "flush_lock");
	MT_cond_init(&lg->excl_flush_cv, "flush_cond");

	if (log_load(fn, lg, filename) == GDK_SUCCEED) {
		return lg;
	}
	return NULL;
}

static logged_range *
do_flush_range_cleanup(logger *lg)
{
	logged_range *frange = lg->flush_ranges;
	logged_range *first = frange;

	if (frange == NULL)
		return NULL;
	while (frange->next) {
		if (ATOMIC_GET(&frange->refcount) > 1)
			break;
		frange = frange->next;
	}
	if (first == frange) {
		return first;
	}

	logged_range *flast = frange;

	lg->flush_ranges = flast;

	for (frange = first; frange && frange != flast; frange = frange->next) {
		ATOMIC_DEC(&frange->refcount);
		if (!LOG_DISABLED(lg) && frange->output_log) {
			TRC_INFO(WAL, "closing output log %s", mnstr_name(frange->output_log));
			close_stream(frange->output_log);
			frange->output_log = NULL;
		}
	}
	return flast;
}

void
log_destroy(logger *lg)
{
	log_close_input(lg);
	logged_range *last = do_flush_range_cleanup(lg);
	(void) last;
	assert(last == lg->current && last == lg->flush_ranges);
	log_close_output(lg);
	for (logged_range * p = lg->pending; p; p = lg->pending) {
		lg->pending = p->next;
		GDKfree(p);
	}
	if (LOG_DISABLED(lg)) {
		lg->saved_id = lg->id;
		lg->saved_tid = lg->tid;
		log_commit(lg, NULL, NULL, 0);
	}
	if (lg->catalog_bid) {
		log_lock(lg);
		BUN p, q;
		BAT *b = lg->catalog_bid;

		/* free resources */
		const log_bid *bids = (const log_bid *) Tloc(b, 0);
		BATloop(b, p, q) {
			bat bid = bids[p];

			BBPrelease(bid);
		}

		BBPrelease(lg->catalog_bid->batCacheid);
		BBPrelease(lg->catalog_id->batCacheid);
		BBPrelease(lg->dcatalog->batCacheid);
		logbat_destroy(lg->catalog_bid);
		logbat_destroy(lg->catalog_id);
		logbat_destroy(lg->dcatalog);

		logbat_destroy(lg->catalog_cnt);
		logbat_destroy(lg->catalog_lid);
		log_unlock(lg);
	}
	MT_lock_destroy(&lg->lock);
	MT_lock_destroy(&lg->rotation_lock);
	MT_lock_destroy(&lg->flush_lock);
	GDKfree(lg->fn);
	GDKfree(lg->dir);
	GDKfree(lg->rbuf);
	GDKfree(lg->wbuf);
	GDKfree(lg);
}

/* Create a new logger */
logger *
log_create(int debug, const char *fn, const char *logdir, int version,
	   preversionfix_fptr prefuncp, postversionfix_fptr postfuncp,
	   void *funcdata)
{
	logger *lg;
	TRC_INFO_IF(WAL) {
		TRC_INFO_ENDIF(WAL, "Started processing logs %s/%s version %d\n", fn, logdir, version);
		GDKtracer_flush_buffer();
	}
	lg = log_new(debug, fn, logdir, version, prefuncp, postfuncp, funcdata);
	if (lg == NULL)
		return NULL;
	TRC_INFO_IF(WAL) {
		TRC_INFO_ENDIF(WAL, "Finished processing logs %s/%s\n", fn, logdir);
		GDKtracer_flush_buffer();
	}
	if (GDKsetenv("recovery", "finished") != GDK_SUCCEED) {
		log_destroy(lg);
		return NULL;
	}
	assert(lg->current == NULL);
	logged_range dummy = {
		.cnt = BATcount(lg->catalog_bid),
	};
	lg->current = &dummy;
	if (log_open_output(lg) != GDK_SUCCEED) {
		lg->current = NULL;
		log_destroy(lg);
		return NULL;
	}
	lg->current = lg->current->next;
	assert(lg->pending == NULL && lg->flush_ranges == NULL);
	lg->pending = lg->current;
	lg->flush_ranges = lg->current;
	return lg;
}

static logged_range *
log_next_logfile(logger *lg, ulng ts)
{
	int m = (ATOMIC_GET(&GDKdebug) & TESTINGMASK) ? 1000 : 100;
	if (!lg->pending || !lg->pending->next)
		return NULL;
	rotation_lock(lg);
	if (ATOMIC_GET(&lg->pending->refcount) == 0 && lg->pending != lg->current && lg->pending != lg->flush_ranges &&
	    (ulng) ATOMIC_GET(&lg->pending->last_ts) == (ulng) ATOMIC_GET(&lg->pending->flushed_ts) &&
	    (ulng) ATOMIC_GET(&lg->pending->flushed_ts) <= ts) {
		logged_range *p = lg->pending;
		for (int i = 1;
		     i < m && ATOMIC_GET(&p->refcount) == 0 && p->next && p->next != lg->current &&
		     p->next != lg->flush_ranges && (ulng) ATOMIC_GET(&p->last_ts) == (ulng) ATOMIC_GET(&p->flushed_ts)
		     && (ulng) ATOMIC_GET(&p->flushed_ts) <= ts; i++)
			p = p->next;
		rotation_unlock(lg);
		return p;
	}
	rotation_unlock(lg);
	return NULL;
}

static void
log_cleanup_range(logger *lg, ulng id)
{
	rotation_lock(lg);
	while (lg->pending && lg->pending->id <= id) {
		logged_range *p;
		p = lg->pending;
		if (p)
			lg->pending = p->next;
		GDKfree(p);
	}
	rotation_unlock(lg);
}

static void
do_rotate(logger *lg)
{
	logged_range *cur = lg->current;
	logged_range *next = cur->next;
	if (next) {
		assert(ATOMIC_GET(&next->refcount) == 1);
		lg->current = next;
		if (!LOG_DISABLED(lg) && ATOMIC_GET(&cur->refcount) == 1 && cur->output_log) {
			close_stream(cur->output_log);
			cur->output_log = NULL;
		}
	}
}

gdk_return
log_activate(logger *lg)
{
	bool flush_cleanup = false;
	gdk_return res = GDK_SUCCEED;

	rotation_lock(lg);
	const lng current_file_size = LOG_DISABLED(lg) ? 0 : (lng) getfilepos(getFile(lg->current->output_log));

	if (current_file_size == -1) {
		rotation_unlock(lg);
		return GDK_FAIL;
	}
	/* file size of 2 means only endian indicator present
	 * (i.e. effectively empty) */
	if (current_file_size <= 2) {
		rotation_unlock(lg);
		return GDK_SUCCEED;
	}

	if (!lg->flushnow &&
	    !lg->current->next &&
	    current_file_size > 2 &&
	    (ATOMIC_GET(&lg->current->drops) > (ulng)lg->max_dropped ||
		    current_file_size > lg->max_file_size ||
		    (GDKusec() - lg->file_age) > lg->max_file_age) &&
	    (ulng) ATOMIC_GET(&lg->current->last_ts) > 0 &&
	    lg->saved_id + 1 == lg->id &&
	    ATOMIC_GET(&lg->current->refcount) == 1 /* no pending work on this file */ ) {
		lg->id++;
		/* start new file */
		res = log_open_output(lg);
		flush_cleanup = true;
		do_rotate(lg);
	}
	if (flush_cleanup)
		(void) do_flush_range_cleanup(lg);
	rotation_unlock(lg);
	return res;
}

gdk_return
log_flush(logger *lg, ulng ts)
{
	logged_range *pending = log_next_logfile(lg, ts);
	ulng lid = pending ? pending->id : 0, olid = lg->saved_id;
	if (LOG_DISABLED(lg)) {
		lg->saved_id = lid;
		lg->saved_tid = lg->tid;
		if (lid)
			log_cleanup_range(lg, lg->saved_id);
		if (log_commit(lg, NULL, NULL, 0) != GDK_SUCCEED)
			TRC_ERROR(GDK, "failed to commit");
		return GDK_SUCCEED;
	}
	if (lg->saved_id >= lid) {
		/* if too many pending */
		if (lg->saved_id + lg->cur_max_pending < lg->id) {
			lg->cur_max_pending *= 2; /* when to warn again */
			TRC_WARNING(GDK, "Too many pending log files " LLFMT "\n", (lg->id - lg->saved_id));
			if (GDKtriggerusr1 &&
			    !(ATOMIC_GET(&GDKdebug) & TESTINGMASK))
				(*GDKtriggerusr1)();
		}
		/* log files went down, reduce cur_max_pending */
		if (lg->cur_max_pending > lg->max_pending && (int)(lg->id - lg->saved_id) < (lg->cur_max_pending/2))
			lg->cur_max_pending /= 2;
		return GDK_SUCCEED;
	}
	rotation_lock(lg);
	ulng lgid = lg->id;
	rotation_unlock(lg);
	if (lg->saved_id + 1 >= lgid)	/* logger should first release the file */
		return GDK_SUCCEED;
	log_return res = LOG_OK;
	ulng cid = olid;
	assert(lid <= lgid);
	uint32_t *updated = NULL;
	BUN nupdated = 0;
	size_t allocated = 0;
	while (cid < lid && res == LOG_OK) {
		if (!lg->input_log) {
			char filename[MAXPATH];
			char id[32];
			if (snprintf(id, sizeof(id), LLFMT, cid + 1) >= (int) sizeof(id)) {
				GDKfree(updated);
				TRC_CRITICAL(GDK, "log_id filename is too large\n");
				return GDK_FAIL;
			}
			if (GDKfilepath(filename, sizeof(filename), BBPselectfarm(PERSISTENT, 0, offheap), lg->dir, LOGFILE, id) != GDK_SUCCEED) {
				GDKfree(updated);
				return GDK_FAIL;
			}
			if (strlen(filename) >= FILENAME_MAX) {
				GDKfree(updated);
				TRC_CRITICAL(GDK, "Logger filename path is too large\n");
				return GDK_FAIL;
			}

			bool filemissing = false;
			if (log_open_input(lg, filename, &filemissing) != GDK_SUCCEED) {
				GDKfree(updated);
				return GDK_FAIL;
			}
		}
		/* we read the full file because skipping is impossible with current log format */
		log_lock(lg);
		if (updated == NULL) {
			nupdated = BATcount(lg->catalog_id);
			allocated = ((nupdated + 31) & ~31) / 8;
			if (allocated == 0)
				allocated = 4;
			updated = GDKzalloc(allocated);
			if (updated == NULL) {
				log_unlock(lg);
				return GDK_FAIL;
			}
		} else if (nupdated < BATcount(lg->catalog_id)) {
			BUN n = BATcount(lg->catalog_id);
			size_t a = ((n + 31) & ~31) / 8;
			if (a > allocated) {
				uint32_t *p = GDKrealloc(updated, a);
				if (p == NULL) {
					GDKfree(updated);
					log_unlock(lg);
					return GDK_FAIL;
				}
				updated = p;
				memset(updated + allocated / 4, 0, a - allocated);
				allocated = a;
			}
			nupdated = n;
		}
		lg->flushing = true;
		res = log_read_transaction(lg, NULL, updated, nupdated, NULL);
		lg->flushing = false;
		log_unlock(lg);
		if (res == LOG_EOF) {
			log_close_input(lg);
			res = LOG_OK;
		}
		cid++;
	}
	if (lid > olid && res == LOG_OK) {
		rotation_lock(lg);	/* protect against concurrent log_tflush rotate check */
		lg->saved_id = lid;
		rotation_unlock(lg);
		if (log_commit(lg, pending, updated, nupdated) != GDK_SUCCEED) {
			TRC_ERROR(GDK, "failed to commit");
			res = LOG_ERR;
			rotation_lock(lg);
			lg->saved_id = olid;	/* reset !! */
			rotation_unlock(lg);
		}
		if (res != LOG_ERR) {
			while (olid < lid) {
				/* Try to cleanup, remove old log file, continue on failure! */
				olid++;
				(void) log_cleanup(lg, olid);
			}
		}
		if (res == LOG_OK)
			log_cleanup_range(lg, lg->saved_id);
	}
	GDKfree(updated);
	return res == LOG_ERR ? GDK_FAIL : GDK_SUCCEED;
}

/* Clean-up write-ahead log files already persisted in the BATs, leaving only the most recent one.
 * Only the bak- files are deleted for the preserved WAL files.
 */
lng
log_changes(logger *lg)
{
	if (LOG_DISABLED(lg))
		return 0;
	rotation_lock(lg);
	lng changes = lg->id - lg->saved_id - 1;
	rotation_unlock(lg);
	return changes;
}

int
log_sequence(logger *lg, int seq, lng *id)
{
	log_lock(lg);
	BUN p = log_find(lg->seqs_id, lg->dseqs, seq);

	if (p != BUN_NONE) {
		*id = *(lng *) Tloc(lg->seqs_val, p);

		log_unlock(lg);
		return 1;
	}
	log_unlock(lg);
	return 0;
}

static gdk_return
log_constant_bulk(logger *lg, int type, const void *val, log_id id, lng offset, lng cnt, lng total_cnt)
{
	bte tpe = find_type(lg, type);
	gdk_return ok = GDK_SUCCEED;
	lng nr = cnt;

	gdk_return(*wt) (const void *, stream *, size_t) = BATatoms[type].atomWrite;

	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (lg->total_cnt == 0) {
		logformat l;
		l.flag = LOG_UPDATE_CB;
		l.id = id;
		if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR ||
		    log_write_format(lg, &l) != GDK_SUCCEED ||
		    !mnstr_writeLng(lg->current->output_log, total_cnt) ||
		    mnstr_write(lg->current->output_log, &tpe, 1, 1) != 1) {
			ok = GDK_FAIL;
			goto bailout;
		}
	}
	lg->total_cnt += cnt;
	if (lg->total_cnt == total_cnt)	/* This is the last to be logged part of this bat, we can already reset the total_cnt */
		lg->total_cnt = 0;
	if (!mnstr_writeLng(lg->current->output_log, cnt) ||
	    !mnstr_writeLng(lg->current->output_log, offset)) {	/* offset = -1 indicates bat was logged in parts */
		ok = GDK_FAIL;
		goto bailout;
	}

	ok = wt(val, lg->current->output_log, 1);

	TRC_DEBUG(WAL, "Logged %d " LLFMT " inserts\n", id, nr);

  bailout:
	if (ok != GDK_SUCCEED) {
		ATOMIC_DEC(&lg->current->refcount);
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
	}
	return ok;
}

gdk_return
log_constant(logger *lg, int type, const void *val, log_id id, lng offset, lng cnt, lng total_cnt)
{
	lng nr = cnt;
	gdk_return ok = GDK_SUCCEED;

	if (LOG_DISABLED(lg) || !nr) {
		/* logging is switched off */
		if (nr) {
			log_lock(lg);
			ok = la_bat_update_count(lg, id, offset + cnt, lg->tid);
			log_unlock(lg);
		}
		return ok;
	}

	if (cnt != total_cnt)
		return log_constant_bulk(lg, type, val, id, offset, cnt, total_cnt);

	bte tpe = find_type(lg, type);
	logformat l;
	l.flag = LOG_UPDATE_CONST;
	l.id = id;

	gdk_return(*wt) (const void *, stream *, size_t) = BATatoms[type].atomWrite;

	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR ||
	    log_write_format(lg, &l) != GDK_SUCCEED ||
	    !mnstr_writeLng(lg->current->output_log, nr) ||
	    mnstr_write(lg->current->output_log, &tpe, 1, 1) != 1 ||
	    !mnstr_writeLng(lg->current->output_log, offset)) {
		ATOMIC_DEC(&lg->current->refcount);
		ok = GDK_FAIL;
		goto bailout;
	}

	ok = wt(val, lg->current->output_log, 1);

	TRC_DEBUG(WAL, "Logged %d " LLFMT " inserts\n", id, nr);

  bailout:
	if (ok != GDK_SUCCEED) {
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
	}
	return ok;
}

static gdk_return
string_writer(logger *lg, BAT *b, lng offset, lng nr)
{
	size_t bufsz = lg->wbufsize, resize = 0;
	BUN end = (BUN) (offset + nr);
	char *buf = lg->wbuf;
	gdk_return res = GDK_SUCCEED;

	if (!buf)
		return GDK_FAIL;
	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR)
		return GDK_FAIL;
	BATiter bi = bat_iterator(b);
	BUN p = (BUN) offset;
	for (; p < end;) {
		size_t sz = 0;
		if (resize) {
			if ((buf = GDKrealloc(lg->wbuf, resize)) == NULL) {
				res = GDK_FAIL;
				break;
			}
			lg->wbuf = buf;
			lg->wbufsize = bufsz = resize;
			resize = 0;
		}
		char *dst = buf;
		for (; p < end && sz < bufsz; p++) {
			const char *s = BUNtvar(bi, p);
			size_t len = strlen(s) + 1;
			if ((sz + len) > bufsz) {
				if (len > bufsz)
					resize = len + bufsz;
				break;
			} else {
				memcpy(dst, s, len);
				dst += len;
				sz += len;
			}
		}
		if (sz &&
		    (!mnstr_writeLng(lg->current->output_log, (lng) sz) ||
		     mnstr_write(lg->current->output_log, buf, sz, 1) != 1)) {
			res = GDK_FAIL;
			break;
		}
	}
	bat_iterator_end(&bi);
	return res;
}

static gdk_return
internal_log_bat(logger *lg, BAT *b, log_id id, lng offset, lng cnt, int sliced, lng total_cnt)
{
	bte tpe = find_type(lg, b->ttype);
	gdk_return ok = GDK_SUCCEED;
	logformat l;
	BUN p;
	lng nr;
	l.flag = LOG_UPDATE_BULK;
	l.id = id;
	nr = cnt;

	if (LOG_DISABLED(lg) || !nr) {
		/* logging is switched off */
		if (nr)
			return la_bat_update_count(lg, id, offset + cnt, lg->tid);
		return GDK_SUCCEED;
	}

	gdk_return(*wt) (const void *, stream *, size_t) = BATatoms[b->ttype].atomWrite;

	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR) {
		ok = GDK_FAIL;
		goto bailout;
	}

	if (lg->total_cnt == 0)	/* signals single bulk message or first part of bat logged in parts */
		if (log_write_format(lg, &l) != GDK_SUCCEED ||
		    !mnstr_writeLng(lg->current->output_log, total_cnt ? total_cnt : cnt) ||
		    mnstr_write(lg->current->output_log, &tpe, 1, 1) != 1 ||
		    !mnstr_writeLng(lg->current->output_log, total_cnt ? -1 : offset)) {	/* offset = -1 indicates bat was logged in parts */
			ok = GDK_FAIL;
			goto bailout;
		}
	if (!total_cnt)
		total_cnt = cnt;
	lg->total_cnt += cnt;

	if (lg->total_cnt == total_cnt)	/* This is the last to be logged part of this bat, we can already reset the total_cnt */
		lg->total_cnt = 0;

	/* if offset is just for the log, but BAT is already sliced, reset offset */
	if (sliced)
		offset = 0;
	BATiter bi = bat_iterator(b);
	if (b->ttype == TYPE_msk) {
		if (offset % 32 == 0) {
			if (!mnstr_writeIntArray(lg->current->output_log, (int *) ((char *) bi.base + offset / 32),
			     (size_t) ((nr + 31) / 32)))
				ok = GDK_FAIL;
		} else {
			for (lng i = 0; i < nr; i += 32) {
				uint32_t v = 0;
				for (int j = 0; j < 32 && i + j < nr; j++)
					v |= (uint32_t) Tmskval(&bi, (BUN) (offset + i + j)) << j;
				if (!mnstr_writeInt(lg->current->output_log, (int) v)) {
					ok = GDK_FAIL;
					break;
				}
			}
		}
	} else if (b->ttype < TYPE_str && bi.h->parentid == b->batCacheid) {
		const void *t = BUNtail(bi, (BUN) offset);

		ok = wt(t, lg->current->output_log, (size_t) nr);
	} else if (b->ttype == TYPE_str) {
		/* efficient string writes */
		ok = string_writer(lg, b, offset, nr);
	} else {
		BUN end = (BUN) (offset + nr);
		for (p = (BUN) offset; p < end && ok == GDK_SUCCEED; p++) {
			const void *t = BUNtail(bi, p);

			ok = wt(t, lg->current->output_log, 1);
		}
	}
	bat_iterator_end(&bi);

	TRC_DEBUG(WAL, "Logged %d " LLFMT " inserts\n", id, nr);

  bailout:
	if (ok != GDK_SUCCEED) {
		ATOMIC_DEC(&lg->current->refcount);
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
	}
	return ok;
}

/*
 * Changes made to the BAT descriptor should be stored in the log
 * files.  Actually, we need to save the descriptor file, perhaps we
 * should simply introduce a versioning scheme.
 */
gdk_return
log_bat_persists(logger *lg, BAT *b, log_id id)
{
	log_lock(lg);
	bte ta = find_type(lg, b->ttype);
	logformat l;

	if (log_add_bat(lg, b, id, -1) != GDK_SUCCEED) {
		log_unlock(lg);
		if (!LOG_DISABLED(lg))
			ATOMIC_DEC(&lg->current->refcount);
		return GDK_FAIL;
	}

	l.flag = LOG_CREATE;
	l.id = id;
	if (!LOG_DISABLED(lg)) {
		assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
		if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR ||
		    log_write_format(lg, &l) != GDK_SUCCEED ||
		    mnstr_write(lg->current->output_log, &ta, 1, 1) != 1) {
			const char *err = mnstr_peek_error(lg->current->output_log);
			TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
			log_unlock(lg);
			ATOMIC_DEC(&lg->current->refcount);
			return GDK_FAIL;
		}
	}
	TRC_DEBUG(WAL, "id (%d) bat (%d)\n", id, b->batCacheid);
	gdk_return r = internal_log_bat(lg, b, id, 0, BATcount(b), 0, 0);
	log_unlock(lg);
	if (r != GDK_SUCCEED)
		ATOMIC_DEC(&lg->current->refcount);
	return r;
}

gdk_return
log_bat_transient(logger *lg, log_id id)
{
	log_lock(lg);
	log_bid bid = internal_find_bat(lg, id, -1);
	logformat l;

	if (bid < 0) {
		log_unlock(lg);
		return GDK_FAIL;
	}
	if (!bid) {
		GDKerror("log_bat_transient failed to find bid for object %d\n", id);
		log_unlock(lg);
		return GDK_FAIL;
	}
	l.flag = LOG_DESTROY;
	l.id = id;

	if (!LOG_DISABLED(lg)) {
		if (log_write_format(lg, &l) != GDK_SUCCEED) {
			const char *err = mnstr_peek_error(lg->current->output_log);
			TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
			log_unlock(lg);
			ATOMIC_DEC(&lg->current->refcount);
			return GDK_FAIL;
		}
	}
	TRC_DEBUG(WAL, "Logged destroyed bat (%d) %d\n", id, bid);
	BAT *b = BBPquickdesc(bid);
	assert(b);
	BUN cnt = BATcount(b);
	ATOMIC_ADD(&lg->current->drops, cnt);
	gdk_return r = log_del_bat(lg, bid);
	log_unlock(lg);
	if (r != GDK_SUCCEED)
		ATOMIC_DEC(&lg->current->refcount);
	return r;
}

static gdk_return
log_bat_group(logger *lg, log_id id)
{
	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	logformat l;
	l.flag = LOG_BAT_GROUP;
	l.id = id;
	gdk_return r = log_write_format(lg, &l);
	if (r != GDK_SUCCEED) {
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
	}
	return r;
}

gdk_return
log_bat_group_start(logger *lg, log_id id)
{
	/*positive table id represent start of logged table */
	return log_bat_group(lg, id);
}

gdk_return
log_bat_group_end(logger *lg, log_id id)
{
	/*negative table id represent end of logged table */
	return log_bat_group(lg, -id);
}

gdk_return
log_bat(logger *lg, BAT *b, log_id id, lng offset, lng cnt, lng total_cnt)
{
	log_lock(lg);
	gdk_return r = internal_log_bat(lg, b, id, offset, cnt, 0, total_cnt);
	log_unlock(lg);
	return r;
}

gdk_return
log_delta(logger *lg, BAT *uid, BAT *uval, log_id id)
{
	log_lock(lg);
	bte tpe = find_type(lg, uval->ttype);
	gdk_return ok = GDK_SUCCEED;
	logformat l;
	BUN p;
	lng nr;

	if (BATtdense(uid)) {
		ok = internal_log_bat(lg, uval, id, uid->tseqbase, BATcount(uval), 1, 0);
		log_unlock(lg);
		if (!LOG_DISABLED(lg) && ok != GDK_SUCCEED)
			ATOMIC_DEC(&lg->current->refcount);
		return ok;
	}

	assert(uid->ttype == TYPE_oid || uid->ttype == TYPE_void);

	l.flag = LOG_UPDATE;
	l.id = id;
	nr = (BATcount(uval));
	assert(nr);

	if (LOG_DISABLED(lg)) {
		/* logging is switched off */
		log_unlock(lg);
		return GDK_SUCCEED;
	}

	BATiter vi = bat_iterator(uval);
	gdk_return(*wh) (const void *, stream *, size_t) = BATatoms[TYPE_oid].atomWrite;
	gdk_return(*wt) (const void *, stream *, size_t) = BATatoms[uval->ttype].atomWrite;

	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR ||
	    log_write_format(lg, &l) != GDK_SUCCEED ||
	    !mnstr_writeLng(lg->current->output_log, nr) ||
	    mnstr_write(lg->current->output_log, &tpe, 1, 1) != 1) {
		ok = GDK_FAIL;
		goto bailout;
	}
	for (p = 0; p < BATcount(uid) && ok == GDK_SUCCEED; p++) {
		const oid id = BUNtoid(uid, p);

		ok = wh(&id, lg->current->output_log, 1);
	}
	if (uval->ttype == TYPE_msk) {
		if (!mnstr_writeIntArray(lg->current->output_log, vi.base,
					 (BATcount(uval) + 31) / 32))
			ok = GDK_FAIL;
	} else if (uval->ttype < TYPE_str && !isVIEW(uval)) {
		const void *t = BUNtail(vi, 0);

		ok = wt(t, lg->current->output_log, (size_t) nr);
	} else if (uval->ttype == TYPE_str) {
		/* efficient string writes */
		ok = string_writer(lg, uval, 0, nr);
	} else {
		for (p = 0; p < BATcount(uid) && ok == GDK_SUCCEED; p++) {
			const void *val = BUNtail(vi, p);

			ok = wt(val, lg->current->output_log, 1);
		}
	}

	TRC_DEBUG(WAL, "Logged %d " LLFMT " inserts\n", id, nr);

  bailout:
	bat_iterator_end(&vi);
	if (ok != GDK_SUCCEED) {
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
		ATOMIC_DEC(&lg->current->refcount);
	}
	log_unlock(lg);
	return ok;
}

static inline bool
check_rotation_conditions(logger *lg)
{
	if (LOG_DISABLED(lg))
		return false;

	if (lg->current->next)
		return false;	/* do not rotate if there is already a prepared next current */
	if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR)
		return true;
	const lng current_file_size = (lng) getfilepos(getFile(lg->current->output_log));

	if (current_file_size == -1)
		return false;

	assert(current_file_size >= 0);

	if (current_file_size == 2)
		return false;

	bool res = (lg->saved_id + 1 >= lg->id && ATOMIC_GET(&lg->current->drops) > (ulng)lg->max_dropped) ||
		current_file_size > lg->max_file_size ||
		(GDKusec() - lg->file_age) > lg->max_file_age;

	return res;
}

gdk_return
log_tend(logger *lg)
{
	TRC_DEBUG(WAL, "tend %d\n", lg->tid);

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	gdk_return result;
	logformat l;
	l.flag = LOG_END;
	l.id = lg->tid;

	if ((result = log_write_format(lg, &l)) == GDK_SUCCEED)
		ATOMIC_INC(&lg->nr_flushers);
	else {
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
	}
	return result;
}

#define flush_lock(lg)		MT_lock_set(&(lg)->flush_lock)
#define flush_unlock(lg)	MT_lock_unset(&(lg)->flush_lock)

static inline gdk_return
do_flush(logged_range *range)
{
	/* assumes flush lock */
	stream *output_log = range->output_log;
	ulng ts = ATOMIC_GET(&range->last_ts);

	if (mnstr_flush(output_log, MNSTR_FLUSH_DATA) ||
	    (!(ATOMIC_GET(&GDKdebug) & NOSYNCMASK) && mnstr_fsync(output_log))) {
		const char *err = mnstr_peek_error(output_log);
		TRC_CRITICAL(GDK, "flush failed%s%s\n", err ? ": " : "", err ? err : "");
		return GDK_FAIL;
	}
	ATOMIC_SET(&range->flushed_ts, ts);
	return GDK_SUCCEED;
}

static inline void
log_tdone(logger *lg, logged_range *range, ulng commit_ts)
{
	(void) lg;
	TRC_DEBUG(WAL, "tdone " LLFMT "\n", commit_ts);

	if ((ulng) ATOMIC_GET(&range->last_ts) < commit_ts)
		ATOMIC_SET(&range->last_ts, commit_ts);
}

gdk_return
log_tflush(logger *lg, ulng file_id, ulng commit_ts)
{
	rotation_lock(lg);
	if (lg->flushnow) {
		logged_range *p = lg->current;
		assert(lg->flush_ranges == lg->current);
		assert(ATOMIC_GET(&lg->current->flushed_ts) == ATOMIC_GET(&lg->current->last_ts));
		log_tdone(lg, lg->current, commit_ts);
		ATOMIC_SET(&lg->current->flushed_ts, commit_ts);
		lg->id++;
		lg->flushnow = false;
		if (log_open_output(lg) != GDK_SUCCEED)
			GDKfatal("Could not create new log file\n");	/* TODO: does not have to be fatal (yet) */
		do_rotate(lg);
		(void) do_flush_range_cleanup(lg);
		assert(lg->flush_ranges == lg->current);
		rotation_unlock(lg);
		return log_commit(lg, p, NULL, 0);
	}

	if (LOG_DISABLED(lg)) {
		rotation_unlock(lg);
		return GDK_SUCCEED;
	}

	logged_range *frange = do_flush_range_cleanup(lg);

	while (frange->next && frange->id < file_id) {
		assert(frange->next);
		frange = frange->next;
	}

	log_tdone(lg, frange, commit_ts);
	gdk_return rc = GDK_SUCCEED;

	if ((ulng) ATOMIC_GET(&frange->flushed_ts) < commit_ts) {
		/* delay needed ? */

		flush_lock(lg);
		/* check it one more time */
		if ((ulng) ATOMIC_GET(&frange->flushed_ts) < commit_ts)
			rc = do_flush(frange);
		flush_unlock(lg);
	}
	/* else somebody else has flushed our log file */

	if (ATOMIC_DEC(&frange->refcount) == 1 && !LOG_DISABLED(lg)) {
		if (frange != lg->current && frange->output_log) {
			close_stream(frange->output_log);
			frange->output_log = NULL;
		}
	}

	if (ATOMIC_DEC(&lg->nr_flushers) == 0) {
		/* I am the last flusher
		 * if present,
		 * wake up the exclusive flusher in log_tstart */
		/* rotation_lock is still being held */
		MT_cond_signal(&lg->excl_flush_cv);
	}
	rotation_unlock(lg);

	return rc;
}

static gdk_return
log_tsequence_(logger *lg, int seq, lng val)
{
	logformat l;

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;
	l.flag = LOG_SEQ;
	l.id = seq;

	TRC_DEBUG(WAL, "tsequence(%d," LLFMT ")\n", seq, val);

	assert(mnstr_errnr(lg->current->output_log) == MNSTR_NO__ERROR);
	if (mnstr_errnr(lg->current->output_log) != MNSTR_NO__ERROR ||
	    log_write_format(lg, &l) != GDK_SUCCEED ||
	    !mnstr_writeLng(lg->current->output_log, val)) {
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
		ATOMIC_DEC(&lg->current->refcount);
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* a transaction in it self */
gdk_return
log_tsequence(logger *lg, int seq, lng val)
{
	BUN p;

	TRC_DEBUG(WAL, "tsequence(%d," LLFMT ")\n", seq, val);

	log_lock(lg);
	MT_lock_set(&lg->seqs_id->theaplock);
	BUN inserted = lg->seqs_id->batInserted;
	MT_lock_unset(&lg->seqs_id->theaplock);
	if ((p = log_find(lg->seqs_id, lg->dseqs, seq)) != BUN_NONE && p >= inserted) {
		assert(lg->seqs_val->hseqbase == 0);
		if (BUNreplace(lg->seqs_val, p, &val, true) != GDK_SUCCEED) {
			log_unlock(lg);
			return GDK_FAIL;
		}
	} else {
		if (p != BUN_NONE) {
			oid pos = p;
			if (BUNappend(lg->dseqs, &pos, true) != GDK_SUCCEED) {
				log_unlock(lg);
				return GDK_FAIL;
			}
		}
		if (BUNappend(lg->seqs_id, &seq, true) != GDK_SUCCEED ||
		    BUNappend(lg->seqs_val, &val, true) != GDK_SUCCEED) {
			log_unlock(lg);
			return GDK_FAIL;
		}
	}
	gdk_return r = log_tsequence_(lg, seq, val);
	log_unlock(lg);
	return r;
}

static gdk_return
bm_commit(logger *lg, logged_range *pending, uint32_t *updated, BUN maxupdated)
{
	log_lock(lg);
	BAT *b = lg->catalog_bid;
	const log_bid *bids;

	bids = (log_bid *) Tloc(b, 0);
	for (BUN p = b->batInserted, cnt = pending ? pending->cnt : BATcount(b); p < cnt; p++) {
		log_bid bid = bids[p];
		BAT *lb = BBP_desc(bid);

		assert(bid);
		if (lb->batCacheid == 0 || BATmode(lb, false) != GDK_SUCCEED) {
			GDKwarning("Failed to set bat (%d%s) persistent\n", bid, !lb ? " gone" : "");
			log_unlock(lg);
			return GDK_FAIL;
		}

		assert(lb->batRestricted != BAT_WRITE);

		TRC_DEBUG(WAL, "create %d (%d)\n", bid, BBP_lrefs(bid));
	}
	/* bm_subcommit releases the lock */
	return bm_subcommit(lg, pending, updated, maxupdated);
}

static gdk_return
log_add_bat(logger *lg, BAT *b, log_id id, int tid)
{
	log_bid bid = internal_find_bat(lg, id, tid);
	lng cnt = 0;
	lng lid = lng_nil;

	assert(b->batRestricted != BAT_WRITE);
	assert(b->batRole == PERSISTENT);
	if (bid < 0)
		return GDK_FAIL;
	if (bid) {
		if (bid != b->batCacheid) {
			if (log_del_bat(lg, bid) != GDK_SUCCEED)
				return GDK_FAIL;
		} else {
			return GDK_SUCCEED;
		}
	}
	bid = b->batCacheid;
	TRC_DEBUG(WAL, "create %d\n", id);
	assert(log_find(lg->catalog_bid, lg->dcatalog, bid) == BUN_NONE);
	if (BUNappend(lg->catalog_bid, &bid, true) != GDK_SUCCEED ||
	    BUNappend(lg->catalog_id, &id, true) != GDK_SUCCEED ||
	    BUNappend(lg->catalog_cnt, &cnt, false) != GDK_SUCCEED ||
	    BUNappend(lg->catalog_lid, &lid, false) != GDK_SUCCEED)
		return GDK_FAIL;
	if (lg->current)
		lg->current->cnt++;
	BBPretain(bid);
	return GDK_SUCCEED;
}

static gdk_return
log_del_bat(logger *lg, log_bid bid)
{
	BUN p = log_find(lg->catalog_bid, lg->dcatalog, bid);
	lng lid = lg->tid;

	assert(p != BUN_NONE);
	if (p == BUN_NONE) {
		GDKerror("cannot find BAT\n");
		return GDK_FAIL;
	}

	assert(lg->catalog_lid->hseqbase == 0);
	return BUNreplace(lg->catalog_lid, p, &lid, false);
}

/* returns -1 on failure, 0 when not found, > 0 when found */
log_bid
log_find_bat(logger *lg, log_id id)
{
	log_lock(lg);
	log_bid bid = internal_find_bat(lg, id, -1);
	log_unlock(lg);
	if (!bid) {
		GDKerror("logger_find_bat failed to find bid for object %d\n", id);
		return GDK_FAIL;
	}
	return bid;
}



gdk_return
log_tstart(logger *lg, bool flushnow, ulng *file_id)
{
	rotation_lock(lg);
	if (flushnow) {
		if (file_id == NULL) {
			/* special case: ask store_manager to rotate log file */
			lg->file_age = 0;
			rotation_unlock(lg);
			return GDK_SUCCEED;
		}
		/* I am now the exclusive flusher */
		while (ATOMIC_GET(&lg->nr_flushers)) {
			/* I am waiting until all existing flushers are done */
			MT_cond_wait(&lg->excl_flush_cv, &lg->rotation_lock);
		}
		assert(ATOMIC_GET(&lg->nr_flushers) == 0);

		if (ATOMIC_GET(&lg->current->last_ts)) {
			lg->id++;
			if (log_open_output(lg) != GDK_SUCCEED)
				GDKfatal("Could not create new log file\n");	/* TODO: does not have to be fatal (yet) */
		}
		do_rotate(lg);
		(void) do_flush_range_cleanup(lg);
		rotation_unlock(lg);

		if (lg->saved_id + 1 < lg->id)
			log_flush(lg, (1ULL << 63));
		lg->flushnow = flushnow;
	} else {
		if (check_rotation_conditions(lg)) {
			lg->id++;
			if (log_open_output(lg) != GDK_SUCCEED)
				GDKfatal("Could not create new log file\n");	/* TODO: does not have to be fatal (yet) */
		}
		do_rotate(lg);
		rotation_unlock(lg);
	}

	if (LOG_DISABLED(lg))
		return GDK_SUCCEED;

	ATOMIC_INC(&lg->current->refcount);
	*file_id = lg->current->id;
	logformat l;
	l.flag = LOG_START;
	l.id = ++lg->tid;

	TRC_DEBUG(WAL, "tstart %d\n", lg->tid);
	if (log_write_format(lg, &l) != GDK_SUCCEED) {
		const char *err = mnstr_peek_error(lg->current->output_log);
		TRC_CRITICAL(GDK, "write failed%s%s\n", err ? ": " : "", err ? err : "");
		ATOMIC_DEC(&lg->current->refcount);
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

void
log_printinfo(logger *lg)
{
	if (!rotation_trylock(lg, 1000)) {
		printf("Logger is currently locked, so no logger information\n");
		return;
	}
	printf("logger %s:\n", lg->fn);
	printf("current log file "ULLFMT", last handled log file "ULLFMT"\n",
	       lg->id, lg->saved_id);
	printf("current transaction id %d, saved transaction id %d\n",
	       lg->tid, lg->saved_tid);
	printf("number of flushers: %d\n", (int) ATOMIC_GET(&lg->nr_flushers));
	printf("number of catalog entries "BUNFMT", of which "BUNFMT" deleted\n",
	       lg->catalog_bid->batCount, lg->dcatalog->batCount);
	for (logged_range *p = lg->pending; p; p = p->next) {
		char buf[32];
		if ((lg->debug & 128 || lg->inmemory) ||
		    p->output_log == NULL ||
		    snprintf(buf, sizeof(buf), ", file size %"PRIu64, (uint64_t) getfilepos(getFile(lg->current->output_log))) >= (int) sizeof(buf))
			buf[0] = 0;
		printf("pending range "ULLFMT": drops %"PRIu64", last_ts %"PRIu64", flushed_ts %"PRIu64", refcount %"PRIu64"%s%s\n", p->id, (uint64_t) ATOMIC_GET(&p->drops), (uint64_t) ATOMIC_GET(&p->last_ts), (uint64_t) ATOMIC_GET(&p->flushed_ts), (uint64_t) ATOMIC_GET(&p->refcount), buf, p == lg->current ? " (current)" : "");
	}
	rotation_unlock(lg);
}
