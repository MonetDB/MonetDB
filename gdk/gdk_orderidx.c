/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define ORDERIDX_VERSION	((oid) 3)

#ifdef PERSISTENTIDX
struct idxsync {
	Heap *hp;
	bat id;
	const char *func;
};

static void
BATidxsync(void *arg)
{
	struct idxsync *hs = arg;
	Heap *hp = hs->hp;
	int fd;
	lng t0 = 0;

	ALGODEBUG t0 = GDKusec();

	if (HEAPsave(hp, hp->filename, NULL) != GDK_SUCCEED ||
	    (fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) < 0) {
		BBPunfix(hs->id);
		GDKfree(arg);
		return;
	}
	((oid *) hp->base)[0] |= (oid) 1 << 24;
	if (write(fd, hp->base, SIZEOF_SIZE_T) < 0)
		perror("write orderidx");
	if (!(GDKdebug & FORCEMITOMASK)) {
#if defined(NATIVE_WIN32)
		_commit(fd);
#elif defined(HAVE_FDATASYNC)
		fdatasync(fd);
#elif defined(HAVE_FSYNC)
		fsync(fd);
#endif
	}
	close(fd);
	BBPunfix(hs->id);
	ALGODEBUG fprintf(stderr, "#%s: persisting orderidx %s (" LLFMT " usec)\n", hs->func, hp->filename, GDKusec() - t0);
	GDKfree(arg);
}
#endif

/* return TRUE if we have a orderidx on the tail, even if we need to read
 * one from disk */
int
BATcheckorderidx(BAT *b)
{
	int ret;
	lng t = 0;

	if (b == NULL)
		return 0;
	assert(b->batCacheid > 0);
	ALGODEBUG t = GDKusec();
	MT_lock_set(&GDKhashLock(b->batCacheid));
	if (b->torderidx == (Heap *) 1) {
		Heap *hp;
		const char *nme = BBP_physical(b->batCacheid);
		int fd;

		b->torderidx = NULL;
		if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, orderidxheap)) >= 0 &&
		    (hp->filename = GDKmalloc(strlen(nme) + 11)) != NULL) {
			sprintf(hp->filename, "%s.torderidx", nme);

			/* check whether a persisted orderidx can be found */
			if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", "torderidx")) >= 0) {
				struct stat st;
				oid hdata[ORDERIDXOFF];

				if (read(fd, hdata, sizeof(hdata)) == sizeof(hdata) &&
				    hdata[0] == (
#ifdef PERSISTENTIDX
					    ((oid) 1 << 24) |
#endif
					    ORDERIDX_VERSION) &&
				    hdata[1] == (oid) BATcount(b) &&
				    (hdata[2] == 0 || hdata[2] == 1) &&
				    fstat(fd, &st) == 0 &&
				    st.st_size >= (off_t) (hp->size = hp->free = (ORDERIDXOFF + hdata[1]) * SIZEOF_OID) &&
				    HEAPload(hp, nme, "torderidx", 0) == GDK_SUCCEED) {
					close(fd);
					b->torderidx = hp;
					ALGODEBUG fprintf(stderr, "#BATcheckorderidx: reusing persisted orderidx %d\n", b->batCacheid);
					MT_lock_unset(&GDKhashLock(b->batCacheid));
					return 1;
				}
				close(fd);
				/* unlink unusable file */
				GDKunlink(hp->farmid, BATDIR, nme, "torderidx");
			}
			GDKfree(hp->filename);
		}
		GDKfree(hp);
		GDKclrerr();	/* we're not currently interested in errors */
	}
	ret = b->torderidx != NULL;
	MT_lock_unset(&GDKhashLock(b->batCacheid));
	ALGODEBUG if (ret) fprintf(stderr, "#BATcheckorderidx: already has orderidx %d, waited " LLFMT " usec\n", b->batCacheid, GDKusec() - t);
	return ret;
}

/* create the heap for an order index; returns NULL on failure */
Heap *
createOIDXheap(BAT *b, int stable)
{
	Heap *m;
	size_t nmelen;
	oid *restrict mv;
	const char *nme;

	nme = BBP_physical(b->batCacheid);
	nmelen = strlen(nme) + 12;
	if ((m = GDKzalloc(sizeof(Heap))) == NULL ||
	    (m->farmid = BBPselectfarm(b->batRole, b->ttype, orderidxheap)) < 0 ||
	    (m->filename = GDKmalloc(nmelen)) == NULL ||
	    snprintf(m->filename, nmelen, "%s.torderidx", nme) < 0 ||
	    HEAPalloc(m, BATcount(b) + ORDERIDXOFF, SIZEOF_OID) != GDK_SUCCEED) {
		if (m)
			GDKfree(m->filename);
		GDKfree(m);
		return NULL;
	}
	m->free = (BATcount(b) + ORDERIDXOFF) * SIZEOF_OID;

	mv = (oid *) m->base;
	*mv++ = ORDERIDX_VERSION;
	*mv++ = (oid) BATcount(b);
	*mv++ = (oid) !!stable;
	return m;
}

/* maybe persist the order index heap */
void
persistOIDX(BAT *b)
{
#ifdef PERSISTENTIDX
	if ((BBP_status(b->batCacheid) & BBPEXISTING) &&
	    b->batInserted == b->batCount) {
		MT_Id tid;
		struct idxsync *hs = GDKmalloc(sizeof(*hs));
		if (hs != NULL) {
			BBPfix(b->batCacheid);
			hs->id = b->batCacheid;
			hs->hp = b->torderidx;
			hs->func = "BATorderidx";
			MT_create_thread(&tid, BATidxsync, hs, MT_THR_DETACHED);
		}
	} else
		ALGODEBUG fprintf(stderr, "#BATorderidx: NOT persisting index %d\n", b->batCacheid);
#else
	(void) b;
#endif
}

gdk_return
BATorderidx(BAT *b, int stable)
{
	Heap *m;
	oid *restrict mv;
	oid seq;
	BUN p, q;
	BAT *bn = NULL;

	if (BATcheckorderidx(b))
		return GDK_SUCCEED;
	MT_lock_set(&GDKhashLock(b->batCacheid));
	if (b->torderidx) {
		MT_lock_unset(&GDKhashLock(b->batCacheid));
		return GDK_SUCCEED;
	}
	if ((m = createOIDXheap(b, stable)) == NULL) {
		MT_lock_unset(&GDKhashLock(b->batCacheid));
		return GDK_FAIL;
	}

	mv = (oid *) m->base + ORDERIDXOFF;

	seq = b->hseqbase;
	for (p = 0, q = BATcount(b); p < q; p++)
		mv[p] = seq + p;

	if (!BATtdense(b)) {
		/* we need to sort a copy of the column so as not to
		 * change the original */
		bn = COLcopy(b, b->ttype, TRUE, TRANSIENT);
		if (bn == NULL) {
			HEAPfree(m, 1);
			GDKfree(m);
			MT_lock_unset(&GDKhashLock(b->batCacheid));
			return GDK_FAIL;
		}
		if (stable) {
			if (GDKssort(Tloc(bn, 0), mv,
				     bn->tvheap ? bn->tvheap->base : NULL,
				     BATcount(bn), Tsize(bn), SIZEOF_OID,
				     bn->ttype) != GDK_SUCCEED) {
				HEAPfree(m, 1);
				GDKfree(m);
				MT_lock_unset(&GDKhashLock(b->batCacheid));
				BBPunfix(bn->batCacheid);
				return GDK_FAIL;
			}
		} else {
			GDKqsort(Tloc(bn, 0), mv,
				 bn->tvheap ? bn->tvheap->base : NULL,
				 BATcount(bn), Tsize(bn), SIZEOF_OID,
				 bn->ttype);
		}
		/* we must unfix after releasing the lock since we
		 * might get deadlock otherwise (we're holding a lock
		 * based on b->batCacheid; unfix tries to get a lock
		 * based on bn->batCacheid, usually but (crucially)
		 * not always a different lock) */
	}

	b->torderidx = m;
	b->batDirtydesc = TRUE;
	persistOIDX(b);
	MT_lock_unset(&GDKhashLock(b->batCacheid));

	if (bn)
		BBPunfix(bn->batCacheid);

	return GDK_SUCCEED;
}

#define BINARY_MERGE(TYPE)						\
	do {								\
		TYPE *v = (TYPE *) Tloc(b, 0);				\
		if (p0 < q0 && p1 < q1) {				\
			if (v[*p0 - b->hseqbase] <= v[*p1 - b->hseqbase]) { \
				*mv++ = *p0++;				\
			} else {					\
				*mv++ = *p1++;				\
			}						\
		} else if (p0 < q0) {					\
			assert(p1 == q1);				\
			*mv++ = *p0++;					\
		} else if (p1 < q1) {					\
			assert(p0 == q0);				\
			*mv++ = *p1++;					\
		} else {						\
			assert(p0 == q0 && p1 == q1);			\
			break;						\
		}							\
		while (p0 < q0 && p1 < q1) {				\
			if (v[*p0 - b->hseqbase] <= v[*p1 - b->hseqbase]) { \
				*mv++ = *p0++;				\
			} else {					\
				*mv++ = *p1++;				\
			}						\
		}							\
		while (p0 < q0) {					\
			*mv++ = *p0++;					\
		}							\
		while (p1 < q1) {					\
			*mv++ = *p1++;					\
		}							\
	} while(0)

#define swap(X,Y,TMP)  (TMP)=(X);(X)=(Y);(Y)=(TMP)

#define left_child(X)  (2*(X)+1)
#define right_child(X) (2*(X)+2)

#define HEAPIFY(X)							\
	do {								\
		int cur, min = X, chld;					\
		do {							\
			cur = min;					\
			if ((chld = left_child(cur)) < n_ar &&		\
			    (minhp[chld] < minhp[min] ||		\
			     (minhp[chld] == minhp[min] &&		\
			      *p[cur] < *p[min]))) {			\
				min = chld;				\
			}						\
			if ((chld = right_child(cur)) < n_ar &&		\
			    (minhp[chld] < minhp[min] ||		\
			     (minhp[chld] == minhp[min] &&		\
			      *p[cur] < *p[min]))) {			\
				min = chld;				\
			}						\
			if (min != cur) {				\
				swap(minhp[cur], minhp[min], t);	\
				swap(p[cur], p[min], t_oid);		\
				swap(q[cur], q[min], t_oid);		\
			}						\
		} while (cur != min);					\
	} while (0)

#define NWAY_MERGE(TYPE)						\
	do {								\
		TYPE *minhp, t;						\
		TYPE *v = (TYPE *) Tloc(b, 0);				\
		if ((minhp = (TYPE *) GDKmalloc(sizeof(TYPE)*n_ar)) == NULL) { \
			goto bailout;					\
		}							\
		/* init min heap */					\
		for (i = 0; i < n_ar; i++) {				\
			minhp[i] = v[*p[i] - b->hseqbase];		\
		}							\
		for (i = n_ar/2; i >=0 ; i--) {				\
			HEAPIFY(i);					\
		}							\
		/* merge */						\
		*mv++ = *(p[0])++;					\
		if (p[0] < q[0]) {					\
			minhp[0] = v[*p[0] - b->hseqbase];		\
			HEAPIFY(0);					\
		} else {						\
			swap(minhp[0], minhp[n_ar-1], t);		\
			swap(p[0], p[n_ar-1], t_oid);			\
			swap(q[0], q[n_ar-1], t_oid);			\
			n_ar--;						\
			HEAPIFY(0);					\
		}							\
		while (n_ar > 1) {					\
			*mv++ = *(p[0])++;				\
			if (p[0] < q[0]) {				\
				minhp[0] = v[*p[0] - b->hseqbase];	\
				HEAPIFY(0);				\
			} else {					\
				swap(minhp[0], minhp[n_ar-1], t);	\
				swap(p[0], p[n_ar-1], t_oid);		\
				swap(q[0], q[n_ar-1], t_oid);		\
				n_ar--;					\
				HEAPIFY(0);				\
			}						\
		}							\
		while (p[0] < q[0]) {					\
			*mv++ = *(p[0])++;				\
		}							\
		GDKfree(minhp);						\
	} while (0)

gdk_return
GDKmergeidx(BAT *b, BAT**a, int n_ar)
{
	Heap *m;
	int i;
	size_t nmelen;
	oid *restrict mv;
	const char *nme = BBP_physical(b->batCacheid);

	if (BATcheckorderidx(b))
		return GDK_SUCCEED;
	MT_lock_set(&GDKhashLock(b->batCacheid));
	if (b->torderidx) {
		MT_lock_unset(&GDKhashLock(b->batCacheid));
		return GDK_SUCCEED;
	}
	nmelen = strlen(nme) + 12;
	if ((m = GDKzalloc(sizeof(Heap))) == NULL ||
	    (m->farmid = BBPselectfarm(b->batRole, b->ttype, orderidxheap)) < 0 ||
	    (m->filename = GDKmalloc(nmelen)) == NULL ||
	    snprintf(m->filename, nmelen, "%s.torderidx", nme) < 0 ||
	    HEAPalloc(m, BATcount(b) + ORDERIDXOFF, SIZEOF_OID) != GDK_SUCCEED) {
		if (m)
			GDKfree(m->filename);
		GDKfree(m);
		MT_lock_unset(&GDKhashLock(b->batCacheid));
		return GDK_FAIL;
	}
	m->free = (BATcount(b) + ORDERIDXOFF) * SIZEOF_OID;

	mv = (oid *) m->base;
	*mv++ = ORDERIDX_VERSION;
	*mv++ = (oid) BATcount(b);
	/* all participating indexes must be stable for the combined
	 * index to be stable */
	*mv = 1;
	for (i = 0; i < n_ar; i++) {
		if ((*mv &= ((const oid *) a[i]->torderidx->base)[2]) == 0)
			break;
	}
	mv++;

	if (n_ar == 1) {
		/* One oid order bat, nothing to merge */
		assert(BATcount(a[0]) == BATcount(b));
		assert((VIEWtparent(a[0]) == b->batCacheid ||
			VIEWtparent(a[0]) == VIEWtparent(b)) &&
		       a[0]->torderidx);
		memcpy(mv, (const oid *) a[0]->torderidx->base + ORDERIDXOFF,
		       BATcount(a[0]) * SIZEOF_OID);
	} else if (n_ar == 2) {
		/* sort merge with 1 comparison per BUN */
		const oid *restrict p0, *restrict p1, *q0, *q1;
		assert(BATcount(a[0]) + BATcount(a[1]) == BATcount(b));
		assert((VIEWtparent(a[0]) == b->batCacheid ||
			VIEWtparent(a[0]) == VIEWtparent(b)) &&
		       a[0]->torderidx);
		assert((VIEWtparent(a[1]) == b->batCacheid ||
			VIEWtparent(a[1]) == VIEWtparent(b)) &&
		       a[1]->torderidx);
		p0 = (const oid *) a[0]->torderidx->base + ORDERIDXOFF;
		p1 = (const oid *) a[1]->torderidx->base + ORDERIDXOFF;
		q0 = p0 + BATcount(a[0]);
		q1 = p1 + BATcount(a[1]);

		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte: BINARY_MERGE(bte); break;
		case TYPE_sht: BINARY_MERGE(sht); break;
		case TYPE_int: BINARY_MERGE(int); break;
		case TYPE_lng: BINARY_MERGE(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: BINARY_MERGE(hge); break;
#endif
		case TYPE_flt: BINARY_MERGE(flt); break;
		case TYPE_dbl: BINARY_MERGE(dbl); break;
		case TYPE_str:
		default:
			/* TODO: support strings, date, timestamps etc. */
			assert(0);
			HEAPfree(m, 1);
			GDKfree(m);
			MT_lock_unset(&GDKhashLock(b->batCacheid));
			return GDK_FAIL;
		}

	} else {
		/* use min-heap */
		oid **p, **q, *t_oid;

		p = (oid **) GDKmalloc(n_ar*sizeof(oid *));
		q = (oid **) GDKmalloc(n_ar*sizeof(oid *));
		if (p == NULL || q == NULL) {
		  bailout:
			GDKfree(p);
			GDKfree(q);
			HEAPfree(m, 1);
			GDKfree(m);
			MT_lock_unset(&GDKhashLock(b->batCacheid));
			return GDK_FAIL;
		}
		for (i = 0; i < n_ar; i++) {
			assert((VIEWtparent(a[i]) == b->batCacheid ||
				VIEWtparent(a[i]) == VIEWtparent(b)) &&
			       a[i]->torderidx);
			p[i] = (oid *) a[i]->torderidx->base + ORDERIDXOFF;
			q[i] = p[i] + BATcount(a[i]);
		}

		switch (ATOMstorage(b->ttype)) {
		case TYPE_bte: NWAY_MERGE(bte); break;
		case TYPE_sht: NWAY_MERGE(sht); break;
		case TYPE_int: NWAY_MERGE(int); break;
		case TYPE_lng: NWAY_MERGE(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: NWAY_MERGE(hge); break;
#endif
		case TYPE_flt: NWAY_MERGE(flt); break;
		case TYPE_dbl: NWAY_MERGE(dbl); break;
		case TYPE_void:
		case TYPE_str:
		case TYPE_ptr:
		default:
			/* TODO: support strings, date, timestamps etc. */
			assert(0);
			goto bailout;
		}
		GDKfree(p);
		GDKfree(q);
	}

#ifdef PERSISTENTIDX
	if ((BBP_status(b->batCacheid) & BBPEXISTING) &&
	    b->batInserted == b->batCount) {
		MT_Id tid;
		struct idxsync *hs = GDKmalloc(sizeof(*hs));
		if (hs != NULL) {
			BBPfix(b->batCacheid);
			hs->id = b->batCacheid;
			hs->hp = m;
			hs->func = "GDKmergeidx";
			MT_create_thread(&tid, BATidxsync, hs, MT_THR_DETACHED);
		}
	} else
		ALGODEBUG fprintf(stderr, "#GDKmergeidx: NOT persisting index %d\n", b->batCacheid);
#endif

	b->batDirtydesc = TRUE;
	b->torderidx = m;
	MT_lock_unset(&GDKhashLock(b->batCacheid));
	return GDK_SUCCEED;
}

void
OIDXfree(BAT *b)
{
	if (b) {
		Heap *hp;

		MT_lock_set(&GDKhashLock(b->batCacheid));
		if ((hp = b->torderidx) != NULL && hp != (Heap *) 1) {
			b->torderidx = (Heap *) 1;
			HEAPfree(hp, 0);
			GDKfree(hp);
		}
		MT_lock_unset(&GDKhashLock(b->batCacheid));
	}
}

void
OIDXdestroy(BAT *b)
{
	if (b) {
		Heap *hp;

		MT_lock_set(&GDKhashLock(b->batCacheid));
		hp = b->torderidx;
		b->torderidx = NULL;
		MT_lock_unset(&GDKhashLock(b->batCacheid));
		if (hp == (Heap *) 1) {
			GDKunlink(BBPselectfarm(b->batRole, b->ttype, orderidxheap),
				  BATDIR,
				  BBP_physical(b->batCacheid),
				  "torderidx");
		} else if (hp != NULL) {
			HEAPdelete(hp, BBP_physical(b->batCacheid), "torderidx");
			GDKfree(hp);
		}
	}
}
