/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define ORDERIDX_VERSION	((oid) 3)

#ifdef PERSISTENTIDX
static void
BATidxsync(void *arg)
{
	BAT *b = arg;
	Heap *hp;
	int fd;
	lng t0  = GDKusec();
	const char *failed = " failed";


	MT_lock_set(&b->batIdxLock);
	if ((hp = b->torderidx) != NULL) {
		if (HEAPsave(hp, hp->filename, NULL, true, hp->free) == GDK_SUCCEED) {
			if (hp->storage == STORE_MEM) {
				if ((fd = GDKfdlocate(hp->farmid, hp->filename, "rb+", NULL)) >= 0) {
					((oid *) hp->base)[0] |= (oid) 1 << 24;
					if (write(fd, hp->base, SIZEOF_OID) >= 0) {
						failed = ""; /* not failed */
						if (!(GDKdebug & NOSYNCMASK)) {
#if defined(NATIVE_WIN32)
							_commit(fd);
#elif defined(HAVE_FDATASYNC)
							fdatasync(fd);
#elif defined(HAVE_FSYNC)
							fsync(fd);
#endif
						}
						hp->dirty = false;
					} else {
						perror("write hash");
					}
					close(fd);
				}
			} else {
				((oid *) hp->base)[0] |= (oid) 1 << 24;
				if (!(GDKdebug & NOSYNCMASK) &&
				    MT_msync(hp->base, SIZEOF_OID) < 0) {
					((oid *) hp->base)[0] &= ~((oid) 1 << 24);
				} else {
					hp->dirty = false;
					failed = ""; /* not failed */
				}
			}
			TRC_DEBUG(ACCELERATOR, "BATidxsync(%s): orderidx persisted"
				  " (" LLFMT " usec)%s\n",
				  BATgetId(b), GDKusec() - t0, failed);
		}
	}
	MT_lock_unset(&b->batIdxLock);
	BBPunfix(b->batCacheid);
}
#endif

/* return TRUE if we have a orderidx on the tail, even if we need to read
 * one from disk */
bool
BATcheckorderidx(BAT *b)
{
	bool ret;
	lng t = GDKusec();

	if (b == NULL)
		return false;
	/* we don't need the lock just to read the value b->torderidx */
	if (b->torderidx == (Heap *) 1) {
		/* but when we want to change it, we need the lock */
		assert(!GDKinmemory(b->theap->farmid));
		MT_lock_set(&b->batIdxLock);
		if (b->torderidx == (Heap *) 1) {
			Heap *hp;
			const char *nme = BBP_physical(b->batCacheid);
			int fd;

			b->torderidx = NULL;
			if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
			    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, orderidxheap)) >= 0) {
				strconcat_len(hp->filename,
					      sizeof(hp->filename),
					      nme, ".torderidx", NULL);
				hp->storage = hp->newstorage = STORE_INVALID;

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
					    HEAPload(hp, nme, "torderidx", false) == GDK_SUCCEED) {
						close(fd);
						ATOMIC_INIT(&hp->refs, 1);
						b->torderidx = hp;
						TRC_DEBUG(ACCELERATOR, "BATcheckorderidx(" ALGOBATFMT "): reusing persisted orderidx\n", ALGOBATPAR(b));
						MT_lock_unset(&b->batIdxLock);
						return true;
					}
					close(fd);
					/* unlink unusable file */
					GDKunlink(hp->farmid, BATDIR, nme, "torderidx");
				}
			}
			GDKfree(hp);
			GDKclrerr();	/* we're not currently interested in errors */
		}
		MT_lock_unset(&b->batIdxLock);
	}
	ret = b->torderidx != NULL;
	if (ret)
		TRC_DEBUG(ACCELERATOR, "BATcheckorderidx(" ALGOBATFMT "): already has orderidx, waited " LLFMT " usec\n", ALGOBATPAR(b), GDKusec() - t);
	return ret;
}

/* create the heap for an order index; returns NULL on failure */
Heap *
createOIDXheap(BAT *b, bool stable)
{
	Heap *m;
	oid *restrict mv;

	if ((m = GDKzalloc(sizeof(Heap))) == NULL ||
	    (m->farmid = BBPselectfarm(b->batRole, b->ttype, orderidxheap)) < 0 ||
	    strconcat_len(m->filename, sizeof(m->filename),
			  BBP_physical(b->batCacheid), ".torderidx",
			  NULL) >= sizeof(m->filename) ||
	    HEAPalloc(m, BATcount(b) + ORDERIDXOFF, SIZEOF_OID, 0) != GDK_SUCCEED) {
		GDKfree(m);
		return NULL;
	}
	m->free = (BATcount(b) + ORDERIDXOFF) * SIZEOF_OID;
	m->dirty = true;

	mv = (oid *) m->base;
	*mv++ = ORDERIDX_VERSION;
	*mv++ = (oid) BATcount(b);
	*mv++ = (oid) stable;
	return m;
}

/* maybe persist the order index heap */
void
persistOIDX(BAT *b)
{
#ifdef PERSISTENTIDX
	if ((BBP_status(b->batCacheid) & BBPEXISTING) &&
	    b->batInserted == b->batCount &&
	    !b->theap->dirty &&
	    !GDKinmemory(b->theap->farmid)) {
		MT_Id tid;
		BBPfix(b->batCacheid);
		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "oidxsync%d", b->batCacheid);
		if (MT_create_thread(&tid, BATidxsync, b,
				     MT_THR_DETACHED, name) < 0)
			BBPunfix(b->batCacheid);
	} else
		TRC_DEBUG(ACCELERATOR, "persistOIDX(" ALGOBATFMT "): NOT persisting order index\n", ALGOBATPAR(b));
#else
	(void) b;
#endif
}

gdk_return
BATorderidx(BAT *b, bool stable)
{
	if (BATcheckorderidx(b))
		return GDK_SUCCEED;
	if (!BATtdense(b)) {
		BAT *on;
		MT_thread_setalgorithm("create order index");
		TRC_DEBUG(ACCELERATOR, "BATorderidx(" ALGOBATFMT ",%d) create index\n", ALGOBATPAR(b), stable);
		if (BATsort(NULL, &on, NULL, b, NULL, NULL, false, false, stable) != GDK_SUCCEED)
			return GDK_FAIL;
		assert(BATcount(b) == BATcount(on));
		if (BATtdense(on)) {
			/* if the order bat is dense, the input was
			 * sorted and we don't need an order index */
			assert(!b->tnosorted);
			if (!b->tsorted) {
				b->tsorted = true;
				b->tnosorted = 0;
			}
		} else {
			/* BATsort quite possibly already created the
			 * order index, but just to be sure... */
			MT_lock_set(&b->batIdxLock);
			if (b->torderidx == NULL) {
				Heap *m;
				if ((m = createOIDXheap(b, stable)) == NULL) {
					MT_lock_unset(&b->batIdxLock);
					return GDK_FAIL;
				}
				memcpy((oid *) m->base + ORDERIDXOFF, Tloc(on, 0), BATcount(on) * sizeof(oid));
				ATOMIC_INIT(&m->refs, 1);
				b->torderidx = m;
				persistOIDX(b);
			}
			MT_lock_unset(&b->batIdxLock);
		}
		BBPunfix(on->batCacheid);
	}
	return GDK_SUCCEED;
}

#define BINARY_MERGE(TYPE)						\
	do {								\
		TYPE *v = (TYPE *) bi.base;				\
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
		TYPE *v = (TYPE *) bi.base;				\
		if ((minhp = GDKmalloc(sizeof(TYPE)*n_ar)) == NULL) {	\
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
	oid *restrict mv;
	const char *nme = BBP_physical(b->batCacheid);

	if (BATcheckorderidx(b))
		return GDK_SUCCEED;
	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_flt:
	case TYPE_dbl:
		break;
	default:
		GDKerror("type %s not supported.\n", ATOMname(b->ttype));
		return GDK_FAIL;
	}
	TRC_DEBUG(ACCELERATOR, "GDKmergeidx(" ALGOBATFMT ") create index\n", ALGOBATPAR(b));
	BATiter bi = bat_iterator(b);
	MT_lock_set(&b->batIdxLock);
	if (b->torderidx) {
		MT_lock_unset(&b->batIdxLock);
		bat_iterator_end(&bi);
		return GDK_SUCCEED;
	}
	if ((m = GDKzalloc(sizeof(Heap))) == NULL ||
	    (m->farmid = BBPselectfarm(b->batRole, b->ttype, orderidxheap)) < 0 ||
	    strconcat_len(m->filename, sizeof(m->filename),
			  nme, ".torderidx", NULL) >= sizeof(m->filename) ||
	    HEAPalloc(m, BATcount(b) + ORDERIDXOFF, SIZEOF_OID, 0) != GDK_SUCCEED) {
		GDKfree(m);
		MT_lock_unset(&b->batIdxLock);
		bat_iterator_end(&bi);
		return GDK_FAIL;
	}
	m->free = (BATcount(b) + ORDERIDXOFF) * SIZEOF_OID;
	m->dirty = true;

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

		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte: BINARY_MERGE(bte); break;
		case TYPE_sht: BINARY_MERGE(sht); break;
		case TYPE_int: BINARY_MERGE(int); break;
		case TYPE_lng: BINARY_MERGE(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: BINARY_MERGE(hge); break;
#endif
		case TYPE_flt: BINARY_MERGE(flt); break;
		case TYPE_dbl: BINARY_MERGE(dbl); break;
		default:
			/* TODO: support strings, date, timestamps etc. */
			assert(0);
			HEAPfree(m, true);
			GDKfree(m);
			MT_lock_unset(&b->batIdxLock);
			bat_iterator_end(&bi);
			return GDK_FAIL;
		}

	} else {
		/* use min-heap */
		oid **p, **q, *t_oid;

		p = GDKmalloc(n_ar*sizeof(oid *));
		q = GDKmalloc(n_ar*sizeof(oid *));
		if (p == NULL || q == NULL) {
		  bailout:
			GDKfree(p);
			GDKfree(q);
			HEAPfree(m, true);
			GDKfree(m);
			MT_lock_unset(&b->batIdxLock);
			bat_iterator_end(&bi);
			return GDK_FAIL;
		}
		for (i = 0; i < n_ar; i++) {
			assert((VIEWtparent(a[i]) == b->batCacheid ||
				VIEWtparent(a[i]) == VIEWtparent(b)) &&
			       a[i]->torderidx);
			p[i] = (oid *) a[i]->torderidx->base + ORDERIDXOFF;
			q[i] = p[i] + BATcount(a[i]);
		}

		switch (ATOMbasetype(b->ttype)) {
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

	ATOMIC_INIT(&m->refs, 1);
	b->torderidx = m;
#ifdef PERSISTENTIDX
	if ((BBP_status(b->batCacheid) & BBPEXISTING) &&
	    b->batInserted == b->batCount) {
		MT_Id tid;
		BBPfix(b->batCacheid);
		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "oidxsync%d", b->batCacheid);
		if (MT_create_thread(&tid, BATidxsync, b,
				     MT_THR_DETACHED, name) < 0)
			BBPunfix(b->batCacheid);
	} else
		TRC_DEBUG(ACCELERATOR, "GDKmergeidx(%s): NOT persisting index\n", BATgetId(b));
#endif

	MT_lock_unset(&b->batIdxLock);
	bat_iterator_end(&bi);
	return GDK_SUCCEED;
}

void
OIDXfree(BAT *b)
{
	if (b && b->torderidx) {
		Heap *hp;

		MT_lock_set(&b->batIdxLock);
		if ((hp = b->torderidx) != NULL && hp != (Heap *) 1) {
			if (GDKinmemory(b->theap->farmid)) {
				b->torderidx = NULL;
				HEAPdecref(hp, true);
			} else {
				b->torderidx = (Heap *) 1;
				HEAPdecref(hp, false);
			}
		}
		MT_lock_unset(&b->batIdxLock);
	}
}

void
OIDXdestroy(BAT *b)
{
	if (b && b->torderidx) {
		Heap *hp;

		MT_lock_set(&b->batIdxLock);
		hp = b->torderidx;
		b->torderidx = NULL;
		MT_lock_unset(&b->batIdxLock);
		if (hp == (Heap *) 1) {
			GDKunlink(BBPselectfarm(b->batRole, b->ttype, orderidxheap),
				  BATDIR,
				  BBP_physical(b->batCacheid),
				  "torderidx");
		} else if (hp != NULL) {
			HEAPdecref(hp, true);
		}
	}
}
