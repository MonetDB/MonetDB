/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define ORDERIDX_VERSION	((oid) 1)

/* return TRUE if we have a orderidx on the tail, even if we need to read
 * one from disk */
int
BATcheckorderidx(BAT *b)
{
	int ret;
	lng t;

	assert(b->batCacheid > 0);
	t = GDKusec();
	MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
	t = GDKusec() - t;
	if (b->torderidx == NULL) {
		Heap *hp;
		const char *nme = BBP_physical(b->batCacheid);
		const char *ext = b->batCacheid > 0 ? "torderidx" : "horderidx";
		int fd;

		if ((hp = GDKzalloc(sizeof(*hp))) != NULL &&
		    (hp->farmid = BBPselectfarm(b->batRole, b->ttype, orderidxheap)) >= 0 &&
		    (hp->filename = GDKmalloc(strlen(nme) + 10)) != NULL) {
			sprintf(hp->filename, "%s.%s", nme, ext);

			/* check whether a persisted orderidx can be found */
			if ((fd = GDKfdlocate(hp->farmid, nme, "rb+", ext)) >= 0) {
				struct stat st;
				oid hdata[ORDERIDXOFF];

				if (read(fd, hdata, sizeof(hdata)) == sizeof(hdata) &&
				    hdata[0] == (((oid) 1 << 24) | ORDERIDX_VERSION) &&
				    hdata[1] == (oid) BATcount(b) &&
				    fstat(fd, &st) == 0 &&
				    st.st_size >= (off_t) (hp->size = hp->free = (ORDERIDXOFF + hdata[1]) * SIZEOF_OID) &&
				    HEAPload(hp, nme, ext, 0) == GDK_SUCCEED) {
					close(fd);
					b->torderidx = hp;
					ALGODEBUG fprintf(stderr, "#BATcheckorderidx: reusing persisted orderidx %d\n", b->batCacheid);
					MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
					return 1;
				}
				close(fd);
				/* unlink unusable file */
				GDKunlink(hp->farmid, BATDIR, nme, ext);
			}
			GDKfree(hp->filename);
		}
		GDKfree(hp);
		GDKclrerr();	/* we're not currently interested in errors */
	}
	ret = b->T->orderidx != NULL;
	MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
	ALGODEBUG if (ret) fprintf(stderr, "#BATcheckorderidx: already has orderidx %d, waited " LLFMT " usec\n", b->batCacheid, t);
	return ret;
}

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
	MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
	if (b->torderidx) {
		MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
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
		MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
		return GDK_FAIL;
	}
	m->free = (BATcount(b) + ORDERIDXOFF) * SIZEOF_OID;

	mv = (oid *) m->base;
	*mv++ = ORDERIDX_VERSION;
	*mv++ = (oid) BATcount(b);

	if (n_ar == 1) {
		/* One oid order bat, nothing to merge */
		memcpy(mv, Tloc(a[0], BUNfirst(a[0])), BATcount(b) * SIZEOF_OID);
	} else {
		/* sort merge with 1 comparison per BUN */
		if (n_ar == 2) {
			const oid *p0, *p1, *q0, *q1;
			p0 = (const oid *) Tloc(a[0], BUNfirst(a[0]));
			q0 = (const oid *) Tloc(a[0], BUNlast(a[0]));
			p1 = (const oid *) Tloc(a[1], BUNfirst(a[1]));
			q1 = (const oid *) Tloc(a[1], BUNlast(a[1]));

#define BINARY_MERGE(TYPE)						\
	do {								\
		TYPE *v = (TYPE *) Tloc(b, BUNfirst(b));		\
		while (p0 < q0 && p1 < q1) {				\
			if (v[*p0 - b->hseqbase] < v[*p1 - b->hseqbase]) { \
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
				MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
				return GDK_FAIL;
			}

		/* use min-heap */
		} else {
			oid **p, **q, *t_oid;

			p = (oid **) GDKmalloc(n_ar*sizeof(oid *));
			q = (oid **) GDKmalloc(n_ar*sizeof(oid *));
			if (p == NULL || q == NULL) {
bailout:
				GDKfree(p);
				GDKfree(q);
				HEAPfree(m, 1);
				GDKfree(m);
				MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
				return GDK_FAIL;
			}
			for (i = 0; i < n_ar; i++) {
				p[i] = (oid *) Tloc(a[i], BUNfirst(a[i]));
				q[i] = (oid *) Tloc(a[i], BUNlast(a[i]));
			}

#define swap(X,Y,TMP)  (TMP)=(X);(X)=(Y);(Y)=(TMP)

#define left_child(X)  (2*(X)+1)
#define right_child(X) (2*(X)+2)

#define HEAPIFY(X)							\
	do {								\
		int __cur, __min = X;					\
		do {							\
			__cur = __min;					\
			if (left_child(__cur) < n_ar &&			\
				minhp[left_child(__cur)] < minhp[__min]) { \
				__min = left_child(__cur);		\
			}						\
			if (right_child(__cur) < n_ar &&		\
				minhp[right_child(__cur)] < minhp[__min]) { \
				__min = right_child(__cur);		\
			}						\
			if (__min != __cur) {				\
				swap(minhp[__cur], minhp[__min], t);	\
				swap(p[__cur], p[__min], t_oid);	\
				swap(q[__cur], q[__min], t_oid);	\
			}						\
		} while (__cur != __min);				\
	} while (0)

#define NWAY_MERGE(TYPE)						\
	do {								\
		TYPE *minhp, t;						\
		TYPE *v = (TYPE *) Tloc(b, BUNfirst(b));		\
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
		while (n_ar > 1) {					\
			*mv++ = *(p[0])++;				\
			if (p[0] < q[0]) {				\
				minhp[0] = v[*p[0] - b->hseqbase];	\
			} else {					\
				swap(minhp[0], minhp[n_ar-1], t);	\
				swap(p[0], p[n_ar-1], t_oid);		\
				swap(q[0], q[n_ar-1], t_oid);		\
				n_ar--;					\
			}						\
			HEAPIFY(0);					\
		}							\
		while (p[0] < q[0]) {					\
			*mv++ = *(p[0])++;				\
		}							\
		GDKfree(minhp);						\
	} while (0)

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
	}

	if ((BBP_status(b->batCacheid) & BBPEXISTING) &&
	    b->batInserted == b->batCount &&
	    HEAPsave(m, nme, "torderidx") == GDK_SUCCEED &&
	    (i = GDKfdlocate(m->farmid, nme, "rb+", "torderidx")) >= 0) {
		ALGODEBUG fprintf(stderr, "#GDKmergeidx: persisting orderidx %d\n", b->batCacheid);
		((oid *) m->base)[0] |= (oid) 1 << 24;
		if (write(i, m->base, SIZEOF_OID) < 0)
			perror("write orderidx");
		if (!(GDKdebug & FORCEMITOMASK)) {
#if defined(NATIVE_WIN32)
			_commit(i);
#elif defined(HAVE_FDATASYNC)
			fdatasync(i);
#elif defined(HAVE_FSYNC)
			fsync(i);
#endif
		}
		close(i);
	} else {
		ALGODEBUG fprintf(stderr, "#GDKmergeidx: NOT persisting orderidx %d\n", b->batCacheid);
	}

	b->batDirtydesc = TRUE;
	b->torderidx = m;
	MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
	return GDK_SUCCEED;
}

void
OIDXdestroy(BAT *b)
{
	if (b) {
		Heap *hp;

		MT_lock_set(&GDKhashLock(abs(b->batCacheid)));
		if ((hp = b->torderidx) != NULL) {
			b->torderidx = NULL;
			HEAPdelete(hp, BBP_physical(b->batCacheid), "torderidx");
			GDKfree(hp);
		}
		MT_lock_unset(&GDKhashLock(abs(b->batCacheid)));
	}
}
