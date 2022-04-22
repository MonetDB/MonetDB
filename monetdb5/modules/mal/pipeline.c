/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_atoms.h"
#include "gdk_time.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "algebra.h"

typedef struct sql_allocator {
    size_t size;
    size_t nr;
    char **blks;
    size_t used;    /* memory used in last block */
    size_t usedmem; /* used memory */
} sql_allocator;

#define SA_BLOCK 16*1024

static void
sa_destroy(sql_allocator* sa)
{
    for (size_t i = 0; i<sa->nr; i++) {
        GDKfree(sa->blks[i]);
    }
    GDKfree(sa->blks);
    GDKfree(sa);
}

static sql_allocator *
sa_create(void)
{
    sql_allocator *sa = (sql_allocator*)GDKmalloc(sizeof(sql_allocator));
    if (sa == NULL)
        return NULL;
    sa->size = 256;
    sa->nr = 1;
    sa->blks = (char**)GDKmalloc(sizeof(char*) * sa->size);
    if (sa->blks == NULL) {
        GDKfree(sa);
        return NULL;
    }
    sa->blks[0] = (char*)GDKmalloc(SA_BLOCK);
    sa->usedmem = SA_BLOCK;
    if (sa->blks[0] == NULL) {
        GDKfree(sa->blks);
        GDKfree(sa);
        return NULL;
    }
    sa->used = 0;
    return sa;
}

static void *
sa_alloc( sql_allocator *sa, size_t sz )
{
    char *r;
    if (sz > (SA_BLOCK-sa->used)) {
        r = GDKmalloc(sz > SA_BLOCK ? sz : SA_BLOCK);
        if (r == NULL)
            return NULL;
        if (sa->nr >= sa->size) {
            char **tmp;
            sa->size *=2;
            tmp = (char**)GDKrealloc(sa->blks, sizeof(char*) * sa->size);
            if (tmp == NULL) {
                sa->size /= 2; /* undo */
                GDKfree(r);
                return NULL;
            }
            sa->blks = tmp;
        }
        if (sz > SA_BLOCK) {
            sa->blks[sa->nr] = sa->blks[sa->nr-1];
            sa->blks[sa->nr-1] = r;
            sa->nr ++;
            sa->usedmem += sz;
        } else {
            sa->blks[sa->nr] = r;
            sa->nr ++;
            sa->used = sz;
            sa->usedmem += SA_BLOCK;
        }
    } else {
        r = sa->blks[sa->nr-1] + sa->used;
        sa->used += sz;
    }
    return r;
}

static char *
sa_strdup( sql_allocator *sa, const char *s )
{
	int l = strlen(s);
    char *r = sa_alloc(sa, l+1);

    if (r)
        memcpy(r, s, l+1);
    return r;
}

static void
BATnegateprops(BAT *b)
{
	/* disable all properties here */
	b->tnonil = false;
	b->tnil = false;
	if (b->ttype) {
		b->tsorted = false;
		b->trevsorted = false;
		b->tnosorted = 0;
		b->tnorevsorted = 0;
	}
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	b->tmaxpos = b->tminpos = BUN_NONE;
}

#define pipeline_lock(p) MT_lock_set(&p->p->l)
#define pipeline_unlock(p) MT_lock_unset(&p->p->l)

#define pipeline_lock1(r) MT_lock_set(&r->batIdxLock)
#define pipeline_unlock1(r) MT_lock_unset(&r->batIdxLock)

#define pipeline_lock2(r) MT_lock_set(&r->theaplock)
#define pipeline_unlock2(r) MT_lock_unset(&r->theaplock)

static int
BATupgrade(BAT *r, BAT *b)
{
	int err = 0;
	MT_lock_set(&b->theaplock);
	//MT_lock_set(&r->theaplock);
	//TODO add upgradevarheap variant which only widens, no resize!
	if (ATOMvarsized(r->ttype) &&
		BATcount(r) == 0 &&
		r->tvheap->parentid == r->batCacheid &&
		r->twidth < b->twidth &&
		GDKupgradevarheap(r, (1 << (8 << (b->tshift - 1))) + GDK_VAROFFSET, 0, 0) != GDK_SUCCEED) {
			err = 1;
	}
	//MT_lock_unset(&r->theaplock);
	MT_lock_unset(&b->theaplock);
	return err;
}

static void
BATswap_heaps(BAT *u, BAT *b, Pipeline *p)
{
	MT_lock_set(&b->theaplock);
	MT_lock_set(&u->theaplock);
	if (p)
		pipeline_lock(p);
	if (ATOMvarsized(u->ttype) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		HEAPdecref(u->tvheap, u->tvheap->parentid == u->batCacheid);
		HEAPincref(b->tvheap);
		u->tvheap = b->tvheap;
		BBPshare(b->tvheap->parentid);
		u->batDirtydesc = true;
	}
	if (p)
		pipeline_unlock(p);
	MT_lock_unset(&u->theaplock);
	MT_lock_unset(&b->theaplock);
}

static str
PPcounter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);

	*res = PIPELINEnext_counter(p);
	(void)cntxt; (void)mb;
	return MAL_SUCCEED;
}

// 	 (mailbox:T, metadata:int) := pipeline.channel(initial_value:T)
static str
PPchannel(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	void *mailbox = getArgReference(stk, pci, 0);
	int *metadata = getArgReference_int(stk, pci, 1);
	void *value = getArgReference(stk, pci, 2);
	int tpe = getArgGDKType(mb, pci, 2);

	if (ATOMvarsized(tpe))
		throw(MAL, "pipeline.chanel", SQLSTATE(42000)"cannot make channel for varsized items");

	if (ATOMputFIX(tpe, mailbox, value) != GDK_SUCCEED) {
		throw(MAL, "pipeline.send", GDK_EXCEPTION);
	}
	*metadata = 0;

	return MAL_SUCCEED;
}

// 	 dummy := pipeline.send(handle:ptr, mailbox:T, metadata:int, value:T)
static str
PPsend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;

	ptr handle = *getArgReference_ptr(stk, pci, 1);
	Pipeline *p = (Pipeline*)handle;
	Pipelines *pp = p->p;

	// Note: these point into the master stack frame not the worker stack frame
	void *mailbox = getArgReference(pp->stk, pci, 2);
	int *metadata = getArgReference_int(pp->stk, pci, 3);

	void *value = getArgReference(stk, pci, 4);
	int tpe = getArgGDKType(mb, pci, 4);

	MT_lock_set(&pp->l);

	const char *ch_name = mb->var[getArg(pci, 2)].name;
	char *formatted = ATOMformat(tpe, value);
	fprintf(stderr, "Iteration %d sending value %s on channel %s\n", pp->counters[p->wid], formatted, ch_name);
	GDKfree(formatted);

	if (!is_int_nil(*metadata)) {
		msg = createException(MAL, "pipeline.send", SQLSTATE(42000)"causality violation detected in iteration %d: %d has sent already",
			pp->counters[p->wid], *metadata);
		goto bailout;
	}

	ATOMunfix(tpe, mailbox);
	if (ATOMputFIX(tpe, mailbox, value) != GDK_SUCCEED) {
		msg = createException(MAL, "pipeline.send", GDK_EXCEPTION);
		goto bailout;
	}
	*metadata = p->p->counters[p->wid] + 1;

	PIPELINEnotify(p, "send");
bailout:
	MT_lock_unset(&pp->l);
	return msg;
}

// 	 value:T := pipeline.recv(handle, mailbox:T, metadata:int)
static str
PPrecv(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;
	bool locked = false;

	void *ret = getArgReference(stk, pci, 0);

	ptr handle = *getArgReference_ptr(stk, pci, 1);
	Pipeline *p = (Pipeline*)handle;
	Pipelines *pp = p->p;

	// Note: these point into the master stack frame not the worker stack frame
	void *mailbox = getArgReference(pp->stk, pci, 2);
	int *metadata = getArgReference_int(pp->stk, pci, 3);
	int prev = int_nil;

	int tpe = getArgGDKType(mb, pci, 2);
	const char *ch_name = mb->var[getArg(pci, 2)].name;

	MT_lock_set(&pp->l);
	locked = true;
	prev = *metadata ^1; // different but no overflow
	while (true) {
		int myself = pp->counters[p->wid];
		if (*metadata == myself) {
			// found it, drop out
			if (ATOMputFIX(tpe, ret, mailbox) != GDK_SUCCEED) {
				msg = createException(MAL, "pipeline.recv", GDK_EXCEPTION);
				goto bailout;
			}
			if (ATOMputFIX(tpe, mailbox, ATOMnilptr(tpe)) != GDK_SUCCEED) {
				msg = createException(MAL, "pipeline.recv", GDK_EXCEPTION);
				goto bailout;
			}
			*metadata = int_nil;
			//
			char *formatted = ATOMformat(tpe, ret);
			fprintf(stderr, "Iteration %d recv'd %s from channel %s\n", pp->counters[p->wid], formatted, ch_name);
			GDKfree(formatted);
			break;
		}
		// value is not in yet, is there still hope?
		bool sender_still_running = false;
		for (int i = 0; i < p->p->nr_workers; i++) {
			if (p->p->counters[i] == myself - 1) {
				sender_still_running = true;
				break;
			}
		}
		if (!sender_still_running) {
			fprintf(stderr, "Iteration %d failed to recv from channel %s because no message was sent\n", p->p->counters[p->wid], ch_name);
			msg = createException(MAL, "pipeline.recv", SQLSTATE(42000)"iteration %d neglected to send a message to %d", myself - 1, myself);
			break;
		}

		if (*metadata != prev) {
			prev = *metadata;
			fprintf(stderr, "Iteration %d waiting to recv from channel %s [currently ", p->p->counters[p->wid], ch_name);
			if (is_int_nil(*metadata))
				fprintf(stderr, "empty]\n");
			else
				fprintf(stderr, "for %d]\n", *metadata);
		}

		// Wait until something changes
		PIPELINEwait(p);
	}

bailout:
	if (locked)
		MT_lock_unset(&p->p->l);
	return msg;
}


#define sum(a,b) a+b
#define prod(a,b) a*b
#define min(a,b) a<b?a:b
#define max(a,b) a>b?a:b

#define uuid_min(a,b) ((cmp((void*)&a,(void*)&b)<0)?a:b)
#define uuid_max(a,b) ((cmp((void*)&a,(void*)&b)>0)?a:b)

#define getArgReference_date(stk, pci, nr) (date*)getArgReference(stk, pci, nr)
#define getArgReference_daytime(stk, pci, nr) (daytime*)getArgReference(stk, pci, nr)
#define getArgReference_timestamp(stk, pci, nr) (timestamp*)getArgReference(stk, pci, nr)

#define aggr(T,f)  \
	if (type == TYPE_##T) {								\
		T val = *getArgReference_##T(stk, pci, 2);		\
		if (!is_##T##_nil(val) && BATcount(b)) {		\
			T *t = Tloc(b, 0);							\
			if (is_##T##_nil(t[0])) {					\
				t[0] = val;								\
			} else										\
				t[0] = f(t[0], val);					\
			b->tnil = false;							\
			b->tnonil = true;							\
		} else if (BATcount(b) == 0) {					\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)\
				err = createException(SQL, "aggr." #f,	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}												\
	}

#define faggr(T,f)  \
	if (type == TYPE_##T) {								\
		T val = *getArgReference_TYPE(stk, pci, 2, T);		\
		int (*cmp)(const void *v1,const void *v2) = ATOMcompare(type); \
		if (!is_##T##_nil(val) && BATcount(b)) {		\
			T *t = Tloc(b, 0);							\
			if (is_##T##_nil(t[0])) {					\
				t[0] = val;								\
			} else										\
				t[0] = f(t[0], val);					\
			b->tnil = false;							\
			b->tnonil = true;							\
		} else if (BATcount(b) == 0) {					\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)\
				err = createException(SQL, "aggr." #f,	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}												\
	}

#define vaggr(T,f)  \
	if (type == TYPE_##T) {								\
		BATiter bi = bat_iterator(b); \
		T val = *getArgReference_##T(stk, pci, 2);		\
		const void *nil = ATOMnilptr(type);						\
		int (*cmp)(const void *v1,const void *v2) = ATOMcompare(type); \
		if (cmp(val,nil) != 0 && BATcount(b)) {		\
			T t = BUNtvar(bi, 0); \
			if (cmp(t,nil) == 0) {					\
				if (BUNreplace(b, 0, val, false) != GDK_SUCCEED)			\
					err = createException(SQL, "2 aggr." #f,	\
						SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
			} else										\
				if (f(t, val) == val)					\
					if (BUNreplace(b, 0, val, false) != GDK_SUCCEED)			\
						err = createException(SQL, "1 aggr." #f,	\
							SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
			b->tnil = false;							\
			b->tnonil = true;							\
		} else if (BATcount(b) == 0) {					\
			if (BUNappend(b, val, false) != GDK_SUCCEED)\
				err = createException(SQL, "3 aggr." #f,	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}												\
		bat_iterator_end(&bi); \
	}

static str
LOCKEDAGGRsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (
#ifdef HAVE_HGE
			type != TYPE_hge &&
#endif
			type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "aggr.sum",	"Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);

#ifdef HAVE_HGE
		aggr(hge,sum);
#endif
		aggr(lng,sum);
		aggr(int,sum);
		aggr(sht,sum);
		aggr(bte,sum);
		aggr(flt,sum);
		aggr(dbl,sum);
		if (!err) {
			BATnegateprops(b);
			BBPkeepref(b);
		} else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.sum",	"Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

#define paggr(T,OT,f)  \
	if (type == TYPE_##T && b->ttype == TYPE_##OT) {	\
		T val = *getArgReference_##T(stk, pci, 2);		\
		if (!is_##T##_nil(val) && BATcount(b)) {		\
			OT *t = Tloc(b, 0);							\
			if (is_##OT##_nil(t[0])) {					\
				t[0] = val;								\
			} else										\
				t[0] = f(t[0], val);					\
			b->tnil = false;							\
			b->tnonil = true;							\
		} else if (BATcount(b) == 0) {					\
			OT ov = val;								\
			if (BUNappend(b, &ov, true) != GDK_SUCCEED)\
				err = createException(SQL, "aggr." #f,	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}												\
	}

static str
LOCKEDAGGRprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (
#ifdef HAVE_HGE
			type != TYPE_hge &&
#endif
			type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "aggr.prod",	"Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		paggr(lng,lng,prod);
		paggr(int,lng,prod);
		paggr(sht,lng,prod);
		paggr(bte,lng,prod);

#ifdef HAVE_HGE
		paggr(hge,hge,prod);
		paggr(lng,hge,prod);
		paggr(int,hge,prod);
		paggr(sht,hge,prod);
		paggr(bte,hge,prod);
#endif

		paggr(flt,flt,prod);
		paggr(dbl,dbl,prod);
		if (!err) {
			BATnegateprops(b);
			BBPkeepref(b);
		} else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.prod",	"Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

#define avg_aggr(T)														\
	if (type == TYPE_##T) {												\
		T val = *getArgReference_##T(stk, pci, pci->retc + 1);			\
		lng cnt = *getArgReference_lng(stk, pci, pci->retc + 2);		\
		if (cnt > 0 && !is_##T##_nil(val) && BATcount(b)) {				\
			T *t = Tloc(b, 0);											\
			lng *tcnt = Tloc(c, 0);										\
			if (is_##T##_nil(t[0])) {									\
				t[0] = val;												\
				tcnt[0] = cnt;											\
			} else {													\
			    dbl tt = (tcnt[0] + cnt);								\
				t[0] = (t[0]*((dbl)tcnt[0]/tt)) + (val*((dbl)cnt/tt));	\
				tcnt[0] += cnt;											\
			}															\
			b->tnil = false;											\
			b->tnonil = true;											\
		} else if (cnt > 0 && BATcount(b) == 0) {						\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)				\
				err = createException(SQL, "aggr.avg",					\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);					\
		}																\
	}

/* return (a * b) % c without intermediate overflow */
static inline lng
mulmod(lng a, lng b, lng c)
{
	lng res = 0;
	a %= c;
	while (b) {
		if (b & 1)
			res = (res + a) % c;
		a = (2 * a) % c;
		b >>= 1;
	}
	return res;
}

#ifdef HAVE___INT128
#define avg_aggr_acc(T)													\
	do {																\
		T a1 = *getArgReference_##T(stk, pci, pci->retc + 1);			\
		lng r1 = *getArgReference_lng(stk, pci, pci->retc + 2);			\
		lng n1 = *getArgReference_lng(stk, pci, pci->retc + 3);			\
		T a2 = *(T*)Tloc(b, 0);											\
		lng r2 = *(lng*)Tloc(r, 0);										\
		lng n2 = *(lng*)Tloc(c, 0);										\
		if (is_##T##_nil(a2)) {											\
			a2 = a1;													\
			r2 = r1;													\
			n2 = n1;													\
		} else if (!is_##T##_nil(a1)) {									\
			/* calculate: */											\
			/* n = n1 + n2 */											\
			/* a = (a1*n1 + r1 + a2*n2 + r2) / (n1 + n2) */				\
			/* r = (a1*n1 + r1 + a2*n2 + r2) % (n1 + n2) */				\
			/* where / and % follow the rule that x==x/y*y + x%y */		\
			/* but x/y is rounded down, so x%y >= 0 */					\
			lng n = n1 + n2;											\
			T a = (T) ((a1 / n) * n1 + ((a1 % n) * (__int128) n1) / n + \
					   (a2 / n) * n2 + ((a2 % n) * (__int128) n2) / n + \
					   (r1 + r2) / n);									\
			lng r = mulmod(a1, n1, n) + mulmod(a2, n2, n) + (r1 + r2) % n; \
			while (r >= n) {											\
				r -= n;													\
				a++;													\
			}															\
			while (r < 0) {												\
				r += n;													\
				a--;													\
			}															\
			a2 = a;														\
			r2 = r;														\
			n2 = n;														\
		}																\
		*(T*)Tloc(b, 0) = a2;											\
		*(lng*)Tloc(r, 0) = r2;											\
		*(lng*)Tloc(c, 0) = n2;											\
	} while (0)
#else
#if defined(_MSC_VER) && _MSC_VER >= 1920
#include <intrin.h>
#pragma intrinsic(_mul128)
#pragma intrinsic(_div128)
#define avg_aggr_acc(T)													\
	do {																\
		T a1 = *getArgReference_##T(stk, pci, pci->retc + 1);			\
		lng r1 = *getArgReference_lng(stk, pci, pci->retc + 2);			\
		lng n1 = *getArgReference_lng(stk, pci, pci->retc + 3);			\
		T a2 = *(T*)Tloc(b, 0);											\
		lng r2 = *(lng*)Tloc(r, 0);										\
		lng n2 = *(lng*)Tloc(c, 0);										\
		if (is_##T##_nil(a2)) {											\
			a2 = a1;													\
			r2 = r1;													\
			n2 = n1;													\
		} else if (!is_##T##_nil(a1)) {									\
			/* calculate: */											\
			/* n = n1 + n2 */											\
			/* a = (a1*n1 + r1 + a2*n2 + r2) / (n1 + n2) */				\
			/* r = (a1*n1 + r1 + a2*n2 + r2) % (n1 + n2) */				\
			/* where / and % follow the rule that x==x/y*y + x%y */		\
			/* but x/y is rounded down, so x%y >= 0 */					\
			lng n = n1 + n2;											\
			T a = (T) ((a1 / n) * n1 +  (a2 / n) * n2 + (r1 + r2) / n);	\
			__int64 xlo, xhi;											\
			xlo = _mul128((__int64) (a1 % n), n1, &xhi);				\
			a += (T) _div128(xhi, xlo, (__int64) n, &rem);				\
			xlo = _mul128((__int64) (a2 % n), n2, &xhi);				\
			a += (T) _div128(xhi, xlo, (__int64) n, &rem);				\
			lng r = mulmod(a1, n1, n) + mulmod(a2, n2, n) + (r1 + r2) % n; \
			while (r >= n) {											\
				r -= n;													\
				a++;													\
			}															\
			while (r < 0) {												\
				r += n;													\
				a--;													\
			}															\
			a2 = a;														\
			r2 = r;														\
			n2 = n;														\
		}																\
		*(T*)Tloc(b, 0) = a2;											\
		*(lng*)Tloc(r, 0) = r2;											\
		*(lng*)Tloc(c, 0) = n2;											\
	} while (0)
#else
#define avg_aggr_acc(T)													\
	do {																\
		T a1 = *getArgReference_##T(stk, pci, pci->retc + 1);			\
		lng r1 = *getArgReference_lng(stk, pci, pci->retc + 2);			\
		lng n1 = *getArgReference_lng(stk, pci, pci->retc + 3);			\
		T a2 = *(T*)Tloc(b, 0);											\
		lng r2 = *(lng*)Tloc(r, 0);										\
		lng n2 = *(lng*)Tloc(c, 0);										\
		if (is_##T##_nil(a2)) {											\
			a2 = a1;													\
			r2 = r1;													\
			n2 = n1;													\
		} else if (!is_##T##_nil(a1)) {									\
			/* calculate: */											\
			/* n = n1 + n2 */											\
			/* a = (a1*n1 + r1 + a2*n2 + r2) / (n1 + n2) */				\
			/* r = (a1*n1 + r1 + a2*n2 + r2) % (n1 + n2) */				\
			/* where / and % follow the rule that x==x/y*y + x%y */		\
			/* but x/y is rounded down, so x%y >= 0 */					\
			lng n = n1 + n2;											\
			lng x1 = a1 % n;											\
			lng x2 = a2 % n;											\
			if ((n1 != 0 &&												\
				 (x1 > GDK_lng_max / n1 || x1 < -GDK_lng_max / n1)) ||	\
				(n2 != 0 &&												\
				 (x2 > GDK_lng_max / n2 || x2 < -GDK_lng_max / n2))) {	\
				BBPunfix(b->batCacheid);								\
				BBPunfix(c->batCacheid);								\
				BBPunfix(r->batCacheid);								\
				throw(SQL, "aggr.avg",									\
					  SQLSTATE(22003) "overflow in calculation");		\
			}															\
			T a = (T) ((a1 / n) * n1 + x1 / n +							\
					   (a2 / n) * n2 + x2 / n +							\
					   (r1 + r2) / n);									\
			lng r = mulmod(a1, n1, n) + mulmod(a2, n2, n) + (r1 + r2) % n; \
			while (r >= n) {											\
				r -= n;													\
				a++;													\
			}															\
			while (r < 0) {												\
				r += n;													\
				a--;													\
			}															\
			a2 = a;														\
			r2 = r;														\
			n2 = n;														\
		}																\
		*(T*)Tloc(b, 0) = a2;											\
		*(lng*)Tloc(r, 0) = r2;											\
		*(lng*)Tloc(c, 0) = n2;											\
	} while (0)
#endif
#endif

static str
LOCKEDAGGRavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat *rcnt = getArgReference_bat(stk, pci, pci->retc - 1);
	bat *rrem = pci->retc == 3 ? getArgReference_bat(stk, pci, 1) : NULL;
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, pci->retc);
	int type = getArgType(mb, pci, pci->retc + 1);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "aggr.avg",	"Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);
		BAT *c = BATdescriptor(*rcnt);

		if (pci->retc == 3) {
			BAT *r = BATdescriptor(*rrem);
			switch (b->ttype) {
			case TYPE_bte:
				avg_aggr_acc(bte);
				break;
			case TYPE_sht:
				avg_aggr_acc(sht);
				break;
			case TYPE_int:
				avg_aggr_acc(int);
				break;
			case TYPE_lng:
				avg_aggr_acc(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				avg_aggr_acc(hge);
				break;
#endif
			}
			BATnegateprops(b);
			BATnegateprops(r);
			BATnegateprops(c);
			BBPkeepref(b);
			BBPkeepref(r);
			BBPkeepref(c);
		} else {
			assert(b->ttype == TYPE_dbl);
			avg_aggr(hge);
			avg_aggr(lng);
			avg_aggr(int);
			avg_aggr(sht);
			avg_aggr(bte);
			avg_aggr(flt);
			avg_aggr(dbl);
			if (!err) {
				BATnegateprops(b);
				BBPkeepref(b);
				BATnegateprops(c);
				BBPkeepref(c);
			} else {
				BBPunfix(b->batCacheid);
				BBPunfix(c->batCacheid);
			}
		}
	} else {
			err = createException(SQL, "aggr.avg",	"Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	return MAL_SUCCEED;
}

#define vmin(a,b) ((cmp(a,b) < 0)?a:b)
#define vmax(a,b) ((cmp(a,b) > 0)?a:b)

static str
LOCKEDAGGRmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte && type != TYPE_bit &&
		type != TYPE_flt && type != TYPE_dbl &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp && type != TYPE_uuid && type != TYPE_str)
			return createException(SQL, "aggr.min",	"Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(date,min);
		aggr(daytime,min);
		aggr(timestamp,min);
		faggr(uuid,uuid_min);
		aggr(hge,min);
		aggr(lng,min);
		aggr(int,min);
		aggr(sht,min);
		aggr(bte,min);
		aggr(bit,min);
		aggr(flt,min);
		aggr(dbl,min);
		vaggr(str,vmin);
		if (!err) {
			BATnegateprops(b);
			//BBPkeepref(*res = b->batCacheid);
			//leave writable
			BBPretain(*res = b->batCacheid);
			BBPunfix(b->batCacheid);
		} else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.min",	"Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LOCKEDAGGRmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte && type != TYPE_bit &&
		type != TYPE_flt && type != TYPE_dbl &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp && type != TYPE_uuid && type != TYPE_str)
			return createException(SQL, "aggr.max",	"Wrong input type (%d)", type);

	pipeline_lock(p);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(date,max);
		aggr(daytime,max);
		aggr(timestamp,max);
		faggr(uuid,uuid_max);
		aggr(hge,max);
		aggr(lng,max);
		aggr(int,max);
		aggr(sht,max);
		aggr(bte,max);
		aggr(bit,max);
		aggr(flt,max);
		aggr(dbl,max);
		vaggr(str,vmax);
		if (!err) {
			BATnegateprops(b);
			//BBPkeepref(*res = b->batCacheid);
			//leave writable
			BBPretain(*res = b->batCacheid);
			BBPunfix(b->batCacheid);
		} else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.max",	"Result is not initialized");
	}
	pipeline_unlock(p);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LALGprojection(bat *result, const ptr *h, const bat *lid, const bat *rid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	pipeline_lock(p);
	res = ALGprojection(result, lid, rid);
	pipeline_unlock(p);
	return res;
}

#define GIDBITS 63
typedef lng gid;

typedef ATOMIC_TYPE hash_key_t;

#define HT_MIN_SIZE 1024*64
#define HT_MAX_SIZE 1024*1024*1024

typedef int (*fcmp)(void *v1, void *v2);
typedef lng (*fhsh)(void *v);

static int
str_cmp(str s1, str s2)
{
	return strcmp(s1,s2);
}

static lng
str_hsh(str v)
{
	lng key = 1;

	if (v) {
		for(;*v; v++) {
			key += ((lng)(*v));
			key += (key<<10);
			key ^= (key>>6);
		}
		key += (key << 3);
		key ^= (key >> 11);
		key += (key << 15);
	}
	return key;
}

typedef struct hash_table {
		Sink s;
        int type;
        int width;
		fcmp cmp;
		fhsh hsh;

        void *vals;			/* hash(ed) values */
        hash_key_t *gids;   /* chain of gids (k, ie mark used/-k mark used and value filled) */
		gid *pgids;			/* id of the parent hash */

		struct hash_table *p;	/* parent hash */
        size_t last;
        size_t size;
        gid mask;
		sql_allocator **allocators;
		int nr_allocators;
} hash_table;

static unsigned int
log_base2(unsigned int n)
{
        unsigned int l ;

        for (l = 0; n; l++) {
                n >>= 1 ;
        }
        return l ;
}

#define HASH_SINK 1

static hash_table *
_ht_init( hash_table *h )
{
        if (h->gids == NULL) {
                h->vals = (char*)GDKmalloc(h->size * (size_t)h->width);
                h->gids = (hash_key_t*)GDKzalloc(sizeof(hash_key_t)* h->size);
				if (h->p) {
					assert(h->s.type == HASH_SINK);
					h->pgids = (gid*)GDKmalloc(sizeof(gid)* h->size);
				}
        }
        return h;
}

static void
ht_destroy(hash_table *ht)
{
	if (ht->vals)
		GDKfree(ht->vals);
	if (ht->gids)
		GDKfree((void*)ht->gids);
	if (ht->pgids)
		GDKfree(ht->pgids);
	for(int i = 0; i<ht->nr_allocators; i++) {
		if(ht->allocators[i])
			sa_destroy(ht->allocators[i]);
	}
	GDKfree(ht);
}

static hash_table *
_ht_create( int type, int size, hash_table *p)
{
        hash_table *h = (hash_table*)GDKzalloc(sizeof(hash_table));
        int bits = log_base2(size-1);

		if (!type)
			type = TYPE_oid;
		h->s.destroy = (sink_destroy)&ht_destroy;
		h->s.type = HASH_SINK;
        if (bits >= GIDBITS)
                bits = GIDBITS-1;
        h->size = (gid)1<<bits;
        h->mask = h->size-1;
        h->type = type;
        h->width = ATOMsize(type);
		h->last = 0;
		h->p = p;
		if (type == TYPE_str) {
			h->cmp = (fcmp)str_cmp;
			h->hsh = (fhsh)str_hsh;
		} else {
			h->cmp = (fcmp)ATOMcompare(type);
		}
        return _ht_init(h);
}

static hash_table *
ht_create(int type, int size, hash_table *p)
{
        if (size < HT_MIN_SIZE)
                size = HT_MIN_SIZE;
        if (size > HT_MAX_SIZE)
                size = HT_MAX_SIZE;
        return _ht_create(type, size, p);
}


#define _hash_bit(X)  ((unsigned int)X)
#define _hash_bte(X)  ((unsigned int)X)
#define _hash_sht(X)  ((unsigned int)X)
#define _hash_int(X)  ((((unsigned int)X)>>7)^(((unsigned int)X)>>13)^(((unsigned int)X)>>21)^((unsigned int)X))
#define _hash_date(X) _hash_int(X)
#define _hash_lng(X)  ((((ulng)X)>>7)^(((ulng)X)>>13)^(((ulng)X)>>21)^(((ulng)X)>>31)^(((ulng)X)>>38)^(((ulng)X)>>46)^(((ulng)X)>>56)^((ulng)X))
#define _hash_oid(X)  _hash_lng(X)
#define _hash_daytime(X) _hash_lng(X)
#define _hash_timestamp(X) _hash_lng(X)
#define _hash_uuid(X) _hash_hge(X)

#define _mix_hge(X)      (((hge) (X) >> 7) ^     \
                         ((hge) (X) >> 13) ^    \
                         ((hge) (X) >> 21) ^    \
                         ((hge) (X) >> 31) ^    \
                         ((hge) (X) >> 38) ^    \
                         ((hge) (X) >> 46) ^    \
                         ((hge) (X) >> 56) ^    \
                         ((hge) (X) >> 65) ^    \
                         ((hge) (X) >> 70) ^    \
                         ((hge) (X) >> 78) ^    \
                         ((hge) (X) >> 85) ^    \
                         ((hge) (X) >> 90) ^    \
                         ((hge) (X) >> 98) ^    \
                         ((hge) (X) >> 107) ^   \
                         ((hge) (X) >> 116) ^   \
                         (hge) (X))
#define _hash_hge(X)  (_hash_lng(((lng)X) ^ _hash_lng((lng)(X>>64))))
//#define hash_hge(X)  ((lng)_mix_hge(X))
#define _hash_flt(X)  (_hash_int(X))
#define _hash_dbl(X)  (_hash_lng(X))
#define _hash_gid(X)  (_hash_lng(X))
#define ROT64(x, y)  ((x << y) | (x >> (64 - y)))
#define combine(X,Y)  (X^Y)

//(_hash_lng(ROT64(X, 3) ^ ROT64((lng)Y, 17)))

static str
UHASHnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *res = getArgReference_bat(s, p, 0);
	int tt = getArgType(m, p, 1);
	int size = *getArgReference_int(s, p, 2);
	hash_table *parent = NULL;
	if (p->argc == 4) {
		bat pid = *getArgReference_bat(s, p, 3);
		BAT *p = BATdescriptor(pid);
		parent = (hash_table*)p->T.sink;
		BBPunfix(p->batCacheid);
	}

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	b->T.sink = (Sink*)ht_create(tt, size*1.2*2.1, parent);
	*res = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

#define unique(Type) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(bp[i])&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && vals[k] != bp[i];) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define funique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && vals[k] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define cunique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && h->cmp(vals+k, bp+i) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define aunique(Type) \
	if (tt == TYPE_##Type) { \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				Type bpi = BUNtvar(bi, i); \
				gid k = (gid)h->hsh(bpi)&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (h->cmp(vals[k], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bpi; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
		bat_iterator_end(&bi); \
	}

static str
LALGunique(bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid)
{
	Pipeline *p = (Pipeline*)*H;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *b = BATdescriptor(*bid);
	int err = 0;
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		if (ATOMvarsized(u->ttype) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid)
			BATswap_heaps(u, b, p);
	}
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *g = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);
		BUN r = 0;

		if (cnt && !err) {
			unique(bit)
			unique(bte)
			unique(sht)
			unique(int)
			unique(date)
			unique(lng)
			unique(daytime)
			unique(timestamp)
			unique(hge)
			funique(flt, int)
			funique(dbl, lng)
			cunique(uuid, hge)
			aunique(str)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BATsetcount(g, r);
			BATnegateprops(g);
			/* props */
			*uid = u->batCacheid;
			*rid = g->batCacheid;
			BBPkeepref(u);
			BBPkeepref(g);
		}
	}
	if (err)
		throw(MAL, "group.unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define gunique(Type) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(bp[i]))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || vals[k] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define gfunique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && vals[k] != bp[i]));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define gcunique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && h->cmp(vals+k, bp+i) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define gaunique(Type) \
	if (tt == TYPE_##Type) { \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				Type bpi = BUNtvar(bi, i); \
				gid k = (gid)combine(p[i], h->hsh(bpi))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || h->cmp(vals[k], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bpi; \
					pgids[k] = p[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
					continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
		bat_iterator_end(&bi); \
	}

static str
LALGgroup_unique(bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid, bat *Gid)
{
	Pipeline *p = (Pipeline*)*H;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *G = BATdescriptor(*Gid);
	BAT *b = BATdescriptor(*bid);
	int err = 0;
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		if (ATOMvarsized(u->ttype) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid)
			BATswap_heaps(u, b, p);
	}
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *ng = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int tt = b->ttype;
		oid *gp = Tloc(ng, 0);
		gid *p = Tloc(G, 0);
		gid *pgids = h->pgids;
		BUN r = 0;

		if (cnt && !err) {
			gunique(bit)
			gunique(bte)
			gunique(sht)
			gunique(int)
			gunique(date)
			gunique(lng)
			gunique(daytime)
			gunique(timestamp)
			gunique(hge)
			gfunique(flt, int)
			gfunique(dbl, lng)
			gcunique(uuid, hge)
			gaunique(str)
		}
		if (!err) {
			BBPunfix(G->batCacheid);
			BBPunfix(b->batCacheid);
			BATsetcount(ng, r);
			BATnegateprops(ng);
			/* props */
			*uid = u->batCacheid;
			*rid = ng->batCacheid;
			BBPkeepref(u);
			BBPkeepref(ng);
		}
	}
	if (err)
		throw(MAL, "group.unique", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define PRE_CLAIM 256
#define group(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)_hash_##Type(bp[i])&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && vals[g] != bp[i];) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define vgroup() \
	if (tt == TYPE_void) { \
		if (!BATtdense(b)) { \
			assert(cnt); \
			int slots = 0; \
			gid slot = 0; \
			oid bpi = b->tseqbase; \
			oid *vals = h->vals; \
			\
			bool fnd = 0; \
			gid k = (gid)_hash_oid(oid_nil)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && vals[g] != bpi;) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			for(BUN i = 0; i<cnt; i++, bpi++) { \
				gp[i] = g-1; \
			} \
		} else { \
			assert(BATtdense(b)); \
			int slots = 0; \
			gid slot = 0; \
			oid bpi = b->tseqbase; \
			oid *vals = h->vals; \
			\
			for(BUN i = 0; i<cnt; i++, bpi++) { \
				bool fnd = 0; \
				gid k = (gid)_hash_oid(bpi)&h->mask; \
				gid g = 0; \
				\
				for(; !fnd; ) { \
					g = ATOMIC_GET(h->gids+k); \
					for(;g && vals[g] != bpi;) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = private?1:PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = bpi; \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
							continue; \
					} \
					fnd = 1; \
				} \
				gp[i] = g-1; \
			} \
		} \
	}

#define fgroup(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define agroup(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)str_hsh(bpi)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (vals[g] && h->cmp(vals[g], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}

#define agroup_(Type,P) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		sql_allocator *sa = h->allocators[P->wid]; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)str_hsh(bpi)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (vals[g] && h->cmp(vals[g], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = sa_strdup(sa, bpi); \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}

static str
LALGgroup(bat *rid, bat *uid, const ptr *H, bat *bid/*, bat *sid*/)
{
	Pipeline *p = (Pipeline*)*H;
	/* private or not */
	bool private = (!*uid || is_bat_nil(*uid)), local_storage = false;
	int err = 0;
	BAT *u, *b = NULL;

   	b = BATdescriptor(*bid);
	if (!b)
		return createException(SQL, "group.group",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (private && *uid && is_bat_nil(*uid)) { /* TODO ... create but how big ??? */
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, NULL);
		u->T.private_bat = 1;
	} else {
		u = BATdescriptor(*uid);
	}
	if (!u) {
		BBPunfix(*bid);
		return createException(SQL, "group.group",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	private = u->T.private_bat;

	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	if (ATOMvarsized(u->ttype) && !VIEWvtparent(b)) {
		local_storage = true;
		if (!h->allocators) {
			pipeline_lock(p);
			if (!h->allocators) {
				h->allocators = (sql_allocator**)GDKzalloc(p->p->nr_workers*sizeof(sql_allocator*));
				if (!h->allocators)
					err = 1;
			}
			pipeline_unlock(p);
		}
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = sa_create();
			if (!h->allocators[p->wid])
				err = 1;
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		if (ATOMvarsized(u->ttype) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid)
			BATswap_heaps(u, b, p);
	}
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);

		if (cnt && !err) {
			vgroup()
			group(bit)
			group(bte)
			group(sht)
			group(int)
			group(date)
			group(lng)
			group(oid)
			group(daytime)
			group(timestamp)
			group(hge)
			fgroup(flt, int)
			fgroup(dbl, lng)
			if (local_storage) {
				agroup_(str, p)
			} else {
				agroup(str)
			}
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BATsetcount(g, cnt);
			BATnegateprops(g);
			/* props */
			gid last = ATOMIC_GET(&h->last);
			/* pass max id */
			g->T.maxval = last;
			g->tkey = FALSE;
			*uid = u->batCacheid;
			*rid = g->batCacheid;
			BBPkeepref(u);
			BBPkeepref(g);
		}
	}
	if (err)
		throw(MAL, "group.group", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define derive(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)combine(gi[i], _hash_##Type(bp[i]))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || vals[g] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define vderive() \
	if (tt == TYPE_void) { \
		int slots = 0; \
		gid slot = 0; \
		oid bpi = b->tseqbase; \
		oid *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)combine(gi[i], _hash_oid(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || vals[g] != bpi);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define fderive(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)combine(gi[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define aderive(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)combine(gi[i], str_hsh(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}

#define aderive_(Type, P) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		sql_allocator *sa = h->allocators[P->wid]; \
		\
		for(BUN i = 0; i<cnt && !msg; i++) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)combine(gi[i], str_hsh(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = sa_strdup(sa, bpi); \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}


static str
LALGderive(bat *rid, bat *uid, const ptr *H, bat *Gid, bat *Ph, bat *bid /*, bat *sid*/)
{
	str msg = MAL_SUCCEED;
	Pipeline *p = (Pipeline*)*H;
	bool private = (!*uid || is_bat_nil(*uid)), local_storage = false;
	int err = 0;
	BAT *u, *b = BATdescriptor(*bid);
	BAT *G = BATdescriptor(*Gid);

	if (!b || !G) {
		if (b)
			BBPunfix(*bid);
		return createException(SQL, "group.group",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (private && *uid && is_bat_nil(*uid)) { /* TODO ... create but how big ??? */
		BAT *H = BATdescriptor(*Ph);
		if (!H) {
			BBPunfix(*bid);
			BBPunfix(*Gid);
			return createException(SQL, "group.group",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		/* Lookup parent hash */
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, (hash_table*)H->T.sink);
		u->T.private_bat = 1;
		BBPunfix(*Ph);
	} else {
		u = BATdescriptor(*uid);
	}
	if (!u) {
		BBPunfix(*Gid);
		BBPunfix(*bid);
		return createException(SQL, "group.group",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	private = u->T.private_bat;
	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	if (ATOMvarsized(u->ttype) && !VIEWvtparent(b)) {
		local_storage = true;
		if (!h->allocators) {
			pipeline_lock(p);
			if (!h->allocators) {
				h->allocators = (sql_allocator**)GDKzalloc(p->p->nr_workers*sizeof(sql_allocator*));
				if (!h->allocators)
					err = 1;
			}
			pipeline_unlock(p);
		}
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = sa_create();
			if (!h->allocators[p->wid])
				err = 1;
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		if (ATOMvarsized(u->ttype) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid)
			BATswap_heaps(u, b, p);
	}
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);
		gid *gi = Tloc(G, 0);
		gid *pgids = h->pgids;

		if (cnt && !err) {
			vderive()
			derive(bit)
			derive(bte)
			derive(sht)
			derive(int)
			derive(date)
			derive(lng)
			derive(oid)
			derive(daytime)
			derive(timestamp)
			derive(hge)
			fderive(flt, int)
			fderive(dbl, lng)
			if (local_storage) {
				aderive_(str,p)
			} else {
				aderive(str)
			}
		}
		if (!err && !msg) {
			BBPunfix(b->batCacheid);
			BBPunfix(G->batCacheid);
			BATsetcount(g, cnt);
			BATnegateprops(g);
			/* props */
			gid last = ATOMIC_GET(&h->last);
			/* pass max id */
			g->T.maxval = last;
			g->tkey = FALSE;
			*uid = u->batCacheid;
			*rid = g->batCacheid;
			BBPkeepref(u);
			BBPkeepref(g);
		}
	}
	if (err)
		throw(MAL, "group.derive", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return msg;
}

#define project(Type) \
	if (ATOMstorage(tt) == TYPE_##Type) { \
		Type *v = Tloc(b, 0); \
		Type *o = Tloc(r, 0); \
		for(BUN i = 0; i<cnt; i++) { \
			o[gp[i]] = v[i]; \
		} \
	}

#define vproject() \
	if (tt == TYPE_void) { \
		oid vi = b->tseqbase; \
		oid *o = Tloc(r, 0); \
		for(BUN i = 0; i<cnt; i++, vi++) { \
			o[gp[i]] = vi; \
		} \
	}

#define aproject(Type,w,Toff) \
	if (ATOMstorage(tt) == TYPE_##Type && b->twidth == w) { \
		Toff *v = Tloc(b, 0); \
		Toff *o = Tloc(r, 0); \
		for(BUN i = 0; i<cnt; i++) { \
			o[gp[i]] = v[i]; \
		} \
	}

/* runs locked ie resizes should work */
#define aproject_(Type) \
	if (ATOMstorage(tt) == TYPE_##Type) { \
		BATiter bi = bat_iterator(b); \
		int ins = 0; \
		for(BUN i = 0; i<cnt && !err; i++) { \
			int w = r->twidth; \
			if(w == 1) { \
				uint8_t *o = Tloc(r, 0); \
				ins = (o[gp[i]] == 0); \
			} else if (w == 2) { \
				uint16_t *o = Tloc(r, 0); \
				ins = (o[gp[i]] == 0); \
			} else if (w == 4) { \
				uint32_t *o = Tloc(r, 0); \
				ins = (o[gp[i]] == 0); \
			} else { \
				var_t *o = Tloc(r, 0); \
				ins = (o[gp[i]] == 0); \
			} \
			if (ins && tfastins_nocheckVAR( r, gp[i], BUNtvar(bi, i)) != GDK_SUCCEED) \
				err = 1; \
			if (w < r->twidth) { \
				BUN sz = BATcapacity(r); \
				memset(Tloc(r, max), 0, r->twidth*(sz-max)); \
			} \
		} \
		bat_iterator_end(&bi); \
	}

/* result := algebra.projections(groupid, input)  */
/* this (possibly) overwrites the values, therefor for expensive (var) types we only write offsets (ie use the heap from
 * the parent) */
static str
LALGproject(bat *rid, bat *gid, bat *bid, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g, *b, *r = NULL;
	int err = 0;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	int tt = b->ttype;
	oid max = BATcount(g)?g->T.maxval:0;
	/* probably need bat resize and create hash */
	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat), local_storage = false;

	if (!tt)
		tt = TYPE_oid;
	if (!private)
		pipeline_lock1(r);
	if (r && BATcount(b)) {
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b))
			err = 1;
		else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_cache(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			assert(r->tvheap->parentid == r->batCacheid);
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			BATswap_heaps(r, b, p);
		}
	} else if (!r) {
		if (ATOMvarsized(tt) && VIEWvtparent(b) && BBP_cache(VIEWvtparent(b))->batRestricted == BAT_READ) {
			r = COLnew2(0, tt, max, TRANSIENT, b->twidth);
			BATswap_heaps(r, b, p);
		} else {
			local_storage = true;
			r = COLnew(0, tt, max, TRANSIENT);

			if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b))
					err = 1;
		}
		assert(private);
		r->T.private_bat = 1;
	}

	BUN cnt = 0;
	if (!err) {
		cnt = BATcount(r);
		if (BATcapacity(r) < max) {
			BUN sz = max*2;
			if (BATextend(r, sz) != GDK_SUCCEED)
				err = 1;
		}
	}

	/* get max id from gid */
	if (!err) {
		if (cnt < max)
			memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));

		cnt = BATcount(b);

		int err = 0;
		oid *gp = Tloc(g, 0);

		tt = b->ttype;
		if (!err && cnt) {
			vproject()
			project(bte)
			project(sht)
			project(int)
			project(lng)
			project(hge)
			project(flt)
			project(dbl)
			if (local_storage) {
				if (!private)
					pipeline_lock2(r);
				if (BATcount(r) < max)
					BATsetcount(r, max);
				if (!private)
					pipeline_unlock2(r);
				aproject_(str)
			} else {
				aproject(str,1,uint8_t)
				aproject(str,2,uint16_t)
				aproject(str,4,uint32_t)
				aproject(str,8,var_t)
			}
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < max)
				BATsetcount(r, max);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
			*rid = r->batCacheid;
			BBPkeepref(r);
		}
	}
	if (!private)
		pipeline_unlock1(r);
	if (err)
		throw(MAL, "aggr.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
LALGcountstar(bat *rid, bat *gid, const ptr *H, bat *pid)
{
	//Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	(void)H;
	BAT *g = BATdescriptor(*gid);
	BAT *r = NULL;
	int err = 0;

	if (*rid)
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew(0, TYPE_lng, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		//printf("count extend %ld\n", sz);
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max)
		memset(Tloc(r, cnt), 0, sizeof(lng)*(max-cnt));

	if (!err) {
		BUN cnt = BATcount(g);

		int err = 0;

		if (!err) {
			oid *v = Tloc(g, 0);
			lng *o = Tloc(r, 0);
			for(BUN i = 0; i<cnt; i++)
				o[v[i]]++;
		}
		if (!err) {
			BBPunfix(g->batCacheid);
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < max)
				BATsetcount(r, max);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
			*rid = r->batCacheid;
			BBPkeepref(r);
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	if (err)
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define gcount(Type) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
			for(BUN i = 0; i<cnt; i++) \
				o[v[i]]+= (!is_##Type##_nil(in[i])); \
	}

#define gfcount(Type) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			for(BUN i = 0; i<cnt; i++) \
				o[v[i]]+= cmp(in+i, &Type##_nil) != 0; \
	}

#define gacount(Type) \
	if (tt == TYPE_##Type) { \
			BATiter bi = bat_iterator(b); \
			const void *nil = ATOMnilptr(tt); \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			for(BUN i = 0; i<cnt; i++) { \
				Type bpi = BUNtvar(bi, i); \
				o[v[i]]+= cmp(bpi, nil)!=0; \
			} \
	}

static str
LALGcount(bat *rid, bat *gid, bat *bid, bit *nonil, const ptr *H, bat *pid)
{
	//Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	if (!(*nonil))
		return LALGcountstar(rid, gid, H, pid);

	/* use bid to check for null values */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0;

	if (!g || !b) {
		if (g)
			BBPunfix(*gid);
		if (b)
			BBPunfix(*bid);
		return createException(SQL, "aggr.count",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (*rid && !is_bat_nil(*rid)) {
		r = BATdescriptor(*rid);
		if (!r) {
			BBPunfix(*gid);
			BBPunfix(*bid);
			return createException(SQL, "aggr.count",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew(0, TYPE_lng, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		//printf("count extend %ld\n", sz);
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max)
		memset(Tloc(r, cnt), 0, sizeof(lng)*(max-cnt));

	if (!err) {
		BUN cnt = BATcount(g);

		int err = 0;

		if (!err) {
			oid *v = Tloc(g, 0);
			lng *o = Tloc(r, 0);
			if (b->tnonil) {
				for(BUN i = 0; i<cnt; i++)
					o[v[i]]++;
			} else { /* per type */
				int tt = b->ttype;

				gcount(bit);
				gcount(bte);
				gcount(sht);
				gcount(int);
				gcount(date);
				gcount(lng);
				gcount(daytime);
				gcount(timestamp);
				gcount(hge);
				gfcount(uuid);
				gcount(flt);
				gcount(dbl);
				gacount(str);
			}
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < max)
				BATsetcount(r, max);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
			*rid = r->batCacheid;
			BBPkeepref(r);
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	if (err)
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

/* TODO do we need to split out the nil check, ie for when we know there are no nils */
#define gsum(OutType, InType) \
	if (tt == TYPE_##InType && ot == TYPE_##OutType) { \
			InType *in = Tloc(b, 0); \
			OutType *o = Tloc(r, 0); \
			for(BUN i = 0; i<cnt; i++) \
				if (!is_##InType##_nil(in[i])) { \
					if (is_##OutType##_nil(o[grp[i]])) \
						o[grp[i]] = in[i]; \
					else \
						o[grp[i]] += in[i]; \
				} \
	}

static str
//LALGsum(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
LALGsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *gid = getArgReference_bat(stk, pci, 1);
	bat *bid = getArgReference_bat(stk, pci, 2);
	//Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 3); /* last arg should move to first argument .. */
	bat *pid = getArgReference_bat(stk, pci, 4);
	BAT *b, *g, *r = NULL;
	int err = 0;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		int tt = getBatType(getArgType(mb, pci, 0));
		r = COLnew(b->hseqbase, tt, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		//printf("sum extend %ld\n", sz);
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i*r->twidth), nil, r->twidth);
	}

	if (!err) {
		BUN cnt = BATcount(g);
		int tt = b->ttype, ot = r->ttype;

		if (!err) {
			oid *grp = Tloc(g, 0);

			gsum(bte,bte);
			gsum(sht,bte);
			gsum(sht,sht);
			gsum(int,bte);
			gsum(int,sht);
			gsum(int,int);
			gsum(lng,bte);
			gsum(lng,sht);
			gsum(lng,int);
			gsum(lng,lng);
			gsum(hge,bte);
			gsum(hge,sht);
			gsum(hge,int);
			gsum(hge,lng);
			gsum(hge,hge);
			gsum(flt,flt);
			gsum(dbl,flt);
			gsum(dbl,dbl);
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < max)
				BATsetcount(r, max);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
			*rid = r->batCacheid;
			BBPkeepref(r);
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	if (err)
		throw(MAL, "aggr.sum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define gprod(OutType, InType) \
	if (tt == TYPE_##InType && ot == TYPE_##OutType) { \
			InType *in = Tloc(b, 0); \
			OutType *o = Tloc(r, 0); \
			for(BUN i = 0; i<cnt; i++) \
				if (!is_##InType##_nil(in[i])) { \
					if (is_##OutType##_nil(o[grp[i]])) \
						o[grp[i]] = in[i]; \
					else \
						o[grp[i]] *= in[i]; \
				} \
	}

static str
//LALGprod(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
LALGprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *gid = getArgReference_bat(stk, pci, 1);
	bat *bid = getArgReference_bat(stk, pci, 2);
	//Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 3); /* last arg should move to first argument .. */
	bat *pid = getArgReference_bat(stk, pci, 4);
	BAT *b, *g, *r = NULL;
	int err = 0;

	g = BATdescriptor(*gid);
	b = BATdescriptor(*bid);
	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		pipeline_lock1(r);
		//pipeline_lock(p);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (!r) {
		int tt = getBatType(getArgType(mb, pci, 0));
		r = COLnew(b->hseqbase, tt, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max) {
		char *d = Tloc(r, 0);
		const char *nil = ATOMnilptr(r->ttype);
		for (BUN i=cnt; i<max; i++)
			memcpy(d+(i*r->twidth), nil, r->twidth);
	}

	if (!err) {
		BUN cnt = BATcount(g);
		int tt = b->ttype, ot = r->ttype;

		if (!err) {
			oid *grp = Tloc(g, 0);

			gprod(lng,bte);
			gprod(lng,sht);
			gprod(lng,int);
			gprod(lng,lng);
#ifdef HAVE_HGE
			gprod(hge,bte);
			gprod(hge,sht);
			gprod(hge,int);
			gprod(hge,lng);
			gprod(hge,hge);
#endif
			gprod(flt,flt);
			gprod(dbl,dbl);
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < max)
				BATsetcount(r, max);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
			*rid = r->batCacheid;
			BBPkeepref(r);
		}
	}
	if (!private)
		pipeline_unlock1(r);
		//pipeline_unlock(p);
	if (err)
		throw(MAL, "aggr.prod", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
//LALGavg(bat *rid, [bat *rremainer,] bat *rcnt, bat *gid, bat *bid, const ptr *H, bat *pid)
//LALGavg(bat *rid, [bat *rremainer,] bat *rcnt, bat *gid, bat *bid, [bat *remainder,] bat *cnt, const ptr *H, bat *pid)
LALGavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;
	return MAL_SUCCEED;
}

/* TODO handle nil based on argument 'skipnil' */
#define gfunc(Type, f) \
	if (tt == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
			Type *o = Tloc(r, 0); \
			for(BUN i = 0; i<cnt; i++) \
				if (is_##Type##_nil(o[grp[i]])) \
					o[grp[i]] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[grp[i]] = f(o[grp[i]], in[i]); \
	}

#define gfunc2(Type, f) \
	if (tt == TYPE_##Type) { \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			Type *in = Tloc(b, 0); \
			Type *o = Tloc(r, 0); \
			for(BUN i = 0; i<cnt; i++) \
				if (is_##Type##_nil(o[grp[i]])) \
					o[grp[i]] = in[i]; \
				else if (!is_##Type##_nil(in[i])) \
					o[grp[i]] = f(o[grp[i]], in[i]); \
	}

/* fron now assume shared heap */
#define vamin(cmp, o, opos, op, in, i, bp, nil) \
	if (!o[opos] || (cmp(bp+in[i], nil) != 0 && cmp(op+o[opos], nil) != 0 && cmp(op+o[opos], bp+in[i]) > 0)) \
		o[opos] = in[i];
#define vamax(cmp, o, opos, op, in, i, bp, nil) \
	if (!o[opos] || (cmp(bp+in[i], nil) != 0 && cmp(op+o[opos], nil) != 0 && cmp(op+o[opos], bp+in[i]) < 0)) \
		o[opos] = in[i];

#define gafunc(f) \
	if (ATOMextern(tt) && ATOMvarsized(tt)) { \
			BATiter bi = bat_iterator(b); \
			BATiter ri = bat_iterator(r); \
			char *bp = bi.vh->base; \
			char *op = ri.vh->base; \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			const char *nil = ATOMnilptr(r->ttype); \
			if (b->twidth == 1) { \
				bp += GDK_VAROFFSET; \
				op += GDK_VAROFFSET; \
				uint8_t *in = Tloc(b, 0); \
				uint8_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 2) { \
				bp += GDK_VAROFFSET; \
				op += GDK_VAROFFSET; \
				uint16_t *in = Tloc(b, 0); \
				uint16_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 4) { \
				uint32_t *in = Tloc(b, 0); \
				uint32_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} else if (b->twidth == 8) { \
				var_t *in = Tloc(b, 0); \
				var_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp, nil); \
			} \
			bat_iterator_end(&bi); \
			bat_iterator_end(&ri); \
	}

/* private (changing) heap */
#define vamin_(cmp, opos, in, i, bp, nil) \
	if (!getoffset(r->theap->base, opos, r->twidth) || (cmp(bp+in[i], nil) != 0 && cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), nil) != 0 && cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), bp+in[i]) > 0)) \
		if (tfastins_nocheckVAR( r, opos, bp+in[i]) != GDK_SUCCEED) \
			err = 1; \

#define vamax_(cmp, opos, in, i, bp, nil) \
	if (!getoffset(r->theap->base, opos, r->twidth) || (cmp(bp+in[i], nil) != 0 && cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), nil) != 0 && cmp(r->tvheap->base+VarHeapVal(r->theap->base, opos, r->twidth), bp+in[i]) < 0)) \
		if (tfastins_nocheckVAR( r, opos, bp+in[i]) != GDK_SUCCEED) \
			err = 1; \

static inline size_t
getoffset(const void *b, BUN p, int w)
{
        switch (w) {
        case 1:
                return (size_t) ((const uint8_t *) b)[p];
        case 2:
                return (size_t) ((const uint16_t *) b)[p];
#if SIZEOF_VAR_T == 8
        case 4:
                return (size_t) ((const uint32_t *) b)[p];
#endif
        default:
                return (size_t) ((const var_t *) b)[p];
        }
}

#define gafunc_(f) \
	if (ATOMextern(tt) && ATOMvarsized(tt)) { \
			BATiter bi = bat_iterator(b); \
			BATiter ri = bat_iterator(r); \
			char *bp = bi.vh->base; \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			const char *nil = ATOMnilptr(r->ttype); \
			if (b->twidth == 1) { \
				bp += GDK_VAROFFSET; \
				uint8_t *in = Tloc(b, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 2) { \
				bp += GDK_VAROFFSET; \
				uint16_t *in = Tloc(b, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 4) { \
				uint32_t *in = Tloc(b, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} else if (b->twidth == 8) { \
				var_t *in = Tloc(b, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f##_(cmp, grp[i], in, i, bp, nil); \
			} \
			bat_iterator_end(&bi); \
			bat_iterator_end(&ri); \
	}

static str
LALGmin(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0, tt = b->ttype;

	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat), local_storage = false;

	if (!private)
		pipeline_lock1(r);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (r && BATcount(b)) {
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b))
			err = 1;
		else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_cache(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			assert(r->tvheap->parentid == r->batCacheid);
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			BATswap_heaps(r, b, p);
		}
	} else if (!r) {
		if (ATOMvarsized(b->ttype) && VIEWvtparent(b) && BBP_cache(VIEWvtparent(b))->batRestricted == BAT_READ) {
			r = COLnew2(0, b->ttype, max, TRANSIENT, b->twidth);
			BATswap_heaps(r, b, p);
		} else {
			local_storage = true;
			r = COLnew(0, b->ttype, max, TRANSIENT);

			if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b))
				err = 1;
		}
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max) {
		if (ATOMextern(r->ttype)) {
			memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));
		} else {
			char *d = Tloc(r, 0);
			const char *nil = ATOMnilptr(r->ttype);
			for (BUN i=cnt; i<max; i++)
				memcpy(d+(i*r->twidth), nil, r->twidth);
		}
	}
	assert(b->twidth == r->twidth || local_storage || !BATcount(b));

	if (!err) {
		BUN cnt = BATcount(g);

		if (!err) {
			oid *grp = Tloc(g, 0);

			gfunc(bit,min);
			gfunc(bte,min);
			gfunc(sht,min);
			gfunc(int,min);
			gfunc(date,min);
			gfunc(lng,min);
			gfunc(daytime,min);
			gfunc(timestamp,min);
			gfunc(hge,min);
			gfunc2(uuid,uuid_min);
			gfunc(flt,min);
			gfunc(dbl,min);
			if (local_storage) {
				gafunc_(vamin);
			} else {
				gafunc(vamin);
			}
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < max)
				BATsetcount(r, max);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
			*rid = r->batCacheid;
			BBPkeepref(r);
		}
	}
	if (!private)
		pipeline_unlock1(r);
	if (err)
		throw(MAL, "aggr.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
LALGmax(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0, tt = b->ttype;

	if (*rid && !is_bat_nil(*rid))
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat), local_storage = false;

	if (!private)
		pipeline_lock1(r);

	BAT *pg = BATdescriptor(*pid);
	oid max = BATcount(pg)?pg->T.maxval:0;
	BBPunfix(pg->batCacheid);

	if (r && BATcount(b)) {
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b))
			err = 1;
		else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_cache(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			BATswap_heaps(r, b, p);
		}
	} else if (!r) {
		if (ATOMvarsized(b->ttype) && VIEWvtparent(b) && BBP_cache(VIEWvtparent(b))->batRestricted == BAT_READ) {
			r = COLnew2(0, b->ttype, max, TRANSIENT, b->twidth);
			BATswap_heaps(r, b, p);
		} else {
			local_storage = true;
			r = COLnew(0, b->ttype, max, TRANSIENT);

			if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b))
				err = 1;
		}
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}
	if (cnt < max) {
		if (ATOMextern(r->ttype)) {
			memset(Tloc(r, cnt), 0, r->twidth*(max-cnt));
		} else {
			char *d = Tloc(r, 0);
			const char *nil = ATOMnilptr(r->ttype);
			for (BUN i=cnt; i<max; i++)
				memcpy(d+(i*r->twidth), nil, r->twidth);
		}
	}
	assert(b->twidth == r->twidth || local_storage || !BATcount(b));

	if (!err) {
		BUN cnt = BATcount(g);

		if (!err) {
			oid *grp = Tloc(g, 0);

			gfunc(bit,max);
			gfunc(bte,max);
			gfunc(sht,max);
			gfunc(int,max);
			gfunc(date,max);
			gfunc(lng,max);
			gfunc(daytime,max);
			gfunc(timestamp,max);
			gfunc(hge,max);
			gfunc2(uuid,uuid_max);
			gfunc(flt,max);
			gfunc(dbl,max);
			if (local_storage) {
				gafunc_(vamax);
			} else {
				gafunc(vamax);
			}
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < max)
				BATsetcount(r, max);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
			*rid = r->batCacheid;
			BBPkeepref(r);
		}
	}
	if (!private)
		pipeline_unlock1(r);
	if (err)
		throw(MAL, "aggr.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

#define SLICE_SIZE 100000
static str
SLICERslice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* return the nth slice of SLICE_SIZE rows from the input bat */
	(void)cntxt;
	(void)mb;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat *bid = getArgReference_bat(stk, pci, 1);
	int nr  = *getArgReference_int(stk, pci, 2);

	BUN s = SLICE_SIZE*nr;
	BAT *b = BATdescriptor(*bid);
	if (!b)
		return createException(SQL, "slicer.slice",	SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BAT *r = NULL;
	if (BATcount(b) < s) {
		r = COLnew(b->hseqbase, b->ttype, 0, TRANSIENT);
	} else {
		r = BATslice(b, s, s+SLICE_SIZE);
	}
	if (!r)
		return createException(SQL, "slicer.slice",	SQLSTATE(HY013) MAL_MALLOC_FAIL);

	*bid = b->batCacheid;
	*res = r->batCacheid;
	BBPkeepref(b);
	BBPkeepref(r);
	return MAL_SUCCEED;
}

static str
ALGcountCND_nil(lng *result, const bat *bid, const bat *cnd, const bit *ignore_nils)
{
	BAT *b, *s = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (cnd && !is_bat_nil(*cnd) && (s = BATdescriptor(*cnd)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "aggr.count", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	lng result1 = 0;
	if (b->ttype == TYPE_msk || mask_cand(b)) {
		BATsum(&result1, TYPE_lng, b, s, *ignore_nils, false, false, false);
	} else if (*ignore_nils) {
		result1 = (lng) BATcount_no_nil(b, s);
	} else {
		struct canditer ci;
		canditer_init(&ci, b, s);
		result1 = (lng) ci.ncand;
	}
	if (is_lng_nil(*result))
		*result = result1;
	else if (!is_lng_nil(result1))
		*result += result1;
	if (s)
		BBPunfix(s->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
ALGcount_nil(lng *result, const bat *bid, const bit *ignore_nils)
{
	return ALGcountCND_nil(result, bid, NULL, ignore_nils);
}

static str
ALGcountCND_bat(lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(result, bid, cnd, &(bit){0});
}

static str
ALGcount_bat(lng *result, const bat *bid)
{
	return ALGcountCND_nil(result, bid, NULL, &(bit){0});
}

static str
ALGcountCND_no_nil(lng *result, const bat *bid, const bat *cnd)
{
	return ALGcountCND_nil(result, bid, cnd, &(bit){1});
}

static str
ALGcount_no_nil(lng *result, const bat *bid)
{
	return ALGcountCND_nil(result, bid, NULL, &(bit){1});
}

static str
ALGminany_skipnil(ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.min", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "algebra.min",
							  "atom '%s' cannot be ordered linearly",
							  ATOMname(b->ttype));
	} else {
		if (ATOMextern(b->ttype)) {
			const void *nil = ATOMnilptr(b->ttype);
			int (*cmp)(const void *v1,const void *v2) = ATOMcompare(b->ttype);

			p = BATmin_skipnil(b, NULL, *skipnil, false);
			if (cmp(*(ptr*)result, nil) == 0 || (cmp(p, nil) != 0 && cmp(p, *(ptr*)result) < 0))
				* (ptr *) result = p;
			else
				GDKfree(p);
		} else {
			p = BATmin_skipnil(b, result, *skipnil, true);
			if ( p != result )
				msg = createException(MAL, "algebra.min", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if (msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "algebra.min", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGminany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGminany_skipnil(result, bid, &skipnil);
}

static str
ALGmaxany_skipnil(ptr result, const bat *bid, const bit *skipnil)
{
	BAT *b;
	ptr p;
	str msg = MAL_SUCCEED;

	if (result == NULL || (b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "algebra.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (!ATOMlinear(b->ttype)) {
		msg = createException(MAL, "algebra.max",
							  "atom '%s' cannot be ordered linearly",
							  ATOMname(b->ttype));
	} else {
		if (ATOMextern(b->ttype)) {
			const void *nil = ATOMnilptr(b->ttype);
			int (*cmp)(const void *v1,const void *v2) = ATOMcompare(b->ttype);

			p = BATmax_skipnil(b, NULL, *skipnil, false);
			if (cmp(*(ptr*)result, nil) == 0 || (cmp(p, nil) != 0 && cmp(p, *(ptr*)result) > 0))
				* (ptr *) result = p;
			else
				GDKfree(p);
		} else {
			p = BATmax_skipnil(b, result, *skipnil, true);
			if ( p != result )
				msg = createException(MAL, "algebra.max", SQLSTATE(HY002) "INTERNAL ERROR");
		}
		if ( msg == MAL_SUCCEED && p == NULL)
			msg = createException(MAL, "algebra.max", GDK_EXCEPTION);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
ALGmaxany(ptr result, const bat *bid)
{
	bit skipnil = TRUE;
	return ALGmaxany_skipnil(result, bid, &skipnil);
}



#include "mel.h"
static mel_func pipeline_init_funcs[] = {
 pattern("pipeline", "counter", PPcounter, true, "return next atomic number [0..n>", args(1,2, arg("", int), arg("pipeline", ptr))),
 pattern("pipeline", "channel", PPchannel, true, "create a new channel", args(2,3,
	argany("mailbox", 1), arg("channel", int),
	 argany("initial", 1)
 )),
 pattern("pipeline", "recv", PPrecv, true, "receive from channel", args(1,4,
	 argany("", 1),
	 arg("handle", ptr), argany("mailbox", 1), arg("channel", int)
 )),
 pattern("pipeline", "send", PPsend, true, "send through channel", args(1,5,
	 arg("", bit),
	 arg("handle", ptr),
	 argany("mailbox",1),
	 arg("channel",int),
	 argany("value", 1)
 )),

 pattern("lockedaggr", "sum", LOCKEDAGGRsum, true, "sum values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "prod", LOCKEDAGGRprod, true, "product of all values, using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 2))),
 pattern("lockedaggr", "avg", LOCKEDAGGRavg, true, "avg values into bat (bat has value, update), using the bat lock", args(2,5, sharedbatargany("", 1), sharedbatarg("rcnt", lng), arg("pipeline", ptr), argany("val", 1), arg("cnt", lng))),
 pattern("lockedaggr", "avg", LOCKEDAGGRavg, true, "avg values into bat (bat has value, update), using the bat lock", args(3,7, sharedbatargany("", 1), sharedbatarg("rremainder", lng), sharedbatarg("rcnt", lng), arg("pipeline", ptr), argany("val", 1), arg("remainder", lng), arg("cnt", lng))),
 pattern("lockedaggr", "min", LOCKEDAGGRmin, true, "min values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "max", LOCKEDAGGRmax, true, "max values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 command("lockedalgebra", "projection", LALGprojection, false, "Project left input onto right input.", args(1,4, batargany("",3), arg("pipeline", ptr), batarg("left",oid),batargany("right",3))),
 command("algebra", "unique", LALGunique, false, "Unique rows.", args(2,5, batarg("gid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid))),
 command("algebra", "unique", LALGgroup_unique, false, "Unique per group rows.", args(2,6, batarg("ngid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid), batarg("gid",oid))),
 command("group", "group", LALGgroup, false, "Group input.", args(2,4, batarg("gid", oid), batargany("sink",3), arg("pipeline", ptr), batargany("b",4))),
 command("group", "group", LALGderive, false, "Sub Group input.", args(2,6, batarg("gid", oid), batargany("sink",3), arg("pipeline", ptr), batarg("pgid", oid), batargany("phash", 5), batargany("b",3))),
 command("algebra", "projection", LALGproject, false, "Project.", args(1,4, batargany("",1), batarg("gid", oid), batargany("b",1), arg("pipeline", ptr))),
 command("aggr", "count", LALGcount, false, "Count per group.", args(1,6, batarg("",lng), batarg("gid", oid), batargany("", 1), arg("nonil", bit), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "count", LALGcountstar, false, "count per group.", args(1,4, batarg("",lng), batarg("gid", oid), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "sum", LALGsum, false, "sum per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "prod", LALGprod, false, "product per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(2,6, batargany("",1), batarg("rcnt", lng), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,7, batargany("",1), batarg("rremainder", lng), batarg("rcnt", lng), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(2,7, batargany("",1), batarg("rcnt", lng), batarg("gid", oid), batargany("", 2), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "avg", LALGavg, false, "avg per group.", args(3,9, batargany("",1), batarg("rremainder", lng), batarg("rcnt", lng), batarg("gid", oid), batargany("", 2), batarg("remainder", lng), batarg("cnt", lng), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "min", LALGmin, false, "Min per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "max", LALGmax, false, "Max per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("hash", "new", UHASHnew, false, "", args(1,3, batargany("sink",1),argany("tt",1),arg("size",int))),
 pattern("hash", "new", UHASHnew, false, "", args(1,4, batargany("sink",1),argany("tt",1),arg("size",int), batargany("p",2))),
 pattern("slicer", "slice", SLICERslice, false, "", args(2,3, batargany("slice",1), batargany("b",1), arg("nr",int))),

 command("iaggr", "count", ALGcount_bat, false, "Return the current size (in number of elements) in a BAT.", args(1,2, arg("",lng),batargany("b",0))),
 command("iaggr", "count", ALGcount_nil, false, "Return the number of elements currently in a BAT ignores\nBUNs with nil-tail iff ignore_nils==TRUE.", args(1,3, arg("",lng),batargany("b",0),arg("ignore_nils",bit))),
 command("iaggr", "count_no_nil", ALGcount_no_nil, false, "Return the number of elements currently\nin a BAT ignoring BUNs with nil-tail", args(1,2, arg("",lng),batargany("b",2))),
 command("iaggr", "count", ALGcountCND_bat, false, "Return the current size (in number of elements) in a BAT.", args(1,3, arg("",lng),batargany("b",0),batarg("cnd",oid))),
 command("iaggr", "count", ALGcountCND_nil, false, "Return the number of elements currently in a BAT ignores\nBUNs with nil-tail iff ignore_nils==TRUE.", args(1,4, arg("",lng),batargany("b",0),batarg("cnd",oid),arg("ignore_nils",bit))),
 command("iaggr", "count_no_nil", ALGcountCND_no_nil, false, "Return the number of elements currently\nin a BAT ignoring BUNs with nil-tail", args(1,3, arg("",lng),batargany("b",2),batarg("cnd",oid))),
 command("iaggr", "min", ALGminany, false, "Return the lowest tail value or nil.", args(1,2, argany("",2),batargany("b",2))),
 command("iaggr", "min", ALGminany_skipnil, false, "Return the lowest tail value or nil.", args(1,3, argany("",2),batargany("b",2),arg("skipnil",bit))),
 command("iaggr", "max", ALGmaxany, false, "Return the highest tail value or nil.", args(1,2, argany("",2),batargany("b",2))),
 command("iaggr", "max", ALGmaxany_skipnil, false, "Return the highest tail value or nil.", args(1,3, argany("",2),batargany("b",2),arg("skipnil",bit))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pipeline", NULL, pipeline_init_funcs); }
