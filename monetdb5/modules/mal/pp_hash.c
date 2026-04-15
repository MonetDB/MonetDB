/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_time.h"
#include "mal_interpreter.h"
#include "mal_instruction.h"
#include "mal_exception.h"
#include "mal_pipelines.h"
#include "pipeline.h"
#include "pp_hash.h"

static int
str_cmp(const char * s1, const char * s2)
{
	return strcmp(s1,s2);
}

static unsigned int
log_base2(size_t n)
{
	unsigned int l ;

	for (l = 0; n; l++) {
		n >>= 1 ;
	}
	return l ;
}

/* ***** HASH TABLE ***** */
static hash_table *
_ht_init(hash_table *h)
{
	if (h->gids == NULL) {
		h->vals = (char*)GDKmalloc(h->size * (size_t)h->width);
		h->gids = (hash_key_t*)GDKzalloc(sizeof(hash_key_t)* h->size);
		if (ATOMvarsized(h->type)) {
			h->pinned = GDKzalloc(sizeof(*h->pinned)*1024);
		}
		if (h->vals == NULL || h->gids == NULL)
			goto error;
		if (h->p) {
			assert(h->s.type == OA_HASH_TABLE_SINK);
			h->pgids = (gid*)GDKmalloc(sizeof(gid)* h->size);
			if (h->pgids == NULL)
				goto error;
		}
	}
	return h;
error:
	GDKfree(h->vals);
	GDKfree((void *)h->gids);
	GDKfree(h->pgids);
	return NULL;
}

static void
ht_destroy(hash_table *ht)
{
	GDKfree(ht->vals);
	GDKfree((void*)ht->gids);
	GDKfree(ht->pgids);
	if (ht->pinned) {
		for(int i=0; i < ht->pinned_nr; i++) {
			BBPunfix(ht->pinned[i]->parentid);
			HEAPdecref(ht->pinned[i], false);
		}
		GDKfree(ht->pinned);
	}
	if (ht->allocators) {
		for(int i = 0; i<ht->nr_allocators; i++) {
			if(ht->allocators[i])
				ma_destroy(ht->allocators[i]);
		}
		GDKfree(ht->allocators);
	}
	GDKfree(ht);
}

static hash_table *
_ht_create( int type, size_t size, hash_table *p)
{
	hash_table *h = (hash_table*)GDKzalloc(sizeof(hash_table));
	if (!h)
		return NULL;
	int bits = log_base2(size-1);

	if (!type)
		type = TYPE_oid;
	h->s.destroy = (sink_destroy)&ht_destroy;
	h->s.type = OA_HASH_TABLE_SINK;
	if (bits >= GIDBITS)
		bits = GIDBITS-1;
	h->bits = bits;
	h->size = (gid)1<<bits;
	h->mask = h->size-1;
	h->type = type;
	h->width = ATOMsize(type);
	h->last = 0;
	h->empty = true;
	h->p = p;
	h->pinned = NULL;
	h->pinned_nr = 0; /* no more than 1024 */
	if (type == TYPE_str) {
		h->cmp = (fcmp)str_cmp;
		h->hsh = (fhsh)str_hsh;
	} else {
		h->cmp = (fcmp)ATOMcompare(type);
		h->hsh = (fhsh)BATatoms[type].atomHash;
		h->len = (flen)BATatoms[type].atomLen;
	}
	h->processed = 0;
	MT_rwlock_init(&h->rwlock, "ht_create");

	hash_table *h2 = _ht_init(h);
	if (h2 == NULL) {
		GDKfree(h);
		return NULL;
	}
	return h2;
}

hash_table *
ht_create(int type, size_t size, hash_table *p)
{
	if (size < HT_MIN_SIZE)
		size = HT_MIN_SIZE;
	if (size > HT_MAX_SIZE)
		size = HT_MAX_SIZE;
	return _ht_create(type, size, p);
}

void
ht_activate(hash_table *ht)
{
	MT_rwlock_rdlock(&ht->rwlock);
}

void
ht_deactivate(hash_table *ht)
{
	MT_rwlock_rdunlock(&ht->rwlock);
}

#define REHASH(Type) \
		for(size_t i = 0; i < oldsize; i++) {		\
			Type *vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid hv = (gid)_hash_##Type(vals[og])&ht->mask, k = hv; \
				gid g = ngids[k];					\
				for (gid l=1; g; l++) {				\
					nextk;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define REHASH_f(Type,Type2) \
		for(size_t i = 0; i < oldsize; i++) {		\
			Type *vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid hv = (gid)_hash_##Type(*(Type2*)(vals+og))&ht->mask, k = hv; \
				gid g = ngids[k];					\
				for (gid l=1; g; l++) {				\
					nextk;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define REHASH_a() \
		for(size_t i = 0; i < oldsize; i++) {		\
			char **vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid hv = (gid)ht->hsh(vals[og])&ht->mask, k = hv; \
				gid g = ngids[k];			\
				for (gid l=1; g; l++) {				\
					nextk;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define CREHASH(Type) \
		for(size_t i = 0; i < oldsize; i++) {		\
			Type *vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid hv = (gid)combine(pgids[og], _hash_##Type(vals[og]), prime)&ht->mask, k = hv; \
				gid g = ngids[k];			\
				for (gid l=1; g; l++) {				\
					nextk;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define CREHASH_f(Type, Type2) \
		for(size_t i = 0; i < oldsize; i++) {		\
			Type *vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid hv = (gid)combine(pgids[og], _hash_##Type(*(Type2*)(vals+og)), prime)&ht->mask, k = hv; \
				gid g = ngids[k];			\
				for (gid l=1; g; l++) {				\
					nextk;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define CREHASH_a() \
		for(size_t i = 0; i < oldsize; i++) {		\
			char **vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid hv = (gid)combine(pgids[og], ht->hsh(vals[og]), prime)&ht->mask, k = hv; \
				gid g = ngids[k];			\
				for (gid l=1; g; l++) {				\
					nextk;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\


int
ht_rehash(hash_table *ht)
{
	size_t size = ht->size;
	ht_deactivate(ht);
	MT_rwlock_wrlock(&ht->rwlock);
	if (ht->size == size) { /* the lucky one ... */
		//dbl ratio = (ht->processed / (dbl)ht->last); /* hit ratio */
		//
		size_t newsize = ht->size * 4; /* later learn from growth and expected (max) number (of influx) */
		size_t oldsize = ht->size;

		int bits = log_base2(newsize-1);
		ht->size = (gid)1<<bits;
		ht->bits = bits;
		ht->mask = ht->size-1;
		/* realloc data */
		ht->vals = (char*)GDKrealloc(ht->vals, ht->size * (size_t)ht->width);
		if (ht->pgids)
			ht->pgids = (gid*)GDKrealloc(ht->pgids, sizeof(gid)* ht->size);
		if (ht->vals == NULL || ht->gids == NULL)
			goto error;

		hash_key_t *ogids = ht->gids;
		hash_key_t *ngids = (hash_key_t*)GDKzalloc(sizeof(hash_key_t)* ht->size);
		if (!ngids)
			goto error;

		int prime = hash_prime_nr[ht->bits-5];
		if (!ht->pgids) {
			switch(ht->type) {
			case TYPE_bit:
				REHASH(bit);
				break;
			case TYPE_bte:
				REHASH(bit);
				break;
			case TYPE_sht:
				REHASH(sht);
				break;
			case TYPE_int:
			case TYPE_inet4:
				REHASH(int);
				break;
			case TYPE_date:
				REHASH(date);
				break;
			case TYPE_lng:
				REHASH(lng);
				break;
			case TYPE_oid:
				REHASH(oid);
				break;
			case TYPE_daytime:
				REHASH(daytime);
				break;
			case TYPE_timestamp:
				REHASH(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				REHASH(hge);
				break;
#endif
			case TYPE_flt:
				REHASH_f(flt, int);
				break;
			case TYPE_dbl:
				REHASH_f(dbl, lng);
				break;
			default:
				if (ATOMvarsized(ht->type)) {
					REHASH_a();
				}
			}
		} else {
			gid *pgids = ht->pgids;
			switch(ht->type) {
			case TYPE_bit:
				CREHASH(bit);
				break;
			case TYPE_bte:
				CREHASH(bit);
				break;
			case TYPE_sht:
				CREHASH(sht);
				break;
			case TYPE_int:
			case TYPE_inet4:
				CREHASH(int);
				break;
			case TYPE_date:
				CREHASH(date);
				break;
			case TYPE_lng:
				CREHASH(lng);
				break;
			case TYPE_oid:
				CREHASH(oid);
				break;
			case TYPE_daytime:
				CREHASH(daytime);
				break;
			case TYPE_timestamp:
				CREHASH(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				CREHASH(hge);
				break;
#endif
			case TYPE_flt:
				CREHASH_f(flt, int);
				break;
			case TYPE_dbl:
				CREHASH_f(dbl, lng);
				break;
			default:
				if (ATOMvarsized(ht->type)) {
					CREHASH_a();
				}
			}
		}
		GDKfree((void*)ht->gids);
		ht->gids = ngids;
		MT_rwlock_wrunlock(&ht->rwlock);
		ht_activate(ht);
	} else {
		MT_rwlock_wrunlock(&ht->rwlock);
		ht_activate(ht);
	}
	return 0;
error:
	GDKfree(ht->vals);
	GDKfree((void *)ht->gids);
	GDKfree(ht->pgids);
	return -1;
}

static str
OAHASHnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *res = getArgReference_bat(s, p, 0);
	int tt = getArgType(m, p, 1);
	int tt2 = getArgType(m, p, 2);
	lng size = 0;
	hash_table *parent = NULL;
	BAT *pht = NULL;

	if (tt2 == TYPE_int) {
		assert(0);
		size = (lng) *getArgReference_int(s, p, 2);
	} else {
		assert(tt2 == TYPE_lng);
		size = *getArgReference_lng(s, p, 2);
	}
	/* multiply with the magic estimation while avoiding overflow */
	size = size > ((dbl)INT64_MAX / 1.2 / 2.1)? INT64_MAX : (lng)(size * 1.2 * 2.1);

	if (p->argc == 4) {
		bat pid = *getArgReference_bat(s, p, 3);
		if ((pht = BATdescriptor(pid)) == NULL)
			return createException(MAL, "oahash.new", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		parent = (hash_table*)pht->tsink;
	}

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	if (b == NULL) {
		BBPreclaim(pht);
		return createException(MAL, "oahash.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	b->tsink = (Sink*)ht_create(tt, (size_t)size, parent);
	BBPreclaim(pht);
	if (b->tsink == NULL) {
		BBPunfix(b->batCacheid);
		return createException(MAL, "oahash.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*res = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

static str
OAHASHhashmark_init(Client ctx, bat *res, const bat *ht_sink, const bat *payload)
{
	(void)ctx;

	BAT *r = NULL, *ht = NULL, *hp = NULL;
	str err = NULL;

	ht = BATdescriptor(*ht_sink);
	if (!ht) {
		err = createException(SQL, "oahash.hashmark_init", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (ht && ht->tsink && ht->tsink->error) {
		err = ht->tsink->error;
		goto error;
	}
	if (payload && !is_bat_nil(*payload)) {
		hp = BATdescriptor(*payload);
		if (!hp) {
			err = createException(SQL, "oahash.hashmark_init", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}

    hash_table *h = (hash_table*)ht->tsink;
	if (hp)
		h = (hash_table*)hp->tsink;
	//assert(h && h->s.type == OA_HASH_TABLE_SINK);
	BUN sz = h?h->last:BATcount(ht); /* no hash ie outer cross product case */

	r = COLnew(0, TYPE_bit, sz, TRANSIENT);
	if (!r) {
		err = createException(SQL, "oahash.hashmark_init", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	bit *mrk = Tloc(r, 0);
	gid g = 0;
	/* init unused GIDs with bit_nil, used GIDs with FALSE */
	if (!h) {
		TIMEOUT_LOOP_IDX_DECL(i, sz, qry_ctx) {
			mrk[i] = false;
		}
	} else {
		TIMEOUT_LOOP_IDX_DECL(i, sz, qry_ctx) {
			mrk[i] = bit_nil;
		}
		TIMEOUT_LOOP_IDX_DECL(i, h->size, qry_ctx) {
			g = ATOMIC_GET(h->gids+i);
			if(g) {
				mrk[g-1] = false;
			}
		}
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.hashmark_init", RUNTIME_QRY_TIMEOUT));
	if (err)
		goto error;

	BATsetcount(r, sz);
	BATnegateprops(r);
	r->tkey = FALSE;
	r->tsorted = r->trevsorted = FALSE;
	//*res = r->batCacheid;
	//BBPkeepref(r);
	//leave writable
	BBPretain(*res = r->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(ht->batCacheid);
	BBPreclaim(hp);
	return MAL_SUCCEED;
error:
	BBPreclaim(r);
	BBPreclaim(ht);
	BBPreclaim(hp);
	return err;
}

static str
UHASHext(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;
	(void)m;

	bat *res = getArgReference_bat(s, p, 0);
	bat *in = getArgReference_bat(s, p, 1);

	BAT *i = BATdescriptor(*in);
	if (!i)
		return createException(MAL, "hash.ext", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	hash_table *h = (hash_table*)i->tsink;
	if (!h || h->s.type != OA_HASH_TABLE_SINK) {
		BBPreclaim(i);
		return createException(MAL, "hash.ext", SQLSTATE(HY002) "Missing hash table");
	}
	BAT *r = BATdense(0, 0, h->last);
	BBPreclaim(i);
	if (!r) {
		return createException(MAL, "hash.ext", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*res = r->batCacheid;
	BBPkeepref(r);
	return MAL_SUCCEED;
}

/* ***** HASH OPERATORS ***** */

#define BATaprep_heap(BT, SB, SK, FName) \
	do { \
		MT_lock_set(&SB->theaplock); \
		MT_lock_set(&BT->theaplock); \
		if (!VIEWvtparent(BT)) { /* TODO this VIEWvtparent probably doesn't need the locks */ \
			MT_lock_unset(&BT->theaplock); \
			MT_lock_unset(&SB->theaplock); \
			local_storage = true; \
			pipeline_lock(p); \
			if (!SK->allocators) { \
				SK->allocators = (allocator**)GDKzalloc(p->p->nr_workers*sizeof(allocator*)); \
				if (!SK->allocators) { \
					pipeline_unlock(p); \
					err = createException(MAL, FName, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					goto error; \
				} else { \
					SK->nr_allocators = p->p->nr_workers; \
				} \
			} \
			pipeline_unlock(p); \
			assert(p->wid < p->p->nr_workers); \
			if (!SK->allocators[p->wid]) { \
				char name[MT_NAME_LEN]; \
				snprintf(name, sizeof(name), "pp%d", p->wid); \
				SK->allocators[p->wid] = create_allocator(name, false); \
				if (!SK->allocators[p->wid]) { \
					err = createException(MAL, FName, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					goto error; \
				} \
			} \
		} else if (BATcount(BT) && BATcount(SB) == 0 && SB->tvheap->parentid == SB->batCacheid) { \
			MT_lock_unset(&BT->theaplock); \
			MT_lock_unset(&SB->theaplock); \
			BATswap_heaps(SB, BT, p); \
		} else if (SB->tvheap->parentid != BT->tvheap->parentid) { \
			int i = 0; \
			for(i = 0; i < SK->pinned_nr; i++) { \
				if (SK->pinned[i] == BT->tvheap) \
					break; \
			} \
			if (i == SK->pinned_nr) { \
				BBPfix(BT->tvheap->parentid); \
				HEAPincref(BT->tvheap); \
				SK->pinned[SK->pinned_nr++] = BT->tvheap; \
				assert(SK->pinned_nr < 1024); \
			} \
			MT_lock_unset(&BT->theaplock); \
			MT_lock_unset(&SB->theaplock); \
		} else { \
			MT_lock_unset(&BT->theaplock); \
			MT_lock_unset(&SB->theaplock); \
		} \
	} while(0)

#define BATgroup(Type) \
	do { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)_hash_##Type(bp[i])&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				assert(g<(gid)h->size); \
				while (g && vals[g] != bp[i]) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					}\
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	} while (0)

#define BATvgroup() \
	do { \
		oid *vals = h->vals; \
		struct canditer ci; \
		canditer_init(&ci, NULL, b); \
		cnt = ci.ncand; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			oid bpi = canditer_next(&ci); \
			assert(bpi != oid_nil); \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)_hash_oid(bpi)&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				while (g && vals[g] != bpi) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					}\
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	} while (0)

#define BATfgroup(Type, BaseType) \
	do { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && \
						vals[g] != bp[i])) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					}\
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	} while (0)

#define BATagroup() \
	do { \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			void *bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)); \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)h->hsh(bpi)&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (vals[g] && h->cmp(vals[g], bpi) != 0)) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					}\
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	} while (0)

#define BATagroup_(P) \
	do { \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		allocator *ma = h->allocators[P->wid]; \
		\
		if (ATOMstorage(tt) == TYPE_str) { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
				char *bpi = (char *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)); \
				gid g = 0; \
				while (!fnd) { \
					gid k = (gid)str_hsh(bpi)&h->mask; \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (vals[g] && h->cmp(vals[g], bpi) != 0)) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = ht_preclaim(private); \
							slot = ATOMIC_ADD(&h->last, slots); \
							if (((slot*100)/70) >= (gid)h->size) { \
								hash_rehash(h, p, err); \
								vals = h->vals; \
								continue; \
							} \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_strdup(ma, bpi); \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
							slots++; \
							slot--; \
							continue; \
						}\
					} \
					fnd = 1; \
				} \
				gp[i] = g-1; \
			} \
		} else { /* other ATOMvarsized, e.g. BLOB */ \
			int (*atomcmp)(const void *, const void *) = ATOMcompare(tt); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
				void *bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)); \
				gid g = 0; \
				while (!fnd) { \
					gid k = (gid)h->hsh(bpi)&h->mask; \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (vals[g] && atomcmp(vals[g], bpi) != 0)) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = ht_preclaim(private); \
							slot = ATOMIC_ADD(&h->last, slots); \
							if (((slot*100)/70) >= (gid)h->size) { \
								hash_rehash(h, p, err); \
								vals = h->vals; \
								continue; \
							} \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_copy(ma, bpi, h->len(bpi)); \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
							slots++; \
							slot--; \
							continue; \
						}\
					} \
					fnd = 1; \
				} \
				gp[i] = g-1; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHbuild_tbl(Client ctx, bat *slot_id, bat *ht_sink, const bat *key, const ptr *H)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H;
	bool private = 0, local_storage = false;
	str err = NULL;
	BAT *g = NULL, *u = NULL, *b = NULL;

	/* for now we only work with shared ht_sink in the build phase */
	assert(*ht_sink && !is_bat_nil(*ht_sink));

   	b = BATdescriptor(*key);
	u = BATdescriptor(*ht_sink);
	if (!b || !u) {
		err = createException(SQL, "oahash.build_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	hash_table *h = (hash_table*)u->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);

	BUN cnt = BATcount(b);
	g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
	if (g == NULL) {
		err = createException(MAL, "oahash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (cnt) {
		ATOMIC_BASE_TYPE expected = 0;
		int tt = b->ttype;
		gid *gp = Tloc(g, 0);

		h->empty = false;
		int slots = 0;
		gid slot = 0;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		ht_activate(h);
		switch(tt) {
			case TYPE_void:
				BATvgroup();
				break;
			case TYPE_bit:
				BATgroup(bit);
				break;
			case TYPE_bte:
				BATgroup(bte);
				break;
			case TYPE_sht:
				BATgroup(sht);
				break;
			case TYPE_int:
			case TYPE_inet4:
				BATgroup(int);
				break;
			case TYPE_date:
				BATgroup(date);
				break;
			case TYPE_lng:
				BATgroup(lng);
				break;
			case TYPE_oid:
				if (BATtdense(b))
					BATvgroup();
				else
					BATgroup(oid);
				break;
			case TYPE_daytime:
				BATgroup(daytime);
				break;
			case TYPE_timestamp:
				BATgroup(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BATgroup(hge);
				break;
#endif
			case TYPE_flt:
				BATfgroup(flt, int);
				break;
			case TYPE_dbl:
				BATfgroup(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaprep_heap(b, u, h, "oahash.build_table");
					if (local_storage) {
						BATagroup_(p);
					} else {
						BATagroup();
					}
				} else {
					err = createException(MAL, "oahash.build_table", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				}
		}
		ht_deactivate(h);
		if (!err)
			TIMEOUT_CHECK(qry_ctx, throw(MAL, "oahash.build_table", RUNTIME_QRY_TIMEOUT));
		h->processed += cnt;
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "oahash.build_table", "pipeline execution error");
		goto error;
	}
	BBPunfix(b->batCacheid);
	BATsetcount(g, cnt);
	BATnegateprops(g);
	gid last = ATOMIC_GET(&h->last);
	g->tmaxval = last;
	g->tkey = FALSE;
	*slot_id = g->batCacheid;
	//skip propcheck
	//BBPkeepref(u);
	BBPretain(u->batCacheid);
	BBPunfix(u->batCacheid);
	BBPkeepref(g);
	(void)private;
	return MAL_SUCCEED;
error:
	BBPreclaim(b);
	BBPreclaim(u);
	BBPreclaim(g);
	return err;
}

#define derive(Type) \
	do { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)combine(gi[i], _hash_##Type(bp[i]), prime)&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || vals[g] != bp[i])) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							pgids = h->pgids; \
							prime = hash_prime_nr[h->bits-5]; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	} while (0)

#define vderive() \
	do { \
		oid *vals = h->vals; \
		struct canditer ci; \
		canditer_init(&ci, NULL, b); \
		cnt = ci.ncand; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			oid bpi = canditer_next(&ci); \
			assert(bpi != oid_nil); \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)combine(gi[i], _hash_oid(bpi), prime)&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || vals[g] != bpi)) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							pgids = h->pgids; \
							prime = hash_prime_nr[h->bits-5]; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	} while (0)

#define fderive(Type, BaseType) \
	do { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)combine(gi[i], _hash_##Type(*(((BaseType*)bp)+i)), prime)&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]))) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/70) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							pgids = h->pgids; \
							prime = hash_prime_nr[h->bits-5]; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	} while (0)

#define aderive() \
	do { \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			void *bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)); \
			gid g = 0; \
			while (!fnd) { \
				gid k = (gid)combine(gi[i], h->hsh(bpi), prime)&h->mask; \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0))) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = ht_preclaim(private); \
						slot = ATOMIC_ADD(&h->last, slots); \
						if (((slot*100)/100) >= (gid)h->size) { \
							hash_rehash(h, p, err); \
							vals = h->vals; \
							pgids = h->pgids; \
							prime = hash_prime_nr[h->bits-5]; \
							continue; \
						} \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					pgids[g] = gi[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
						slots++; \
						slot--; \
						continue; \
					} \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	} while (0)

#define aderive_(P) \
	do { \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		allocator *ma = h->allocators[P->wid]; \
		\
		if (ATOMstorage(tt) == TYPE_str) { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
				char *bpi = (char *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)); \
				gid g = 0; \
				while (!fnd) { \
					gid k = (gid)combine(gi[i], str_hsh(bpi), prime)&h->mask; \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0))) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = ht_preclaim(private); \
							slot = ATOMIC_ADD(&h->last, slots); \
							if (((slot*100)/100) >= (gid)h->size) { \
								hash_rehash(h, p, err); \
								vals = h->vals; \
								pgids = h->pgids; \
								prime = hash_prime_nr[h->bits-5]; \
								continue; \
							} \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_strdup(ma, bpi); \
						pgids[g] = gi[i]; \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
							slots++; \
							slot--; \
							continue; \
						} \
					} \
					fnd = 1; \
				} \
				gp[i] = g-1; \
			} \
			bat_iterator_end(&bi); \
		} else { /* other ATOMvarsized, e.g. BLOB */ \
			int (*atomcmp)(const void *, const void *) = ATOMcompare(tt); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
				void *bpi = (void *) ((bi).vh->base+VarHeapVal(bi.base,i,bi.width)); \
				gid g = 0; \
				while (!fnd) { \
					gid k = (gid)combine(gi[i], h->hsh(bpi), prime)&h->mask; \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (pgids[g] != gi[i] || (vals[g] && atomcmp(vals[g], bpi) != 0))) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = ht_preclaim(private); \
							slot = ATOMIC_ADD(&h->last, slots); \
							if (((slot*100)/70) >= (gid)h->size) \
								hash_rehash(h, p, err); \
								vals = h->vals; \
								pgids = h->pgids; \
								prime = hash_prime_nr[h->bits-5]; \
								continue; \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_copy(ma, bpi, h->len(bpi)); \
						pgids[g] = gi[i]; \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) { \
							slots++; \
							slot--; \
							continue; \
						} \
					} \
					fnd = 1; \
				} \
				gp[i] = g-1; \
			} \
			bat_iterator_end(&bi); \
		} \
	} while (0)

static str
OAHASHbuild_tbl_cmbd(Client ctx, bat *slot_id, bat *ht_sink, const bat *key, const bat *parent_slotid, const ptr *H)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H;
	bool private = 0, local_storage = false;
	str err = NULL;
	BAT *u = NULL, *b = NULL, *G = NULL, *g = NULL;

	/* for now we only work with shared ht_sink in the build phase */
	assert(*ht_sink && !is_bat_nil(*ht_sink));

	u = BATdescriptor(*ht_sink);
	b = BATdescriptor(*key);
	G = BATdescriptor(*parent_slotid);
	if (!b || !G || !u) {
		err = createException(MAL, "oahash.build_combined_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	hash_table *h = (hash_table*)u->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);

	BUN cnt = BATcount(b);
	g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
	if (g == NULL) {
		err = createException(MAL, "oahash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (cnt) {
		ht_activate(h);
		ATOMIC_BASE_TYPE expected = 0;
		int tt = b->ttype;
		gid *gp = Tloc(g, 0);
		gid *gi = Tloc(G, 0);
		gid *pgids = h->pgids;
		int prime = hash_prime_nr[h->bits-5];

		h->empty = false;
		int slots = 0;
		gid slot = 0;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		switch(tt) {
			case TYPE_void:
				vderive();
				break;
			case TYPE_bit:
				derive(bit);
				break;
			case TYPE_bte:
				derive(bte);
				break;
			case TYPE_sht:
				derive(sht);
				break;
			case TYPE_int:
			case TYPE_inet4:
				derive(int);
				break;
			case TYPE_date:
				derive(date);
				break;
			case TYPE_lng:
				derive(lng);
				break;
			case TYPE_oid:
				if (BATtdense(b))
					vderive();
				else
					derive(oid);
				break;
			case TYPE_daytime:
				derive(daytime);
				break;
			case TYPE_timestamp:
				derive(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				derive(hge);
				break;
#endif
			case TYPE_flt:
				fderive(flt, int);
				break;
			case TYPE_dbl:
				fderive(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaprep_heap(b, u, h, "oahash.build_combined_table");
					if (local_storage) {
						aderive_(p);
					} else {
						aderive();
					}
				} else {
					err = createException(MAL, "oahash.build_combined_table", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				}
		}
		ht_deactivate(h);
		if (!err)
			TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.build_combined_table", RUNTIME_QRY_TIMEOUT));
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "oahash.build_combined_table", "pipeline execution error");
		goto error;
	}
	BATsetcount(g, cnt);
	pipeline_lock2(g);
	BATnegateprops(g);
	pipeline_unlock2(g);
	gid last = ATOMIC_GET(&h->last);
	g->tmaxval = last;
	g->tkey = FALSE;
	*slot_id = g->batCacheid;
	//skip propcheck
	//BBPkeepref(u);
	BBPretain(u->batCacheid);
	BBPunfix(u->batCacheid);
	BBPkeepref(g);
	(void)private;

	BBPunfix(b->batCacheid);
	BBPunfix(G->batCacheid);
	return MAL_SUCCEED;
error:
	BBPreclaim(u);
	BBPreclaim(b);
	BBPreclaim(G);
	BBPreclaim(g);
	return err;
}

static str
OAHASHadd_freq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	assert(pci->retc == 1 || pci->retc == 2);

	bat *occrrence_idx = pci->retc == 2? getArgReference_bat(stk, pci, 0) : NULL;
	bat *frequencies = getArgReference_bat(stk, pci, pci->retc - 1);
	bat *slot_id = getArgReference_bat(stk, pci, pci->retc);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, pci->retc + 1);
	BAT *frq = NULL, *slt = NULL, *res = NULL;
	bool locked = false;
	str err = NULL;

	assert(frequencies && !is_bat_nil(*frequencies));

	frq = BATdescriptor(*frequencies);
	slt = BATdescriptor(*slot_id);
	if (!frq || !slt) {
		err = createException(MAL, "oahash.frequency", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	assert(frq->ttype == TYPE_lng);

	BUN cnt = BATcount(slt);

	if (occrrence_idx) {
		res = COLnew(slt->hseqbase, TYPE_oid, cnt, TRANSIENT);
		if (!res) {
			err = createException(MAL, "oahash.frequency", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
	}

	if (cnt) {
		pipeline_lock1(frq);
		locked = true;

		if (BATcapacity(frq) < slt->tmaxval) {
			BUN sz = slt->tmaxval * 2;
			if (BATextend(frq, sz) != GDK_SUCCEED) {
				err = createException(MAL, "oahash.frequency", MAL_MALLOC_FAIL);
				goto error;
			}
		}

		BUN cnt2 = BATcount(frq);
		if (cnt2 < slt->tmaxval) {
			memset(Tloc(frq, cnt2), 0, frq->twidth*(slt->tmaxval-cnt2));
			pipeline_lock2(frq);
			BATsetcount(frq, slt->tmaxval);
			// TODO: would be nice to have a bat.new variant that initiates props
			BATnegateprops(frq);
			frq->tnonil = true;
			pipeline_unlock2(frq);
		}

		gid *sltid = Tloc(slt, 0);
		lng *freqs = Tloc(frq, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		if (occrrence_idx) {
			gid *occIdx = Tloc(res, 0);
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
				occIdx[i] = freqs[sltid[i]];
				freqs[sltid[i]]++;
			}
		} else {
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
				freqs[sltid[i]]++;
			}
		}
		pipeline_unlock1(frq);
		locked = false;

		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.frequency", RUNTIME_QRY_TIMEOUT));
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "oahash.frequency", "pipeline execution error");
		goto error;
	}

	BBPunfix(slt->batCacheid);
	//skip propcheck
	//BBPkeepref(frq);
	BBPretain(frq->batCacheid);
	BBPunfix(frq->batCacheid);
	if (occrrence_idx) {
		BATsetcount(res, cnt);
		BATnegateprops(res);
		*occrrence_idx = res->batCacheid;
		BBPkeepref(res);
	}
	return MAL_SUCCEED;
error:
	if (locked) pipeline_unlock1(frq);
	BBPreclaim(frq);
	BBPreclaim(slt);
	BBPreclaim(res);
	return err;
}

#define BATvprobe() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		keycnt = ci.ncand; \
		oid *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			oid ky = canditer_next(&ci); \
			assert(ky != oid_nil); \
			if (!(*semantics) && ky == oid_nil) { \
				if (!match && empty) { \
					oid_mtd[mtdcnt] = off+i; \
					slt[mtdcnt] = oid_nil; \
					mtdcnt++; \
				}\
				continue; \
			} \
			gid k = (gid)_hash_oid(ky)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && vals[slot] != ky) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = slot - 1; \
				mtdcnt++; \
				if (match && *single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
	} while (0)

#define _BATprobe(Type, ne) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			if (!(*semantics) && is_##Type##_nil(ky[i])) { \
				if (!match && empty) { \
					oid_mtd[mtdcnt] = off+i; \
					slt[mtdcnt] = oid_nil; \
					mtdcnt++; \
				}\
				continue; \
			} \
			gid k = (gid)_hash_##Type(ky[i])&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && (!(is_##Type##_nil(ky[i]) && is_##Type##_nil(vals[slot])) && (ne))) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = match?(oid)(slot - 1):oid_nil; \
				mtdcnt++; \
				if (match && *single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
	} while (0)

#define BATprobe(Type) \
	_BATprobe(Type, vals[slot] != ky[i])

#define BATcprobe(Type) \
	_BATprobe(Type, memcmp(vals+slot, ky+i, sizeof(Type))!=0)

#define BATfprobe(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			if (!(*semantics) && is_##Type##_nil(ky[i])) { \
				if (!match && empty) { \
					oid_mtd[mtdcnt] = off+i; \
					slt[mtdcnt] = oid_nil; \
					mtdcnt++; \
				}\
				continue; \
			} \
			gid k = (gid)_hash_##Type(*(((BaseType*)ky)+i))&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && (!(is_##Type##_nil(ky[i]) && is_##Type##_nil(vals[slot])) && vals[slot] != ky[i])) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = match?(oid)(slot - 1):oid_nil; \
				mtdcnt++; \
				if (match && *single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
	} while (0)

#define BATaprobe() \
	do { \
		BATiter bi = bat_iterator(k); \
		char **vals = ht->vals; \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			char *val = (bi).vh->base+VarHeapVal(bi.base,i,bi.width); \
			if (!(*semantics) && atomcmp(val, nil) == 0) { \
				if (!match && empty) { \
					oid_mtd[mtdcnt] = off+i; \
					slt[mtdcnt] = oid_nil; \
					mtdcnt++; \
				}\
				continue; \
			} \
			gid k = (gid)str_hsh(val)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && atomcmp(vals[slot], val) != 0) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = match?(oid)(slot - 1):oid_nil; \
				mtdcnt++; \
				if (match && *single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHprobe1(Client ctx, bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H, bit match)
{
	(void)ctx;
	BAT *o = NULL, *s = NULL, *k = NULL, *t = NULL, *f = NULL;
	BUN keycnt, mtdcnt = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
	assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	t = BATdescriptor(*HSH_ht);
	if (!k || !t) {
		err = createException(SQL, "oahash.probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (*single) {
		/* frequency is required to check if a match is single */
		f = BATdescriptor(*frequency);
		if (!f) {
			err = createException(SQL, "oahash.probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		freq = Tloc(f, 0);
	}

	keycnt = BATcount(k);
	o = COLnew(0, TYPE_oid, keycnt, TRANSIENT);
	s = COLnew(0, TYPE_oid, keycnt, TRANSIENT);
	if (!o || !s) {
		err = createException(SQL, "oahash.probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (keycnt) {
		if (t->tsink->error) {
			err = t->tsink->error;
			goto error;
		}

		hash_table *ht = (hash_table*)t->tsink;
		bool empty = (ht->last == 0);
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		oid *oid_mtd = Tloc(o, 0);
		oid *slt = Tloc(s, 0);

		BUN off = k->hseqbase;
		switch(tt) {
			case TYPE_void:
				BATvprobe();
				break;
			case TYPE_bit:
				BATprobe(bit);
				break;
			case TYPE_bte:
				BATprobe(bte);
				break;
			case TYPE_sht:
				BATprobe(sht);
				break;
			case TYPE_int:
				BATprobe(int);
				break;
			case TYPE_date:
				BATprobe(date);
				break;
			case TYPE_inet4:
				BATcprobe(inet4);
				break;
			case TYPE_lng:
				BATprobe(lng);
				break;
			case TYPE_oid:
				BATprobe(oid);
				break;
			case TYPE_daytime:
				BATprobe(daytime);
				break;
			case TYPE_timestamp:
				BATprobe(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BATprobe(hge);
				break;
#endif
			case TYPE_flt:
				BATfprobe(flt, int);
				break;
			case TYPE_dbl:
				BATfprobe(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaprobe();
				} else {
					err = createException(MAL, "oahash.probe", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
					goto error;
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.probe", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}

	BBPreclaim(k);
	BBPunfix(t->batCacheid);
	BBPreclaim(f);
	BATsetcount(o, mtdcnt);
	BATsetcount(s, mtdcnt);
	BATnegateprops(o);
	BATnegateprops(s);
	o->tnonil = true;
	s->tnonil = match?true:false;
	o->tsorted = true;
	BATkey(o, true);
	*PRB_oid = o->batCacheid;
	*HSH_slotid = s->batCacheid;
	BBPkeepref(o);
	BBPkeepref(s);
	return MAL_SUCCEED;
error:
	BBPreclaim(o);
	BBPreclaim(s);
	BBPreclaim(k);
	BBPreclaim(t);
	BBPreclaim(f);
	return err;
}

static str
OAHASHprobe_single(Client ctx, bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHprobe1(ctx, PRB_oid, HSH_slotid, PRB_key, HSH_ht, frequency, single, semantics, H, true);
}

static str
OAHASHprobe(Client ctx, bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHprobe1(ctx, PRB_oid, HSH_slotid, PRB_key, HSH_ht, NULL, single, semantics, H, true);
}

static str
OAHASHnprobe_single(Client ctx, bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHprobe1(ctx, PRB_oid, HSH_slotid, PRB_key, HSH_ht, frequency, single, semantics, H, false);
}

static str
OAHASHnprobe(Client ctx, bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHprobe1(ctx, PRB_oid, HSH_slotid, PRB_key, HSH_ht, NULL, single, semantics, H, false);
}

#define BATvoprobe() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		keycnt = ci.ncand; \
		oid *vals = ht->vals; \
		\
		if (any) { \
			gid k = (gid)_hash_oid(oid_nil)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && vals[slot] != oid_nil) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) \
				has_nil = bit_nil; \
		} \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			oid ky = canditer_next(&ci); \
			assert(ky != oid_nil); \
			if (!(*semantics) && ky == oid_nil) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = any?empty:false; \
				mtdcnt++; \
				continue; \
			} \
			gid k = (gid)_hash_oid(ky)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && vals[slot] != ky) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			oid_mtd[mtdcnt] = off+i; \
			if (slot) { \
				slt[mtdcnt] = slot - 1; \
				mark[i] = true; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
			} \
			mtdcnt++; \
		} \
	} while (0)

#define _BAToprobe(Type, ne) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		if (any) { \
			gid k = (gid)_hash_##Type(Type##_nil)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && !is_##Type##_nil(vals[slot])) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) \
				has_nil = bit_nil; \
		} \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			if (!(*semantics) && is_##Type##_nil(ky[i])) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = any?empty:false; \
				mtdcnt++; \
				continue; \
			} \
			gid k = (gid)_hash_##Type(ky[i])&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && (!(is_##Type##_nil(ky[i]) && is_##Type##_nil(vals[slot])) && (ne))) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			oid_mtd[mtdcnt] = off+i; \
			if (slot) { \
				slt[mtdcnt] = (oid)(slot - 1); \
				mark[i] = true; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
			} \
			mtdcnt++; \
		} \
	} while (0)

#define BAToprobe(Type) \
	_BAToprobe(Type, vals[slot] != ky[i])

#define BATcoprobe(Type) \
	_BAToprobe(Type, memcmp(vals+slot, ky+i, sizeof(Type))!=0)

#define BATfoprobe(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		if (any) { \
			gid k = (gid)_hash_##Type((BaseType)Type##_nil)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && !is_##Type##_nil(vals[slot])) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) \
				has_nil = bit_nil; \
		} \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			if (!(*semantics) && is_##Type##_nil(ky[i])) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = any?empty:false; \
				mtdcnt++; \
				continue; \
			} \
			gid k = (gid)_hash_##Type(*(((BaseType*)ky)+i))&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && (!(is_##Type##_nil(ky[i]) && is_##Type##_nil(vals[slot])) && vals[slot] != ky[i])) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			oid_mtd[mtdcnt] = off+i; \
			if (slot) { \
				slt[mtdcnt] = (oid)(slot - 1); \
				mark[i] = true; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
			} \
			mtdcnt++; \
		} \
	} while (0)

#define BATaoprobe() \
	do { \
		BATiter bi = bat_iterator(k); \
		char **vals = ht->vals; \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		\
		if (any) { \
			gid k = (gid)ht->hsh((void*)nil)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && atomcmp(vals[slot], nil) != 0) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) \
				has_nil = bit_nil; \
		} \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			char *val = (bi).vh->base+VarHeapVal(bi.base,i,bi.width); \
			if (!(*semantics) && atomcmp(val, nil) == 0) { \
				oid_mtd[mtdcnt] = off+i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = any?empty:false; \
				mtdcnt++; \
				continue; \
			} \
			gid k = (gid)str_hsh(val)&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && atomcmp(vals[slot], val) != 0) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			oid_mtd[mtdcnt] = off+i; \
			if (slot) { \
				slt[mtdcnt] = (oid)(slot - 1); \
				mark[i] = true; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.oprobe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} else { \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
			} \
			mtdcnt++; \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHomprobe(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H, bool any)
{
	(void)ctx;
	BAT *o = NULL, *s = NULL, *m = NULL, *k = NULL, *t = NULL, *f = NULL;
	BUN keycnt, mtdcnt = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
	assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	t = BATdescriptor(*HSH_ht);
	if (!k || !t) {
		err = createException(SQL, "oahash.probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (*single) {
		/* frequency is required to check if a match is single */
		f = BATdescriptor(*frequency);
		if (!f) {
			err = createException(SQL, "oahash.probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		freq = Tloc(f, 0);
	}

	keycnt = BATcount(k);
	o = COLnew(0, TYPE_oid, keycnt, TRANSIENT);
	s = COLnew(0, TYPE_oid, keycnt, TRANSIENT);
	m = COLnew(k->hseqbase, TYPE_bit, keycnt, TRANSIENT);
	if (!o || !s || !m) {
		err = createException(SQL, "oahash.probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (keycnt) {
		if (t->tsink->error) {
			err = t->tsink->error;
			goto error;
		}

		hash_table *ht = (hash_table*)t->tsink;
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		oid *oid_mtd = Tloc(o, 0);
		oid *slt = Tloc(s, 0);
		bit *mark = Tloc(m, 0);
		bit has_nil = false, empty = ht->empty?false:bit_nil;

		BUN off = k->hseqbase;
		switch(tt) {
			case TYPE_void:
				BATvoprobe();
				break;
			case TYPE_bit:
				BAToprobe(bit);
				break;
			case TYPE_bte:
				BAToprobe(bte);
				break;
			case TYPE_sht:
				BAToprobe(sht);
				break;
			case TYPE_int:
				BAToprobe(int);
				break;
			case TYPE_date:
				BAToprobe(date);
				break;
			case TYPE_inet4:
				BATcoprobe(inet4);
				break;
			case TYPE_lng:
				BAToprobe(lng);
				break;
			case TYPE_oid:
				BAToprobe(oid);
				break;
			case TYPE_daytime:
				BAToprobe(daytime);
				break;
			case TYPE_timestamp:
				BAToprobe(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BAToprobe(hge);
				break;
#endif
			case TYPE_flt:
				BATfoprobe(flt, int);
				break;
			case TYPE_dbl:
				BATfoprobe(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaoprobe();
				} else {
					err = createException(MAL, "oahash.probe", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
					goto error;
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.probe", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}

	BBPunfix(k->batCacheid);
	BBPunfix(t->batCacheid);
	BBPreclaim(f);
	BATsetcount(o, mtdcnt);
	BATsetcount(s, mtdcnt);
	BATsetcount(m, keycnt); /* aligned with input key */
	BATnegateprops(o);
	BATnegateprops(s);
	BATnegateprops(m);
	o->tnonil = true;
	s->tnonil = false;
	o->tsorted = true;
	BATkey(o, true);
	*PRB_oid = o->batCacheid;
	*HSH_slotid = s->batCacheid;
	*PRB_mark = m->batCacheid;
	BBPkeepref(o);
	BBPkeepref(s);
	BBPkeepref(m);
	return MAL_SUCCEED;
error:
	BBPreclaim(o);
	BBPreclaim(s);
	BBPreclaim(m);
	BBPreclaim(k);
	BBPreclaim(t);
	BBPreclaim(f);
	return err;
}

static str
OAHASHoprobe_single(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, HSH_ht, frequency, single, semantics, H, false);
}

static str
OAHASHoprobe(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, HSH_ht, NULL, single, semantics, H, false);
}

static str
OAHASHmprobe_single(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, HSH_ht, frequency, single, semantics, H, true);
}

static str
OAHASHmprobe(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, HSH_ht, NULL, single, semantics, H, true);
}

#define BATvprobe_cmbd() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		oid *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			oid ky = canditer_idx(&ci, sltd[i]-off); \
			assert(ky != oid_nil); \
			if (!(*semantics) && ky == oid_nil) \
				continue; \
			\
			gid hsh = (gid)combine(gi[i], _hash_oid(ky), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || vals[slot] != ky)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
	} while (0)

#define _BATprobe_cmbd(Type, ne) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			Type val = ky[sltd[i]-off]; \
			if (!(*semantics) && is_##Type##_nil(val)) \
				continue; \
			\
			gid hsh = (gid)combine(gi[i], _hash_##Type(val), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || (is_##Type##_nil(vals[slot]) != is_##Type##_nil(val)) || (ne))) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
	} while (0)

#define BATprobe_cmbd(Type) \
	_BATprobe_cmbd(Type, vals[slot] != val)

#define BATcprobe_cmbd(Type) \
	_BATprobe_cmbd(Type, memcmp(vals+slot, &val, sizeof(Type))!=0)

#define BATfprobe_cmbd(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			Type val = ky[sltd[i]-off]; \
			if (!(*semantics) && is_##Type##_nil(val)) \
				continue; \
			\
			gid hsh = (gid)combine(gi[i], _hash_##Type(*(((BaseType*)ky)+sltd[i]-off)), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || \
						((*semantics) && is_##Type##_nil(val) && !is_##Type##_nil(vals[slot])) || \
						(!is_##Type##_nil(val) && vals[slot] != val))) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
	} while (0)

#define BATaprobe_cmbd() \
	do { \
		BATiter bi = bat_iterator(k); \
		char **vals = ht->vals; \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			char *val = (bi).vh->base+VarHeapVal(bi.base,sltd[i]-off,bi.width); \
			if (!(*semantics) && atomcmp(val, nil) == 0) \
				continue; \
			\
			gid hsh = (gid)combine(gi[i], str_hsh(val), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || atomcmp(vals[slot], val) != 0)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHprobe_cmbd_single(Client ctx, bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	(void)ctx;
	BAT *res_o = NULL, *res_s = NULL, *k = NULL, *s = NULL, *t = NULL, *p = NULL, *f = NULL;
	BUN mtdcnt, mtdcnt2 = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
	assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	s = BATdescriptor(*PRB_selected);
	p = BATdescriptor(*HSH_gid);
	t = BATdescriptor(*HSH_ht);
	if (!k || !s || !t || !p) {
		err = createException(SQL, "oahash.combined_probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (*single) {
		/* frequency is required to check if a match is single */
		f = BATdescriptor(*frequency);
		if (!f) {
			err = createException(SQL, "oahash.probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		freq = Tloc(f, 0);
	}

	mtdcnt = BATcount(s);
	res_o = COLnew(0, TYPE_oid, mtdcnt, TRANSIENT);
	res_s = COLnew(0, TYPE_oid, mtdcnt, TRANSIENT);
	if (!res_o || !res_s) {
		err = createException(SQL, "oahash.combined_probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (mtdcnt) {
		if (t->tsink->error) {
			err = t->tsink->error;
			goto error;
		}

		hash_table *ht = (hash_table*)t->tsink;
		unsigned int prime = hash_prime_nr[ht->bits-5];
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		oid *sltd = Tloc(s, 0);
		oid *oid_mtd = Tloc(res_o, 0);
		oid *slt = Tloc(res_s, 0);
		lng *gi = Tloc(p, 0);
		lng *pgids = ht->pgids;

		BUN off = k->hseqbase;
		switch(tt) {
			case TYPE_void:
				BATvprobe_cmbd();
				break;
			case TYPE_bit:
				BATprobe_cmbd(bit);
				break;
			case TYPE_bte:
				BATprobe_cmbd(bte);
				break;
			case TYPE_sht:
				BATprobe_cmbd(sht);
				break;
			case TYPE_int:
				BATprobe_cmbd(int);
				break;
			case TYPE_date:
				BATprobe_cmbd(date);
				break;
			case TYPE_inet4:
				BATcprobe_cmbd(inet4);
				break;
			case TYPE_lng:
				BATprobe_cmbd(lng);
				break;
			case TYPE_oid:
				if (BATtdense(k))
					BATvprobe_cmbd();
				else
					BATprobe_cmbd(oid);
				break;
			case TYPE_daytime:
				BATprobe_cmbd(daytime);
				break;
			case TYPE_timestamp:
				BATprobe_cmbd(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BATprobe_cmbd(hge);
				break;
#endif
			case TYPE_flt:
				BATfprobe_cmbd(flt, int);
				break;
			case TYPE_dbl:
				BATfprobe_cmbd(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaprobe_cmbd();
				} else {
					err = createException(MAL, "oahash.combined_probe", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
					goto error;
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.combined_probe", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}

	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);
	BBPunfix(t->batCacheid);
	BBPunfix(p->batCacheid);
	BBPreclaim(f);
	BATsetcount(res_o, mtdcnt2);
	BATsetcount(res_s, mtdcnt2);
	BATnegateprops(res_o);
	BATnegateprops(res_s);
	res_o->tnonil = true;
	res_s->tnonil = true;
	res_o->tsorted = true;
	BATkey(res_o, true);
	*PRB_oid = res_o->batCacheid;
	*HSH_slotid = res_s->batCacheid;
	BBPkeepref(res_o);
	BBPkeepref(res_s);
	return MAL_SUCCEED;
error:
	BBPreclaim(res_o);
	BBPreclaim(res_s);
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(t);
	BBPreclaim(p);
	BBPreclaim(f);
	return err;
}

static str
OAHASHprobe_cmbd(Client ctx, bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHprobe_cmbd_single(ctx, PRB_oid, HSH_slotid, PRB_key, PRB_selected, HSH_gid, HSH_ht, NULL, single, semantics, H);
}

#define BATvoprobe_cmbd() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		oid *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			oid ky = canditer_idx(&ci, sltd[i]-off); \
			assert(ky != oid_nil); \
			if (!mark[i] || (!(*semantics) && ky == oid_nil)) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				mark[i] = (any && mark[i])?bit_nil:false; \
				mtdcnt2++; \
				continue; \
			} \
			gid hsh = (gid)combine(gi[i], _hash_oid(ky), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || vals[slot] != ky)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				bit has_nil = false; \
				if (any) { \
					gid hsh = (gid)combine(gi[i], _hash_oid(oid_nil), prime)&ht->mask; \
					slot = ATOMIC_GET(ht->gids+hsh); \
					while (slot && (pgids[slot] != gi[i] || vals[slot] != oid_nil)) { \
						hsh++; \
						hsh &= ht->mask; \
						slot = ATOMIC_GET(ht->gids+hsh); \
					} \
					if (slot) \
						has_nil = bit_nil; \
				} \
				mark[i] = (any)?has_nil:false; \
				mtdcnt2++; \
			} \
		} \
	} while (0)

#define _BAToprobe_cmbd(Type, ne) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			Type val = ky[sltd[i]-off]; \
			if (!mark[i] || (!(*semantics) && is_##Type##_nil(val))) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				mark[i] = (any && mark[i])?bit_nil:false; \
				mtdcnt2++; \
				continue; \
			} \
			gid hsh = (gid)combine(gi[i], _hash_##Type(val), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || (is_##Type##_nil(vals[slot]) != is_##Type##_nil(val)) || (ne))) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				bit has_nil = false; \
				if (any) { \
					gid hsh = (gid)combine(gi[i], _hash_##Type(Type##_nil), prime)&ht->mask; \
					slot = ATOMIC_GET(ht->gids+hsh); \
					while (slot && (pgids[slot] != gi[i] || !is_##Type##_nil(vals[slot]))) { \
						hsh++; \
						hsh &= ht->mask; \
						slot = ATOMIC_GET(ht->gids+hsh); \
					} \
					if (slot) \
						has_nil = bit_nil; \
				} \
				mark[i] = (any)?has_nil:false; \
				mtdcnt2++; \
			} \
		} \
	} while (0)

#define BAToprobe_cmbd(Type) \
	_BAToprobe_cmbd(Type, vals[slot] != val)

#define BATcoprobe_cmbd(Type) \
	_BAToprobe_cmbd(Type, memcmp(vals+slot, &val, sizeof(Type))!=0);

#define BATfoprobe_cmbd(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *vals = ht->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			Type val = ky[sltd[i]-off]; \
			if (!mark[i] || (!(*semantics) && is_##Type##_nil(val))) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				mark[i] = (any && mark[i])?bit_nil:false; \
				mtdcnt2++; \
				continue; \
			} \
			gid hsh = (gid)combine(gi[i], _hash_##Type(*(((BaseType*)ky)+sltd[i]-off)), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || \
						((*semantics) && is_##Type##_nil(val) && !is_##Type##_nil(vals[slot])) || \
						(!is_##Type##_nil(val) && vals[slot] != val))) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				bit has_nil = false; \
				if (any) { \
					gid hsh = (gid)combine(gi[i], _hash_##Type((BaseType)Type##_nil), prime)&ht->mask; \
					slot = ATOMIC_GET(ht->gids+hsh); \
					while (slot && (pgids[slot] != gi[i] || !is_##Type##_nil(vals[slot]))) { \
						hsh++; \
						hsh &= ht->mask; \
						slot = ATOMIC_GET(ht->gids+hsh); \
					} \
					if (slot) \
						has_nil = bit_nil; \
				} \
				mark[i] = (any)?has_nil:false; \
				mtdcnt2++; \
			} \
		} \
	} while (0)

#define BATaoprobe_cmbd() \
	do { \
		BATiter bi = bat_iterator(k); \
		char **vals = ht->vals; \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			char *val = (bi).vh->base+VarHeapVal(bi.base,sltd[i]-off,bi.width); \
			if (!mark[i] || (!(*semantics) && atomcmp(val, nil) == 0)) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				mark[i] = (any && mark[i])?bit_nil:false; \
				mtdcnt2++; \
				continue; \
			} \
			gid hsh = (gid)combine(gi[i], str_hsh(val), prime)&ht->mask; \
			gid slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || atomcmp(vals[slot], val) != 0)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt2] = sltd[i]; \
				slt[mtdcnt2] = oid_nil; \
				bit has_nil = false; \
				if (any) { \
					gid hsh = (gid)combine(gi[i], ht->hsh((void*)nil), prime)&ht->mask; \
					slot = ATOMIC_GET(ht->gids+hsh); \
					while (slot && (pgids[slot] != gi[i] || atomcmp(vals[slot], nil) != 0)) { \
						hsh++; \
						hsh &= ht->mask; \
						slot = ATOMIC_GET(ht->gids+hsh); \
					} \
					if (slot) \
						has_nil = bit_nil; \
				} \
				mark[i] = (any)?has_nil:false; \
				mtdcnt2++; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHomprobe_cmbd(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H, bool any)
{
	(void)ctx;
	BAT *res_o = NULL, *res_s = NULL, *res_m = NULL, *k = NULL, *s = NULL, *t = NULL, *p = NULL, *f = NULL;
	BUN mtdcnt, mtdcnt2 = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
    assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	s = BATdescriptor(*PRB_selected);
	p = BATdescriptor(*HSH_gid);
	t = BATdescriptor(*HSH_ht);
	res_m = BATdescriptor(*PRB_mark);
	if (!k || !s || !t || !p || !res_m) {
		err = createException(SQL, "oahash.combined_probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (*single) {
		/* frequency is required to check if a match is single */
		f = BATdescriptor(*frequency);
		if (!f) {
			err = createException(SQL, "oahash.probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		freq = Tloc(f, 0);
	}

	mtdcnt = BATcount(s);
	res_o = COLnew(0, TYPE_oid, mtdcnt, TRANSIENT);
	res_s = COLnew(0, TYPE_oid, mtdcnt, TRANSIENT);
	if (!res_o || !res_s) {
		err = createException(SQL, "oahash.combined_probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (mtdcnt) {
		if (t->tsink->error) {
			err = t->tsink->error;
			goto error;
		}

		hash_table *ht = (hash_table*)t->tsink;
		unsigned int prime = hash_prime_nr[ht->bits-5];
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		oid *sltd = Tloc(s, 0);
		oid *oid_mtd = Tloc(res_o, 0);
		oid *slt = Tloc(res_s, 0);
		lng *gi = Tloc(p, 0);
		lng *pgids = ht->pgids;
		bit *mark = Tloc(res_m, 0);
		BUN off = k->hseqbase;
		switch(tt) {
			case TYPE_void:
				BATvoprobe_cmbd();
				break;
			case TYPE_bit:
				BAToprobe_cmbd(bit);
				break;
			case TYPE_bte:
				BAToprobe_cmbd(bte);
				break;
			case TYPE_sht:
				BAToprobe_cmbd(sht);
				break;
			case TYPE_int:
				BAToprobe_cmbd(int);
				break;
			case TYPE_date:
				BAToprobe_cmbd(date);
				break;
			case TYPE_inet4:
				BATcoprobe_cmbd(inet4);
				break;
			case TYPE_lng:
				BAToprobe_cmbd(lng);
				break;
			case TYPE_oid:
				if (BATtdense(k))
					BATvoprobe_cmbd();
				else
					BAToprobe_cmbd(oid);
				break;
			case TYPE_daytime:
				BAToprobe_cmbd(daytime);
				break;
			case TYPE_timestamp:
				BAToprobe_cmbd(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BAToprobe_cmbd(hge);
				break;
#endif
			case TYPE_flt:
				BATfoprobe_cmbd(flt, int);
				break;
			case TYPE_dbl:
				BATfoprobe_cmbd(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaoprobe_cmbd();
				} else {
					err = createException(MAL, "oahash.combined_probe", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
					goto error;
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.combined_probe", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}

	assert(BATcount(res_m) == BATcount(k));
	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);
	BBPunfix(t->batCacheid);
	BBPunfix(p->batCacheid);
	BBPreclaim(f);
	BATsetcount(res_o, mtdcnt2);
	BATsetcount(res_s, mtdcnt2);
	BATnegateprops(res_o);
	BATnegateprops(res_s);
	BATnegateprops(res_m);
	res_o->tnonil = true;
	//res_s->tnil = true;
	res_o->tsorted = true;
	BATkey(res_o, true);
	*PRB_oid = res_o->batCacheid;
	*HSH_slotid = res_s->batCacheid;
	BBPkeepref(res_o);
	BBPkeepref(res_s);
	BBPkeepref(res_m);
	return MAL_SUCCEED;
error:
	BBPreclaim(res_o);
	BBPreclaim(res_s);
	BBPreclaim(res_m);
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(t);
	BBPreclaim(p);
	BBPreclaim(f);
	return err;
}

static str
OAHASHoprobe_cmbd_single(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe_cmbd(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_selected, HSH_gid, HSH_ht, frequency, single, semantics, H, false);
}

static str
OAHASHoprobe_cmbd(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe_cmbd(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_selected, HSH_gid, HSH_ht, NULL, single, semantics, H, false);
}

static str
OAHASHmprobe_cmbd_single(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe_cmbd(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_selected, HSH_gid, HSH_ht, frequency, single, semantics, H, true);
}

static str
OAHASHmprobe_cmbd(Client ctx, bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return OAHASHomprobe_cmbd(ctx, PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_selected, HSH_gid, HSH_ht, NULL, single, semantics, H, true);
}

static str
OAHASHexpand(Client ctx, bat *expanded, const bat *selected, const bat *slotid, const bat *frequency, const bit *left_outer)
{
	(void)ctx;
	BAT *e = NULL, *s = NULL, *l = NULL, *f = NULL;
	BUN selcnt, ttlcnt = 0, xpdcnt = 0;
	lng *freq = NULL;
	str err = NULL;

	s = BATdescriptor(*selected);
	l = BATdescriptor(*slotid);
	if (!s || !l) {
		err = createException(SQL, "oahash.expand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (frequency && !is_bat_nil(*frequency)) {
		f = BATdescriptor(*frequency);
		if (!f) {
			err = createException(SQL, "oahash.expand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		freq = Tloc(f, 0);
	}

	assert(BATcount(s) == BATcount(l));

	gid *sid = Tloc(l, 0);
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	selcnt = BATcount(s);
	if (selcnt) {
		if (freq) {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
				if (sid[i] != lng_nil && freq[sid[i]])
					xpdcnt += freq[sid[i]];
				else
					xpdcnt++;
			}
			TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand", RUNTIME_QRY_TIMEOUT));
		} else {
			xpdcnt = selcnt;
		}
		if (err)
			goto error;
	}
	ttlcnt = xpdcnt;

	e = COLnew(0, TYPE_oid, ttlcnt, TRANSIENT);
	if (!e) {
		err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	BUN idx = 0;
	oid *sel = Tloc(s, 0);
	oid *res = Tloc(e, 0);

	if (*left_outer && freq) {
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
			oid s = sid[i];
			if (s != oid_nil) {
				gid frq = (gid)freq[s];
				frq = frq?frq:1;
				TIMEOUT_LOOP_IDX_DECL(f, frq, qry_ctx) {
					res[idx++] = sel[i];
				}
			} else {
				res[idx++] = sel[i];
			}
		}
	} else if (freq) {
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
			gid frq = (gid)freq[sid[i]];
			frq = frq?frq:1;
			TIMEOUT_LOOP_IDX_DECL(j, frq, qry_ctx) {
				res[idx++] = sel[i];
			}
		}
	} else {
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
			res[idx++] = sel[i];
		}
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand", RUNTIME_QRY_TIMEOUT));
	if (err)
		goto error;

	assert(idx == ttlcnt);

	BATsetcount(e, ttlcnt);
	BATnegateprops(e);
	e->tsorted = s->tsorted;
	e->trevsorted = s->trevsorted;
	*expanded = e->batCacheid;
	BBPkeepref(e);
	BBPunfix(s->batCacheid);
	BBPunfix(l->batCacheid);
	BBPreclaim(f);
	return MAL_SUCCEED;
error:
	BBPreclaim(e);
	BBPreclaim(s);
	BBPreclaim(l);
	BBPreclaim(f);
	return err;
}

static str
OAHASHexpand_cart(Client ctx, bat *expanded, const bat *col, const bat *rowrepeat, const bit *left_outer)
{
	(void)ctx;
	BAT *e = NULL, *k = NULL, *d;
	BUN keycnt, ttlcnt, repcnt;
	str err = NULL;

	k = BATdescriptor(*col);
	d = BATdescriptor(*rowrepeat);
	if (!k || !d) {
		err = createException(SQL, "oahash.expand_cartesian", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	keycnt = BATcount(k);
	repcnt = BATcount(d);
	if (*left_outer && repcnt == 0)
		repcnt = 1;
	ttlcnt = keycnt * repcnt;

	e = COLnew(0, TYPE_oid, ttlcnt, TRANSIENT);
	if (!e) {
		err = createException(SQL, "oahash.expand_cartesian", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	BUN idx = 0;

	oid *res = Tloc(e, 0);
	BUN off = k->hseqbase;
	TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) {
		TIMEOUT_LOOP_IDX_DECL(j, repcnt, qry_ctx) {
			res[idx++] = off+i;
		}
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand_cart", RUNTIME_QRY_TIMEOUT));
	if (err)
		goto error;

	assert(idx == ttlcnt);

	BATsetcount(e, ttlcnt);
	BATnegateprops(e);
	*expanded = e->batCacheid;
	e->tsorted = TRUE;
	e->trevsorted = FALSE;
	BBPkeepref(e);
	BBPunfix(k->batCacheid);
	BBPunfix(d->batCacheid);
	return MAL_SUCCEED;
error:
	BBPreclaim(e);
	BBPreclaim(k);
	BBPreclaim(d);
	return err;
}

static str
OAHASHexplode(Client ctx, bat *fetched, const bat *slotid, const bat *frequency, const bat *ht_sink, const bit *left_outer)
{
	(void)ctx;
	BAT *f = NULL, *l = NULL, *h = NULL, *r = NULL;
	BUN selcnt, fchcnt = 0;
	str err = NULL;

	l = BATdescriptor(*slotid);
	f = BATdescriptor(*frequency);
	h = BATdescriptor(*ht_sink);
	if (!l || !f || !h) {
		err = createException(SQL, "oahash.explode", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	oid *sid = Tloc(l, 0);
	lng *freq = Tloc(f, 0);
	hash_table *ht = (hash_table*)h->tsink;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	selcnt = BATcount(l);
	if (selcnt) {
		if (*left_outer) {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
				if (sid[i] != oid_nil)
					fchcnt += freq[sid[i]];
				else
					fchcnt++;
			}
		} else {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx)
				fchcnt += freq[sid[i]];
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.explode", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}

	r = COLnew(0, TYPE_oid, fchcnt, TRANSIENT);
	if (!r) {
		err = createException(SQL, "oahash.explode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (fchcnt) {
		BUN idx = 0;

		int prime = hash_prime_nr[ht->bits-5];
		oid *res = Tloc(r, 0);
		oid *vals = ht->vals;
		oid *pgids = (oid*)ht->pgids;
		if (*left_outer) {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
				oid s = sid[i];
				if (s != oid_nil) {
					gid frq = (gid)freq[s];
					TIMEOUT_LOOP_IDX_DECL(j, frq, qry_ctx) {
						oid k = (gid)combine(s, _hash_oid(j), prime)&ht->mask;
						oid g = ht->gids[k];
						while (g && (pgids[g] != s || vals[g] != j)) {
							k++;
							k &= ht->mask;
							g = ht->gids[k];
						}
						assert(g>0);
						res[idx++] = g-1;
					}
				} else {
					res[idx++] = oid_nil;
				}
			}
		} else {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
				oid s = sid[i];
				gid frq = (gid)freq[s];
				TIMEOUT_LOOP_IDX_DECL(j, frq, qry_ctx) {
					oid k = (gid)combine(s, _hash_oid(j), prime)&ht->mask;
					oid g = ht->gids[k];
					while (g && (pgids[g] != s || vals[g] != j)) {
						k++;
						k &= ht->mask;
						g = ht->gids[k];
					}
					assert(g>0);
					res[idx++] = g-1;
				}
			}
		}

		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.explode", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;

		assert(idx == fchcnt);
	}

	BBPunfix(f->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(h->batCacheid);

	r->tseqbase = 0;
	BATsetcount(r, fchcnt);
	BATnegateprops(r);
	*fetched = r->batCacheid;
	BBPkeepref(r);
	return MAL_SUCCEED;
error:
	BBPreclaim(f);
	BBPreclaim(l);
	BBPreclaim(h);
	BBPreclaim(r);
	return err;
}

static str
OAHASHexplode_cart(Client ctx, bat *fetched, const bat *col, const bat *setrepeat, const bit *left_outer)
{
	(void)ctx;
	BAT *f = NULL, *k = NULL, *d = NULL;
	BUN ttlcnt, keycnt, repcnt;
	bool append_nulls = false;
	str err = NULL;

	k = BATdescriptor(*col);
	d = BATdescriptor(*setrepeat);
	if (!k || !d) {
		err = createException(SQL, "oahash.fetch_payload_cartesian", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	keycnt = BATcount(k);
	if (*left_outer && keycnt == 0) {
		append_nulls = true;
		keycnt = 1;
	}
	repcnt = BATcount(d);
	ttlcnt = keycnt * repcnt;
	f = COLnew(0, TYPE_oid, ttlcnt, TRANSIENT);
	if (!f) {
		err = createException(SQL, "oahash.fetch_payload_cartesian", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (ttlcnt) {
		BUN idx = 0;

		if (append_nulls) {
			oid *res = Tloc(f, 0);
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) {
				res[idx++] = oid_nil;
			}
		} else {
			oid *res = Tloc(f, 0);
			BUN off = k->hseqbase;
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) {
				TIMEOUT_LOOP_IDX_DECL(j, keycnt, qry_ctx) {
					res[idx++] = off+j;
				}
			}
		}

		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.fetch_payload_cartesian", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;

		assert(idx == ttlcnt);
	}

	BBPunfix(k->batCacheid);
	BBPunfix(d->batCacheid);

	BATsetcount(f, ttlcnt);
	BATnegateprops(f);
	*fetched = f->batCacheid;
	BBPkeepref(f);
	return MAL_SUCCEED;
error:
	BBPreclaim(f);
	BBPreclaim(k);
	BBPreclaim(d);
	return err;
}

static str
OAHASHexplode_unmatched(Client ctx, bat *res, const bat *ht_sink, const bat *unmatched, const bat *frequency)
{
	(void)ctx;
	(void)ht_sink;

	BAT *r = NULL, *u = NULL, *f = NULL;
	str err = NULL;

	u = BATdescriptor(*unmatched);
	f = BATdescriptor(*frequency);
	if (!u || !f) {
		err = createException(SQL, "oahash.explode_unmatched", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	lng *freq = Tloc(f, 0);
	oid *umrk = Tloc(u, 0);

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	BUN cnt = BATcount(u), ttlcnt = 0;
	// FIXME: replace this hacky solution with proper BATiter!!!
	if (umrk)
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
					ttlcnt += freq[umrk[i]];
			}
	else /* !umrk: u has dense tail */
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
					ttlcnt += freq[u->hseqbase+i];
			}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.explode_unmatched", RUNTIME_QRY_TIMEOUT));
	if (err)
		goto error;

	r = COLnew(0, TYPE_oid, ttlcnt, TRANSIENT);
	if (!r) {
		err = createException(SQL, "oahash.explode_unmatched", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}
	BATsetcount(r, ttlcnt);
	BATnegateprops(r);
	r->tsorted = r->trevsorted = true;
	*res = r->batCacheid;
	BBPkeepref(r);
	BBPunfix(u->batCacheid);
	BBPunfix(f->batCacheid);
	return MAL_SUCCEED;
error:
	BBPreclaim(r);
	BBPreclaim(f);
	BBPreclaim(u);
	return err;
}

static str
OAHASHno_slices(Client ctx, int *no_slices, bat *ht_sink)
{
	(void)ctx;
	/* return nr of slices */
	assert(*ht_sink && !is_bat_nil(*ht_sink));
	BAT *b = BATdescriptor(*ht_sink);
	if (!b)
		return createException(SQL, "oahash.no_slices",	SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	hash_table *h = (hash_table*)b->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);

	if (h->size < SLICE_SIZE )
		*no_slices = 1;
	else
		*no_slices = (int)((h->size+SLICE_SIZE-1)/SLICE_SIZE);
	FORCEMITODEBUG
	if (*no_slices < GDKnr_threads)
		*no_slices = GDKnr_threads;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
OAHASHnth_slice(Client ctx, bat *slice, bat *ht_sink, int *slice_nr)
{
	(void)ctx;
	/* return the nth slice */
	assert(*ht_sink && !is_bat_nil(*ht_sink));
	BAT *b = BATdescriptor(*ht_sink);
	if (!b)
		return createException(SQL, "oahash.nth_slice",	SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	hash_table *h = (hash_table*)b->tsink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);
	BUN s = *slice_nr * SLICE_SIZE, e = s + SLICE_SIZE;
	BAT *r = NULL;

	if (h->size < s) {
		r = COLnew(b->hseqbase, TYPE_oid, 0, TRANSIENT);
	} else {
		r = COLnew(b->hseqbase, TYPE_oid, SLICE_SIZE, TRANSIENT);
	}
	if (e > h->size)
		e = h->size;
	if (!r) {
		BBPunfix(b->batCacheid);
		return createException(SQL, "slicer.nth_slice",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	// FIXME: replace the hacky Tloc with proper BUNappend or something
	oid *o = Tloc(r, 0);
	BUN j = 0;
	for (BUN i = s; i<e; i++) {
		if (h->gids[i])
			o[j++] = h->gids[i]-1;
	}
	BATsetcount(r, j);
	BATnegateprops(r);

	*ht_sink = b->batCacheid;
	*slice = r->batCacheid;
	BBPunfix(b->batCacheid);
	BBPkeepref(r);
	return MAL_SUCCEED;
}

#define hashloop(T) { \
	T *v = Tloc(i, 0); \
	for (BUN j = 0; j<cnt; j++) \
		h[j] = _hash_##T((T)v[j]); \
} break;

#define hashloopf(T, BT) { \
	T *v = Tloc(i, 0); \
	for (BUN j = 0; j<cnt; j++) \
		h[j] = _hash_##T(*(BT*)(v+j)); \
} break;

static str
OAHASHhash(Client cntxt, MalBlkPtr m, MalStkPtr stk, InstrPtr p)
{
	(void)cntxt;
	/* value case skipped for now */
	int tt = getArgType(m, p, 1);
	assert(isaBatType(tt));

	bat *rb = getArgReference_bat(stk, p, 0);
	bat ib = *getArgReference_bat(stk, p, 1);

	BAT *r, *i = BATdescriptor(ib);

	if (!i)
		return createException(MAL, "oahash.hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	BUN cnt = BATcount(i);
	r = COLnew(i->hseqbase, TYPE_lng, cnt, TRANSIENT);
	if (!r) {
		BBPreclaim(i);
		return createException(MAL, "oahash.hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	lng *h = Tloc(r, 0);
	tt = getBatType(tt);
	switch(tt) {
	case TYPE_bit:
	case TYPE_bte:
		hashloop(bte);
	case TYPE_sht:
		hashloop(sht);
	case TYPE_int:
	case TYPE_date:
	case TYPE_inet4:
		hashloop(int);
	case TYPE_oid:
	case TYPE_lng:
	case TYPE_daytime:
	case TYPE_timestamp:
		hashloop(lng);
#ifdef HAVE_HGE
	case TYPE_hge:
	case TYPE_uuid:
		hashloop(hge);
#endif
	case TYPE_flt:
		hashloopf(flt, int);
	case TYPE_dbl:
		hashloopf(dbl, lng);
	default:
		printf("todo\n");
	}
	BBPreclaim(i);
	BATsetcount(r, cnt);
	BATnegateprops(r);
	*rb = r->batCacheid;
	BBPkeepref(r);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func oa_hash_init_funcs[] = {
 pattern("oahash", "new", OAHASHnew, false, "", args(1,3, batargany("ht_sink",1),argany("tt",1),arg("size",int))),
 pattern("oahash", "new", OAHASHnew, false, "", args(1,4, batargany("ht_sink",1),argany("tt",1),arg("size",int),batargany("p",2))),
 pattern("oahash", "new", OAHASHnew, false, "", args(1,3, batargany("ht_sink",1),argany("tt",1),arg("size",lng))),
 pattern("oahash", "new", OAHASHnew, false, "", args(1,4, batargany("ht_sink",1),argany("tt",1),arg("size",lng),batargany("p",2))),

 command("oahash", "hashmark_init", OAHASHhashmark_init, false, "", args(1,3, batarg("hashmark",bit),batargany("ht_sink",1),batargany("payload",2))),
 pattern("hash", "ext", UHASHext, false, "", args(1,2, batarg("ext",oid),batargany("in",1))),

 command("oahash", "build_table", OAHASHbuild_tbl, false, "Add the `key`-s to the hash table. Returns the `slot_id` per `key` and the updated `ht_sink`", args(2,4, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),arg("pipeline",ptr))),

 command("oahash", "build_combined_table", OAHASHbuild_tbl_cmbd, false, "Add the `key`-s with a `parent_slotid` to the hash table. Returns the `slot_id` per `key` and the updated `ht_sink`", args(2,5, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),batarg("parent_slotid",oid),arg("pipeline",ptr))),

 pattern("oahash", "frequency", OAHASHadd_freq, false, "Add `slot_id` to the shared `frequencies` BAT. Returns the updated `frequencies`", args(1,3, batarg("frequencies",lng),batarg("slot_id",oid),arg("pipeline",ptr))),
 pattern("oahash", "frequency", OAHASHadd_freq, false, "Add `slot_id` to the shared `frequencies` BAT. Returns the occurrence index for each `slot_id` (i.e. it is the n-th time the `slot_id` is seen so far) and the updated `frequencies`", args(2,4, batarg("occrrence_idx",oid),batarg("frequencies",lng),batarg("slot_id",oid),arg("pipeline",ptr))),

 command("oahash", "probe", OAHASHprobe_single, false, "Probe the `key`-s in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,8, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batargany("HSH_ht",1),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "probe", OAHASHprobe, false, "Probe the `key` in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,7, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batargany("HSH_ht",1),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "nprobe", OAHASHnprobe_single, false, "Probe the `key`-s in the hash table. For a not-matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,8, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batargany("HSH_ht",1),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "nprobe", OAHASHnprobe, false, "Probe the `key` in the hash table. For a not-matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,7, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batargany("HSH_ht",1),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "oprobe", OAHASHoprobe_single, false, "Probe the `key`-s in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,9, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batargany("HSH_ht",1),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "oprobe", OAHASHoprobe, false, "Probe the `key`-s in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,8, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batargany("HSH_ht",1),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "mprobe", OAHASHmprobe_single, false, "Probe the `key`-s in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,9, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batargany("HSH_ht",1),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "mprobe", OAHASHmprobe, false, "Probe the `key`-s in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,8, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batargany("HSH_ht",1),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "combined_probe", OAHASHprobe_cmbd_single, false, "Probe the selected `key`-s in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(2,10, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",1),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "combined_probe", OAHASHprobe_cmbd, false, "Probe the selected `key`-s in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(2,9, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",1),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "combined_oprobe", OAHASHoprobe_cmbd_single, false, "Probe the selected `key`-s in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,11, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",1),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "combined_oprobe", OAHASHoprobe_cmbd, false, "Probe the selected `key`-s in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,10, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",1),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "combined_mprobe", OAHASHmprobe_cmbd_single, false, "Probe the selected `key`-s pairs in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,11, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",1),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "combined_mprobe", OAHASHmprobe_cmbd, false, "Probe the selected `key`-s in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,10, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",1),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "expand", OAHASHexpand, false, "Expand the selected keys according to their frequencies in the hash table. If 'left_outer' is true, append the not 'selected' keys", args(1,5,batarg("expanded",oid),batarg("selected",oid),batarg("slotid",oid),batarg("frequency",lng),arg("left_outer",bit))),

 command("oahash", "expand_cartesian", OAHASHexpand_cart, false, "Duplicate each value in 'col' the number of times as the count of 'rowrepeat'. For a left/right-outer join, if 'rowrepeat' is empty, output the values in 'col' once.", args(1,4, batarg("expanded",oid),batargany("col",1),batargany("rowrepeat",2),arg("left_outer",bit))),

 command("oahash", "explode", OAHASHexplode, false, "Explode the result vector 'frequency' times and return payload heap slot ids. If 'left_outer' is true, fill the not 'selected' slot with oid_nil", args(1,5, batarg("fetched",oid),batarg("slotid",oid),batarg("frequency",lng),batargany("hash_sink",2),arg("left_outer",bit))),

 command("oahash", "explode_cartesian", OAHASHexplode_cart, false, "Duplicate the whole 'col' the number of times as the count of 'setrepeat'.  For a left/right-ourter join, if 'col' is empty, output NULLs.", args(1,4, batarg("fetched",oid),batargany("col",1),batarg("setrepeat",2),arg("left_outer",bit))),

 command("oahash", "explode_unmatched", OAHASHexplode_unmatched, false, "Expand the count of 'unmatched' with 'frequency'.  Returns the count in a VOID BAT.", args(1,4, batarg("",oid),batargany("ht_sink",1),batarg("unmatched",oid),batarg("frequency",lng))),

 command("oahash", "no_slices", OAHASHno_slices, false, "Get the number of slices for this hashtable.", args(1,2, arg("slices",int),batargany("ht_sink",1))),
 command("oahash", "nth_slice", OAHASHnth_slice, false, "Get the nth slice of this hashtable.", args(1,3, batarg("slice",oid),batargany("ht_sink",1),arg("slice_nr",int))),

 pattern("oahash", "hash", OAHASHhash, false, "Compute hash.", args(1,2, arg("hash", lng), argany("in",1))),
 pattern("batoahash", "hash", OAHASHhash, false, "Compute hash.", args(1,2, batarg("hash", lng), batargany("in",1))),

 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_oa_hash_mal)
{ mal_module("oa_hash", NULL, oa_hash_init_funcs); }
