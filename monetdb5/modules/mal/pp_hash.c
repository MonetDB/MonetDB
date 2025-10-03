/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

/*
lng
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
*/

static unsigned int
log_base2(unsigned int n)
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
	if(h->vals) GDKfree(h->vals);
	if(h->gids) GDKfree((void *)h->gids);
	if(h->pgids) GDKfree(h->pgids);
	return NULL;
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
	if (h->size > 64*1024*1024)
		h->size -= 1024;
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
ht_create(int type, int size, hash_table *p)
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
				gid k = (gid)_hash_##Type(vals[og])&ht->mask; \
				gid g = ngids[k];					\
				while (g) {							\
					k++;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define REHASH_f(Type) \
		for(size_t i = 0; i < oldsize; i++) {		\
			Type *vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid k = (gid)_hash_##Type(vals[og])&ht->mask; \
				gid g = ngids[k];					\
				while (g) {							\
					k++;							\
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
				gid k = (gid)ht->hsh(vals[og])&ht->mask; \
				gid g = ngids[k];			\
				while (g) {							\
					k++;							\
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
				gid k = (gid)combine(pgids[og], _hash_##Type(vals[og]), prime)&ht->mask; \
				gid g = ngids[k];			\
				while (g) {							\
					k++;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define CREHASH_f(Type) \
		for(size_t i = 0; i < oldsize; i++) {		\
			Type *vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid k = (gid)combine(pgids[og], _hash_##Type(vals[og]), prime)&ht->mask; \
				gid g = ngids[k];			\
				while (g) {							\
					k++;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

#define CREHASH_a(TYPE) \
		for(size_t i = 0; i < oldsize; i++) {		\
			char **vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid k = (gid)combine(pgids[og], ht->hsh(vals[og]), prime)&ht->mask; \
				gid g = ngids[k];			\
				while (g) {							\
					k++;							\
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
		//dbl ratio = (ht->processed / ht->last); /* hit ratio */
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
				REHASH_f(flt);
				break;
			case TYPE_dbl:
				REHASH_f(dbl);
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
				CREHASH(int);
				break;
			case TYPE_date:
				CREHASH(date);
				break;
			case TYPE_lng:
				CREHASH(lng);
				break;
			case TYPE_oid:
		//		CREHASH(oid);
		for(size_t i = 0; i < oldsize; i++) {		\
			oid *vals = ht->vals;					\
			gid og = ogids[i];						\
			if (og) {								\
				gid k = (gid)combine(pgids[og], _hash_oid(vals[og]), prime)&ht->mask; \
				gid g = ngids[k];			\
				while (g) {							\
					k++;							\
					k &= ht->mask;					\
					g = ngids[k];					\
				}									\
				assert(!g);							\
				ngids[k] = og;						\
			}										\
		}											\

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
				CREHASH_f(flt);
				break;
			case TYPE_dbl:
				CREHASH_f(dbl);
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
	if(ht->vals) GDKfree(ht->vals);
	if(ht->gids) GDKfree((void *)ht->gids);
	if(ht->pgids) GDKfree(ht->pgids);
	return -1;
}

static str
OAHASHnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *res = getArgReference_bat(s, p, 0);
	int tt = getArgType(m, p, 1);
	int size = *getArgReference_int(s, p, 2);
	hash_table *parent = NULL;
	BAT *pht = NULL;

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
	b->tsink = (Sink*)ht_create(tt, size*1.2*2.1, parent);
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

#if 0
#define aprep_heap(SK, ERR, FName) \
	do { \
		pipeline_lock(p); \
		if (!SK->allocators) { \
			SK->allocators = (mallocator**)GDKzalloc(p->p->nr_workers*sizeof(mallocator*)); \
			if (!SK->allocators) { \
				ERR = createException(MAL, FName, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			} else { \
				SK->nr_allocators = p->p->nr_workers; \
			} \
		} \
		pipeline_unlock(p); \
		assert(p->wid < p->p->nr_workers); \
		if (SK->allocators && !SK->allocators[p->wid]) { \
			SK->allocators[p->wid] = ma_create(); \
			if (!SK->allocators[p->wid]) { \
				ERR = createException(MAL, FName, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			} \
		} \
	} while(0)
#endif

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
				SK->allocators = (mallocator**)GDKzalloc(p->p->nr_workers*sizeof(mallocator*)); \
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
				SK->allocators[p->wid] = ma_create(); \
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
		int slots = 0; \
		gid slot = 0; \
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
		oid *vals = h->vals; \
		\
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		mallocator *ma = h->allocators[P->wid]; \
		if (ATOMstorage(tt) == TYPE_str) { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
				char *bpi = (char *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
							slots = private?1:HT_PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
				void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
							slots = private?1:HT_PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
BAT_OAHASHbuild_tbl(bat *slot_id, bat *ht_sink, const bat *key, const ptr *H)
{
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
	g = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	if (g == NULL) {
		err = createException(MAL, "oahash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (cnt) {
		ATOMIC_BASE_TYPE expected = 0;
		int tt = b->ttype;
		gid *gp = Tloc(g, 0);

		h->empty = false;
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
	/* props */
	gid last = ATOMIC_GET(&h->last);
	/* pass max id */
	g->tmaxval = last;
	g->tkey = FALSE;
	/* ht_sink is an in&out var, so it should already have the correct bat(Cache)id */
	//*ht_sink = u->batCacheid;
	*slot_id = g->batCacheid;
	BBPkeepref(u);
	BBPkeepref(g);
	return MAL_SUCCEED;
error:
	BBPreclaim(b);
	BBPreclaim(u);
	BBPreclaim(g);
	return err;
}

#define derive(Type) \
	do { \
		int slots = 0; \
		gid slot = 0; \
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
		oid *vals = h->vals; \
		\
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
						slots = private?1:HT_PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		char **vals = h->vals; \
		mallocator *ma = h->allocators[P->wid]; \
		if (ATOMstorage(tt) == TYPE_str) { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
				char *bpi = (char *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
							slots = private?1:HT_PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
				void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
							slots = private?1:HT_PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:HT_PRE_CLAIM); \
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
OAHASHbuild_tbl_cmbd(bat *slot_id, bat *ht_sink, const bat *key, const bat *parent_slotid, const ptr *H)
{
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
	g = COLnew(0, TYPE_oid, cnt, TRANSIENT);
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
	/* props */
	gid last = ATOMIC_GET(&h->last);
	/* pass max id */
	g->tmaxval = last;
	g->tkey = FALSE;
	/* ht_sink is an in&out var, so it should already have the correct bat(Cache)id */
	//*ht_sink = u->batCacheid;
	*slot_id = g->batCacheid;
	BBPkeepref(u);
	BBPkeepref(g);

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

	bat *payload_pos = pci->retc == 2? getArgReference_bat(stk, pci, 0) : NULL;
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

	if (payload_pos) {
		res = COLnew(0, TYPE_oid, cnt, TRANSIENT);
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
			BATsetcount(frq, slt->tmaxval);
			// TODO: would be nice to have a bat.new variant that initiates props
			BATnegateprops(frq);
			frq->tnonil = true;
		}

		gid *sltid = Tloc(slt, 0);
		lng *freqs = Tloc(frq, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		if (payload_pos) {
			gid *ppos = Tloc(res, 0);
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
				ppos[i] = freqs[sltid[i]];
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
	BBPkeepref(frq);
	if (payload_pos) {
		BATsetcount(res, cnt);
		BATnegateprops(res);
		*payload_pos = res->batCacheid;
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

#define BATvhash() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		cnt = ci.ncand; \
		\
		gid *hs = Tloc(h, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			oid ky = canditer_next(&ci); \
			assert(ky != oid_nil); \
			\
			hs[i] = (gid)_hash_oid(ky); \
		} \
	} while (0)

#define BAThash(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)_hash_##Type(ky[i]); \
		} \
	} while (0)

#define BATfhash(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)_hash_##Type(*(((BaseType*)ky)+i)); \
		} \
	} while (0)

#define BATahash() \
	do { \
		BATiter bi = bat_iterator(k); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			/* TODO might need to pass the HT for this str_hsh func */ \
			hs[i] = (gid)str_hsh(bpi); \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
BAT_OAHASHhash(bat *hsh, const bat *key, const ptr *H)
{
	BAT *h = NULL, *k = NULL;
	BUN cnt;
	str err = NULL;

	(void) H;

	k = BATdescriptor(*key);
	if (!k)
		return createException(SQL, "oahash.hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	cnt = BATcount(k);
	h = COLnew(k->hseqbase, TYPE_lng, cnt, TRANSIENT);
	if (!h) {
		err = createException(SQL, "oahash.hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (cnt) {
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		switch(tt) {
			case TYPE_void:
				BATvhash();
				break;
			case TYPE_bit:
				BAThash(bit);
				break;
			case TYPE_bte:
				BAThash(bte);
				break;
			case TYPE_sht:
				BAThash(sht);
				break;
			case TYPE_int:
				BAThash(int);
				break;
			case TYPE_date:
				BAThash(date);
				break;
			case TYPE_lng:
				BAThash(lng);
				break;
			case TYPE_oid:
				if (BATtdense(k))
					BATvhash();
				else
					BAThash(oid);
				break;
			case TYPE_daytime:
				BAThash(daytime);
				break;
			case TYPE_timestamp:
				BAThash(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BAThash(hge);
				break;
#endif
			case TYPE_flt:
				BATfhash(flt, int);
				break;
			case TYPE_dbl:
				BATfhash(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATahash();
				} else {
					err = createException(MAL, "oahash.hash", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
					goto error;
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.hash", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}
	BBPunfix(*key);
	BATsetcount(h, cnt);
	BATnegateprops(h);
	*hsh = h->batCacheid;
	BBPkeepref(h);
	return MAL_SUCCEED;
error:
	BBPreclaim(k);
	BBPreclaim(h);
	return err;
}

#define BATvhash_cmbd() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		cnt = ci.ncand; \
		\
		gid *hs = Tloc(h, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			oid ky = canditer_next(&ci); \
			assert(ky != oid_nil); \
			hs[i] = (gid)combine(ps[i], _hash_oid(ky), prime); \
		} \
	} while (0)

#define BAThash_cmbd(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)combine(ps[i], _hash_##Type(ky[sl[i]]), prime); \
		} \
	} while (0)

#define BATfhash_cmbd(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)combine(ps[i], _hash_##Type(*(((BaseType*)ky)+sl[i])), prime); \
		} \
	} while (0)

#define BATahash_cmbd() \
	do { \
		BATiter bi = bat_iterator(k); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,sl[i])); \
			hs[i] = (gid)combine(ps[i], str_hsh(bpi), prime); \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
BAT_OAHASHhash_cmbd(bat *hsh, const bat *key, const bat *selected, const bat *parent_slotid, const bat *ht_sink)
{
	BAT *h = NULL, *k = NULL, *s = NULL, *p = NULL, *t = NULL;
	BUN cnt;
	str err = NULL;

	k = BATdescriptor(*key);
	s = BATdescriptor(*selected);
	p = BATdescriptor(*parent_slotid);
	t = BATdescriptor(*ht_sink);
	if (!k || !s || !p || !t) {
		err = createException(SQL, "oahash.combined_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	hash_table *ht = (hash_table*)t->tsink;
	/* only compute hashes for the 'selected' ones */
	cnt = BATcount(s);
	h = COLnew(k->hseqbase, TYPE_lng, cnt, TRANSIENT);
	if (!h) {
		err = createException(SQL, "oahash.combined_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (cnt) {
		int tt = k->ttype;
		oid  *sl = Tloc(s, 0);
		gid  *ps = Tloc(p, 0);
		unsigned int prime = hash_prime_nr[ht->bits-5];

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		switch(tt) {
			case TYPE_void:
				BATvhash_cmbd();
				break;
			case TYPE_bit:
				BAThash_cmbd(bit);
				break;
			case TYPE_bte:
				BAThash_cmbd(bte);
				break;
			case TYPE_sht:
				BAThash_cmbd(sht);
				break;
			case TYPE_int:
				BAThash_cmbd(int);
				break;
			case TYPE_date:
				BAThash_cmbd(date);
				break;
			case TYPE_lng:
				BAThash_cmbd(lng);
				break;
			case TYPE_oid:
				if (BATtdense(k))
					BATvhash_cmbd();
				else
					BAThash_cmbd(oid);
				break;
			case TYPE_daytime:
				BAThash_cmbd(daytime);
				break;
			case TYPE_timestamp:
				BAThash_cmbd(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BAThash_cmbd(hge);
				break;
#endif
			case TYPE_flt:
				BATfhash_cmbd(flt, int);
				break;
			case TYPE_dbl:
				BATfhash_cmbd(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATahash_cmbd();
				} else {
					err = createException(MAL, "oahash.combined_hash", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
					goto error;
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.combined_hash", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}
	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);
	BBPunfix(p->batCacheid);
	BBPunfix(t->batCacheid);
	BATsetcount(h, cnt);
	BATnegateprops(h);
	*hsh = h->batCacheid;
	BBPkeepref(h);
	return MAL_SUCCEED;
error:
	BBPreclaim(h);
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(t);
	BBPreclaim(p);
	return err;
}

#define BATvprobe() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		keycnt = ci.ncand; \
		\
		gid *hs = Tloc(h, 0); \
		oid *vals = ht->vals; \
		oid *oid_mtd = Tloc(o, 0); \
		oid *slt = Tloc(s, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			oid ky = canditer_next(&ci); \
			assert(ky != oid_nil); \
			if (!(*semantics) && ky == oid_nil) { \
				if (!match && empty) { \
					oid_mtd[mtdcnt] = i; \
					slt[mtdcnt] = oid_nil; \
					mtdcnt++; \
				}\
				continue; \
			} \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && vals[slot] != ky) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mtdcnt++; \
				if (match && *single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
	} while (0)

#define BATprobe(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		Type *vals = ht->vals; \
		oid *oid_mtd = Tloc(o, 0); \
		oid *slt = Tloc(s, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			if (!(*semantics) && is_##Type##_nil(ky[i])) { \
				if (!match && empty) { \
					oid_mtd[mtdcnt] = i; \
					slt[mtdcnt] = oid_nil; \
					mtdcnt++; \
				}\
				continue; \
			} \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && (!(is_##Type##_nil(ky[i]) && is_##Type##_nil(vals[slot])) && vals[slot] != ky[i])) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				oid_mtd[mtdcnt] = i; \
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
		gid *hs = Tloc(h, 0); \
		char **vals = ht->vals; \
		oid *oid_mtd = Tloc(o, 0); \
		oid *slt = Tloc(s, 0); \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			char *val = (bi).vh->base+BUNtvaroff(bi,i); \
			if (!(*semantics) && atomcmp(val, nil) == 0) { \
				if (!match && empty) { \
					oid_mtd[mtdcnt] = i; \
					slt[mtdcnt] = oid_nil; \
					mtdcnt++; \
				}\
				continue; \
			} \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && atomcmp(vals[slot], val) != 0) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				oid_mtd[mtdcnt] = i; \
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
BAT_OAHASHprobe1(bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H, bit match)
{
	BAT *o = NULL, *s = NULL, *k = NULL, *h = NULL, *t = NULL, *f = NULL;
	BUN keycnt, mtdcnt = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
	assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	h = BATdescriptor(*PRB_hash);
	t = BATdescriptor(*HSH_ht);
	if (!k || !h || !t) {
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
	o = COLnew(k->hseqbase, TYPE_oid, keycnt, TRANSIENT);
	s = COLnew(k->hseqbase, TYPE_oid, keycnt, TRANSIENT);
	if (!o || !s) {
		err = createException(SQL, "oahash.probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (keycnt) {
		hash_table *ht = (hash_table*)t->tsink;

		bool empty = (ht->last == 0);
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

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
				BATprobe(flt);
				break;
			case TYPE_dbl:
				BATprobe(dbl);
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

	BBPunfix(k->batCacheid);
	BBPunfix(h->batCacheid);
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
	BBPreclaim(h);
	BBPreclaim(t);
	BBPreclaim(f);
	return err;
}

static str
BAT_OAHASHprobe_single(bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHprobe1(PRB_oid, HSH_slotid, PRB_key, PRB_hash, HSH_ht, frequency, single, semantics, H, true);
}

static str
BAT_OAHASHprobe(bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHprobe1(PRB_oid, HSH_slotid, PRB_key, PRB_hash, HSH_ht, NULL, single, semantics, H, true);
}

static str
BAT_OAHASHnprobe_single(bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHprobe1(PRB_oid, HSH_slotid, PRB_key, PRB_hash, HSH_ht, frequency, single, semantics, H, false);
}

static str
BAT_OAHASHnprobe(bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHprobe1(PRB_oid, HSH_slotid, PRB_key, PRB_hash, HSH_ht, NULL, single, semantics, H, false);
}

#define BATvoprobe() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		keycnt = ci.ncand; \
		\
		gid *hs = Tloc(h, 0); \
		oid *vals = ht->vals; \
		oid *oid_mtd = Tloc(o, 0); \
		oid *slt = Tloc(s, 0); \
		bit *mark = Tloc(m, 0); \
		bit has_nil = false, empty = bit_nil; \
		\
		if (ht->empty) \
			empty = false; \
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
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = any?empty:false; \
				mtdcnt++; \
				continue; \
			} \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && vals[slot] != ky) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mark[i] = true; \
				mtdcnt++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
				mtdcnt++; \
			} \
		} \
	} while (0)

#define BAToprobe(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		Type *vals = ht->vals; \
		oid *oid_mtd = Tloc(o, 0); \
		oid *slt = Tloc(s, 0); \
		bit *mark = Tloc(m, 0); \
		bit has_nil = false, empty = bit_nil; \
		\
		if (ht->empty) \
			empty = false; \
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
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = any?empty:false; \
				mtdcnt++; \
				continue; \
			} \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && (!(is_##Type##_nil(ky[i]) && is_##Type##_nil(vals[slot])) && vals[slot] != ky[i])) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = (oid)(slot - 1); \
				mark[i] = true; \
				mtdcnt++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
				mtdcnt++; \
			} \
		} \
	} while (0)

#define BATaoprobe() \
	do { \
		BATiter bi = bat_iterator(k); \
		gid *hs = Tloc(h, 0); \
		char **vals = ht->vals; \
		oid *oid_mtd = Tloc(o, 0); \
		oid *slt = Tloc(s, 0); \
		bit *mark = Tloc(m, 0); \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		bit has_nil = false, empty = bit_nil; \
		\
		if (ht->empty) \
			empty = false; \
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
			char *val = (bi).vh->base+BUNtvaroff(bi,i); \
			if (!(*semantics) && atomcmp(val, nil) == 0) { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = any?empty:false; \
				mtdcnt++; \
				continue; \
			} \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && atomcmp(vals[slot], val) != 0) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = (oid)(slot - 1); \
				mark[i] = true; \
				mtdcnt++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.oprobe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt] = i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
				mtdcnt++; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
BAT_OAHASHomprobe(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H, bool any)
{
	BAT *o = NULL, *s = NULL, *m = NULL, *k = NULL, *h = NULL, *t = NULL, *f = NULL;
	BUN keycnt, mtdcnt = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
	assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	h = BATdescriptor(*PRB_hash);
	t = BATdescriptor(*HSH_ht);
	if (!k || !h || !t) {
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
	o = COLnew(k->hseqbase, TYPE_oid, keycnt, TRANSIENT);
	s = COLnew(k->hseqbase, TYPE_oid, keycnt, TRANSIENT);
	m = COLnew(k->hseqbase, TYPE_bit, keycnt, TRANSIENT);
	if (!o || !s || !m) {
		err = createException(SQL, "oahash.probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (keycnt) {
		hash_table *ht = (hash_table*)t->tsink;

		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

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
				BAToprobe(flt);
				break;
			case TYPE_dbl:
				BAToprobe(dbl);
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
	BBPunfix(h->batCacheid);
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
	BBPreclaim(h);
	BBPreclaim(t);
	BBPreclaim(f);
	return err;
}

static str
BAT_OAHASHoprobe_single(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe(PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, HSH_ht, frequency, single, semantics, H, false);
}

static str
BAT_OAHASHoprobe(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe(PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, HSH_ht, NULL, single, semantics, H, false);
}

static str
BAT_OAHASHmprobe_single(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe(PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, HSH_ht, frequency, single, semantics, H, true);
}

static str
BAT_OAHASHmprobe(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe(PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, HSH_ht, NULL, single, semantics, H, true);
}

#define BATvprobe_cmbd() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		\
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(s, 0); \
		oid *vals = ht->vals; \
		oid *oid_mtd = Tloc(res_o, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			oid ky = canditer_idx(&ci, mt[i]); \
			assert(ky != oid_nil); \
			if (!(*semantics) && ky == oid_nil) \
				continue; \
			\
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || vals[slot] != ky)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = mt[i]; \
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
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(s, 0); \
		Type *vals = ht->vals; \
		oid *oid_mtd = Tloc(res_o, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			Type val = ky[mt[i]]; \
			if (!(*semantics) && is_##Type##_nil(val)) \
				continue; \
			\
			slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || (is_##Type##_nil(vals[slot]) != is_##Type##_nil(val)) || vals[slot] != val)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = mt[i]; \
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
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(s, 0); \
		char **vals = ht->vals; \
		oid *oid_mtd = Tloc(res_o, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			char *val = (bi).vh->base+BUNtvaroff(bi,mt[i]); \
			if (!(*semantics) && atomcmp(val, nil) == 0) \
				continue; \
			\
			slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || atomcmp(vals[slot], val) != 0)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = mt[i]; \
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
BAT_OAHASHprobe_cmbd_single(bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_hash, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	BAT *res_o = NULL, *res_s = NULL, *k = NULL, *h = NULL, *s = NULL, *t = NULL, *p = NULL, *f = NULL;
	BUN mtdcnt, mtdcnt2 = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
	assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	h = BATdescriptor(*PRB_hash);
	s = BATdescriptor(*PRB_selected);
	p = BATdescriptor(*HSH_gid);
	t = BATdescriptor(*HSH_ht);
	if (!k || !h || !s || !t || !p) {
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
	res_o = COLnew(k->hseqbase, TYPE_oid, mtdcnt, TRANSIENT);
	res_s = COLnew(k->hseqbase, TYPE_oid, mtdcnt, TRANSIENT);
	if (!res_o || !res_s) {
		err = createException(SQL, "oahash.combined_probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (mtdcnt) {
		hash_table *ht = (hash_table*)t->tsink;

		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

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
				BATprobe_cmbd(flt);
				break;
			case TYPE_dbl:
				BATprobe_cmbd(dbl);
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
	BBPunfix(h->batCacheid);
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
	BBPreclaim(h);
	BBPreclaim(s);
	BBPreclaim(t);
	BBPreclaim(p);
	BBPreclaim(f);
	return err;
}

static str
BAT_OAHASHprobe_cmbd(bat *PRB_oid, bat *HSH_slotid, const bat *PRB_key, const bat *PRB_hash, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHprobe_cmbd_single(PRB_oid, HSH_slotid, PRB_key, PRB_hash, PRB_selected, HSH_gid, HSH_ht, NULL, single, semantics, H);
}

#define BATvoprobe_cmbd() \
	do { \
		unsigned int prime = hash_prime_nr[ht->bits-5]; \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		\
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(m, 0); \
		oid *vals = ht->vals; \
		oid *oid_mtd = Tloc(res_o, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		bit *mark = Tloc(res_m, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			oid ky = canditer_idx(&ci, mt[i]); \
			assert(ky != oid_nil); \
			if (!mark[i] || (!(*semantics) && ky == oid_nil)) { \
				oid_mtd[mtdcnt2] = i; \
				slt[mtdcnt2] = oid_nil; \
				mark[i] = (any && mark[i])?bit_nil:false; \
				mtdcnt2++; \
				continue; \
			} \
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || vals[slot] != ky)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt2] = i; \
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

#define BAToprobe_cmbd(Type) \
	do { \
		unsigned int prime = hash_prime_nr[ht->bits-5]; \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(m, 0); \
		Type *vals = ht->vals; \
		oid *oid_mtd = Tloc(res_o, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		bit *mark = Tloc(res_m, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			Type val = ky[mt[i]]; \
			if (!mark[i] || (!(*semantics) && is_##Type##_nil(val))) { \
				oid_mtd[mtdcnt2] = i; \
				slt[mtdcnt2] = oid_nil; \
				mark[i] = (any && mark[i])?bit_nil:false; \
				mtdcnt2++; \
				continue; \
			} \
			slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || (is_##Type##_nil(vals[slot]) != is_##Type##_nil(val)) || vals[slot] != val)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt2] = i; \
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

#define BATaoprobe_cmbd() \
	do { \
		unsigned int prime = hash_prime_nr[ht->bits-5]; \
		BATiter bi = bat_iterator(k); \
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(m, 0); \
		char **vals = ht->vals; \
		oid *oid_mtd = Tloc(res_o, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		bit *mark = Tloc(res_m, 0); \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			char *val = (bi).vh->base+BUNtvaroff(bi,mt[i]); \
			if (!mark[i] || (!(*semantics) && atomcmp(val, nil) == 0)) { \
				oid_mtd[mtdcnt2] = i; \
				slt[mtdcnt2] = oid_nil; \
				mark[i] = (any && mark[i])?bit_nil:false; \
				mtdcnt2++; \
				continue; \
			} \
			slot = ATOMIC_GET(ht->gids+hsh); \
			while (slot && (pgids[slot] != gi[i] || atomcmp(vals[slot], val) != 0)) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ATOMIC_GET(ht->gids+hsh); \
			} \
			if (slot) { \
				oid_mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && freq[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} else { \
				oid_mtd[mtdcnt2] = i; \
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
BAT_OAHASHomprobe_cmbd(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H, bool any)
{
	BAT *res_o = NULL, *res_s = NULL, *res_m = NULL, *k = NULL, *h = NULL, *m = NULL, *t = NULL, *p = NULL, *f = NULL;
	BUN mtdcnt, mtdcnt2 = 0;
	lng *freq = NULL;
	str err = NULL;

	(void) H;
    assert(((*single) && frequency) || !(*single));

	k = BATdescriptor(*PRB_key);
	h = BATdescriptor(*PRB_hash);
	m = BATdescriptor(*PRB_selected);
	p = BATdescriptor(*HSH_gid);
	t = BATdescriptor(*HSH_ht);
	res_m = BATdescriptor(*PRB_mark);
	if (!k || !h || !m || !t || !p || !res_m) {
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

	mtdcnt = BATcount(m);
	res_o = COLnew(k->hseqbase, TYPE_oid, mtdcnt, TRANSIENT);
	res_s = COLnew(k->hseqbase, TYPE_oid, mtdcnt, TRANSIENT);
	if (!res_o || !res_s) {
		err = createException(SQL, "oahash.combined_probe", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (mtdcnt) {
		hash_table *ht = (hash_table*)t->tsink;

		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

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
				BAToprobe_cmbd(flt);
				break;
			case TYPE_dbl:
				BAToprobe_cmbd(dbl);
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
	BBPunfix(h->batCacheid);
	BBPunfix(m->batCacheid);
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
	BBPreclaim(h);
	BBPreclaim(m);
	BBPreclaim(t);
	BBPreclaim(p);
	BBPreclaim(f);
	return err;
}

static str
BAT_OAHASHoprobe_cmbd_single(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe_cmbd( PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, PRB_selected, HSH_gid, HSH_ht, frequency, single, semantics, H, false);
}

static str
BAT_OAHASHoprobe_cmbd(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe_cmbd( PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, PRB_selected, HSH_gid, HSH_ht, NULL, single, semantics, H, false);
}

static str
BAT_OAHASHmprobe_cmbd_single(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bat *frequency, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe_cmbd( PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, PRB_selected, HSH_gid, HSH_ht, frequency, single, semantics, H, true);
}

static str
BAT_OAHASHmprobe_cmbd(bat *PRB_oid, bat *HSH_slotid, bat *PRB_mark, const bat *PRB_key, const bat *PRB_hash, const bat *PRB_selected, const bat *HSH_gid, const bat *HSH_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe_cmbd( PRB_oid, HSH_slotid, PRB_mark, PRB_key, PRB_hash, PRB_selected, HSH_gid, HSH_ht, NULL, single, semantics, H, true);
}


#define vproject() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		\
		oid *res = Tloc(e, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, rescnt, qry_ctx) { \
			oid val = canditer_idx(&ci, sel[i]); \
			assert(val != oid_nil); \
			\
			res[idx++] = val; \
		} \
	} while (0)

#define project(Type) \
	do { \
		Type *val = Tloc(k, 0); \
		Type *res = Tloc(e, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, rescnt, qry_ctx) { \
			res[idx++] = val[sel[i]]; \
		} \
	} while (0)

#define aproject() \
	do { \
		BATiter bi = bat_iterator(k); \
		TIMEOUT_LOOP_IDX_DECL(i, rescnt, qry_ctx) { \
			void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,sel[i])); \
			if (BUNappend(e, v, false) != GDK_SUCCEED) { \
				err = createException(SQL, "oahash.project", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				break; \
			} \
			idx++; \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHproject(bat *res, const bat *key, const bat *selected, const ptr *H)
{
	BAT *e = NULL, *k = NULL, *s = NULL;
	BUN rescnt = 0;
	str err = NULL;

	k = BATdescriptor(*key);
	s = BATdescriptor(*selected);
	if (!k || !s) {
		err = createException(SQL, "oahash.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	assert(BATcount(s) <= BATcount(k));

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	rescnt = BATcount(s);
	int tt = k->ttype;
	e = COLnew(0, tt?tt:TYPE_oid, rescnt, TRANSIENT);
	if (!e) {
		err = createException(SQL, "oahash.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (rescnt) {
		BUN idx = 0;
		oid *sel = Tloc(s, 0);

		switch(tt) {
			case TYPE_void:
				vproject();
				break;
			case TYPE_bit:
				project(bit);
				break;
			case TYPE_bte:
				project(bte);
				break;
			case TYPE_sht:
				project(sht);
				break;
			case TYPE_int:
				project(int);
				break;
			case TYPE_date:
				project(date);
				break;
			case TYPE_lng:
				project(lng);
				break;
			case TYPE_oid:
				if (BATtdense(k))
					vproject();
				else
					project(oid);
				break;
			case TYPE_daytime:
				project(daytime);
				break;
			case TYPE_timestamp:
				project(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				project(hge);
				break;
#endif
			case TYPE_flt:
				project(flt);
				break;
			case TYPE_dbl:
				project(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					aproject();
				} else {
					err = createException(MAL, "oahash.project", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.project", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}

	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);

	BATsetcount(e, rescnt);
	BATnegateprops(e);
	*res = e->batCacheid;
	BBPkeepref(e);

	(void) H;
	return MAL_SUCCEED;
error:
	BBPreclaim(e);
	BBPreclaim(k);
	BBPreclaim(s);
	return err;
}

#define BATvexpand() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		keycnt = ci.ncand; \
		\
		oid *res = Tloc(e, 0); \
		if (*leftouter && freq) { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
				oid s = sid[i]; \
				oid val = canditer_idx(&ci, i); \
				assert(val != oid_nil); \
				if (s != oid_nil) {\
					gid frq = (gid)freq[s]; \
					frq = frq?frq:1; \
					TIMEOUT_LOOP_IDX_DECL(f, frq, qry_ctx) { \
						res[idx++] = val; \
					} \
				} else { \
					res[idx++] = val; \
				} \
			} \
		} else if (freq) { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				oid val = canditer_idx(&ci, sel[i]); \
				gid frq = (gid)freq[sid[i]]; \
				frq = frq?frq:1; \
				TIMEOUT_LOOP_IDX_DECL(f, frq, qry_ctx) { \
					res[idx++] = val; \
				} \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				res[idx++] = canditer_idx(&ci, sel[i]); \
			} \
		} \
	} while (0)

#define BATexpand(Type) \
	do { \
		Type *val = Tloc(k, 0); \
		Type *res = Tloc(e, 0); \
		if (*leftouter && freq) { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
				oid s = sid[i]; \
				Type v = val[i]; \
				if (s != oid_nil) {\
					gid frq = (gid)freq[s]; \
					frq = frq?frq:1; \
					TIMEOUT_LOOP_IDX_DECL(f, frq, qry_ctx) { \
						res[idx++] = v; \
					} \
				} else { \
					res[idx++] = v; \
				} \
			} \
		} else if (freq) { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				Type v = val[sel[i]]; \
				gid frq = (gid)freq[sid[i]]; \
				frq = frq?frq:1; \
				TIMEOUT_LOOP_IDX_DECL(j, frq, qry_ctx) { \
					res[idx++] = v; \
				} \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				res[idx++] = val[sel[i]]; \
			} \
		} \
	} while (0)

#define BATaexpand() \
	do { \
		BATiter bi = bat_iterator(k); \
		if (*leftouter && freq) { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
				oid s = sid[i]; \
				void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
				if (s != oid_nil) {\
					gid frq = (gid)freq[s]; \
					TIMEOUT_LOOP_IDX_DECL(f, frq, qry_ctx) { \
						if (BUNappend(e, v, false) != GDK_SUCCEED) { \
							err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
							break; \
						} \
						idx++; \
					} \
				} else { \
					if (BUNappend(e, v, false) != GDK_SUCCEED) { \
						err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						break; \
					} \
					idx++; \
				} \
			} \
		} else if (freq) { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,sel[i])); \
				gid frq = (gid)freq[sid[i]]; \
				TIMEOUT_LOOP_IDX_DECL(j, frq, qry_ctx) { \
					if (BUNappend(e, v, false) != GDK_SUCCEED) { \
						err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						break; \
					} \
					idx++; \
				} \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,sel[i])); \
				if (BUNappend(e, v, false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				} \
				idx++; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
BAT_OAHASHexpand(bat *expanded, const bat *key, const bat *selected, const bat *slotid, const bat *frequency, const bit *leftouter, const ptr *H)
{
	BAT *e = NULL, *k = NULL, *s = NULL, *l = NULL, *f = NULL;
	BUN keycnt, selcnt, ttlcnt = 0, xpdcnt = 0;
	lng *freq = NULL;
	str err = NULL;

	k = BATdescriptor(*key);
	s = BATdescriptor(*selected);
	l = BATdescriptor(*slotid);
	if (!k || !s || !l) {
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
	assert(BATcount(s) <= BATcount(k));

	gid *sid = Tloc(l, 0);
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	keycnt = BATcount(k);
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

	int tt = k->ttype;
	e = COLnew(0, tt?tt:TYPE_oid, ttlcnt, TRANSIENT);
	if (!e) {
		err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (ttlcnt) {
		BUN idx = 0;
		oid *sel = Tloc(s, 0);

		switch(tt) {
			case TYPE_void:
				BATvexpand();
				break;
			case TYPE_bit:
				BATexpand(bit);
				break;
			case TYPE_bte:
				BATexpand(bte);
				break;
			case TYPE_sht:
				BATexpand(sht);
				break;
			case TYPE_int:
				BATexpand(int);
				break;
			case TYPE_date:
				BATexpand(date);
				break;
			case TYPE_lng:
				BATexpand(lng);
				break;
			case TYPE_oid:
				if (BATtdense(k))
					BATvexpand();
				else
					BATexpand(oid);
				break;
			case TYPE_daytime:
				BATexpand(daytime);
				break;
			case TYPE_timestamp:
				BATexpand(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				BATexpand(hge);
				break;
#endif
			case TYPE_flt:
				BATexpand(flt);
				break;
			case TYPE_dbl:
				BATexpand(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaexpand();
				} else {
					err = createException(MAL, "oahash.expand", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;

		assert(idx == ttlcnt);
	}

	BATsetcount(e, ttlcnt);
	BATnegateprops(e);
	e->tsorted = k->tsorted;
	e->trevsorted = k->trevsorted;
	*expanded = e->batCacheid;
	BBPkeepref(e);

	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);
	BBPunfix(l->batCacheid);
	BBPreclaim(f);

	(void) H;
	return MAL_SUCCEED;
error:
	BBPreclaim(e);
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(l);
	BBPreclaim(f);
	return err;
}

#define vexpand_cart() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		keycnt = ci.ncand; \
		\
		oid *res = Tloc(e, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			oid val = canditer_idx(&ci, i); \
			assert(val != oid_nil); \
			TIMEOUT_LOOP_IDX_DECL(j, repcnt, qry_ctx) { \
				res[idx++] = val; \
			} \
		} \
	} while (0)

#define expand_cart(Type) \
	do { \
		Type *val = Tloc(k, 0); \
		Type *res = Tloc(e, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			TIMEOUT_LOOP_IDX_DECL(j, repcnt, qry_ctx) { \
				res[idx++] = val[i]; \
			} \
		} \
	} while (0)

#define aexpand_cart() \
	do { \
		BATiter bi = bat_iterator(k); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			TIMEOUT_LOOP_IDX_DECL(j, repcnt, qry_ctx) { \
				if (BUNappend(e, v, false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				} \
				idx++; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

#define expand_fixed() \
	do { \
		int w = k->twidth; \
		char *val = Tloc(k, 0); \
		char *res = Tloc(e, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			TIMEOUT_LOOP_IDX_DECL(j, repcnt, qry_ctx) { \
				memcpy(res+(w*idx), val+(w*i), w); \
				idx++; \
			} \
		} \
	} while (0)

static str
BAT_OAHASHexpand_cart(bat *expanded, const bat *col, const bat *rowrepeat, const bit *LRouter, const ptr *H)
{
	(void) H;

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
	if (*LRouter && repcnt == 0)
		repcnt = 1;
	ttlcnt = keycnt * repcnt;

	int tt = k->ttype;
	e = COLnew(0, tt?tt:TYPE_oid, ttlcnt, TRANSIENT);
	if (!e) {
		err = createException(SQL, "oahash.expand_cartesian", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	BUN idx = 0;
	if (ATOMvarsized(tt)) {
		aexpand_cart();
	} else if (tt == TYPE_void) {
		vexpand_cart();
	} else {
		switch(ATOMsize(tt)) {
		case 1:
			expand_cart(bte);
			break;
		case 2:
			expand_cart(sht);
			break;
		case 4:
			expand_cart(int);
			break;
		case 8:
			expand_cart(lng);
			break;
#ifdef HAVE_HGE
		case 16:
			expand_cart(hge);
			break;
#endif
		default:
			expand_fixed();
		}
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand_cart", RUNTIME_QRY_TIMEOUT));
	if (err)
		goto error;

	assert(idx == ttlcnt);

	BATsetcount(e, ttlcnt);
	BATnegateprops(e);
	*expanded = e->batCacheid;
	e->tsorted = k->tsorted;
	e->trevsorted = k->trevsorted;
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
BAT_OAHASHexplode(bat *fetched, const bat *slotid, const bat *frequency, const bat *ht_sink, const bit *leftouter)
{
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
		if (*leftouter) {
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
		if (*leftouter) {
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

	BBPunfix(l->batCacheid);
	BBPunfix(f->batCacheid);
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

#define vfetch_cart() \
	do { \
		if (append_nulls) { \
			oid *res = Tloc(f, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				res[idx++] = oid_nil; \
			} \
		} else { \
			struct canditer ci; \
			canditer_init(&ci, NULL, k); \
			keycnt = ci.ncand; \
			\
			oid *res = Tloc(f, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				TIMEOUT_LOOP_IDX_DECL(j, keycnt, qry_ctx) { \
					oid val = canditer_idx(&ci, j); \
					assert(val != oid_nil); \
					\
					res[idx++] = val; \
				} \
			} \
		} \
	} while (0)

#define fetch_cart(Type) \
	do { \
		if (append_nulls) { \
			Type *res = Tloc(f, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				res[idx++] = Type##_nil; \
			} \
		} else { \
			Type *val = Tloc(k, 0); \
			Type *res = Tloc(f, 0); \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				TIMEOUT_LOOP_IDX_DECL(j, keycnt, qry_ctx) { \
					res[idx++] = val[j]; \
				} \
			} \
		} \
	} while (0)

#define sfetch_cart() \
	do { \
		if (append_nulls) { \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				if (BUNappend(f, str_nil, false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.fetch", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				} \
				idx++; \
			} \
		} else { \
			BATiter bi = bat_iterator(k); \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				TIMEOUT_LOOP_IDX_DECL(j, keycnt, qry_ctx) { \
					void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,j)); \
					if (BUNappend(f, v, false) != GDK_SUCCEED) { \
						err = createException(SQL, "oahash.fetch", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						break; \
					} \
					idx++; \
				} \
			} \
			bat_iterator_end(&bi); \
		} \
	} while (0)

#define afetch_cart() \
	do { \
		if (append_nulls) { \
			const void *nil = ATOMnilptr(f->ttype); \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				if (BUNappend(f, nil, false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.fetch", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				} \
				idx++; \
			} \
		} else { \
			BATiter bi = bat_iterator(k); \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				TIMEOUT_LOOP_IDX_DECL(j, keycnt, qry_ctx) { \
					void *v =  BUNtail(bi, j); \
					if (BUNappend(f, v, false) != GDK_SUCCEED) { \
						err = createException(SQL, "oahash.fetch", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						break; \
					} \
					idx++; \
				} \
			} \
			bat_iterator_end(&bi); \
		} \
	} while (0)

static str
BAT_OAHASHfetch_pld_cart(bat *fetched, const bat *col, const bat *setrepeat, const bit *LRouter, const ptr *H)
{
	(void) H;

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
	if (*LRouter && keycnt == 0) {
		append_nulls = true;
		keycnt = 1;
	}
	repcnt = BATcount(d);
	ttlcnt = keycnt * repcnt;
	int tt = k->ttype;
	f = COLnew(0, tt?tt:TYPE_oid, ttlcnt, TRANSIENT);
	if (!f) {
		err = createException(SQL, "oahash.fetch_payload_cartesian", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (ttlcnt) {
		BUN idx = 0;

		// TODO: use memcpy to copy the whole array at once?
		switch(tt) {
			case TYPE_void:
				vfetch_cart();
				break;
			case TYPE_bit:
				fetch_cart(bit);
				break;
			case TYPE_bte:
				fetch_cart(bte);
				break;
			case TYPE_sht:
				fetch_cart(sht);
				break;
			case TYPE_int:
				fetch_cart(int);
				break;
			case TYPE_date:
				fetch_cart(date);
				break;
			case TYPE_lng:
				fetch_cart(lng);
				break;
			case TYPE_oid:
				if (BATtdense(k))
					vfetch_cart();
				else
					fetch_cart(oid);
				break;
			case TYPE_daytime:
				fetch_cart(daytime);
				break;
			case TYPE_timestamp:
				fetch_cart(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
			case TYPE_uuid:
				fetch_cart(hge);
				break;
#endif
			case TYPE_flt:
				fetch_cart(flt);
				break;
			case TYPE_dbl:
				fetch_cart(dbl);
				break;
			default:
				if (tt == TYPE_str)
					sfetch_cart();
				else
					afetch_cart();
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

#define SLICE_SIZE 100000

static str
OAHASHno_slices(int *no_slices, bat *ht_sink)
{
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
		*no_slices = (h->size+SLICE_SIZE-1)/SLICE_SIZE;
	FORCEMITODEBUG
	if (*no_slices < GDKnr_threads)
		*no_slices = GDKnr_threads;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
OAHASHnth_slice(bat *slice, bat *ht_sink, int *slice_nr)
{
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
	BBPkeepref(b);
	BBPkeepref(r);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func oa_hash_init_funcs[] = {
 pattern("oahash", "new", OAHASHnew, false, "", args(1,3, batargany("ht_sink",1),argany("tt",1),arg("size",int))),
 pattern("oahash", "new", OAHASHnew, false, "", args(1,4, batargany("ht_sink",1),argany("tt",1),arg("size",int),batargany("p",2))),
 pattern("hash", "ext", UHASHext, false, "", args(1,2, batarg("ext",oid),batargany("in",1))),

 command("oahash", "build_table", BAT_OAHASHbuild_tbl, false, "Build a hash table for the keys. Returns the slot ID per key and the hash table sink", args(2,4, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),arg("pipeline",ptr))),

 command("oahash", "build_combined_table", OAHASHbuild_tbl_cmbd, false, "Build a hash table for the keys with a parent column. Returns the slot ID per key and the hash table sink", args(2,5, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),batarg("parent_slotid",oid),arg("pipeline",ptr))),

 pattern("oahash", "frequency", OAHASHadd_freq, false, "Add the frequencies of the slot IDs to the shared frequency BAT", args(1,3, batarg("frequencies",lng),batarg("slot_id",oid),arg("pipeline",ptr))),
 pattern("oahash", "frequency", OAHASHadd_freq, false, "Add the frequencies of the slot IDs to the shared frequency BAT and return combined_hash(slot_id, freq) for payload_pos", args(2,4, batarg("payload_pos",oid),batarg("frequencies",lng),batarg("slot_id",oid),arg("pipeline",ptr))),

 command("oahash", "hash", BAT_OAHASHhash, false, "Compute the hashs for the keys", args(1,3, batarg("hsh",lng),batargany("key",1),arg("pipeline",ptr))),

 command("oahash", "probe", BAT_OAHASHprobe_single, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,9, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "probe", BAT_OAHASHprobe, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,8, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "nprobe", BAT_OAHASHnprobe_single, false, "Probe the (key, hash) pairs in the hash table. For a not-matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,9, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "nprobe", BAT_OAHASHnprobe, false, "Probe the (key, hash) pairs in the hash table. For a not-matched key, return its OID in the 'key' column and the slot ID in the hash table", args(2,8, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "oprobe", BAT_OAHASHoprobe_single, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,10, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "oprobe", BAT_OAHASHoprobe, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,9, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "mprobe", BAT_OAHASHmprobe_single, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,10, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "mprobe", BAT_OAHASHmprobe, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the 'key' column and the slot ID in the hash table", args(3,9, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batargany("HSH_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),


 command("oahash", "combined_hash", BAT_OAHASHhash_cmbd, false, "For the selected keys, compute the combined hash of key+parent_slotid", args(1,5,batarg("hsh",lng),batargany("key",1),batarg("selected",oid),batarg("parent_slotid",oid),batargany("ht_sink",2))),

 command("oahash", "combined_probe", BAT_OAHASHprobe_cmbd_single, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(2,11, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_hash",lng),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",2),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "combined_probe", BAT_OAHASHprobe_cmbd, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(2,10, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batargany("PRB_key",1),batarg("PRB_hash",lng),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "combined_oprobe", BAT_OAHASHoprobe_cmbd_single, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,12, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",2),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "combined_oprobe", BAT_OAHASHoprobe_cmbd, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,11, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_mark",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "combined_mprobe", BAT_OAHASHmprobe_cmbd_single, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,12, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",2),batarg("frequency",lng),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),
 command("oahash", "combined_mprobe", BAT_OAHASHmprobe_cmbd, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the 'key' column and the slot ID in the hash table", args(3,11, batarg("PRB_oid",oid),batarg("HSH_slotid",oid),batarg("PRB_matched",bit),batargany("PRB_key",1),batarg("PRB_hash",lng),batarg("PRB_selected",oid),batarg("HSH_pgids",oid),batargany("HSH_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "project", OAHASHproject, false, "Project the selected OIDs onto the keys", args(1,4, batargany("res",1),batargany("key",1),batarg("selected",oid),arg("pipeline",ptr))),

 command("oahash", "expand", BAT_OAHASHexpand, false, "Expand the selected keys according to their frequencies in the hash table. If 'leftouter' is true, append the not 'selected' keys", args(1,7,batargany("expanded",1),batargany("key",1),batarg("selected",oid),batarg("slotid",oid),batarg("frequency",lng),arg("leftouter",bit),arg("pipeline",ptr))),

 command("oahash", "expand_cartesian", BAT_OAHASHexpand_cart, false, "Duplicate each value in 'col' the number of times as the count of 'rowrepeat'. For a left/right-outer join, if 'rowrepeat' is empty, output the values in 'col' once.", args(1,5, batargany("expanded",1),batargany("col",1),batargany("rowrepeat",2),arg("LRouter",bit),arg("pipeline",ptr))),

 command("oahash", "explode", BAT_OAHASHexplode, false, "Explode the result vector 'frequency' times and return payload heap slot ids. If 'leftouter' is true, fill the not 'selected' slot with oid_nil", args(1,5, batarg("fetched",oid),batarg("slotid",oid),batarg("frequency",lng),batargany("hash_sink",2),arg("leftouter",bit))),

 command("oahash", "fetch_payload_cartesian", BAT_OAHASHfetch_pld_cart, false, "Duplicate the whole 'col' the number of times as the count of 'setrepeat'.  For a left/right-ourter join, if 'col' is empty, output NULLs.", args(1,5, batargany("fetched",1),batargany("col",1),batarg("setrepeat",2),arg("LRouter",bit),arg("pipeline",ptr))),

 command("oahash", "no_slices", OAHASHno_slices, false, "Get the number of slices for this hashtable.", args(1,2, arg("slices",int),batargany("ht_sink",1))),
 command("oahash", "nth_slice", OAHASHnth_slice, false, "Get the nth slice of this hashtable.", args(2,3, batarg("slice",oid),batargany("ht_sink",1),arg("slice_nr",int))),

 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("oa_hash", NULL, oa_hash_init_funcs); }
