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

// FIXME: need to rethink how to _always_ get the correct hash_prime_nr
static unsigned int
find_hash_prime(unsigned int n)
{
	// TODO better size estimation
	unsigned int nn = n * 1.2 * 2.1;
	if (nn < HT_MIN_SIZE)
		nn = HT_MIN_SIZE;
	if (nn > HT_MAX_SIZE)
		nn = HT_MAX_SIZE;

	unsigned int bits = log_base2(nn - 1);
	if (bits >= GIDBITS)
		bits = GIDBITS - 1;

	return hash_prime_nr[bits - 5];
}

/* ***** HASH TABLE ***** */
static hash_table *
_ht_init(hash_table *h, bool freq)
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

	if (freq) {
		h->frequency = (ATOMIC_TYPE *)GDKzalloc(sizeof(ATOMIC_TYPE) * h->size);
		h->matched = (bool *)GDKzalloc(sizeof(bool) * h->size);
		if (!h->frequency || !h->matched)
			goto error;
	}

	return h;
error:
	if(h->vals) GDKfree(h->vals);
	if(h->gids) GDKfree((void *)h->gids);
	if(h->pgids) GDKfree(h->pgids);
	if(h->frequency) GDKfree((void *)h->frequency);
	if(h->matched) GDKfree(h->matched);
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
			HEAPdecref(ht->pinned[i], false);
			BBPunfix(ht->pinned[i]->parentid);
		}
		GDKfree(ht->pinned);
	}
	if (ht->frequency)
		GDKfree((void*)ht->frequency);
	if (ht->matched)
		GDKfree(ht->matched);
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
_ht_create( int type, size_t size, bool freq, hash_table *p)
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
	h->rehash = 0;
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

	hash_table *h2 = _ht_init(h, freq);
	if (h2 == NULL) {
		GDKfree(h);
		return NULL;
	}
	return h2;
}

hash_table *
ht_create(int type, int size, bool freq, hash_table *p)
{
	if (size < HT_MIN_SIZE)
		size = HT_MIN_SIZE;
	if (size > HT_MAX_SIZE)
		size = HT_MAX_SIZE;
	return _ht_create(type, size, freq, p);
}

void
ht_rehash(hash_table *ht)
{
	ht->rehash = 1;
	if (ht->p)
		ht_rehash(ht->p);
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
	bool freq = false;

	if (p->argc >= 4) {
		freq = *getArgReference_bit(s, p, 3);
	}
	if (p->argc == 5) {
		bat pid = *getArgReference_bat(s, p, 4);
		if ((pht = BATdescriptor(pid)) == NULL)
			return createException(MAL, "oahash.new", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		parent = (hash_table*)pht->tsink;
	}

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	if (b == NULL) {
		BBPreclaim(pht);
		return createException(MAL, "oahash.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	b->tsink = (Sink*)ht_create(tt, size*1.2*2.1, freq, parent);
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

/* ***** HASH PAYLOAD ***** */
static void
hp_destroy(hash_payload *hp)
{
	if (hp->payload)
		GDKfree(hp->payload);
	if (hp->pinned) {
		for(int i=0; i < hp->pinned_nr; i++)
			HEAPdecref(hp->pinned[i], false);
		GDKfree(hp->pinned);
	}
	if (hp->allocators) {
		for(int i = 0; i < hp->nr_allocators; i++) {
			if(hp->allocators[i])
				ma_destroy(hp->allocators[i]);
		}
		GDKfree(hp->allocators);
	}
	GDKfree(hp);
}

static hash_payload *
_hp_create(int type, size_t nplds, hash_table *parent)
{
	hash_payload *hp = (hash_payload *)GDKzalloc(sizeof(hash_payload));
	if (!hp) return NULL;

	int bits = log_base2(nplds-1);
	int atype = type?type:TYPE_oid;

	hp->s.destroy = (sink_destroy)&hp_destroy;
	hp->s.type = OA_HASH_PAYLOAD_SINK;
	if (bits >= GIDBITS)
		bits = GIDBITS-1;
	hp->bits = bits;
	hp->nr_payloads = (gid)1<<bits;
	hp->mask = hp->nr_payloads-1;
	hp->type = type;

	hp->width = ATOMsize(atype);
	hp->rehash = 0;
	hp->pinned = NULL;
	hp->pinned_nr = 0; /* no more than 1024 */
	if (atype == TYPE_str) {
		hp->pinned = (Heap**)GDKzalloc(sizeof(Heap*)*1024);
		hp->cmp = (fcmp)str_cmp;
		hp->hsh = (fhsh)str_hsh;
	} else {
		hp->cmp = (fcmp)ATOMcompare(atype);
		hp->hsh = (fhsh)BATatoms[atype].atomHash;
		hp->len = (flen)BATatoms[atype].atomLen;
	}
	hp->parent = parent;

	hp->payload = (char *)GDKmalloc((size_t)hp->width * hp->nr_payloads);
	if (!hp->payload) {
		GDKfree(hp);
		return NULL;
	}
	return hp;
}

/* Returns NULL if a memory allocation has failed.
 */
hash_payload *
hp_create(int type, size_t nplds, hash_table *parent)
{
	if (nplds < parent->size)
		nplds = parent->size;
	if (nplds < HP_MIN_SIZE)
		nplds = HP_MIN_SIZE;
	if (nplds > HP_MAX_SIZE)
		nplds = HP_MAX_SIZE;
	return _hp_create(type, nplds, parent);
}

void
hp_rehash(hash_payload *hp)
{
	hp->rehash = 1;
	if (hp->parent)
		ht_rehash(hp->parent);
}

static str
OAHASHnew_pld(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *res = getArgReference_bat(s, p, 0);
	int tt = getArgType(m, p, 1);
	int nplds = *getArgReference_int(s, p, 2);
	bat pid = *getArgReference_bat(s, p, 3);
	str err = NULL;

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	if (b == NULL)
		return createException(MAL, "oahash.new_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BAT *prnt = BATdescriptor(pid);
	if (prnt == NULL) {
		err = createException(MAL, "oahash.new_payload", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	hash_table *parent = (hash_table*)prnt->tsink;

	b->tsink = (Sink *)hp_create(tt, nplds*1.2*2.1, parent);
	if (!b->tsink) {
		err = createException(MAL, "oahash.new_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}
	BBPunfix(prnt->batCacheid);
	*res = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
error:
	BBPreclaim(b);
	BBPreclaim(prnt);
	return err;
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

#define PRE_CLAIM 256

#define BATgroup(Type) \
	do { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			bool fnd = 0; \
			gid k = (gid)_hash_##Type(bp[i])&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				assert(g<(gid)h->size); \
				while (g && vals[g] != bp[i]) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
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
			gid k = (gid)_hash_oid(bpi)&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				while (g && vals[g] != bpi) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
						hash_rehash(h, p, err); \
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
			gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && \
						vals[g] != bp[i])) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
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
			gid k = (gid)h->hsh(bpi)&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (vals[g] && h->cmp(vals[g], bpi) != 0)) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
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
				gid k = (gid)str_hsh(bpi)&h->mask; \
				gid g = 0; \
				\
				while (!fnd) { \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (vals[g] && h->cmp(vals[g], bpi) != 0)) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = private?1:PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
							if (((slot*100)/70) >= (gid)h->size) \
								hash_rehash(h, p, err); \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_strdup(ma, bpi); \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
							continue; \
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
				gid k = (gid)h->hsh(bpi)&h->mask; \
				gid g = 0; \
				\
				while (!fnd) { \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (vals[g] && atomcmp(vals[g], bpi) != 0)) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = private?1:PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
							if (((slot*100)/70) >= (gid)h->size) \
								hash_rehash(h, p, err); \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_copy(ma, bpi, h->len(bpi)); \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
							continue; \
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

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

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
		if (!err)
			TIMEOUT_CHECK(qry_ctx, throw(MAL, "oahash.build_table", RUNTIME_QRY_TIMEOUT));
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "oahash.build_table", "pipeline execution error");
		goto error;
	}
	BBPunfix(b->batCacheid);
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
			gid k = (gid)combine(gi[i], _hash_##Type(bp[i]), prime)&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || vals[g] != bp[i])) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
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
			gid k = (gid)combine(gi[i], _hash_oid(bpi), prime)&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || vals[g] != bpi)) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
						hash_rehash(h, p, err); \
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
			gid k = (gid)combine(gi[i], _hash_##Type(*(((BaseType*)bp)+i)), prime)&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]))) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/70) >= (gid)h->size) \
							hash_rehash(h, p, err); \
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
			gid k = (gid)combine(gi[i], h->hsh(bpi), prime)&h->mask; \
			gid g = 0; \
			\
			while (!fnd) { \
				g = ATOMIC_GET(h->gids+k); \
				while (g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0))) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = private?1:PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
						if (((slot*100)/100) >= (gid)h->size) \
							hash_rehash(h, p, err); \
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
				gid k = (gid)combine(gi[i], str_hsh(bpi), prime)&h->mask; \
				gid g = 0; \
				\
				while (!fnd) { \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (pgids[g] != gi[i] || (vals[g] && h->cmp(vals[g], bpi) != 0))) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = private?1:PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
							if (((slot*100)/100) >= (gid)h->size) \
								hash_rehash(h, p, err); \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_strdup(ma, bpi); \
						pgids[g] = gi[i]; \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
							continue; \
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
				gid k = (gid)combine(gi[i], h->hsh(bpi), prime)&h->mask; \
				gid g = 0; \
				\
				while (!fnd) { \
					g = ATOMIC_GET(h->gids+k); \
					while (g && (pgids[g] != gi[i] || (vals[g] && atomcmp(vals[g], bpi) != 0))) { \
						k++; \
						k &= h->mask; \
						g = ATOMIC_GET(h->gids+k); \
					} \
					if (!g) { \
						if (slots == 0) { \
							slots = private?1:PRE_CLAIM; \
							slot = ATOMIC_ADD(&h->last, private?1:PRE_CLAIM); \
							if (((slot*100)/70) >= (gid)h->size) \
								hash_rehash(h, p, err); \
						} \
						slots--; \
						g = ++slot; \
						vals[g] = ma_copy(ma, bpi, h->len(bpi)); \
						pgids[g] = gi[i]; \
						if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
							continue; \
					} \
					fnd = 1; \
				} \
				gp[i] = g-1; \
			} \
			bat_iterator_end(&bi); \
		} \
	} while (0)

static str
OAHASHbuild_tbl_cmbd(bat *slot_id, bat *ht_sink, const bat *key, const bat *parent_slotid, const bat *parent_ht, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H;
	bool private = 0, local_storage = false;
	str err = NULL;
	BAT *u = NULL, *b = NULL, *G = NULL, *g = NULL;

	/* for now we only work with shared ht_sink in the build phase */
	assert(*ht_sink && !is_bat_nil(*ht_sink));
	(void) parent_ht;

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
		ATOMIC_BASE_TYPE expected = 0;
		int tt = b->ttype;
		gid *gp = Tloc(g, 0);
		gid *gi = Tloc(G, 0);
		gid *pgids = h->pgids;
		int prime = hash_prime_nr[h->bits-5];

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
OAHASHcmpt_freq(bat *ht_sink, const bat *slot_id, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H;
	str err = NULL;
	BAT *hts = NULL, *slt = NULL;

	assert(ht_sink && !is_bat_nil(*ht_sink));

	hts = BATdescriptor(*ht_sink);
	slt = BATdescriptor(*slot_id);
	if (!hts || !slt) {
		err = createException(MAL, "oahash.compute_frequencies", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	hash_table *ht = (hash_table*)hts->tsink;
	assert(ht && ht->s.type == OA_HASH_TABLE_SINK);

	BUN cnt = BATcount(slt);
	if (cnt) {
		gid *sltid = Tloc(slt, 0);
		ATOMIC_TYPE *freqs = ht->frequency;

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
			ATOMIC_INC(&freqs[sltid[i]]);
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.compute_frequencies", RUNTIME_QRY_TIMEOUT));
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "oahash.compute_frequencies", "pipeline execution error");
		goto error;
	}

	BBPunfix(slt->batCacheid);
	BBPkeepref(hts);
	return MAL_SUCCEED;
error:
	BBPreclaim(hts);
	BBPreclaim(slt);
	return err;
}

static str
OAHASHcmpt_freq_pos(bat *payload_pos, bat *ht_sink, const bat *slot_id, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H;
	str err = NULL;
	BAT *hts = NULL, *slt = NULL, *res = NULL;

	assert(ht_sink && !is_bat_nil(*ht_sink));

	hts = BATdescriptor(*ht_sink);
	slt = BATdescriptor(*slot_id);
	if (!hts || !slt) {
		err = createException(MAL, "oahash.compute_frequencies", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	hash_table *ht = (hash_table*)hts->tsink;
	assert(ht && ht->s.type == OA_HASH_TABLE_SINK);

	BUN cnt = BATcount(slt);
	res = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	if (!res) {
			err = createException(MAL, "oahash.compute_frequencies", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
	}
	if (cnt) {
		gid *sltid = Tloc(slt, 0);
		ATOMIC_TYPE *freqs = ht->frequency;
		gid *ppos = Tloc(res, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		int prime = hash_prime_nr[ht->bits-5];
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
			size_t old_freq = ATOMIC_ADD(&freqs[sltid[i]], 1);
			/* TODO: how can we make sure the result hash is unique? */
			ppos[i] = (gid)combine(old_freq, _hash_lng(sltid[i]), prime)&ht->mask;
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.compute_frequencies", RUNTIME_QRY_TIMEOUT));
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "oahash.compute_frequencies", "pipeline execution error");
		goto error;
	}

	BBPunfix(slt->batCacheid);
	BATsetcount(res, cnt);
	/* payload_pos MUST always be unique. */
	// TODO check and indicate unique
	BATnegateprops(res);
	*payload_pos = res->batCacheid;
	BBPkeepref(res);
	BBPkeepref(hts);
	return MAL_SUCCEED;
error:
	BBPreclaim(hts);
	BBPreclaim(slt);
	BBPreclaim(res);
	return err;
}

#define hp_check_rehash() \
	do { \
		if (ppos[i] >= (gid)hp->nr_payloads) { \
			hp->rehash = 1; \
			err = createException(MAL, "oahash.add_payload", "hash payload needs rehash"); \
			goto error; \
		} \
	} while (0)

#define vaddpld() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, pld); \
		cnt = ci.ncand; \
		oid *hpvals = hp->payload; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hp_check_rehash(); \
			oid pvals = canditer_next(&ci); \
			assert(pvals != oid_nil); \
			hpvals[ppos[i]] = pvals; \
		} \
	} while (0)

#define addpld(Type) \
	do { \
		assert(BATcount(pld) == BATcount(pos)); \
		Type *pvals = Tloc(pld, 0); \
		Type *hpvals = hp->payload; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hp_check_rehash(); \
			hpvals[ppos[i]] = pvals[i]; \
		} \
	} while (0)

#define a_addpld() \
	do { \
		assert(BATcount(pld) == BATcount(pos)); \
		BATiter bi = bat_iterator(pld); \
		char **hpvals = hp->payload; \
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hp_check_rehash(); \
			void *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			hpvals[ppos[i]] = bpi; \
		} \
		bat_iterator_end(&bi); \
	} while (0)

#define a_addpld_(P) \
	do { \
		BATiter bi = bat_iterator(pld); \
		char **hpvals = hp->payload; \
		mallocator *ma = hp->allocators[P->wid]; \
		if (ATOMstorage(tt) == TYPE_str) { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				hp_check_rehash(); \
				char *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
				hpvals[ppos[i]] = ma_strdup(ma, bpi); \
			} \
		} else { /* other ATOMvarsized, e.g. BLOB */ \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				hp_check_rehash(); \
				char *bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
				hpvals[ppos[i]] = ma_copy(ma, bpi, hp->len(bpi)); \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHadd_pld(bat *hp_sink, const bat *payload, const bat *payload_pos, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H;
	bool local_storage = false;
	str err = NULL;
	BAT *res = NULL, *pld = NULL, *pos = NULL;

	assert(hp_sink && !is_bat_nil(*hp_sink));

	res = BATdescriptor(*hp_sink);
	pld = BATdescriptor(*payload);
	pos = BATdescriptor(*payload_pos);
	if (!res || !pld || !pos) {
		err = createException(MAL, "oahash.add_payload", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	hash_payload *hp = (hash_payload*)res->tsink;
	assert(hp && hp->s.type == OA_HASH_PAYLOAD_SINK);

	BUN cnt = BATcount(pld);
	if (cnt) {
		int tt = pld->ttype;
		gid *ppos = Tloc(pos, 0);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		switch(tt) {
			case TYPE_void:
				vaddpld();
				break;
			case TYPE_bit:
				addpld(bit);
				break;
			case TYPE_bte:
				addpld(bte);
				break;
			case TYPE_sht:
				addpld(sht);
				break;
			case TYPE_int:
				addpld(int);
				break;
			case TYPE_date:
				addpld(date);
				break;
			case TYPE_lng:
				addpld(lng);
				break;
			case TYPE_oid:
				if (BATtdense(pld))
					vaddpld();
				else
					addpld(oid);
				break;
			case TYPE_daytime:
				addpld(daytime);
				break;
			case TYPE_timestamp:
				addpld(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				addpld(hge);
				break;
#endif
			case TYPE_flt:
				addpld(flt);
				break;
			case TYPE_dbl:
				addpld(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATaprep_heap(pld, res, hp, "oahash.add_payload");
					if (local_storage) {
						a_addpld_(p);
					} else {
						a_addpld();
					}
				} else {
					err = createException(MAL, "oahash.add_payload", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
					goto error;
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.add_payload", RUNTIME_QRY_TIMEOUT));
	}
	if (err || p->p->status) {
		if (!err)
			err = createException(MAL, "oahash.add_payload", "pipeline execution error");
		goto error;
	}

	BBPkeepref(res);
	BBPunfix(pld->batCacheid);
	BBPunfix(pos->batCacheid);
	return MAL_SUCCEED;
error:
	BBPreclaim(res);
	BBPreclaim(pld);
	BBPreclaim(pos);
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
BAT_OAHASHhash_cmbd(bat *hsh, const bat *key, const bat *selected, const bat *parent_slotid, const ptr *H)
{
	BAT *h = NULL, *k = NULL, *s = NULL, *p = NULL;
	BUN cnt;
	str err = NULL;

	(void) H;

	k = BATdescriptor(*key);
	s = BATdescriptor(*selected);
	p = BATdescriptor(*parent_slotid);
	if (!k || !s || !p) {
		err = createException(SQL, "oahash.combined_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

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
		unsigned int prime = find_hash_prime(cnt);

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
	BATsetcount(h, cnt);
	BATnegateprops(h);
	*hsh = h->batCacheid;
	BBPkeepref(h);
	return MAL_SUCCEED;
error:
	BBPreclaim(h);
	BBPreclaim(k);
	BBPreclaim(s);
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
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			oid ky = canditer_next(&ci); \
			assert(ky != oid_nil); \
			if (!(*semantics) && ky == oid_nil) \
				continue; \
			\
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && vals[slot] != ky) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mtdcnt++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
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
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			if (!(*semantics) && is_##Type##_nil(ky[i])) \
				continue; \
			\
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && (!(is_##Type##_nil(ky[i]) && is_##Type##_nil(vals[slot])) && vals[slot] != ky[i])) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mtdcnt++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
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
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			char *val = (bi).vh->base+BUNtvaroff(bi,i); \
			if (!(*semantics) && atomcmp(val, nil) == 0) \
				continue; \
			\
			while (slot && atomcmp(vals[slot], val) != 0) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if ((slot?1:0) == match) { \
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mtdcnt++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
BAT_OAHASHprobe1(bat *LHS_matched, bat *RHS_slotid, const bat *LHS_key, const bat *LHS_hash, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H, bit match)
{
	BAT *m = NULL, *s = NULL, *k = NULL, *h = NULL, *t = NULL;
	BUN keycnt, mtdcnt = 0;
	str err = NULL;

	(void) H;

	k = BATdescriptor(*LHS_key);
	h = BATdescriptor(*LHS_hash);
	t = BATdescriptor(*RHS_ht);
	if (!k || !h || !t) {
		err = createException(SQL, "oahash.probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	keycnt = BATcount(k);
	m = COLnew(k->hseqbase, TYPE_oid, keycnt, TRANSIENT);
	s = COLnew(k->hseqbase, TYPE_oid, keycnt, TRANSIENT);
	if (!m || !s) {
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
	BATsetcount(m, mtdcnt);
	BATsetcount(s, mtdcnt);
	BATnegateprops(m);
	BATnegateprops(s);
	m->tnonil = true;
	s->tnonil = true;
	m->tsorted = true;
	BATkey(m, true);
	*LHS_matched = m->batCacheid;
	*RHS_slotid = s->batCacheid;
	BBPkeepref(m);
	BBPkeepref(s);
	return MAL_SUCCEED;
error:
	BBPreclaim(m);
	BBPreclaim(s);
	BBPreclaim(k);
	BBPreclaim(h);
	BBPreclaim(t);
	return err;
}

static str
BAT_OAHASHprobe(bat *LHS_matched, bat *RHS_slotid, const bat *LHS_key, const bat *LHS_hash, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHprobe1(LHS_matched, RHS_slotid, LHS_key, LHS_hash, RHS_ht, single, semantics, H, true);
}

static str
BAT_OAHASHnprobe(bat *LHS_matched, bat *RHS_slotid, const bat *LHS_key, const bat *LHS_hash, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHprobe1(LHS_matched, RHS_slotid, LHS_key, LHS_hash, RHS_ht, single, semantics, H, false);
}


#define BATvprobe_cmbd() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		\
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(m, 0); \
		oid *vals = ht->vals; \
		oid *mtd = Tloc(res_m, 0); \
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
				mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
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
		oid *mt = Tloc(m, 0); \
		Type *vals = ht->vals; \
		oid *mtd = Tloc(res_m, 0); \
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
				mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
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
		oid *mt = Tloc(m, 0); \
		char **vals = ht->vals; \
		oid *mtd = Tloc(res_m, 0); \
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
				mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
BAT_OAHASHprobe_cmbd(bat *LHS_matched, bat *RHS_slotid, const bat *LHS_key, const bat *LHS_hash, const bat *LHS_selected, const bat *RHS_gid, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H)
{
	BAT *res_m = NULL, *res_s = NULL, *k = NULL, *h = NULL, *m = NULL, *t = NULL, *p = NULL;
	BUN mtdcnt, mtdcnt2 = 0;
	str err = NULL;

	(void) H;

	k = BATdescriptor(*LHS_key);
	h = BATdescriptor(*LHS_hash);
	m = BATdescriptor(*LHS_selected);
	p = BATdescriptor(*RHS_gid);
	t = BATdescriptor(*RHS_ht);
	if (!k || !h || !m || !t || !p) {
		err = createException(SQL, "oahash.combined_probe", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	mtdcnt = BATcount(m);
	res_m = COLnew(k->hseqbase, TYPE_oid, mtdcnt, TRANSIENT);
	res_s = COLnew(k->hseqbase, TYPE_oid, mtdcnt, TRANSIENT);
	if (!res_m || !res_s) {
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
	BBPunfix(m->batCacheid);
	BBPunfix(t->batCacheid);
	BBPunfix(p->batCacheid);
	BATsetcount(res_m, mtdcnt2);
	BATsetcount(res_s, mtdcnt2);
	BATnegateprops(res_m);
	BATnegateprops(res_s);
	res_m->tnonil = true;
	res_s->tnonil = true;
	res_m->tsorted = true;
	BATkey(res_m, true);
	*LHS_matched = res_m->batCacheid;
	*RHS_slotid = res_s->batCacheid;
	BBPkeepref(res_m);
	BBPkeepref(res_s);
	return MAL_SUCCEED;
error:
	BBPreclaim(res_m);
	BBPreclaim(res_s);
	BBPreclaim(k);
	BBPreclaim(h);
	BBPreclaim(m);
	BBPreclaim(t);
	BBPreclaim(p);
	return err;
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
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
			oid val = canditer_idx(&ci, sel[i]); \
			assert(val != oid_nil); \
			\
			gid freq = (gid)ht->frequency[sid[i]]; \
			TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
				res[idx++] = val; \
			} \
		} \
		if (*outer) { \
			for (BUN i = 0, j = 0; i < keycnt; i++) { \
				if (j < selcnt && i == sel[j]) { \
					j++; \
				} else { \
					oid val = canditer_idx(&ci, i); \
					(void)val;\
					assert(val != oid_nil); \
					res[idx++] = i; \
				} \
			} \
		} \
	} while (0)

#define BATexpand(Type) \
	do { \
		Type *val = Tloc(k, 0); \
		Type *res = Tloc(e, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
			Type v = val[sel[i]]; \
			gid freq = (gid)ht->frequency[sid[i]]; \
			TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
				res[idx++] = v; \
			} \
		} \
		if (*outer) { \
			for (BUN i = 0, j = 0; i < keycnt; i++) { \
				if (j < selcnt && i == sel[j]) j++; \
				else res[idx++] = val[i]; \
			} \
		} \
	} while (0)

#define BATaexpand() \
	do { \
		BATiter bi = bat_iterator(k); \
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
			void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,sel[i])); \
			gid freq = (gid)ht->frequency[sid[i]]; \
			TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
				if (BUNappend(e, v, false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				} \
				idx++; \
			} \
		} \
		if (*outer) { \
			for (BUN i = 0, j = 0; i < keycnt; i++) { \
				if (j < selcnt && i == sel[j]) { \
					j++; \
				} else { \
					void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
					if (BUNappend(e, v, false) != GDK_SUCCEED) { \
						err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						break; \
					} \
					idx++; \
				} \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

#if 0
// TODO maybe we should use BUNappend iso of res[idx]?
#define aexpand_(P) \
	do { \
		BATiter bi = bat_iterator(k); \
		char **res = Tloc(e, 0); \
		mallocator *ma = ht->allocators[P->wid]; \
		if (ATOMstorage(tt) == TYPE_str) { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,sel[i])); \
				gid freq = (gid)ht->frequency[sid[i]]; \
				TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
					res[idx++] = ma_strdup(ma, v); \
				} \
			} \
		} else { /* other ATOMvarsized, e.g. BLOB */ \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,sel[i])); \
				gid freq = (gid)ht->frequency[sid[i]]; \
				TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
					res[idx++] = ma_copy(ma, v, ht->len(v)); \
				} \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)
#endif

static str
BAT_OAHASHexpand(bat *expanded, const bat *key, const bat *selected, const bat *slotid, const bat *freq_sink, const bit *outer, const ptr *H)
{
	BAT *e = NULL, *k = NULL, *s = NULL, *l = NULL, *h = NULL;
	BUN keycnt, selcnt, ttlcnt = 0, xpdcnt = 0;
	str err = NULL;

	k = BATdescriptor(*key);
	s = BATdescriptor(*selected);
	l = BATdescriptor(*slotid);
	h = BATdescriptor(*freq_sink);
	if (!k || !s || !l || !h) {
		err = createException(SQL, "oahash.expand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	assert(BATcount(s) == BATcount(l));
	assert(BATcount(s) <= BATcount(k));

	gid *sid = Tloc(l, 0);
	hash_table *ht = (hash_table*)h->tsink;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	keycnt = BATcount(k);
	selcnt = BATcount(s);
	if (selcnt) {
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
			xpdcnt += ht->frequency[sid[i]];
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}
	ttlcnt = xpdcnt;
	if (*outer) {
		ttlcnt += (keycnt - selcnt);
	}

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

	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(h->batCacheid);

	BATsetcount(e, ttlcnt);
	BATnegateprops(e);
	*expanded = e->batCacheid;
	BBPkeepref(e);

	(void) H;
	return MAL_SUCCEED;
error:
	BBPreclaim(e);
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(l);
	BBPreclaim(h);
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
	switch(tt) {
		case TYPE_void:
			vexpand_cart();
			break;
		case TYPE_bit:
			expand_cart(bit);
			break;
		case TYPE_bte:
			expand_cart(bte);
			break;
		case TYPE_sht:
			expand_cart(sht);
			break;
		case TYPE_int:
			expand_cart(int);
			break;
		case TYPE_date:
			expand_cart(date);
			break;
		case TYPE_lng:
			expand_cart(lng);
			break;
		case TYPE_oid:
			if (BATtdense(k))
				vexpand_cart();
			else
				expand_cart(oid);
			break;
		case TYPE_daytime:
			expand_cart(daytime);
			break;
		case TYPE_timestamp:
			expand_cart(timestamp);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			expand_cart(hge);
			break;
#endif
		case TYPE_flt:
			expand_cart(flt);
			break;
		case TYPE_dbl:
			expand_cart(dbl);
			break;
		default:
			if (ATOMvarsized(tt)) {
				aexpand_cart();
			} else {
				err = createException(MAL, "oahash.expand_cart", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
			}
	}
	TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand_cart", RUNTIME_QRY_TIMEOUT));
	if (err)
		goto error;

	assert(idx == ttlcnt);

	BBPunfix(k->batCacheid);
	BBPunfix(d->batCacheid);

	BATsetcount(e, ttlcnt);
	BATnegateprops(e);
	*expanded = e->batCacheid;
	BBPkeepref(e);
	return MAL_SUCCEED;
error:
	BBPreclaim(e);
	BBPreclaim(k);
	BBPreclaim(d);
	return err;
}

#if 0
#define BATvfetch() \
	do { \
		oid val = ((oid*)hp->payload)[0]; \
		oid *res = Tloc(f, 0); \
		TIMEOUT_LOOP_IDX(idx, rescnt, qry_ctx) { \
			res[idx] = val; \
		} \
	} while (0)
#endif

#define BATfetch(Type) \
	do { \
		int prime = hash_prime_nr[ht->bits-5]; \
		Type *vals = hp->payload; \
		Type *res = Tloc(f, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
			gid freq = (gid)ht->frequency[sid[i]]; \
			TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
				gid hsh = (gid)combine(j, _hash_lng(sid[i]), prime)&ht->mask; \
				res[idx++] = vals[hsh]; \
			} \
		} \
		if (*outer) { \
			for (BUN i = fchcnt; i < ttlcnt; i++) \
				res[idx++] = Type##_nil; \
		} \
	} while (0)

#define BATafetch() \
	do { \
		int prime = hash_prime_nr[ht->bits-5]; \
		char **vals = hp->payload; \
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
			gid freq = (gid)ht->frequency[sid[i]]; \
			TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
				gid hsh = (gid)combine(j, _hash_lng(sid[i]), prime)&ht->mask; \
				if (BUNappend(f, vals[hsh], false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				} \
				idx++; \
			} \
		} \
		if (*outer) { \
			for (BUN i = fchcnt; i < ttlcnt; i++) { \
				if (BUNappend(f, ATOMnilptr(tt), false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				} \
				idx++; \
			} \
		} \
	} while (0)

static str
BAT_OAHASHfetch_pld(bat *fetched, const bat *hp_sink, const bat *slotid, const bat *freq_sink, const lng *norows_prb, const bit *outer, const ptr *H)
{
	BAT *f = NULL, *l = NULL, *hps = NULL, *hts = NULL;
	BUN nllcnt, selcnt, ttlcnt = 0, fchcnt =  0;
	str err = NULL;

	l = BATdescriptor(*slotid);
	hps = BATdescriptor(*hp_sink);
	hts = BATdescriptor(*freq_sink);
	if (!l || !hps || !hts) {
		err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	gid *sid = Tloc(l, 0);
	hash_payload *hp = (hash_payload*)hps->tsink;
	hash_table *ht = (hash_table*)hts->tsink;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	selcnt = BATcount(l);
	if (selcnt) {
		TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
			fchcnt += ht->frequency[sid[i]];
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.fetch_payload", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;
	}
	ttlcnt = fchcnt;
	if (*outer) {
		nllcnt = (*norows_prb) - selcnt;
		ttlcnt += nllcnt;
	}

	int tt = hp->type;
	f = COLnew(0, tt?tt:TYPE_oid, ttlcnt, TRANSIENT);
	if (!f) {
		err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (ttlcnt) {
		BUN idx = 0;

		switch(tt) {
			case TYPE_void:
				BATfetch(oid);
				break;
			case TYPE_bit:
				BATfetch(bit);
				break;
			case TYPE_bte:
				BATfetch(bte);
				break;
			case TYPE_sht:
				BATfetch(sht);
				break;
			case TYPE_int:
				BATfetch(int);
				break;
			case TYPE_date:
				BATfetch(date);
				break;
			case TYPE_lng:
				BATfetch(lng);
				break;
			case TYPE_oid:
				BATfetch(oid);
				break;
			case TYPE_daytime:
				BATfetch(daytime);
				break;
			case TYPE_timestamp:
				BATfetch(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				BATfetch(hge);
				break;
#endif
			case TYPE_flt:
				BATfetch(flt);
				break;
			case TYPE_dbl:
				BATfetch(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					BATafetch();
				} else {
					err = createException(MAL, "oahash.fetch_payload", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.fetch_payload", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;

		assert(idx == ttlcnt);
	}

	BBPunfix(l->batCacheid);
	BBPunfix(hps->batCacheid);
	BBPunfix(hts->batCacheid);

	f->tseqbase = 0;
	BATsetcount(f, ttlcnt);
	BATnegateprops(f);
	*fetched = f->batCacheid;
	BBPkeepref(f);

	(void) H;
	return MAL_SUCCEED;
error:
	BBPreclaim(f);
	BBPreclaim(l);
	BBPreclaim(hps);
	BBPreclaim(hts);
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

#define afetch_cart() \
	do { \
		if (append_nulls) { \
			TIMEOUT_LOOP_IDX_DECL(i, repcnt, qry_ctx) { \
				if (BUNappend(f, str_nil, false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
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
						err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
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
	bit append_nulls = false;
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
				if (ATOMvarsized(tt)) {
					afetch_cart();
				} else {
					err = createException(MAL, "oahash.fetch_payload_cartesian", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
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
 pattern("oahash", "new", OAHASHnew, false, "", args(1,4, batargany("ht_sink",1),argany("tt",1),arg("size",int),arg("freq",bit))),
 pattern("hash", "ext", UHASHext, false, "", args(1,2, batarg("ext", oid), batargany("in", 1))),

 pattern("oahash", "new", OAHASHnew, false, "", args(1,5, batargany("ht_sink",1),argany("tt",1),arg("size",int),arg("freq",bit),batargany("p",2))),
 pattern("oahash", "new_payload", OAHASHnew_pld, false, "", args(1,5, batargany("hp_sink",1),argany("tt",1),arg("nr_payloads",int),batargany("parent",2), batargany("dummy",3))),

 command("oahash", "build_table", BAT_OAHASHbuild_tbl, false, "Build a hash table for the keys. Returns the slot IDs and the sink containing the hash table", args(2,4, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),arg("pipeline",ptr))),

 command("oahash", "build_combined_table", OAHASHbuild_tbl_cmbd, false, "Build a hash table for the keys in combination with the hash table of its parent column. Returns the slot IDs and the sink containing the hash table", args(2,6, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),batarg("parent_slotid",oid),batargany("parent_ht",2),arg("pipeline",ptr))),

 command("oahash", "compute_frequencies", OAHASHcmpt_freq, false, "Compute the frequencies of the slot IDs and store them in the hash-table", args(1,3, batargany("ht_sink",1),batarg("slot_id",oid),arg("pipeline",ptr))),
 command("oahash", "compute_frequencies", OAHASHcmpt_freq_pos, false, "Compute the frequencies of the slot IDs and store them in the hash-table. Return combined_hash(slot_id, freq) for payload_pos", args(2,4, batarg("payload_pos",oid),batargany("ht_sink",1),batarg("slot_id",oid),arg("pipeline",ptr))),

 command("oahash", "add_payload", OAHASHadd_pld, false, "Add 'payload' at 'position' in 'hp_sink'", args(1,4, batargany("hp_sink",1),batargany("payload",1),batarg("payload_pos",oid),arg("pipeline",ptr))),

 command("oahash", "hash", BAT_OAHASHhash, false, "Compute the hashs for the keys", args(1,3, batarg("hsh",lng),batargany("key",1),arg("pipeline",ptr))),

 command("oahash", "probe", BAT_OAHASHprobe, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,8, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batarg("LHS_hash",lng),batargany("RHS_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "nprobe", BAT_OAHASHnprobe, false, "Probe the (key, hash) pairs in the hash table. For a not-matched key, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,8, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batarg("LHS_hash",lng),batargany("RHS_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "combined_hash", BAT_OAHASHhash_cmbd, false, "For the selected keys, compute the combined hash of key+parent_slotid", args(1,5, batarg("hsh",lng),batargany("key",1),batarg("selected",oid),batarg("parent_slotid",oid),arg("pipeline",ptr))),

 command("oahash", "combined_probe", BAT_OAHASHprobe_cmbd, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,10, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batarg("LHS_hash",lng),batarg("LHS_selected",oid),batarg("RHS_pgids", oid), batargany("RHS_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "project", OAHASHproject, false, "Project the selected OIDs onto the keys", args(1,4, batargany("res",1),batargany("key",1),batarg("selected",oid),arg("pipeline",ptr))),

 command("oahash", "expand", BAT_OAHASHexpand, false, "Expand the selected keys according to their frequencies in the hash table. If 'outer' is true, append the not 'selected' keys", args(1,7, batargany("expanded",1),batargany("key",1),batarg("selected",oid),batarg("slotid",oid),batargany("freq_sink",2),arg("outer",bit),arg("pipeline",ptr))),
 command("oahash", "expand_cartesian", BAT_OAHASHexpand_cart, false, "Duplicate each value in 'col' the number of times as the count of 'rowrepeat'. For a left/right-outer join, if 'rowrepeat' is empty, output the values in 'col' once.", args(1,5, batargany("expanded",1),batargany("col",1),batargany("rowrepeat",2),arg("LRouter",bit),arg("pipeline",ptr))),

 command("oahash", "fetch_payload", BAT_OAHASHfetch_pld, false, "Fetch the hash-payloads correspond to the slot IDs and expand them according to their frequencies in the hash table. If 'outer' is true, append NULLs for the unmatched keys", args(1,7, batargany("fetched",1),batargany("hp_sink",1),batarg("slotid",oid),batargany("freq_sink",2),arg("norows_prb",lng),arg("outer",bit),arg("pipeline",ptr))),

 command("oahash", "fetch_payload_cartesian", BAT_OAHASHfetch_pld_cart, false, "Duplicate the whole 'col' the number of times as the count of 'setrepeat'.  For a left/right-ourter join, if 'col' is empty, output NULLs.", args(1,5, batargany("fetched",1),batargany("col",1),batarg("setrepeat",2),arg("LRouter",bit),arg("pipeline",ptr))),

 command("oahash", "no_slices", OAHASHno_slices, false, "Get the number of slices for this hashtable.", args(1,2, arg("slices", int), batargany("ht_sink", 1))),
 command("oahash", "nth_slice", OAHASHnth_slice, false, "Get the nth slice of this hashtable.", args(2,3, batarg("slice", oid), batargany("ht_sink", 1), arg("slice_nr", int))),

 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("oa_hash", NULL, oa_hash_init_funcs); }
