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
			BBPunfix(ht->pinned[i]->parentid);
			HEAPdecref(ht->pinned[i], false);
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
	h->rehash = false;
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
	ht->rehash = true;
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

		h->empty = false;
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
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) {
			ppos[i] = ATOMIC_ADD(&freqs[sltid[i]], 1);
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
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			oid ky = canditer_next(&ci); \
			assert(ky != oid_nil); \
			if (!(*semantics) && ky == oid_nil) { \
				if (!match && empty) { \
					mtd[mtdcnt] = i; \
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
			if (!(*semantics) && is_##Type##_nil(ky[i])) { \
				if (!match && empty) { \
					mtd[mtdcnt] = i; \
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
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = match?(oid)(slot - 1):oid_nil; \
				mtdcnt++; \
				if (match && *single && ht->frequency[slot - 1] > 1) { \
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
			char *val = (bi).vh->base+BUNtvaroff(bi,i); \
			if (!(*semantics) && atomcmp(val, nil) == 0) { \
				if (!match && empty) { \
					mtd[mtdcnt] = i; \
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
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = match?(oid)(slot - 1):oid_nil; \
				mtdcnt++; \
				if (match && *single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					bat_iterator_end(&bi); \
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
	BATsetcount(m, mtdcnt);
	BATsetcount(s, mtdcnt);
	BATnegateprops(m);
	BATnegateprops(s);
	m->tnonil = true;
	s->tnonil = match?true:false;
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

#define BATvoprobe() \
	do { \
		struct canditer ci; \
		canditer_init(&ci, NULL, k); \
		keycnt = ci.ncand; \
		\
		gid *hs = Tloc(h, 0); \
		oid *vals = ht->vals; \
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		bit *mark = Tloc(o, 0); \
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
				mtd[mtdcnt] = i; \
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
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mark[i] = true; \
				mtdcnt++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				mtd[mtdcnt] = i; \
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
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		bit *mark = Tloc(o, 0); \
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
				mtd[mtdcnt] = i; \
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
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = (oid)(slot - 1); \
				mark[i] = true; \
				mtdcnt++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				mtd[mtdcnt] = i; \
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
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		bit *mark = Tloc(o, 0); \
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
				mtd[mtdcnt] = i; \
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
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = (oid)(slot - 1); \
				mark[i] = true; \
				mtdcnt++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.oprobe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} else { \
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = oid_nil; \
				mark[i] = (any)?has_nil:false; \
				mtdcnt++; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
BAT_OAHASHomprobe(bat *LHS_matched, bat *RHS_slotid, bat *outer, const bat *LHS_key, const bat *LHS_hash, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H, bool any)
{
	BAT *m = NULL, *s = NULL, *o = NULL, *k = NULL, *h = NULL, *t = NULL;
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
	o = COLnew(k->hseqbase, TYPE_bit, keycnt, TRANSIENT);
	if (!m || !s || !o) {
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
	BATsetcount(m, mtdcnt);
	BATsetcount(s, mtdcnt);
	BATsetcount(o, keycnt); /* aligned with input key */
	BATnegateprops(m);
	BATnegateprops(s);
	BATnegateprops(o);
	m->tnonil = true;
	s->tnonil = false;
	m->tsorted = true;
	BATkey(m, true);
	*LHS_matched = m->batCacheid;
	*RHS_slotid = s->batCacheid;
	*outer = o->batCacheid;
	BBPkeepref(m);
	BBPkeepref(s);
	BBPkeepref(o);
	return MAL_SUCCEED;
error:
	BBPreclaim(m);
	BBPreclaim(s);
	BBPreclaim(o);
	BBPreclaim(k);
	BBPreclaim(h);
	BBPreclaim(t);
	return err;
}

static str
BAT_OAHASHoprobe(bat *LHS_matched, bat *RHS_slotid, bat *outer, const bat *LHS_key, const bat *LHS_hash, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe(LHS_matched, RHS_slotid, outer, LHS_key, LHS_hash, RHS_ht, single, semantics, H, false);
}

static str
BAT_OAHASHmprobe(bat *LHS_matched, bat *RHS_slotid, bat *outer, const bat *LHS_key, const bat *LHS_hash, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe(LHS_matched, RHS_slotid, outer, LHS_key, LHS_hash, RHS_ht, single, semantics, H, true);
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
					bat_iterator_end(&bi); \
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

#define BATvoprobe_cmbd() \
	do { \
		unsigned int prime = hash_prime_nr[ht->bits-5]; \
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
		bit *mark = Tloc(res_o, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			oid ky = canditer_idx(&ci, mt[i]); \
			assert(ky != oid_nil); \
			if (!mark[i] || (!(*semantics) && ky == oid_nil)) { \
				mtd[mtdcnt2] = i; \
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
				mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				mtd[mtdcnt2] = i; \
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
		oid *mtd = Tloc(res_m, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		bit *mark = Tloc(res_o, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			Type val = ky[mt[i]]; \
			if (!mark[i] || (!(*semantics) && is_##Type##_nil(val))) { \
				mtd[mtdcnt2] = i; \
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
				mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					goto error; \
				} \
			} else { \
				mtd[mtdcnt2] = i; \
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
		oid *mtd = Tloc(res_m, 0); \
		oid *slt = Tloc(res_s, 0); \
		lng *gi = Tloc(p, 0); \
		lng *pgids = ht->pgids; \
		bit *mark = Tloc(res_o, 0); \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		const void *nil = ATOMnilptr(tt); \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[i]&ht->mask; \
			gid slot = 0; \
			char *val = (bi).vh->base+BUNtvaroff(bi,mt[i]); \
			if (!mark[i] || (!(*semantics) && atomcmp(val, nil) == 0)) { \
				mtd[mtdcnt2] = i; \
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
				mtd[mtdcnt2] = mt[i]; \
				slt[mtdcnt2] = slot - 1; \
				mark[i] = true; \
				mtdcnt2++; \
				if (*single && ht->frequency[slot - 1] > 1) { \
					err = createException(SQL, "oahash.combined_probe", "more than one match"); \
					bat_iterator_end(&bi); \
					goto error; \
				} \
			} else { \
				mtd[mtdcnt2] = i; \
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
BAT_OAHASHomprobe_cmbd(bat *LHS_matched, bat *RHS_slotid, bat *outer, const bat *LHS_key, const bat *LHS_hash, const bat *LHS_selected, const bat *RHS_gid, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H, bool any)
{
	BAT *res_m = NULL, *res_s = NULL, *res_o = NULL, *k = NULL, *h = NULL, *m = NULL, *t = NULL, *p = NULL;
	BUN mtdcnt, mtdcnt2 = 0;
	str err = NULL;

	(void) H;

	k = BATdescriptor(*LHS_key);
	h = BATdescriptor(*LHS_hash);
	m = BATdescriptor(*LHS_selected);
	p = BATdescriptor(*RHS_gid);
	t = BATdescriptor(*RHS_ht);
	res_o = BATdescriptor(*outer);
	if (!k || !h || !m || !t || !p || !res_o) {
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

	assert(BATcount(res_o) == BATcount(k));
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
	res_s->tnil = true;
	res_m->tsorted = true;
	BATkey(res_m, true);
	*LHS_matched = res_m->batCacheid;
	*RHS_slotid = res_s->batCacheid;
	BBPkeepref(res_m);
	BBPkeepref(res_s);
	BBPkeepref(res_o);
	return MAL_SUCCEED;
error:
	BBPreclaim(res_m);
	BBPreclaim(res_s);
	BBPreclaim(res_o);
	BBPreclaim(k);
	BBPreclaim(h);
	BBPreclaim(m);
	BBPreclaim(t);
	BBPreclaim(p);
	return err;
}

static str
BAT_OAHASHoprobe_cmbd(bat *LHS_matched, bat *RHS_slotid, bat *outer, const bat *LHS_key, const bat *LHS_hash, const bat *LHS_selected, const bat *RHS_gid, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe_cmbd( LHS_matched, RHS_slotid, outer, LHS_key, LHS_hash, LHS_selected, RHS_gid, RHS_ht, single, semantics, H, false);
}

static str
BAT_OAHASHmprobe_cmbd(bat *LHS_matched, bat *RHS_slotid, bat *outer, const bat *LHS_key, const bat *LHS_hash, const bat *LHS_selected, const bat *RHS_gid, const bat *RHS_ht, const bit *single, const bit *semantics, const ptr *H)
{
	return BAT_OAHASHomprobe_cmbd( LHS_matched, RHS_slotid, outer, LHS_key, LHS_hash, LHS_selected, RHS_gid, RHS_ht, single, semantics, H, true);
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
		if (*outer && ht->frequency) { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
				oid s = sid[i]; \
				oid val = canditer_idx(&ci, i); \
				assert(val != oid_nil); \
				if (s != oid_nil) {\
					gid freq = (gid)ht->frequency[s]; \
					freq = freq?freq:1; \
					TIMEOUT_LOOP_IDX_DECL(f, freq, qry_ctx) { \
						res[idx++] = val; \
					} \
				} else { \
					res[idx++] = val; \
				} \
			} \
		} else if (ht->frequency) { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				oid val = canditer_idx(&ci, sel[i]); \
				gid freq = (gid)ht->frequency[sid[i]]; \
				freq = freq?freq:1; \
				TIMEOUT_LOOP_IDX_DECL(f, freq, qry_ctx) { \
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
		if (*outer && ht->frequency) { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
				oid s = sid[i]; \
				Type v = val[i]; \
				if (s != oid_nil) {\
					gid freq = (gid)ht->frequency[s]; \
					freq = freq?freq:1; \
					TIMEOUT_LOOP_IDX_DECL(f, freq, qry_ctx) { \
						res[idx++] = v; \
					} \
				} else { \
					res[idx++] = v; \
				} \
			} \
		} else if (ht->frequency) { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				Type v = val[sel[i]]; \
				gid freq = (gid)ht->frequency[sid[i]]; \
				freq = freq?freq:1; \
				TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
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
		if (*outer && ht->frequency) { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
				oid s = sid[i]; \
				void *v =  (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
				if (s != oid_nil) {\
					gid freq = (gid)ht->frequency[s]; \
					TIMEOUT_LOOP_IDX_DECL(f, freq, qry_ctx) { \
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
		} else if (ht->frequency) { \
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
		if (ht->frequency) {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
				if (sid[i] != lng_nil && ht->frequency[sid[i]])
					xpdcnt += ht->frequency[sid[i]];
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
	BBPunfix(h->batCacheid);

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
BAT_OAHASHexplode(bat *fetched, const bat *slotid, const bat *freq_sink, const bat *ht_sink, const bit *outer)
{
	BAT *f = NULL, *l = NULL, *h = NULL, *r = NULL;
	BUN selcnt, fchcnt = 0;
	str err = NULL;

	l = BATdescriptor(*slotid);
	f = BATdescriptor(*freq_sink);
	h = BATdescriptor(*ht_sink);
	if (!l || !f || !h) {
		err = createException(SQL, "oahash.explode", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	oid *sid = Tloc(l, 0);
	hash_table *ft = (hash_table*)f->tsink;
	hash_table *ht = (hash_table*)h->tsink;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	selcnt = BATcount(l);
	if (selcnt) {
		if (*outer) {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
				if (sid[i] != oid_nil)
					fchcnt += ft->frequency[sid[i]];
				else
					fchcnt++;
			}
		} else {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx)
				fchcnt += ft->frequency[sid[i]];
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
		if (*outer) {
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) {
				oid s = sid[i];
				if (s != oid_nil) {
					gid freq = (gid)ft->frequency[s];
					TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) {
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
				gid freq = (gid)ft->frequency[s];
				TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) {
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

#define afetch_cart() \
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

 command("oahash", "build_table", BAT_OAHASHbuild_tbl, false, "Build a hash table for the keys. Returns the slot IDs and the sink containing the hash table", args(2,4, batarg("slot_id",oid), batargany("ht_sink",1), batargany("key",1), arg("pipeline",ptr))),

 command("oahash", "build_combined_table", OAHASHbuild_tbl_cmbd, false, "Build a hash table for the keys in combination with the hash table of its parent column. Returns the slot IDs and the sink containing the hash table", args(2,5, batarg("slot_id",oid), batargany("ht_sink",1), batargany("key",1), batarg("parent_slotid",oid), arg("pipeline",ptr))),

 command("oahash", "compute_frequencies", OAHASHcmpt_freq, false, "Compute the frequencies of the slot IDs and store them in the hash-table", args(1,3, batargany("ht_sink",1),batarg("slot_id",oid),arg("pipeline",ptr))),
 command("oahash", "compute_frequencies", OAHASHcmpt_freq_pos, false, "Compute the frequencies of the slot IDs and store them in the hash-table. Return combined_hash(slot_id, freq) for payload_pos", args(2,4, batarg("payload_pos",oid), batargany("ht_sink",1), batarg("slot_id",oid), arg("pipeline",ptr))),

 command("oahash", "hash", BAT_OAHASHhash, false, "Compute the hashs for the keys", args(1,3, batarg("hsh",lng),batargany("key",1),arg("pipeline",ptr))),

 command("oahash", "probe", BAT_OAHASHprobe, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,8, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batarg("LHS_hash",lng),batargany("RHS_ht",2),arg("single",bit),arg("semantics",bit),arg("pipeline",ptr))),

 command("oahash", "nprobe", BAT_OAHASHnprobe, false, "Probe the (key, hash) pairs in the hash table. For a not-matched key, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,8, batarg("LHS_matched",oid), batarg("RHS_slotid",oid), batargany("LHS_key",1), batarg("LHS_hash",lng), batargany("RHS_ht",2), arg("single",bit), arg("semantics",bit), arg("pipeline",ptr))),

 command("oahash", "oprobe", BAT_OAHASHoprobe, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(3,9, batarg("LHS_matched",oid), batarg("RHS_slotid",oid), batarg("outer", bit), batargany("LHS_key",1), batarg("LHS_hash",lng), batargany("RHS_ht",2), arg("single",bit), arg("semantics",bit), arg("pipeline",ptr))),

 command("oahash", "mprobe", BAT_OAHASHmprobe, false, "Probe the (key, hash) pairs in the hash table. For a matched key, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(3,9, batarg("LHS_matched",oid), batarg("RHS_slotid",oid), batarg("mark", bit), batargany("LHS_key",1), batarg("LHS_hash",lng), batargany("RHS_ht",2), arg("single",bit), arg("semantics",bit), arg("pipeline",ptr))),

 command("oahash", "combined_hash", BAT_OAHASHhash_cmbd, false, "For the selected keys, compute the combined hash of key+parent_slotid", args(1,5, batarg("hsh",lng),batargany("key",1),batarg("selected",oid),batarg("parent_slotid",oid),batargany("ht_sink",2))),

 command("oahash", "combined_probe", BAT_OAHASHprobe_cmbd, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,10, batarg("LHS_matched",oid), batarg("RHS_slotid",oid), batargany("LHS_key",1), batarg("LHS_hash",lng), batarg("LHS_selected",oid), batarg("RHS_pgids", oid), batargany("RHS_ht",2), arg("single",bit), arg("semantics",bit), arg("pipeline",ptr))),

 command("oahash", "combined_oprobe", BAT_OAHASHoprobe_cmbd, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(3,11, batarg("LHS_matched",oid), batarg("RHS_slotid",oid), batarg("outer", bit), batargany("LHS_key",1), batarg("LHS_hash",lng), batarg("LHS_selected",oid), batarg("RHS_pgids", oid), batargany("RHS_ht",2), arg("single",bit), arg("semantics",bit), arg("pipeline",ptr))),

 command("oahash", "combined_mprobe", BAT_OAHASHmprobe_cmbd, false, "Probe the selected (key, hash) pairs in the hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(3,11, batarg("LHS_matched",oid), batarg("RHS_slotid",oid), batarg("mark", bit), batargany("LHS_key",1), batarg("LHS_hash",lng), batarg("LHS_selected",oid), batarg("RHS_pgids", oid), batargany("RHS_ht",2), arg("single",bit), arg("semantics",bit), arg("pipeline",ptr))),

 command("oahash", "project", OAHASHproject, false, "Project the selected OIDs onto the keys", args(1,4, batargany("res",1),batargany("key",1),batarg("selected",oid),arg("pipeline",ptr))),

 command("oahash", "expand", BAT_OAHASHexpand, false, "Expand the selected keys according to their frequencies in the hash table. If 'outer' is true, append the not 'selected' keys", args(1,7, batargany("expanded",1),batargany("key",1),batarg("selected",oid),batarg("slotid",oid),batargany("freq_sink",2),arg("outer",bit),arg("pipeline",ptr))),

 command("oahash", "expand_cartesian", BAT_OAHASHexpand_cart, false, "Duplicate each value in 'col' the number of times as the count of 'rowrepeat'. For a left/right-outer join, if 'rowrepeat' is empty, output the values in 'col' once.", args(1,5, batargany("expanded",1),batargany("col",1),batargany("rowrepeat",2),arg("LRouter",bit),arg("pipeline",ptr))),

 command("oahash", "explode", BAT_OAHASHexplode, false, "Explode the result vector 'frequency' times and return payload heap slot ids.", args(1,5, batarg("fetched",oid), batarg("slotid",oid), batargany("freq_sink", 1), batargany("hash_sink", 2), arg("outer",bit))),

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
