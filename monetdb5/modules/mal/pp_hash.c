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
str_cmp(str s1, str s2)
{
	return strcmp(s1,s2);
}

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

static unsigned int
log_base2(unsigned int n)
{
	unsigned int l ;

	for (l = 0; n; l++) {
		n >>= 1 ;
	}
	return l ;
}

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
ht_create(int type, size_t size, bool freq, hash_table *p)
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
		parent = (hash_table*)pht->T.sink;
	}

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	if (b == NULL) {
		BBPreclaim(pht);
		return createException(MAL, "oahash.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	b->T.sink = (Sink*)ht_create(tt, size*1.2*2.1, freq, parent);
	BBPreclaim(pht);
	if (b->T.sink == NULL) {
		BBPunfix(b->batCacheid);
		return createException(MAL, "oahash.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*res = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

/* ***** HASH PAYLOAD ***** */
static void
hp_destroy(hash_payload *hp)
{
	if (hp->payload)
		GDKfree(hp->payload);
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
	if (atype == TYPE_str) {
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
OAHASHnew_payload(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
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
	hash_table *parent = (hash_table*)prnt->T.sink;

	b->T.sink = (Sink *)hp_create(tt, nplds*1.2*2.1, parent);
	if (!b->T.sink) {
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

#define aprep_heap(BT, SB, SK, FName) \
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
		} else { \
			MT_lock_unset(&BT->theaplock); \
			MT_lock_unset(&SB->theaplock); \
		} \
	} while(0)

#define PRE_CLAIM 256
#define group(Type) \
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

#define vgroup() \
	do { \
		assert(BATtdense(b) || \
				/* A not-dense void BAT must contain only oid_nil.
				 * Otherwise it's a candidate list, which should never have
				 * reached this place. */ \
				(!BATtdense(b) && b->tseqbase == oid_nil && cnt)); \
		\
		int slots = 0; \
		gid slot = 0; \
		oid bpi = b->tseqbase; \
		oid *vals = h->vals; \
		if (!BATtdense(b)) { \
			bool fnd = 0; \
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
			for(BUN i = 0; i<cnt; i++) { \
				gp[i] = g-1; \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
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
				bpi++; \
			} \
		} \
	} while (0)

#define fgroup(Type, BaseType) \
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

#define agroup() \
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

#define agroup_(P) \
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
OAHASHbuild_table(bat *slot_id, bat *ht_sink, bat *key, const ptr *H)
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
	hash_table *h = (hash_table*)u->T.sink;
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

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		switch(tt) {
			case TYPE_void:
				vgroup();
				break;
			case TYPE_bit:
				group(bit);
				break;
			case TYPE_bte:
				group(bte);
				break;
			case TYPE_sht:
				group(sht);
				break;
			case TYPE_int:
				group(int);
				break;
			case TYPE_date:
				group(date);
				break;
			case TYPE_lng:
				group(lng);
				break;
			case TYPE_oid:
				group(oid);
				break;
			case TYPE_daytime:
				group(daytime);
				break;
			case TYPE_timestamp:
				group(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				group(hge);
				break;
#endif
			case TYPE_flt:
				fgroup(flt, int);
				break;
			case TYPE_dbl:
				fgroup(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					aprep_heap(b, u, h, "oahash.build_table");
					if (local_storage) {
						agroup_(p);
					} else {
						agroup();
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
	g->T.maxval = last;
	g->tkey = FALSE;
	*ht_sink = u->batCacheid;
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
		assert(BATtdense(b) || \
				/* A not-dense void BAT must contain only oid_nil.
				 * Otherwise it's a candidate list, which should never have
				 * reached this place. */ \
				(!BATtdense(b) && b->tseqbase == oid_nil && cnt)); \
		\
		int slots = 0; \
		gid slot = 0; \
		oid bpi = b->tseqbase; \
		oid *vals = h->vals; \
		\
		if (!BATtdense(b)) { \
			gid hsh = _hash_oid(bpi); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
				gid k = (gid)combine(gi[i], hsh, prime)&h->mask; \
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
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				bool fnd = 0; \
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
				bpi++; \
			} \
		}\
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
OAHASHbuild_combined_table(bat *slot_id, bat *ht_sink, bat *key, bat *parent_slotid, bat *parent_ht, const ptr *H)
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
	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == OA_HASH_TABLE_SINK);

	BUN cnt = BATcount(b);
	g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
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
					aprep_heap(b, u, h, "oahash.build_combined_table");
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
	g->T.maxval = last;
	g->tkey = FALSE;
	*ht_sink = u->batCacheid;
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
OAHASHcompute_frequencies(bat *ht_sink, bat *slot_id, const ptr *H)
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
	hash_table *ht = (hash_table*)hts->T.sink;
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
OAHASHcompute_frequencies_pos(bat *payload_pos, bat *ht_sink, bat *slot_id, const ptr *H)
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
	hash_table *ht = (hash_table*)hts->T.sink;
	assert(ht && ht->s.type == OA_HASH_TABLE_SINK);

	BUN cnt = BATcount(slt);
	res = COLnew(slt->hseqbase, TYPE_oid, cnt, TRANSIENT);
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
		assert(BATtdense(pld) || \
				/* A not-dense void BAT must contain only oid_nil.
				 * Otherwise it's a candidate list, which should never have
				 * reached this place. */ \
				(!BATtdense(pld) && pld->tseqbase == oid_nil && cnt)); \
		\
		oid pvals = pld->tseqbase; \
		oid *hpvals = hp->payload; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hp_check_rehash(); \
			/* TODO more memory efficient way to store TYPE_void payload.
			 * This materialisation seems rather overkill.
			 */ \
			hpvals[ppos[i]] = pvals; \
			pvals += (pld->tseqbase != oid_nil); \
		} \
	} while (0)

#define addpld(Type) \
	do { \
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
OAHASHadd_payload(bat *hp_sink, bat *payload, bat *payload_pos, const ptr *H)
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
	assert(BATcount(pld) == BATcount(pos));
	hash_payload *hp = (hash_payload*)res->T.sink;
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
					aprep_heap(pld, res, hp, "oahash.add_payload");
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

#define vhash() \
	do { \
		assert(BATtdense(k) || (!BATtdense(k) && k->tseqbase == oid_nil && cnt)); \
		\
		oid ky = k->tseqbase; \
		gid *hs = Tloc(h, 0); \
		\
		if (!BATtdense(k)) { \
			gid hsh = (gid)_hash_oid(ky); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				hs[i] = hsh; \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				hs[i] = (gid)_hash_oid(ky); \
				ky++; \
			} \
		} \
	} while (0)

#define hash(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)_hash_##Type(ky[i]); \
		} \
	} while (0)

#define fhash(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)_hash_##Type(*(((BaseType*)ky)+i)); \
		} \
	} while (0)

#define ahash() \
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
OAHASHhash(bat *hsh, bat *key, const ptr *H)
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
				vhash();
				break;
			case TYPE_bit:
				hash(bit);
				break;
			case TYPE_bte:
				hash(bte);
				break;
			case TYPE_sht:
				hash(sht);
				break;
			case TYPE_int:
				hash(int);
				break;
			case TYPE_date:
				hash(date);
				break;
			case TYPE_lng:
				hash(lng);
				break;
			case TYPE_oid:
				hash(oid);
				break;
			case TYPE_daytime:
				hash(daytime);
				break;
			case TYPE_timestamp:
				hash(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				hash(hge);
				break;
#endif
			case TYPE_flt:
				fhash(flt, int);
				break;
			case TYPE_dbl:
				fhash(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					ahash();
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
	BBPunfix(*key);
	return err;
}

#define vhash_combined() \
	do { \
		assert(BATtdense(k) || \
				(!BATtdense(k) && k->tseqbase == oid_nil && BATcount(k))); \
		\
		oid ky = k->tseqbase; \
		gid *hs = Tloc(h, 0); \
		if (!BATtdense(k)) { \
			gid hsh = _hash_oid(ky); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				hs[i] = (gid)combine(ps[i], hsh, prime); \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				hs[i] = (gid)combine(ps[i], _hash_oid(ky), prime); \
				ky++; \
			} \
		} \
	} while (0)

#define hash_combined(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)combine(ps[i], _hash_##Type(ky[sl[i]]), prime); \
		} \
	} while (0)

#define fhash_combined(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
			hs[i] = (gid)combine(ps[i], _hash_##Type(*(((BaseType*)ky)+sl[i])), prime); \
		} \
	} while (0)

#define ahash_combined() \
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
OAHASHcombined_hash(bat *hsh, bat *key, bat *selected, bat *parent_slotid, const ptr *H)
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
				vhash_combined();
				break;
			case TYPE_bit:
				hash_combined(bit);
				break;
			case TYPE_bte:
				hash_combined(bte);
				break;
			case TYPE_sht:
				hash_combined(sht);
				break;
			case TYPE_int:
				hash_combined(int);
				break;
			case TYPE_date:
				hash_combined(date);
				break;
			case TYPE_lng:
				hash_combined(lng);
				break;
			case TYPE_oid:
				hash_combined(oid);
				break;
			case TYPE_daytime:
				hash_combined(daytime);
				break;
			case TYPE_timestamp:
				hash_combined(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				hash_combined(hge);
				break;
#endif
			case TYPE_flt:
				fhash_combined(flt, int);
				break;
			case TYPE_dbl:
				fhash_combined(dbl, lng);
				break;
			default:
				if (ATOMvarsized(tt)) {
					ahash_combined();
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
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(p);
	return err;
}

#define vprobe() \
	do { \
		assert(BATtdense(k) || (!BATtdense(k) && k->tseqbase == oid_nil && keycnt)); \
		\
		oid ky = k->tseqbase; \
		gid *hs = Tloc(h, 0); \
		oid *vals = ht->vals; \
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		\
		if (!BATtdense(k)) { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
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
					mtdcnt++; \
				} \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
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
					mtdcnt++; \
				} \
				ky++; \
			} \
		} \
	} while (0)

#define probe(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		Type *vals = ht->vals; \
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			while (slot && vals[slot] != ky[i]) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) { \
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mtdcnt++; \
			} \
		} \
	} while (0)

#define aprobe() \
	do { \
		BATiter bi = bat_iterator(k); \
		gid *hs = Tloc(h, 0); \
		char **vals = ht->vals; \
		oid *mtd = Tloc(m, 0); \
		oid *slt = Tloc(s, 0); \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		TIMEOUT_LOOP_IDX_DECL(i, keycnt, qry_ctx) { \
			gid k = hs[i]&ht->mask; \
			gid slot = ht->gids[k]; \
			char *val = (bi).vh->base+BUNtvaroff(bi,i); \
			while (slot && atomcmp(vals[slot], val) != 0) { \
				k++; \
				k &= ht->mask; \
				slot = ht->gids[k]; \
			} \
			if (slot) { \
				mtd[mtdcnt] = i; \
				slt[mtdcnt] = slot - 1; \
				mtdcnt++; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHprobe(bat *LHS_matched, bat *RHS_slotid, bat *LHS_key, bat *LHS_hash, bat *RHS_ht, const ptr *H)
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
		hash_table *ht = (hash_table*)t->T.sink;
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		switch(tt) {
			case TYPE_void:
				vprobe();
				break;
			case TYPE_bit:
				probe(bit);
				break;
			case TYPE_bte:
				probe(bte);
				break;
			case TYPE_sht:
				probe(sht);
				break;
			case TYPE_int:
				probe(int);
				break;
			case TYPE_date:
				probe(date);
				break;
			case TYPE_lng:
				probe(lng);
				break;
			case TYPE_oid:
				probe(oid);
				break;
			case TYPE_daytime:
				probe(daytime);
				break;
			case TYPE_timestamp:
				probe(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				probe(hge);
				break;
#endif
			case TYPE_flt:
				probe(flt);
				break;
			case TYPE_dbl:
				probe(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					aprobe();
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

#define combined_vprobe() \
	do { \
		assert(BATtdense(k) || (!BATtdense(k) && k->tseqbase == oid_nil && BATcount(k))); \
		\
		oid ky = k->tseqbase; \
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(m, 0); \
		oid *vals = ht->vals; \
		oid *mtd = Tloc(res_m, 0); \
		oid *slt = Tloc(res_s, 0); \
		\
		if (!BATtdense(k)) { \
			TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
				gid hsh = hs[mt[i]]&ht->mask; \
				gid slot = ht->gids[hsh]; \
				while (slot && vals[slot] != ky) { \
					hsh++; \
					hsh &= ht->mask; \
					slot = ht->gids[hsh]; \
				} \
				if (slot) { \
					mtd[mtdcnt2] = i; \
					slt[mtdcnt2] = slot - 1; \
					mtdcnt2++; \
				} \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
				gid hsh = hs[mt[i]]&ht->mask; \
				gid slot = ht->gids[hsh]; \
				while (slot && vals[slot] != (ky + mt[i])) { \
					hsh++; \
					hsh &= ht->mask; \
					slot = ht->gids[hsh]; \
				} \
				if (slot) { \
					mtd[mtdcnt2] = i; \
					slt[mtdcnt2] = slot - 1; \
					mtdcnt2++; \
				} \
			} \
		} \
	} while (0)

#define combined_probe(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(m, 0); \
		Type *vals = ht->vals; \
		oid *mtd = Tloc(res_m, 0); \
		oid *slt = Tloc(res_s, 0); \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[mt[i]]&ht->mask; \
			gid slot = ht->gids[hsh]; \
			Type val = ky[mt[i]]; \
			while (slot && vals[slot] != val) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ht->gids[hsh]; \
			} \
			if (slot) { \
				mtd[mtdcnt2] = i; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
			} \
		} \
	} while (0)

#define combined_aprobe() \
	do { \
		BATiter bi = bat_iterator(k); \
		gid *hs = Tloc(h, 0); \
		oid *mt = Tloc(m, 0); \
		char **vals = ht->vals; \
		oid *mtd = Tloc(res_m, 0); \
		oid *slt = Tloc(res_s, 0); \
		int (*atomcmp)(const void *, const void *) = ATOMstorage(tt) == TYPE_str? (int (*)(const void *, const void *)) str_cmp : ATOMcompare(tt); \
		TIMEOUT_LOOP_IDX_DECL(i, mtdcnt, qry_ctx) { \
			gid hsh = hs[mt[i]]&ht->mask; \
			gid slot = ht->gids[hsh]; \
			char *val = (bi).vh->base+BUNtvaroff(bi,mt[i]); \
			while (slot && atomcmp(vals[slot], val) != 0) { \
				hsh++; \
				hsh &= ht->mask; \
				slot = ht->gids[hsh]; \
			} \
			if (slot) { \
				mtd[mtdcnt2] = i; \
				slt[mtdcnt2] = slot - 1; \
				mtdcnt2++; \
			} \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHcombined_probe(bat *LHS_matched, bat *RHS_slotid, bat *LHS_key, bat *LHS_hash, bat *LHS_selected, bat *RHS_ht, const ptr *H)
{
	BAT *res_m = NULL, *res_s = NULL, *k = NULL, *h = NULL, *m = NULL, *t = NULL;
	BUN mtdcnt, mtdcnt2 = 0;
	str err = NULL;

	(void) H;

	k = BATdescriptor(*LHS_key);
	h = BATdescriptor(*LHS_hash);
	m = BATdescriptor(*LHS_selected);
	t = BATdescriptor(*RHS_ht);
	if (!k || !h || !m || !t) {
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
		hash_table *ht = (hash_table*)t->T.sink;
		int tt = k->ttype;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

		switch(tt) {
			case TYPE_void:
				combined_vprobe();
				break;
			case TYPE_bit:
				combined_probe(bit);
				break;
			case TYPE_bte:
				combined_probe(bte);
				break;
			case TYPE_sht:
				combined_probe(sht);
				break;
			case TYPE_int:
				combined_probe(int);
				break;
			case TYPE_date:
				combined_probe(date);
				break;
			case TYPE_lng:
				combined_probe(lng);
				break;
			case TYPE_oid:
				combined_probe(oid);
				break;
			case TYPE_daytime:
				combined_probe(daytime);
				break;
			case TYPE_timestamp:
				combined_probe(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				combined_probe(hge);
				break;
#endif
			case TYPE_flt:
				combined_probe(flt);
				break;
			case TYPE_dbl:
				combined_probe(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					combined_aprobe();
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
	return err;
}

#define vproject() \
	do { \
		assert(BATtdense(k) || \
				(!BATtdense(k) && k->tseqbase == oid_nil && BATcount(k))); \
		\
		oid *res = Tloc(e, 0); \
		if (!BATtdense(k)) { \
			TIMEOUT_LOOP_IDX(idx, rescnt, qry_ctx) { \
				res[idx] = k->tseqbase; \
			} \
		} else { \
			memcpy(res, sel, rescnt * sizeof(oid)); \
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
				err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				break; \
			} \
			idx++; \
		} \
		bat_iterator_end(&bi); \
	} while (0)

static str
OAHASHproject(bat *pos, bat *res, bat *key, bat *selected, bat *ht, bit *first, const ptr *H)
{
	BAT *o = NULL, *e = NULL, *k = NULL, *s = NULL, *h = NULL;
	BUN rescnt = 0;
	str err = NULL;

	k = BATdescriptor(*key);
	s = BATdescriptor(*selected);
	h = BATdescriptor(*ht);
	if (!k || !s || !h) {
		err = createException(SQL, "oahash.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	assert(BATcount(s) <= BATcount(k));

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
	rescnt = BATcount(s);
	int tt = k->ttype;
	e = COLnew(k->hseqbase, tt?tt:TYPE_oid, rescnt, TRANSIENT);
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

	/* NB, this only works once.
	 * If we want to reuse the hash, we need to reset the position info.
	 */
	oid tseq = (*first)? ATOMIC_ADD(&h->T.maxval, rescnt) : 0;
	o = BATdense(0, tseq, rescnt);
	if (!o) {
		err = createException(SQL, "oahash.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}
	o->T.maxval = tseq + rescnt;

	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);
	BBPunfix(h->batCacheid);

	*pos = o->batCacheid;
	BBPkeepref(o);

	BATsetcount(e, rescnt);
	BATnegateprops(e);
	*res = e->batCacheid;
	BBPkeepref(e);

	(void) H;
	return MAL_SUCCEED;
error:
	BBPreclaim(o);
	BBPreclaim(e);
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(h);
	return err;
}

#define vexpand() \
	do { \
		assert(BATtdense(k) || \
				(!BATtdense(k) && k->tseqbase == oid_nil && BATcount(k))); \
		\
		oid *res = Tloc(e, 0); \
		if (!BATtdense(k)) { \
			TIMEOUT_LOOP_IDX(idx, ttlcnt, qry_ctx) { \
				res[idx] = k->tseqbase; \
			} \
		} else { \
			TIMEOUT_LOOP_IDX_DECL(i, selcnt, qry_ctx) { \
				gid freq = (gid)ht->frequency[sid[i]]; \
				TIMEOUT_LOOP_IDX_DECL(j, freq, qry_ctx) { \
					res[idx++] = sel[i]; \
				} \
			} \
			if (append_vals) { \
				for (BUN i = 0, j = 0; i < keycnt; i++) { \
					if (j < selcnt && i == sel[j]) j++;\
					else res[idx++] = i; \
				} \
			} \
		} \
	} while (0)

#define expand(Type) \
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
		if (append_vals) { \
			for (BUN i = 0, j = 0; i < keycnt; i++) { \
				if (j < selcnt && i == sel[j]) j++;\
				else res[idx++] = val[i]; \
			} \
		} \
	} while (0)

#define aexpand() \
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
		if (append_vals) { \
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
OAHASHexpand(bat *pos, bat *expanded, bat *key, bat *selected, bat *slotid, bat *freq_sink, bit *first, bit *append_vals, const ptr *H)
{
	BAT *o = NULL, *e = NULL, *k = NULL, *s = NULL, *l = NULL, *h = NULL;
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
	hash_table *ht = (hash_table*)h->T.sink;
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
	if (append_vals) {
		ttlcnt += (keycnt - selcnt);
	}

	int tt = k->ttype;
	e = COLnew(k->hseqbase, tt?tt:TYPE_oid, ttlcnt, TRANSIENT);
	if (!e) {
		err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (ttlcnt) {
		BUN idx = 0;
		oid *sel = Tloc(s, 0);

		switch(tt) {
			case TYPE_void:
				vexpand();
				break;
			case TYPE_bit:
				expand(bit);
				break;
			case TYPE_bte:
				expand(bte);
				break;
			case TYPE_sht:
				expand(sht);
				break;
			case TYPE_int:
				expand(int);
				break;
			case TYPE_date:
				expand(date);
				break;
			case TYPE_lng:
				expand(lng);
				break;
			case TYPE_oid:
				expand(oid);
				break;
			case TYPE_daytime:
				expand(daytime);
				break;
			case TYPE_timestamp:
				expand(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				expand(hge);
				break;
#endif
			case TYPE_flt:
				expand(flt);
				break;
			case TYPE_dbl:
				expand(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					aexpand();
				} else {
					err = createException(MAL, "oahash.expand", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.expand", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;

		assert(idx == ttlcnt);
	}

	/* NB, this only works once.
	 * If we want to reuse the hash, we need to reset the position info.
	 */
	oid tseq = (*first)? ATOMIC_ADD(&h->T.maxval, ttlcnt) : 0;
	o = BATdense(0, tseq, ttlcnt);
	if (!o) {
		err = createException(SQL, "oahash.expand", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}
	o->T.maxval = tseq + ttlcnt;

	BBPunfix(k->batCacheid);
	BBPunfix(s->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(h->batCacheid);

	*pos = o->batCacheid;
	BBPkeepref(o);

	BATsetcount(e, ttlcnt);
	BATnegateprops(e);
	*expanded = e->batCacheid;
	BBPkeepref(e);

	(void) H;
	return MAL_SUCCEED;
error:
	BBPreclaim(o);
	BBPreclaim(e);
	BBPreclaim(k);
	BBPreclaim(s);
	BBPreclaim(l);
	BBPreclaim(h);
	return err;
}

#if 0
#define vfetch() \
	do { \
		oid val = ((oid*)hp->payload)[0]; \
		oid *res = Tloc(f, 0); \
		TIMEOUT_LOOP_IDX(idx, rescnt, qry_ctx) { \
			res[idx] = val; \
		} \
	} while (0)
#endif

#define fetch(Type) \
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
		if (append_vals) { \
			for (BUN i = fchcnt; i < ttlcnt; i++) \
				res[idx++] = Type##_nil; \
		} \
	} while (0)

#define afetch() \
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
				}\
				idx++; \
			} \
		} \
		if (append_vals) { \
			for (BUN i = fchcnt; i < ttlcnt; i++) { \
				if (BUNappend(f, ATOMnilptr(tt), false) != GDK_SUCCEED) { \
					err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
					break; \
				}\
				idx++; \
			} \
		} \
	} while (0)

static str
OAHASHfetch_payload(bat *pos, bat *fetched, bat *hp_sink, bat *slotid, bat *freq_sink, bat *probe_col, bit *first, bit *append_vals, const ptr *H)
{
	BAT *o = NULL, *f = NULL, *l = NULL, *hps = NULL, *hts = NULL;
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
	hash_payload *hp = (hash_payload*)hps->T.sink;
	hash_table *ht = (hash_table*)hts->T.sink;
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
	if (append_vals) {
		BAT *p = BATdescriptor(*probe_col);
		if (!p) {
			err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		nllcnt = BATcount(p) - selcnt;
		ttlcnt += nllcnt;
		BBPunfix(p->batCacheid);
	}

	int tt = hp->type;
	f = COLnew(hps->hseqbase, tt?tt:TYPE_oid, ttlcnt, TRANSIENT);
	if (!f) {
		err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (ttlcnt) {
		BUN idx = 0;

		switch(tt) {
			case TYPE_void:
				fetch(oid);
				break;
			case TYPE_bit:
				fetch(bit);
				break;
			case TYPE_bte:
				fetch(bte);
				break;
			case TYPE_sht:
				fetch(sht);
				break;
			case TYPE_int:
				fetch(int);
				break;
			case TYPE_date:
				fetch(date);
				break;
			case TYPE_lng:
				fetch(lng);
				break;
			case TYPE_oid:
				fetch(oid);
				break;
			case TYPE_daytime:
				fetch(daytime);
				break;
			case TYPE_timestamp:
				fetch(timestamp);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				fetch(hge);
				break;
#endif
			case TYPE_flt:
				fetch(flt);
				break;
			case TYPE_dbl:
				fetch(dbl);
				break;
			default:
				if (ATOMvarsized(tt)) {
					afetch();
				} else {
					err = createException(MAL, "oahash.fetch_payload", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				}
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "oahash.fetch_payload", RUNTIME_QRY_TIMEOUT));
		if (err)
			goto error;

		assert(idx == ttlcnt);
	}

	/* NB, this only works once.
	 * If we want to reuse the hash, we need to reset the position info.
	 */
	oid tseq = (*first)? ATOMIC_ADD(&hps->T.maxval, ttlcnt) : 0;
	o = BATdense(0, tseq, ttlcnt);
	if (!o) {
		err = createException(SQL, "oahash.fetch_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}
	o->T.maxval = tseq + ttlcnt;

	BBPunfix(l->batCacheid);
	BBPunfix(hps->batCacheid);
	BBPunfix(hts->batCacheid);

	*pos = o->batCacheid;
	BBPkeepref(o);

	BATsetcount(f, ttlcnt);
	BATnegateprops(f);
	*fetched = f->batCacheid;
	BBPkeepref(f);

	(void) H;
	return MAL_SUCCEED;
error:
	BBPreclaim(o);
	BBPreclaim(f);
	BBPreclaim(l);
	BBPreclaim(hps);
	BBPreclaim(hts);
	return err;
}

#include "mel.h"
static mel_func oa_hash_init_funcs[] = {
 pattern("oahash", "new", OAHASHnew, false, "", args(1,3, batargany("ht_sink",1),argany("tt",1),arg("size",int))),
 pattern("oahash", "new", OAHASHnew, false, "", args(1,4, batargany("ht_sink",1),argany("tt",1),arg("size",int),arg("freq",bit))),
 pattern("oahash", "new", OAHASHnew, false, "", args(1,5, batargany("ht_sink",1),argany("tt",1),arg("size",int),arg("freq",bit),batargany("p",2))),
 pattern("oahash", "new_payload", OAHASHnew_payload, false, "", args(1,5, batargany("hp_sink",1),argany("tt",1),arg("nr_payloads",int),batargany("parent",2), batargany("dummy",3))),

 command("oahash", "build_table", OAHASHbuild_table, false, "Build a hash table for the given column. Returns the slot ID for each key and the sink containing the hash table", args(2,4, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),arg("pipeline",ptr))),
 command("oahash", "build_combined_table", OAHASHbuild_combined_table, false, "Build a hash table for the given column in combination with the hash table of a parent column. Returns the slot ID for each key and the sink containing the hash table", args(2,6, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),batarg("parent_slotid",oid),batargany("parent_ht",2),arg("pipeline",ptr))),

 command("oahash", "compute_frequencies", OAHASHcompute_frequencies, false, "Add the frequencies of the given slot IDs to the hash-table", args(1,3, batargany("ht_sink",1),batarg("slot_id",oid),arg("pipeline",ptr))),
 command("oahash", "compute_frequencies", OAHASHcompute_frequencies_pos, false, "Add the frequencies of the given slot IDs to the hash-table. In addition, return combined_hash(slot_id, freq) for payload_pos", args(2,4, batarg("payload_pos",oid),batargany("ht_sink",1),batarg("slot_id",oid),arg("pipeline",ptr))),

 command("oahash", "add_payload", OAHASHadd_payload, false, "Add 'payload' at 'position' in 'hp_sink'", args(1,4, batargany("hp_sink",1),batargany("payload",1),batarg("payload_pos",oid),arg("pipeline",ptr))),

 command("oahash", "hash", OAHASHhash, false, "Compute the hashs for the given column", args(1,3, batarg("hsh",lng),batargany("key",1),arg("pipeline",ptr))),
 command("oahash", "combined_hash", OAHASHcombined_hash, false, "Compute the hashs for the selected items in the given column in combination with the slot IDs of a parent column", args(1,5, batarg("hsh",lng),batargany("key",1),batarg("selected",oid),batarg("parent_slotid",oid),arg("pipeline",ptr))),

 command("oahash", "probe", OAHASHprobe, false, "Probe the given column with its hashs in the given hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,6, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batarg("LHS_hash",lng),batargany("RHS_ht",2),arg("pipeline",ptr))),
 command("oahash", "combined_probe", OAHASHcombined_probe, false, "Probe the selected items in the given column with their hashs in the given hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,7, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batarg("LHS_hash",lng),batarg("LHS_selected",oid),batargany("RHS_ht",2),arg("pipeline",ptr))),

 command("oahash", "project", OAHASHproject, false, "Project the selected OIDs on the given column", args(2,7, batarg("pos",oid),batargany("res",1),batargany("key",1),batarg("selected",oid),batargany("ht",2),arg("first",bit),arg("pipeline",ptr))),
 command("oahash", "expand", OAHASHexpand, false, "Duplicate the selected items in the given column according to their frequencies denoted in the hash table. If 'first', generate global row IDs. If 'append_vals', append the not 'selected' values (for outer joins).", args(2,9, batarg("pos",oid),batargany("expanded",1),batargany("key",1),batarg("selected",oid),batarg("slotid",oid),batargany("freq_sink",2),arg("first",bit),arg("append_vals",bit),arg("pipeline",ptr))),
 command("oahash", "fetch_payload", OAHASHfetch_payload, false, "For each given hash slot, fetch its associated payloads from the hash-payload.", args(2,9, batarg("pos",oid),batargany("fetched",1),batargany("hp_sink",1),batarg("slotid",oid),batargany("freq_sink",2),batargany("probe_col",1),arg("first",bit),arg("append_vals",bit),arg("pipeline",ptr))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("oa_hash", NULL, oa_hash_init_funcs); }
