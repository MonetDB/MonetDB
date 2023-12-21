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
compute_mask(unsigned int n)
{
	unsigned int nn = n * 1.2 * 2.1; // this magic is copied from UHASHnew
	if (nn < HT_MIN_SIZE)
		nn = HT_MIN_SIZE;
	if (nn > HT_MAX_SIZE)
		nn = HT_MAX_SIZE;

	unsigned int bits = log_base2(nn - 1);
	if (bits >= GIDBITS)
		bits = GIDBITS - 1;

	return (1 << bits) - 1;
}

static unsigned int
compute_hash_prime_idx(unsigned int n)
{
	unsigned int nn = n * 1.2 * 2.1; // this magic is copied from UHASHnew
	if (nn < HT_MIN_SIZE)
		nn = HT_MIN_SIZE;
	if (nn > HT_MAX_SIZE)
		nn = HT_MAX_SIZE;

	unsigned int bits = log_base2(nn - 1);
	if (bits >= GIDBITS)
		bits = GIDBITS - 1;

	return bits - 5;
}

/* ***** HASH TABLE ***** */
static hash_table *
_ht_init( hash_table *h )
{
	if (h->gids == NULL) {
		h->vals = (char*)GDKmalloc(h->size * (size_t)h->width);
		h->gids = (hash_key_t*)GDKzalloc(sizeof(hash_key_t)* h->size);
		if (h->vals == NULL || h->gids == NULL)
			goto error;
		if (h->p) {
			assert(h->s.type == HASH_SINK);
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
_ht_create( int type, int size, hash_table *p)
{
	hash_table *h = (hash_table*)GDKzalloc(sizeof(hash_table));
	if (!h)
		return NULL;
	int bits = log_base2(size-1);

	if (!type)
		type = TYPE_oid;
	h->s.destroy = (sink_destroy)&ht_destroy;
	h->s.type = HASH_SINK;
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
ht_rehash(hash_table *ht)
{
	ht->rehash = 1;
	if (ht->p)
		ht_rehash(ht->p);
}

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
		if (p == NULL)
			return createException(MAL, "hash.new", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		parent = (hash_table*)p->T.sink;
		BBPunfix(p->batCacheid);
	}

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	if (b == NULL)
		return createException(MAL, "hash.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b->T.sink = (Sink*)ht_create(tt, size*1.2*2.1, parent);
	if (b->T.sink == NULL) {
		BBPunfix(b->batCacheid);
		return createException(MAL, "hash.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	if (hp->frequency)
		GDKfree(hp->frequency);
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
_hp_create(int type, int size, hash_table *parent)
{
	hash_payload *hp = (hash_payload *)GDKzalloc(sizeof(hash_payload));
	if (!hp) return NULL;

	int bits = log_base2(size-1);

	if (!type)
		type = TYPE_oid;
	hp->s.destroy = (sink_destroy)&hp_destroy;
	hp->s.type = HASH_SINK;
	if (bits >= GIDBITS)
		bits = GIDBITS-1;
	hp->bits = bits;
	hp->size = (gid)1<<bits;
	hp->mask = hp->size-1;
	hp->type = type;
	hp->width = ATOMsize(type);
	hp->last = 0;
	hp->rehash = 0;
	hp->parent = parent;
	if (type == TYPE_str) {
		hp->cmp = (fcmp)str_cmp;
		hp->hsh = (fhsh)str_hsh;
	} else {
		hp->cmp = (fcmp)ATOMcompare(type);
		hp->hsh = (fhsh)BATatoms[type].atomHash;
		hp->len = (flen)BATatoms[type].atomLen;
	}

	hp->payload = (char *)GDKmalloc((size_t)hp->width * hp->size);
	if (!hp->payload) {
		GDKfree(hp);
		return NULL;
	}
	hp->frequency = (size_t *)GDKzalloc(sizeof(size_t) * hp->size);
	if (!hp->frequency) {
		GDKfree(hp->payload);
		GDKfree(hp);
		return NULL;
	}

	return hp;
}

/* Returns NULL if a memory allocation has failed.
 */
hash_payload *
hp_create(int type, int size, hash_table *parent)
{
	if (size < HP_MIN_SIZE)
		size = HP_MIN_SIZE;
	if (size > HP_MAX_SIZE)
		size = HP_MAX_SIZE;
	return _hp_create(type, size, parent);
}

void
hp_rehash(hash_payload *hp)
{
	hp->rehash = 1;
	if (hp->parent)
		ht_rehash(hp->parent);
}

/* X_nn:bat[:int] := hash.new_payload(nil:int, 42:int, X_nn:bat[:int]); */
static str
UHASHnew_payload(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *res = getArgReference_bat(s, p, 0);
	int tt = getArgType(m, p, 1);
	int size = *getArgReference_int(s, p, 2);

	hash_table *parent = NULL;
	bat pid = *getArgReference_bat(s, p, 3);
	BAT *prnt = BATdescriptor(pid);
	if (prnt == NULL)
		return createException(MAL, "hash.new_payload", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	parent = (hash_table*)prnt->T.sink;
	BBPunfix(prnt->batCacheid);

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	if (b == NULL)
		return createException(MAL, "hash.new_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b->T.sink = (Sink *)hp_create(tt, size*1.2*2.1, parent);
	if (!b->T.sink) {
		BBPunfix(b->batCacheid);
		throw(MAL, "hash.new_payload", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*res = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

/* ***** HASH OPERATORS ***** */

#define PRE_CLAIM 256
#define group(Type) \
	do { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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
		} \
	} while (0)

#define fgroup(Type, BaseType) \
	do { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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

#define agroup(Type) \
	do { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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

#define agroup_(Type,P) \
	do { \
		if (ATOMstorage(tt) == TYPE_str) { \
			int slots = 0; \
			gid slot = 0; \
			BATiter bi = bat_iterator(b); \
			Type *vals = h->vals; \
			mallocator *ma = h->allocators[P->wid]; \
			\
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
				bool fnd = 0; \
				Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
			bat_iterator_end(&bi); \
		} else \
		if (ATOMvarsized(tt)) { \
			int slots = 0; \
			gid slot = 0; \
			BATiter bi = bat_iterator(b); \
			char **vals = h->vals; \
			mallocator *ma = h->allocators[P->wid]; \
			\
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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
						vals[g] = ma_copy(ma, bpi, h->len(bpi)); \
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

/* (X_nn:bat[:oid], !X_nn:bat[:any1]) := hash.build_table(X_nn:bat[:any1], ptr); */
static str
UHASHbuild_table(bat *slot_id, bat *ht_sink, bat *key, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H;
	/* private or not */
	bool private = (!*ht_sink || is_bat_nil(*ht_sink)), local_storage = false;
	str err = NULL;
	BAT *u, *b = NULL;
	lng timeoffset = 0;

   	b = BATdescriptor(*key);
	if (!b)
		return createException(SQL, "hash.build_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (private && *ht_sink && is_bat_nil(*ht_sink)) { /* TODO ... create but how big ??? */
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		if (!u) {
			err = createException(MAL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, NULL);
		if (!u) {
			err = createException(MAL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		u->T.private_bat = 1;
	} else {
		u = BATdescriptor(*ht_sink);
	}
	if (!u) {
		BBPunfix(*key);
		return createException(SQL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	private = u->T.private_bat;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) && !VIEWvtparent(b)) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (mallocator**)GDKzalloc(p->p->nr_workers*sizeof(mallocator*));
			if (!h->allocators) {
				pipeline_unlock(p);
				err = createException(MAL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			} else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = ma_create();
			if (!h->allocators[p->wid]) {
				err = createException(MAL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		if (g == NULL) {
			err = createException(MAL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}

		if (cnt && !err) {
			int tt = b->ttype;
			oid *gp = Tloc(g, 0);

			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
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
			case TYPE_str:
				if (local_storage) {
					agroup_(str, p);
				} else {
					agroup(str);
				}
				break;
			default:
				err = createException(MAL, "hash.build_table", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
			}
			if (!err)
				TIMEOUT_CHECK(timeoffset, throw(MAL, "hash.build_table", RUNTIME_QRY_TIMEOUT));
		}
		if (err || p->p->status) {
			BBPunfix(g->batCacheid);
			/* We don't want to overwrite existing error message.
			 * p->p->status doesn't carry much info. yet.
			 */
			if (!err)
				err = createException(MAL, "hash.build_table", "pipeline execution error");
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
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
error:
	// FIXME: is it correct to destroy the sink here?
	if (u && u->T.sink)
		u->T.sink->destroy(u->T.sink);
	BBPreclaim(b);
	BBPreclaim(u);
	return err;
}

#define derive(Type) \
	do { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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
		oid bpi = b->tseqbase; \
		oid *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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
		} \
	} while (0)

#define fderive(Type, BaseType) \
	do { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
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

#define aderive(Type) \
	do { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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

#define aderive_(Type, P) \
	do { \
		if (ATOMstorage(tt) == TYPE_str) { \
			int slots = 0; \
			gid slot = 0; \
			BATiter bi = bat_iterator(b); \
			Type *vals = h->vals; \
			mallocator *ma = h->allocators[P->wid]; \
			\
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
				bool fnd = 0; \
				Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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
		} else \
		if (ATOMvarsized(tt)) { \
			int slots = 0; \
			gid slot = 0; \
			BATiter bi = bat_iterator(b); \
			Type *vals = h->vals; \
			mallocator *ma = h->allocators[P->wid]; \
			\
			TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
				bool fnd = 0; \
				Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
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

/* (X_nn:bat[:oid], !X_nn:bat[:any1]) := hash.build_combined_table(X_nn:bat[:any1], X_nn:bat[:oid], X_nn:bat[:any2], ptr); */
static str
UHASHbuild_combined_table(bat *slot_id, bat *ht_sink, bat *key, bat *parent_slotid, bat *parent_ht, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H;
	bool private = (!*ht_sink || is_bat_nil(*ht_sink)), local_storage = false;
	str err = NULL;
	BAT *u =  NULL;
	lng timeoffset = 0;

	BAT *b = BATdescriptor(*key);
	BAT *G = BATdescriptor(*parent_slotid);
	if (!b || !G) {
		err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	if (private && *ht_sink && is_bat_nil(*ht_sink)) { /* TODO ... create but how big ??? */
		BAT *H = BATdescriptor(*parent_ht);
		if (!H) {
			err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		if (!u) {
			BBPunfix(H->batCacheid);
			err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		/* Lookup parent hash */
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, (hash_table*)H->T.sink);
		if (u->T.sink == NULL) {
			err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		u->T.private_bat = 1;
		BBPunfix(*parent_ht);
	} else {
		u = BATdescriptor(*ht_sink);
		if (!u) {
			err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}
	private = u->T.private_bat;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	MT_lock_set(&u->theaplock);
	MT_lock_set(&b->theaplock);
	if (ATOMvarsized(u->ttype) && !VIEWvtparent(b)) {
		local_storage = true;
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		pipeline_lock(p);
		if (!h->allocators) {
			h->allocators = (mallocator**)GDKzalloc(p->p->nr_workers*sizeof(mallocator*));
			if (!h->allocators) {
				pipeline_unlock(p);
				err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			} else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = ma_create();
			if (!h->allocators[p->wid]) {
				err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
		}
	} else if (ATOMvarsized(u->ttype) && BATcount(b) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
		BATswap_heaps(u, b, p);
	} else {
		MT_lock_unset(&b->theaplock);
		MT_lock_unset(&u->theaplock);
	}
	if (h) {
		BUN cnt = BATcount(b);
		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);
		if (g == NULL) {
			err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}

		if (cnt && !err) {
			ATOMIC_BASE_TYPE expected = 0;
			int tt = b->ttype;
			oid *gp = Tloc(g, 0);
			gid *gi = Tloc(G, 0);
			gid *pgids = h->pgids;
			int prime = hash_prime_nr[h->bits-5];

			QryCtx *qry_ctx = MT_thread_get_qry_ctx();
			if (qry_ctx != NULL) {
				timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
			}
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
			case TYPE_str:
				if (local_storage) {
					aderive_(str,p);
				} else {
					aderive(str);
				}
				break;
			default:
				err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
			}
			if (!err)
				TIMEOUT_CHECK(timeoffset, err = createException(SQL, "hash.build_combined_table", RUNTIME_QRY_TIMEOUT));
		}
		if (err || p->p->status) {
			BBPunfix(g->batCacheid);
			if (!err)
				err = createException(MAL, "hash.build_combined_table", "pipeline execution error");
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
	}
	BBPunfix(b->batCacheid);
	BBPunfix(G->batCacheid);
	return MAL_SUCCEED;
error:
	if (u && u->T.sink)
		u->T.sink->destroy(u->T.sink);
	BBPreclaim(u);
	BBPreclaim(b);
	BBPreclaim(G);
	return err;
}

/* !X_nn:bat[:any1] := hash.add_payload(X_nn:bat[:any1], X_nn:bat[:oid], ptr); */
static str
HASHadd_payload(bat *hp_sink, bat *payload, bat *parent_slotid, const ptr *H)
{
	(void) hp_sink;
	(void) payload;
	(void) parent_slotid;
	(void) H;

	return MAL_SUCCEED;
}

#define vhash() \
	do { \
		oid *hs = Tloc(h, 0); \
		oid hsh = _hash_oid(k->tseqbase) & mask; \
	\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			hs[i] = hsh; \
		} \
	} while (0)

#define hash(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *hs = Tloc(h, 0); \
	\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			hs[i] = _hash_##Type(ky[i]) & mask; \
		} \
	} while (0)

#define fhash(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *hs = Tloc(h, 0); \
	\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			hs[i] = _hash_##Type(*(((BaseType*)ky)+i)) & mask; \
		} \
	} while (0)

/* X_nn:bat[:any1] := hash.hash(X_nn:bat[:any1]); */
static str
UHASHhash(bat *hsh, bat *key)
{
	BAT *h = NULL, *k = NULL;
	BUN cnt;
	lng timeoffset = 0;
	str err = NULL;

	k = BATdescriptor(*key);
	if (!k)
		return createException(SQL, "hash.hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	cnt = BATcount(k);
	h = COLnew(k->hseqbase, k->ttype?k->ttype:TYPE_oid, cnt, TRANSIENT);
	if (!h) {
		err = createException(SQL, "hash.hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (cnt) {
		unsigned int mask = compute_mask(cnt);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		if (qry_ctx != NULL) {
			timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
		}
		switch(k->ttype) {
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
			case TYPE_str:
				//if (local_storage) {
				//	ahash_(str, p);
				//} else {
				//	ahash(str);
				//}
				//break;
				err =  createException(MAL, "hash.hash", "TODO: TYPE_str");
				goto error;
			default:
				err = createException(MAL, "hash.hash", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				goto error;
		}
		TIMEOUT_CHECK(timeoffset, err = createException(SQL, "hash.hash", RUNTIME_QRY_TIMEOUT));
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
		oid *hs = Tloc(h, 0); \
		oid hsh = _hash_oid(k->tseqbase); \
	\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			hs[i] = combine(ps[i], hsh, prime) & mask; \
		} \
	} while (0)

#define hash_combined(Type) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *hs = Tloc(h, 0); \
	\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			hs[i] = combine(ps[i], _hash_##Type(ky[sl[i]]), prime) & mask; \
		} \
	} while (0)

#define fhash_combined(Type, BaseType) \
	do { \
		Type *ky = Tloc(k, 0); \
		Type *hs = Tloc(h, 0); \
	\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { \
			hs[i] = combine(ps[i], _hash_##Type(*(((BaseType*)ky)+sl[i])), prime) & mask; \
		} \
	} while (0)

/* X_nn:bat[:any1] := hash.combined_hash(X_nn:bat[:any1], X_nn:bat[:oid], X_nn:bat[:oid]); */
static str
UHASHcombined_hash(bat *hsh, bat *key, bat *selected, bat *parent_slotid)
{
	BAT *h = NULL, *k = NULL, *s = NULL, *p = NULL;
	BUN cnt;
	lng timeoffset = 0;
	str err = NULL;

	k = BATdescriptor(*key);
	s = BATdescriptor(*selected);
	p = BATdescriptor(*parent_slotid);
	if (!k || !s || !p) {
		err = createException(SQL, "hash.combined_hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}

	/* only compute hashes for the 'selected' ones */
	cnt = BATcount(s);
	h = COLnew(k->hseqbase, k->ttype?k->ttype:TYPE_oid, cnt, TRANSIENT);
	if (!h) {
		err = createException(SQL, "hash.combined_hash", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto error;
	}

	if (cnt) {
		oid  *sl = Tloc(s, 0);
		oid  *ps = Tloc(p, 0);
		unsigned int mask = compute_mask(cnt);
		unsigned int prime = compute_hash_prime_idx(cnt);

		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		if (qry_ctx != NULL) {
			timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
		}
		switch(k->ttype) {
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
			case TYPE_str:
				//if (local_storage) {
				//	ahash_combined_(str, p);
				//} else {
				//	ahash_combined(str);
				//}
				//break;
				err =  createException(MAL, "hash.combined_hash", "TODO: TYPE_str");
				goto error;
			default:
				err = createException(MAL, "hash.combined_hash", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
				goto error;
		}
		TIMEOUT_CHECK(timeoffset, err = createException(SQL, "hash.combined_hash", RUNTIME_QRY_TIMEOUT));
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

/* (X_nn:bat[:oid], X_nn:bat[:oid]) := hash.probe(X_nn:bat[:any1], X_nn:bat[:any1], X_nn:bat[:any2]); */
static str
UHASHprobe(bat *LHS_matched, bat *RHS_slotid, bat *LHS_key, bat *LHS_hash, bat *RHS_ht)
{
	(void) LHS_matched;
	(void) RHS_slotid;
	(void) LHS_key;
	(void) LHS_hash;
	(void) RHS_ht;

	return MAL_SUCCEED;
}

/* (X_nn:bat[:oid], X_nn:bat[:oid]) := hash.combined_probe(X_nn:bat[:any1], X_nn:bat[:any1], X_nn:bat[:oid], X_nn:bat[:any2]); */
static str
UHASHcombined_probe(bat *LHS_matched, bat *RHS_slotid, bat *LHS_key, bat *LHS_hash, bat *LHS_selected, bat *RHS_ht)
{
	(void) LHS_matched;
	(void) RHS_slotid;
	(void) LHS_key;
	(void) LHS_hash;
	(void) LHS_selected;
	(void) RHS_ht;

	return MAL_SUCCEED;
}

/* X_nn:bat[:any1] := hash.expand(X_nn:bat[:any1], X_nn:bat[:oid], X_nn:bat[:oid], X_nn:bat[:any2]); */
static str
HASHexpand(bat *expanded, bat *key, bat *selected, bat *slotid, bat *hp_sink)
{
	(void) expanded;
	(void) key;
	(void) selected;
	(void) slotid;
	(void) hp_sink;

	return MAL_SUCCEED;
}

/* X_nn:bat[:any1] := hash.fetch_payload(X_nn:bat[:oid], X_nn:bat[:any1]); */
static str
HASHfetch_payload(bat *payload, bat *slotid, bat *hp_sink)
{
	(void) payload;
	(void) slotid;
	(void) hp_sink;

	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func pp_hash_init_funcs[] = {
 pattern("hash", "new", UHASHnew, false, "", args(1,3, batargany("sink",1),argany("tt",1),arg("size",int))),
 pattern("hash", "new", UHASHnew, false, "", args(1,4, batargany("sink",1),argany("tt",1),arg("size",int), batargany("p",2))),
 pattern("hash", "new_payload", UHASHnew_payload, false, "", args(1,4, batargany("sink",1),argany("tt",1),arg("size",int), batargany("parent",2))),

 command("hash", "build_table", UHASHbuild_table, false, "Build a hash table for the given column. Returns the slot ID for each key and the sink containing the hash table", args(2,4, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),arg("pipeline",ptr))),
 command("hash", "build_combined_table", UHASHbuild_combined_table, false, "Build a hash table for the given column in combination with the hash table of a parent column. Returns the slot ID for each key and the sink containing the hash table", args(2,6, batarg("slot_id",oid),batargany("ht_sink",1),batargany("key",1),batarg("parent_slotid",oid),batargany("parent_ht",2),arg("pipeline",ptr))),
 command("hash", "add_payload", HASHadd_payload, false, "Add a payload column with the given hash table. Returns a sink containing the hash payload", args(1,4, batargany("hp_sink",1),batargany("payload",1),batarg("parent_slotid",oid),arg("pipeline",ptr))),

 command("hash", "hash", UHASHhash, false, "Compute the hashs for the given column", args(1,2, batargany("hsh",1),batargany("key",1))),
 command("hash", "combined_hash", UHASHcombined_hash, false, "Compute the hashs for the selected items in the given column in combination with the slot IDs of a parent column", args(1,4, batargany("hsh",1),batargany("key",1),batarg("selected",oid),batarg("parent_slotid",oid))),

 command("hash", "probe", UHASHprobe, false, "Probe the given column with its hashs in the given hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,5, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batargany("LHS_hash",1),batargany("RHS_ht",2))),
 command("hash", "combined_probe", UHASHcombined_probe, false, "Probe the selected items in the given column with their hashs in the given hash table. For a matched item, return its OID in the left-hand-side column and the slot ID in the right-hand-side hash table", args(2,6, batarg("LHS_matched",oid),batarg("RHS_slotid",oid),batargany("LHS_key",1),batargany("LHS_hash",1),batarg("LHS_selected",oid),batargany("RHS_ht",2))),

 command("hash", "expand", HASHexpand, false, "Duplicate the selected items in the given column according to their frequencies denoted in the hash payload", args(1,5, batargany("expanded",1),batargany("key",1),batarg("selected",oid),batarg("slotid",oid),batargany("hp_sink",2))),
 command("hash", "fetch_payload", HASHfetch_payload, false, "For each given hash slot, fetch its associated payloads from the hash payload.", args(1,3, batargany("payload",1),batarg("slotid",oid),batargany("hp_sink",1))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pp_hash", NULL, pp_hash_init_funcs); }
