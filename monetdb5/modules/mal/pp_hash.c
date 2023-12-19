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
	unsigned int nn = (n < HT_MIN_SIZE)? HT_MIN_SIZE :
 			 ((n > HT_MAX_SIZE)? HT_MAX_SIZE : n);
	int bits = log_base2(nn - 1);

	bits = (bits >= GIDBITS)? GIDBITS - 1 : bits;
	return (1 << bits) - 1;
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
	parent = (hash_table*)prnt->T.sink;
	BBPunfix(prnt->batCacheid);

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	b->T.sink = (Sink *)hp_create(tt, size*1.2*2.1, parent);
	if (!b->T.sink) {
		BBPunfix(b->batCacheid);
		/* HY001 stands for "memory allocation error".
		 * It's a better SQLSTATE code than HY013, 
		 *  which stands for "memory management error".
		 */
		throw(MAL, "hash.new_payload", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		// FIXME: check if this error code is OK
		return createException(SQL, "hash.build_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	if (private && *ht_sink && is_bat_nil(*ht_sink)) { /* TODO ... create but how big ??? */
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, NULL);
		u->T.private_bat = 1;
	} else {
		u = BATdescriptor(*ht_sink);
	}
	if (!u) {
		BBPunfix(*key);
		return createException(SQL, "hash.build_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			if (!h->allocators)
				err = createException(MAL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = ma_create();
			if (!h->allocators[p->wid])
				err = createException(MAL, "hash.build_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);

		if (cnt && !err) {
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
				throw(MAL, "hash.build_table", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
			}
		}
		if (!err) {
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
		}
	}
	// TODO 0: do we have a timeout exception, instead of GDK_EXCEPTION?
	// TODO 1: shouldn't we unfix 'u' and 'g' above?
	TIMEOUT_CHECK(timeoffset, throw(MAL, "hash.build_table", GDK_EXCEPTION));
	if (err || p->p->status) // TODO: check the error code, shouild be something about runtime_error
		throw(MAL, "hash.build_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
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
			gid k = (gid)combine(gi[i], _hash_##Type(bp[i]))&h->mask; \
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
			gid k = (gid)combine(gi[i], _hash_oid(bpi))&h->mask; \
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
			gid k = (gid)combine(gi[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
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
			gid k = (gid)combine(gi[i], h->hsh(bpi))&h->mask; \
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
				gid k = (gid)combine(gi[i], str_hsh(bpi))&h->mask; \
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
				gid k = (gid)combine(gi[i], h->hsh(bpi))&h->mask; \
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

static str
UHASHbuild_combined_table(bat *slot_id, bat *ht_sink, bat *key, bat *parent_slotid, bat *parent_ht, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H;
	bool private = (!*ht_sink || is_bat_nil(*ht_sink)), local_storage = false;
	str err = NULL;
	BAT *u, *b = BATdescriptor(*key);
	BAT *G = BATdescriptor(*parent_slotid);
	lng timeoffset = 0;

	if (!b || !G) {
		if (b)
			BBPunfix(*key);
		// TODO: check if this error code is ok
		return createException(SQL, "hash.build_combined_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	if (private && *ht_sink && is_bat_nil(*ht_sink)) { /* TODO ... create but how big ??? */
		BAT *H = BATdescriptor(*parent_ht);
		if (!H) {
			BBPunfix(*key);
			BBPunfix(*parent_slotid);
			// TODO: check if this error code is ok
			return createException(SQL, "hash.build_combined_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		u = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
		/* Lookup parent hash */
		u->T.sink = (Sink*)ht_create(b->ttype?b->ttype:TYPE_oid, 1, (hash_table*)H->T.sink);
		u->T.private_bat = 1;
		BBPunfix(*parent_ht);
	} else {
		u = BATdescriptor(*ht_sink);
	}
	if (!u) {
		BBPunfix(*parent_slotid);
		BBPunfix(*key);
		return createException(SQL, "hash.build_combined_table", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			if (!h->allocators)
				err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			else
				h->nr_allocators = p->p->nr_workers;
		}
		pipeline_unlock(p);
		assert(p->wid < p->p->nr_workers);
		if (!h->allocators[p->wid]) {
			h->allocators[p->wid] = ma_create();
			if (!h->allocators[p->wid])
				err = createException(MAL, "hash.build_combined_table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		int tt = b->ttype;
		oid *gp = Tloc(g, 0);
		gid *gi = Tloc(G, 0);
		gid *pgids = h->pgids;

		if (cnt && !err) {
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
				throw(MAL, "hash.build_combined_table", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
			}
		}
		if (!err) { // TODO: shouldn't the BBPunfix be done also with an error?
			BBPunfix(b->batCacheid);
			BBPunfix(G->batCacheid);
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
	}
	TIMEOUT_CHECK(timeoffset, throw(MAL, "hash.build_combined_table", GDK_EXCEPTION));
	if (err || p->p->status)
		throw(MAL, "hash.build_combined_table", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
}

static str
HASHadd_payload(bat *hp_sink, bat *payload, bat *parent_slotid, const ptr *H)
{
	(void) hp_sink;
	(void) payload;
	(void) parent_slotid;
	(void) H;

	return MAL_SUCCEED;
}

#define vhash(Type) 						\
	do { 							\
		Type *ky = Tloc(k, 0); 				\
		Type *hs = Tloc(h, 0); 				\
								\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { 	\
			hs[i] = _hash_##Type(ky[i]) & mask; 	\
		} 						\
	} while (0)

#define hash(Type) 						\
	do { 							\
		Type *ky = Tloc(k, 0); 				\
		Type *hs = Tloc(h, 0); 				\
								\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { 	\
			hs[i] = _hash_##Type(ky[i]) & mask; 	\
		} 						\
	} while (0)

#define fhash(Type, BaseType) 							\
	do { 									\
		Type *ky = Tloc(k, 0); 						\
		Type *hs = Tloc(h, 0); 						\
										\
		TIMEOUT_LOOP_IDX_DECL(i, cnt, timeoffset) { 			\
			hs[i] = _hash_##Type(*(((BaseType*)ky)+i)) & mask; 	\
		} 								\
	} while (0)

static str
UHASHhash(bat *hsh, bat *key)
{
	BAT *h = NULL, *k = NULL;
	BUN cnt;
	unsigned int mask;
	lng timeoffset = 0;

	k = BATdescriptor(*key);
	if (!k) // TODO: shouldn't this error be about RUNTIME_OBJECT_MISSING?
		return createException(SQL, "hash.hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	cnt = BATcount(k);
	h = COLnew(k->hseqbase, k->ttype?k->ttype:TYPE_oid, cnt, TRANSIENT);
	if (!h) { // TODO: check all use of COLnew in pipeline code that it checks the result
		BBPunfix(*key);
		return createException(SQL, "hash.hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	if (cnt) {
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		if (qry_ctx != NULL) {
			timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
		}
		mask = compute_mask(cnt);
		switch(k->ttype) {
			case TYPE_void:
				//vhash();
				throw(MAL, "hash.hash", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
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
				throw(MAL, "hash.hash", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
			default:
				throw(MAL, "hash.hash", SQLSTATE(HY000) TYPE_NOT_SUPPORTED);
		}
	}
	BBPunfix(*key);
	BATsetcount(h, cnt);
	BATnegateprops(h);
	*hsh = h->batCacheid;
	BBPkeepref(h);

	// TODO: do we have a timeout exception, instead of GDK_EXCEPTION?
	TIMEOUT_CHECK(timeoffset, throw(MAL, "hash.hash", GDK_EXCEPTION));
	return MAL_SUCCEED;
}

	static str
UHASHcombined_hash(bat *hsh, bat *key, bat *selected, bat *parent_slotid)
{
	(void) hsh;
	(void) key;
	(void) selected;
	(void) parent_slotid;

	return MAL_SUCCEED;
}

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

static str
HASHexpand(bat *expanded, bat *key, bat *selected, bat *hp_sink)
{
	(void) expanded;
	(void) key;
	(void) selected;
	(void) hp_sink;

	return MAL_SUCCEED;
}

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

 command("hash", "expand", HASHexpand, false, "Duplicate the selected items in the given column according to their frequencies denoted in the hash payload", args(1,5, batargany("expanded",1),batargany("key",1),batarg("selected",oid),batargany("hp_sink",2))),
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
