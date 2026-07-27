/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _GDK_SEARCH_H_
#define _GDK_SEARCH_H_

#define XXH_INLINE_ALL
#include <xxhash.h>

struct Hash {
	int type;		/* type of index entity */
	uint8_t width;		/* width of hash entries */
	BUN mask1;		/* .mask1 < .nbucket <= .mask2 */
	BUN mask2;		/* ... both are power-of-two minus one */
	BUN nbucket;		/* number of valid hash buckets */
	BUN nunique;		/* number of unique values */
	BUN nheads;		/* number of chain heads */
	void *Bckt;		/* hash buckets, points into .heapbckt */
	void *Link;		/* collision list, points into .heaplink */
	Heap heaplink;		/* heap where the hash links are stored */
	Heap heapbckt;		/* heap where the hash buckets are stored */
};

static inline BUN
HASHbucket(const Hash *h, BUN v)
{
	return (v &= h->mask2) < h->nbucket ? v : v & h->mask1;
}

gdk_export gdk_return BAThash(BAT *b);
gdk_export void HASHdestroy(BAT *b);
gdk_export BUN HASHprobe(const Hash *h, const void *v);
gdk_export BUN HASHlist(Hash *h, BUN i);
gdk_export size_t HASHsize(BAT *b);

#define BUN2 2
#define BUN4 4
#if SIZEOF_BUN == 8
#define BUN8 8
#endif
#ifdef BUN2
typedef uint16_t BUN2type;
#endif
typedef uint32_t BUN4type;
#if SIZEOF_BUN > 4
typedef uint64_t BUN8type;
#endif
#ifdef BUN2
#define BUN2_NONE ((BUN2type) UINT16_C(0xFFFF))
#endif
#define BUN4_NONE ((BUN4type) UINT32_C(0xFFFFFFFF))
#ifdef BUN8
#define BUN8_NONE ((BUN8type) UINT64_C(0xFFFFFFFFFFFFFFFF))
#endif

/* play around with h->Bckt[i] and h->Link[j] */

static inline void
HASHput(Hash *h, BUN i, BUN v)
{
	/* if v == BUN_NONE, assigning the value to a BUN2type
	 * etc. automatically converts to BUN2_NONE etc. */
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		((BUN2type *) h->Bckt)[i] = (BUN2type) v;
		break;
#endif
	case BUN4:
		((BUN4type *) h->Bckt)[i] = (BUN4type) v;
		break;
#ifdef BUN8
	case BUN8:
		((BUN8type *) h->Bckt)[i] = (BUN8type) v;
		break;
#endif
	default:
		MT_UNREACHABLE();
	}
}

static inline void
HASHputlink(Hash *h, BUN i, BUN v)
{
	/* if v == BUN_NONE, assigning the value to a BUN2type
	 * etc. automatically converts to BUN2_NONE etc. */
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		assert(v == BUN_NONE || v == BUN2_NONE || v < i);
		((BUN2type *) h->Link)[i] = (BUN2type) v;
		break;
#endif
	case BUN4:
		assert(v == BUN_NONE || v == BUN4_NONE || v < i);
		((BUN4type *) h->Link)[i] = (BUN4type) v;
		break;
#ifdef BUN8
	case BUN8:
		assert(v == BUN_NONE || v == BUN8_NONE || v < i);
		((BUN8type *) h->Link)[i] = (BUN8type) v;
		break;
#endif
	default:
		MT_UNREACHABLE();
	}
}

__attribute__((__pure__))
static inline BUN
HASHget(const Hash *h, BUN i)
{
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		i = (BUN) ((BUN2type *) h->Bckt)[i];
		return i == BUN2_NONE ? BUN_NONE : i;
#endif
	case BUN4:
		i = (BUN) ((BUN4type *) h->Bckt)[i];
		return i == BUN4_NONE ? BUN_NONE : i;
#ifdef BUN8
	case BUN8:
		i = (BUN) ((BUN8type *) h->Bckt)[i];
		return i == BUN8_NONE ? BUN_NONE : i;
#endif
	default:
		MT_UNREACHABLE();
	}
}

__attribute__((__pure__))
static inline BUN
HASHgetlink(const Hash *h, BUN i)
{
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		i = (BUN) ((BUN2type *) h->Link)[i];
		return i == BUN2_NONE ? BUN_NONE : i;
#endif
	case BUN4:
		i = (BUN) ((BUN4type *) h->Link)[i];
		return i == BUN4_NONE ? BUN_NONE : i;
#ifdef BUN8
	case BUN8:
		i = (BUN) ((BUN8type *) h->Link)[i];
		return i == BUN8_NONE ? BUN_NONE : i;
#endif
	default:
		MT_UNREACHABLE();
	}
}

#if SIZEOF_BUN == 4
#define XXHASHFUNC	XXH32
#else
#define XXHASHFUNC	XXH64
#endif

__attribute__((__pure__))
static inline BUN
bteHash(const void *x)
{
	return (BUN) *(uint8_t *) x;
}

__attribute__((__pure__))
static inline BUN
shtHash(const void *x)
{
	return (BUN) *(uint16_t *) x;
}

__attribute__((__pure__))
static inline BUN
intHash(const void *x)
{
	return (BUN) XXH32(x, sizeof(int), 0);
}

__attribute__((__pure__))
static inline BUN
lngHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(lng), 0);
}

#ifdef HAVE_HGE
__attribute__((__pure__))
static inline BUN
hgeHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(hge), 0);
}
#endif

__attribute__((__pure__))
static inline BUN
fltHash(const void *x)
{
	if (is_flt_nil(*(const flt *)x)) /* any NaN */
		return intHash(&(uint32_t){UINT32_C(0x7FC00000)});
	if (*(const flt *)x == 0) /* +0 or -0 */
		return (BUN) intHash(&(uint32_t){0});
	return intHash(x);
}

__attribute__((__pure__))
static inline BUN
dblHash(const void *x)
{
	if (is_dbl_nil(*(const dbl *)x)) /* any NaN */
		return lngHash(&(uint64_t){UINT64_C(0x7FF8000000000000)});
	if (*(const dbl *)x == 0) /* +0 or -0 */
		return lngHash(&(uint64_t){0});
	return lngHash(x);
}

__attribute__((__pure__))
static inline BUN
strHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, strlen(x), 0);
}

__attribute__((__pure__))
static inline BUN
uuidHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(uuid), 0);
}

__attribute__((__pure__))
static inline BUN
inet4Hash(const void *x)
{
	return (BUN) XXH32(x, sizeof(inet4), 0);
}

__attribute__((__pure__))
static inline BUN
inet6Hash(const void *x)
{
	return (BUN) XXHASHFUNC(x, sizeof(inet6), 0);
}

__attribute__((__pure__))
static inline BUN
blobHash(const void *x)
{
	return (BUN) XXHASHFUNC(x, blobsize(((const blob *) x)->nitems), 0);
}

#define hash_loc(H,V)	hash_any(H,V)
#define hash_var(H,V)	hash_any(H,V)
#define hash_any(H,V)	HASHbucket(H, ATOMhash((H)->type, (V)))
#define hash_bte(H,V)	(assert((H)->nbucket >= 256), bteHash(V))
#define hash_sht(H,V)	(assert((H)->nbucket >= 65536), shtHash(V))
#define hash_int(H,V)	HASHbucket(H, intHash(V))
/* XXX return size_t-sized value for 8-byte oid? */
#define hash_lng(H,V)	HASHbucket(H, lngHash(V))
#ifdef HAVE_HGE
#define hash_hge(H,V)	HASHbucket(H, hgeHash(V))
#endif
#if SIZEOF_OID == SIZEOF_INT
#define hash_oid(H,V)	hash_int(H,V)
#else
#define hash_oid(H,V)	hash_lng(H,V)
#endif
#define hash_inet4(H,V)	HASHbucket(H, inet4Hash(V))

#define hash_flt(H,V)	HASHbucket(H, ATOMhash(TYPE_flt, (V)))
#define hash_dbl(H,V)	HASHbucket(H, ATOMhash(TYPE_dbl, (V)))

#define hash_uuid(H,V)	HASHbucket(H, uuidHash(V))

#define hash_inet6(H,V)	HASHbucket(H, inet6Hash(V))

/*
 * @- hash-table supported loop over BUNs The first parameter `bi' is
 * a BAT iterator, the second (`h') should point to the Hash
 * structure, and `v' a pointer to an atomic value (corresponding to
 * the head column of `b'). The 'hb' is an BUN index, pointing out the
 * `hb'-th BUN.
 */
#define HASHloop(bi, h, hb, v)					\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMeq(h->type, v, BUNtail(bi, hb)))
#define HASHloop_str(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHbucket(h, strHash(v)));	\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (strEQ(v, BUNtvar(bi, hb)))

#define HASHlooploc(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMeq(h->type, v, BUNtloc(bi, hb)))
#define HASHloopvar(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != BUN_NONE;					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMeq(h->type, v, BUNtvar(bi, hb)))

#define HASHloop_TYPE(bi, h, hb, v, TYPE)				\
	for (hb = HASHget(h, hash_##TYPE(h, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (* (const TYPE *) (v) == * (const TYPE *) BUNtloc(bi, hb))

/* need to take special care comparing nil floating point values */
#define HASHloop_fTYPE(bi, h, hb, v, TYPE)				\
	for (hb = HASHget(h, hash_##TYPE(h, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (is_##TYPE##_nil(* (const TYPE *) (v))		\
		    ? is_##TYPE##_nil(* (const TYPE *) BUNtloc(bi, hb)) \
		    : * (const TYPE *) (v) == * (const TYPE *) BUNtloc(bi, hb))

#define HASHloop_bte(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, bte)
#define HASHloop_sht(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, sht)
#define HASHloop_int(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, int)
#define HASHloop_lng(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, lng)
#ifdef HAVE_HGE
#define HASHloop_hge(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, hge)
#endif
#define HASHloop_flt(bi, h, hb, v)	HASHloop_fTYPE(bi, h, hb, v, flt)
#define HASHloop_dbl(bi, h, hb, v)	HASHloop_fTYPE(bi, h, hb, v, dbl)
#define HASHloop_inet4(bi, hsh, hb, v)					\
	for (hb = HASHget(hsh, hash_inet4(hsh, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(hsh,hb))					\
		if (((const inet4 *) (v))->align == ((const inet4 *) BUNtloc(bi, hb))->align)
#ifdef HAVE_HGE
#define HASHloop_uuid(bi, hsh, hb, v)					\
	for (hb = HASHget(hsh, hash_uuid(hsh, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(hsh,hb))					\
		if (((const uuid *) (v))->h == ((const uuid *) BUNtloc(bi, hb))->h)
#define HASHloop_inet6(bi, hsh, hb, v)					\
	for (hb = HASHget(hsh, hash_inet6(hsh, v));			\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(hsh,hb))					\
		if (((const inet6 *) (v))->align == ((const inet6 *) BUNtloc(bi, hb))->align)
#else
#define HASHloop_uuid(bi, h, hb, v)					\
	for (hb = HASHget(h, hash_uuid(h, v));				\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (memcmp((const uuid *) (v), (const uuid *) BUNtloc(bi, hb), 16) == 0)
//		if (((const uuid *) (v))->l[0] == ((const uuid *) BUNtloc(bi, hb))->l[0] && ((const uuid *) (v))->l[1] == ((const uuid *) BUNtloc(bi, hb))->l[1])
#define HASHloop_inet6(bi, h, hb, v)					\
	for (hb = HASHget(h, hash_inet6(h, v));				\
	     hb != BUN_NONE;						\
	     hb = HASHgetlink(h,hb))					\
		if (memcmp((const inet6 *) (v), (const inet6 *) BUNtloc(bi, hb), 16) == 0)
//		if (((const inet6 *) (v))->align[0] == ((const inet6 *) BUNtloc(bi, hb))->align[0] && ((const inet6 *) (v))->align[1] == ((const inet6 *) BUNtloc(bi, hb))->align[1])
#endif

#endif /* _GDK_SEARCH_H_ */
