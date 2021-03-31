/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _GDK_SEARCH_H_
#define _GDK_SEARCH_H_

struct Hash {
	int type;		/* type of index entity */
	uint8_t width;		/* width of hash entries */
	BUN mask1;		/* .mask1 < .nbucket <= .mask2 */
	BUN mask2;		/* ... both are power-of-two minus one */
	BUN nbucket;		/* number of valid hash buckets */
	BUN nil;		/* nil representation */
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
gdk_export gdk_return HASHgrowbucket(BAT *b);

#define HASHnil(H)	(H)->nil

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
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		((BUN2type *) h->Bckt)[i] = (BUN2type) v;
		break;
#endif
	default:		/* BUN4 */
		((BUN4type *) h->Bckt)[i] = (BUN4type) v;
		break;
#ifdef BUN8
	case BUN8:
		((BUN8type *) h->Bckt)[i] = (BUN8type) v;
		break;
#endif
	}
}

static inline void
HASHputlink(Hash *h, BUN i, BUN v)
{
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		((BUN2type *) h->Link)[i] = (BUN2type) v;
		break;
#endif
	default:		/* BUN4 */
		((BUN4type *) h->Link)[i] = (BUN4type) v;
		break;
#ifdef BUN8
	case BUN8:
		((BUN8type *) h->Link)[i] = (BUN8type) v;
		break;
#endif
	}
}

static inline BUN __attribute__((__pure__))
HASHget(Hash *h, BUN i)
{
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		return (BUN) ((BUN2type *) h->Bckt)[i];
#endif
	default:		/* BUN4 */
		return (BUN) ((BUN4type *) h->Bckt)[i];
#ifdef BUN8
	case BUN8:
		return (BUN) ((BUN8type *) h->Bckt)[i];
#endif
	}
}

static inline BUN __attribute__((__pure__))
HASHgetlink(Hash *h, BUN i)
{
	switch (h->width) {
#ifdef BUN2
	case BUN2:
		return (BUN) ((BUN2type *) h->Link)[i];
#endif
	default:		/* BUN4 */
		return (BUN) ((BUN4type *) h->Link)[i];
#ifdef BUN8
	case BUN8:
		return (BUN) ((BUN8type *) h->Link)[i];
#endif
	}
}

/* mix_bte(0x80) == 0x80 */
#define mix_bte(X)	((unsigned int) (unsigned char) (X))
/* mix_sht(0x8000) == 0x8000 */
#define mix_sht(X)	((unsigned int) (unsigned short) (X))
/* mix_int(0x81060038) == 0x80000000 */
#define mix_int(X)	(((unsigned int) (X) >> 7) ^	\
			 ((unsigned int) (X) >> 13) ^	\
			 ((unsigned int) (X) >> 21) ^	\
			 (unsigned int) (X))
/* mix_lng(0x810600394347424F) == 0x8000000000000000 */
#define mix_lng(X)	(((ulng) (X) >> 7) ^	\
			 ((ulng) (X) >> 13) ^	\
			 ((ulng) (X) >> 21) ^	\
			 ((ulng) (X) >> 31) ^	\
			 ((ulng) (X) >> 38) ^	\
			 ((ulng) (X) >> 46) ^	\
			 ((ulng) (X) >> 56) ^	\
			 (ulng) (X))
#ifdef HAVE_HGE
/* mix_hge(0x810600394347424F90AC1429D6BFCC57) ==
 * 0x80000000000000000000000000000000 */
#define mix_hge(X)	(((uhge) (X) >> 7) ^	\
			 ((uhge) (X) >> 13) ^	\
			 ((uhge) (X) >> 21) ^	\
			 ((uhge) (X) >> 31) ^	\
			 ((uhge) (X) >> 38) ^	\
			 ((uhge) (X) >> 46) ^	\
			 ((uhge) (X) >> 56) ^	\
			 ((uhge) (X) >> 65) ^	\
			 ((uhge) (X) >> 70) ^	\
			 ((uhge) (X) >> 78) ^	\
			 ((uhge) (X) >> 85) ^	\
			 ((uhge) (X) >> 90) ^	\
			 ((uhge) (X) >> 98) ^	\
			 ((uhge) (X) >> 107) ^	\
			 ((uhge) (X) >> 116) ^	\
			 (uhge) (X))
#endif
#define hash_loc(H,V)	hash_any(H,V)
#define hash_var(H,V)	hash_any(H,V)
#define hash_any(H,V)	HASHbucket(H, ATOMhash((H)->type, (V)))
#define hash_bte(H,V)	(assert((H)->nbucket >= 256), (BUN) mix_bte(*(const unsigned char*) (V)))
#define hash_sht(H,V)	(assert((H)->nbucket >= 65536), (BUN) mix_sht(*(const unsigned short*) (V)))
#define hash_int(H,V)	HASHbucket(H, (BUN) mix_int(*(const unsigned int *) (V)))
/* XXX return size_t-sized value for 8-byte oid? */
#define hash_lng(H,V)	HASHbucket(H, (BUN) mix_lng(*(const ulng *) (V)))
#ifdef HAVE_HGE
#define hash_hge(H,V)	HASHbucket(H, (BUN) mix_hge(*(const uhge *) (V)))
#endif
#if SIZEOF_OID == SIZEOF_INT
#define hash_oid(H,V)	hash_int(H,V)
#else
#define hash_oid(H,V)	hash_lng(H,V)
#endif

#define hash_flt(H,V)	hash_int(H,V)
#define hash_dbl(H,V)	hash_lng(H,V)

static inline BUN __attribute__((__const__))
mix_uuid(uuid u)
{
	ulng u1, u2;

	u1 = (ulng) (uint8_t) u.u[0] << 56 | (ulng) (uint8_t) u.u[1] << 48 |
		(ulng) (uint8_t) u.u[2] << 40 | (ulng) (uint8_t) u.u[3] << 32 |
		(ulng) (uint8_t) u.u[4] << 24 | (ulng) (uint8_t) u.u[5] << 16 |
		(ulng) (uint8_t) u.u[6] << 8 | (ulng) (uint8_t) u.u[7];
	u2 = (ulng) (uint8_t) u.u[8] << 56 | (ulng) (uint8_t) u.u[9] << 48 |
		(ulng) (uint8_t) u.u[10] << 40 | (ulng) (uint8_t) u.u[11] << 32 |
		(ulng) (uint8_t) u.u[12] << 24 | (ulng) (uint8_t) u.u[13] << 16 |
		(ulng) (uint8_t) u.u[14] << 8 | (ulng) (uint8_t) u.u[15];
	/* we're not using mix_hge since this way we get the same result
	 * on systems with and without 128 bit integer support */
	return (BUN) (mix_lng(u1) ^ mix_lng(u2));
}
#define hash_uuid(H,V)	HASHbucket(H, mix_uuid(*(const uuid *) (V)))

/*
 * @- hash-table supported loop over BUNs The first parameter `bi' is
 * a BAT iterator, the second (`h') should point to the Hash
 * structure, and `v' a pointer to an atomic value (corresponding to
 * the head column of `b'). The 'hb' is an BUN index, pointing out the
 * `hb'-th BUN.
 */
#define HASHloop(bi, h, hb, v)					\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMcmp(h->type, v, BUNtail(bi, hb)) == 0)
#define HASHloop_str_hv(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHbucket(h, ((BUN *) (v))[-1]));	\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h, hb))				\
		if (strEQ(v, BUNtvar(bi, hb)))
#define HASHloop_str(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHbucket(h, strHash(v)));	\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h, hb))				\
		if (strEQ(v, BUNtvar(bi, hb)))

#define HASHlooploc(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMcmp(h->type, v, BUNtloc(bi, hb)) == 0)
#define HASHloopvar(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHprobe(h, v));			\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h, hb))				\
		if (ATOMcmp(h->type, v, BUNtvar(bi, hb)) == 0)

#define HASHloop_TYPE(bi, h, hb, v, TYPE)				\
	for (hb = HASHget(h, hash_##TYPE(h, v));			\
	     hb != HASHnil(h);						\
	     hb = HASHgetlink(h,hb))					\
		if (* (const TYPE *) (v) == * (const TYPE *) BUNtloc(bi, hb))

/* need to take special care comparing nil floating point values */
#define HASHloop_fTYPE(bi, h, hb, v, TYPE)				\
	for (hb = HASHget(h, hash_##TYPE(h, v));			\
	     hb != HASHnil(h);						\
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
#ifdef HAVE_HGE
#define HASHloop_uuid(bi, h, hb, v)					\
	for (hb = HASHget(h, hash_uuid(h, v));				\
	     hb != HASHnil(h);						\
	     hb = HASHgetlink(h,hb))					\
		if (((const uuid *) (v))->h == ((const uuid *) BUNtloc(bi, hb))->h)
#else
#define HASHloop_uuid(bi, h, hb, v)					\
	for (hb = HASHget(h, hash_uuid(h, v));				\
	     hb != HASHnil(h);						\
	     hb = HASHgetlink(h,hb))					\
		if (memcmp((const uuid *) (v), (const uuid *) BUNtloc(bi, hb), 16) == 0)
//		if (((const uuid *) (v))->l[0] == ((const uuid *) BUNtloc(bi, hb))->l[0] && ((const uuid *) (v))->l[1] == ((const uuid *) BUNtloc(bi, hb))->l[1])
#endif

#define HASHfnd_str(x,y,z)						\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if (BAThash((y).b) == GDK_SUCCEED) {			\
			MT_rwlock_rdlock(&(y).b->thashlock);		\
			HASHloop_str((y), (y).b->thash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
			MT_rwlock_rdunlock(&(y).b->thashlock);		\
		} else							\
			goto hashfnd_failed;				\
	} while (0)
#define HASHfnd(x,y,z)						\
	do {							\
		BUN _i;						\
		(x) = BUN_NONE;					\
		if (BAThash((y).b) == GDK_SUCCEED) {		\
			MT_rwlock_rdlock(&(y).b->thashlock);	\
			HASHloop((y), (y).b->thash, _i, (z)) {	\
				(x) = _i;			\
				break;				\
			}					\
			MT_rwlock_rdunlock(&(y).b->thashlock);	\
		} else						\
			goto hashfnd_failed;			\
	} while (0)
#define HASHfnd_TYPE(x,y,z,TYPE)					\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if (BAThash((y).b) == GDK_SUCCEED) {			\
			MT_rwlock_rdlock(&(y).b->thashlock);		\
			HASHloop_##TYPE((y), (y).b->thash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
			MT_rwlock_rdunlock(&(y).b->thashlock);		\
		} else							\
			goto hashfnd_failed;				\
	} while (0)
#define HASHfnd_bte(x,y,z)	HASHfnd_TYPE(x,y,z,bte)
#define HASHfnd_sht(x,y,z)	HASHfnd_TYPE(x,y,z,sht)
#define HASHfnd_int(x,y,z)	HASHfnd_TYPE(x,y,z,int)
#define HASHfnd_lng(x,y,z)	HASHfnd_TYPE(x,y,z,lng)
#ifdef HAVE_HGE
#define HASHfnd_hge(x,y,z)	HASHfnd_TYPE(x,y,z,hge)
#endif
#define HASHfnd_flt(x,y,z)	HASHfnd_TYPE(x,y,z,flt)
#define HASHfnd_dbl(x,y,z)	HASHfnd_TYPE(x,y,z,dbl)
#define HASHfnd_uuid(x,y,z)	HASHfnd_TYPE(x,y,z,uuid)

#endif /* _GDK_SEARCH_H_ */
