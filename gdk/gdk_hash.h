/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _GDK_SEARCH_H_
#define _GDK_SEARCH_H_

typedef struct Hash {
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
} Hash;

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
#if SIZEOF_BUN > 4
#define BUN8 8
#endif
typedef uint16_t BUN2type;
typedef uint32_t BUN4type;
#if SIZEOF_BUN > 4
typedef uint64_t BUN8type;
#endif
#define BUN2_NONE ((BUN2type) UINT16_C(0xFFFF))
#define BUN4_NONE ((BUN4type) UINT32_C(0xFFFFFFFF))
#if SIZEOF_BUN > 4
#define BUN8_NONE ((BUN8type) UINT64_C(0xFFFFFFFFFFFFFFFF))
#endif

/* play around with h->Bckt[i] and h->Link[j] */

static inline void
HASHput(Hash *h, BUN i, BUN v)
{
	switch (h->width) {
	default:		/* BUN2 */
		((BUN2type *) h->Bckt)[i] = (BUN2type) v;
		break;
	case BUN4:
		((BUN4type *) h->Bckt)[i] = (BUN4type) v;
		break;
#if SIZEOF_BUN == 8
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
	default:		/* BUN2 */
		((BUN2type *) h->Link)[i] = (BUN2type) v;
		break;
	case BUN4:
		((BUN4type *) h->Link)[i] = (BUN4type) v;
		break;
#if SIZEOF_BUN == 8
	case BUN8:
		((BUN8type *) h->Link)[i] = (BUN8type) v;
		break;
#endif
	}
}

static inline BUN
HASHget(Hash *h, BUN i)
{
	switch (h->width) {
	default:		/* BUN2 */
		return (BUN) ((BUN2type *) h->Bckt)[i];
	case BUN4:
		return (BUN) ((BUN4type *) h->Bckt)[i];
#if SIZEOF_BUN == 8
	case BUN8:
		return (BUN) ((BUN8type *) h->Bckt)[i];
#endif
	}
}

static inline BUN
HASHgetlink(Hash *h, BUN i)
{
	switch (h->width) {
	default:		/* BUN2 */
		return (BUN) ((BUN2type *) h->Link)[i];
	case BUN4:
		return (BUN) ((BUN4type *) h->Link)[i];
#if SIZEOF_BUN == 8
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
		if (GDK_STREQ(v, BUNtvar(bi, hb)))
#define HASHloop_str(bi, h, hb, v)				\
	for (hb = HASHget(h, HASHbucket(h, strHash(v)));	\
	     hb != HASHnil(h);					\
	     hb = HASHgetlink(h, hb))				\
		if (GDK_STREQ(v, BUNtvar(bi, hb)))

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

#define HASHloop_bte(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, bte)
#define HASHloop_sht(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, sht)
#define HASHloop_int(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, int)
#define HASHloop_lng(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, lng)
#ifdef HAVE_HGE
#define HASHloop_hge(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, hge)
#endif
#define HASHloop_flt(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, flt)
#define HASHloop_dbl(bi, h, hb, v)	HASHloop_TYPE(bi, h, hb, v, dbl)

#define HASHfnd_str(x,y,z)						\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if (BAThash((y).b) == GDK_SUCCEED) {			\
			HASHloop_str((y), (y).b->thash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
		} else							\
			goto hashfnd_failed;				\
	} while (0)
#define HASHfnd(x,y,z)						\
	do {							\
		BUN _i;						\
		(x) = BUN_NONE;					\
		if (BAThash((y).b) == GDK_SUCCEED) {		\
			HASHloop((y), (y).b->thash, _i, (z)) {	\
				(x) = _i;			\
				break;				\
			}					\
		} else						\
			goto hashfnd_failed;			\
	} while (0)
#define HASHfnd_TYPE(x,y,z,TYPE)					\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if (BAThash((y).b) == GDK_SUCCEED) {			\
			HASHloop_##TYPE((y), (y).b->thash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
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

/*
 * A new entry is added with HASHins using the BAT, the BUN index, and
 * a pointer to the value to be stored.
 *
 * HASHins receives a BAT* param and is adaptive, killing wrongly
 * configured hash tables.  Also, persistent hashes cannot be
 * maintained, so must be destroyed before this macro is called. */
#define HASHins(b,i,v)							\
	do {								\
		if ((b)->thash) {					\
			MT_lock_set(&(b)->batIdxLock);			\
			Hash *_h = (b)->thash;				\
			if (_h == (Hash *) 1 ||				\
			    _h == NULL ||				\
			    (ATOMsize(b->ttype) > 2 &&			\
			     HASHgrowbucket(b) != GDK_SUCCEED) ||	\
			    (((i) + 1) * _h->width > _h->heaplink.size && \
			     HEAPextend(&_h->heaplink,			\
					(i) * _h->width + GDK_mmap_pagesize, \
					true) != GDK_SUCCEED)) {	\
				MT_lock_unset(&(b)->batIdxLock);	\
				HASHdestroy(b);				\
			} else {					\
				_h->Link = _h->heaplink.base;		\
				BUN _c = HASHprobe(_h, (v));		\
				_h->heaplink.free += _h->width;		\
				BUN _hb = HASHget(_h, _c);		\
				BUN _hb2;				\
				BATiter _bi = bat_iterator(b);		\
				for (_hb2 = _hb;			\
				     _hb2 != HASHnil(_h);		\
				     _hb2 = HASHgetlink(_h, _hb2)) {	\
					if (ATOMcmp(_h->type,		\
						    (v),		\
						    BUNtail(_bi, _hb2)) == 0) \
						break;			\
				}					\
				_h->nheads += _hb == HASHnil(_h);	\
				_h->nunique += _hb2 == HASHnil(_h);	\
				HASHputlink(_h, i, _hb);		\
				HASHput(_h, _c, i);			\
				_h->heapbckt.dirty = true;		\
				_h->heaplink.dirty = true;		\
				MT_lock_unset(&(b)->batIdxLock);	\
			}						\
		}							\
	} while (0)

#endif /* _GDK_SEARCH_H_ */
