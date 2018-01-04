/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _GDK_SEARCH_H_
#define _GDK_SEARCH_H_
/*
 * @+ Hash indexing
 *
 * This is a highly efficient implementation of simple bucket-chained
 * hashing.
 *
 * In the past, we used integer modulo for hashing, with bucket chains
 * of mean size 4.  This was shown to be inferior to direct hashing
 * with integer anding. The new implementation reflects this.
 */
gdk_export void HASHdestroy(BAT *b);
gdk_export BUN HASHprobe(const Hash *h, const void *v);
gdk_export BUN HASHlist(Hash *h, BUN i);


#define HASHnil(H)	(H)->nil

/* play around with h->Hash[i] and h->Link[j] */
#define HASHget2(h,i)		((BUN) ((BUN2type*) (h)->Hash)[i])
#define HASHput2(h,i,v)		(((BUN2type*) (h)->Hash)[i] = (BUN2type) (v))
#define HASHgetlink2(h,i)	((BUN) ((BUN2type*) (h)->Link)[i])
#define HASHputlink2(h,i,v)	(((BUN2type*) (h)->Link)[i] = (BUN2type) (v))
#define HASHget4(h,i)		((BUN) ((BUN4type*) (h)->Hash)[i])
#define HASHput4(h,i,v)		(((BUN4type*) (h)->Hash)[i] = (BUN4type) (v))
#define HASHgetlink4(h,i)	((BUN) ((BUN4type*) (h)->Link)[i])
#define HASHputlink4(h,i,v)	(((BUN4type*) (h)->Link)[i] = (BUN4type) (v))
#if SIZEOF_BUN == 8
#define HASHget8(h,i)		((BUN) ((BUN8type*) (h)->Hash)[i])
#define HASHput8(h,i,v)		(((BUN8type*) (h)->Hash)[i] = (BUN8type) (v))
#define HASHgetlink8(h,i)	((BUN) ((BUN8type*) (h)->Link)[i])
#define HASHputlink8(h,i,v)	(((BUN8type*) (h)->Link)[i] = (BUN8type) (v))
#endif

#if SIZEOF_BUN <= 4
#define HASHget(h,i)				\
	((h)->width == BUN4 ? HASHget4(h,i) : HASHget2(h,i))
#define HASHput(h,i,v)				\
	do {					\
		if ((h)->width == 2) {		\
			HASHput2(h,i,v);	\
		} else {			\
			HASHput4(h,i,v);	\
		}				\
	} while (0)
#define HASHgetlink(h,i)				\
	((h)->width == BUN4 ? HASHgetlink4(h,i) : HASHgetlink2(h,i))
#define HASHputlink(h,i,v)			\
	do {					\
		if ((h)->width == 2) {		\
			HASHputlink2(h,i,v);	\
		} else {			\
			HASHputlink4(h,i,v);	\
		}				\
	} while (0)
#define HASHputall(h, i, v)					\
	do {							\
		if ((h)->width == 2) {				\
			HASHputlink2(h, i, HASHget2(h, v));	\
			HASHput2(h, v, i);			\
		} else {					\
			HASHputlink4(h, i, HASHget4(h, v));	\
			HASHput4(h, v, i);			\
		}						\
	} while (0)
#else
#define HASHget(h,i)					\
	((h)->width == BUN8 ? HASHget8(h,i) :		\
	 (h)->width == BUN4 ? HASHget4(h,i) :		\
	 HASHget2(h,i))
#define HASHput(h,i,v)				\
	do {					\
		switch ((h)->width) {		\
		case 2:				\
			HASHput2(h,i,v);	\
			break;			\
		case 4:				\
			HASHput4(h,i,v);	\
			break;			\
		case 8:				\
			HASHput8(h,i,v);	\
			break;			\
		}				\
	} while (0)
#define HASHgetlink(h,i)				\
	((h)->width == BUN8 ? HASHgetlink8(h,i) :	\
	 (h)->width == BUN4 ? HASHgetlink4(h,i) :	\
	 HASHgetlink2(h,i))
#define HASHputlink(h,i,v)			\
	do {					\
		switch ((h)->width) {		\
		case 2:				\
			HASHputlink2(h,i,v);	\
			break;			\
		case 4:				\
			HASHputlink4(h,i,v);	\
			break;			\
		case 8:				\
			HASHputlink8(h,i,v);	\
			break;			\
		}				\
	} while (0)
#define HASHputall(h, i, v)					\
	do {							\
		switch ((h)->width) {				\
		case 2:						\
			HASHputlink2(h, i, HASHget2(h, v));	\
			HASHput2(h, v, i);			\
			break;					\
		case 4:						\
			HASHputlink4(h, i, HASHget4(h, v));	\
			HASHput4(h, v, i);			\
			break;					\
		case 8:						\
			HASHputlink8(h, i, HASHget8(h, v));	\
			HASHput8(h, v, i);			\
			break;					\
		}						\
	} while (0)
#endif

#define mix_bte(X)	((unsigned int) (unsigned char) (X))
#define mix_sht(X)	((unsigned int) (unsigned short) (X))
#define mix_int(X)	(((unsigned int) (X) >> 7) ^	\
			 ((unsigned int) (X) >> 13) ^	\
			 ((unsigned int) (X) >> 21) ^	\
			 (unsigned int) (X))
#define mix_lng(X)	(((ulng) (X) >> 7) ^	\
			 ((ulng) (X) >> 13) ^	\
			 ((ulng) (X) >> 21) ^	\
			 ((ulng) (X) >> 31) ^	\
			 ((ulng) (X) >> 38) ^	\
			 ((ulng) (X) >> 46) ^	\
			 ((ulng) (X) >> 56) ^	\
			 (ulng) (X))
#ifdef HAVE_HGE
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
#define hash_any(H,V)	(ATOMhash((H)->type, (V)) & (H)->mask)
#define heap_hash_any(hp,H,V)	((hp) && (hp)->hashash ? ((BUN *) (V))[-1] & (H)->mask : hash_any(H,V))
#define hash_bte(H,V)	(assert(((H)->mask & 0xFF) == 0xFF), (BUN) mix_bte(*(const unsigned char*) (V)))
#define hash_sht(H,V)	(assert(((H)->mask & 0xFFFF) == 0xFFFF), (BUN) mix_sht(*(const unsigned short*) (V)))
#define hash_int(H,V)	((BUN) mix_int(*(const unsigned int *) (V)) & (H)->mask)
/* XXX return size_t-sized value for 8-byte oid? */
#define hash_lng(H,V)	((BUN) mix_lng(*(const ulng *) (V)) & (H)->mask)
#ifdef HAVE_HGE
#define hash_hge(H,V)	((BUN) mix_hge(*(const uhge *) (V)) & (H)->mask)
#endif
#if SIZEOF_OID == SIZEOF_INT
#define hash_oid(H,V)	hash_int(H,V)
#else
#define hash_oid(H,V)	hash_lng(H,V)
#endif

#define hash_flt(H,V)	hash_int(H,V)
#define hash_dbl(H,V)	hash_lng(H,V)

#define HASHfnd_str(x,y,z)						\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if (BAThash((y).b, 0) == GDK_SUCCEED) {			\
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
		if (BAThash((y).b, 0) == GDK_SUCCEED) {		\
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
		if (BAThash((y).b, 0) == GDK_SUCCEED) {			\
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
 * configured hash tables.
 * Use HASHins_any or HASHins_<tpe> instead if you know what you're
 * doing or want to keep the hash. */
#define HASHins(b,i,v)							\
	do {								\
		if ((b)->thash) {					\
			if (((i) & 1023) == 1023 && HASHgonebad((b), (v))) { \
				HASHdestroy(b);				\
			} else {					\
				BUN _c = HASHprobe((b)->thash, (v));	\
				HASHputall((b)->thash, (i), _c);	\
				(b)->thash->heap->dirty = TRUE;		\
			}						\
		}							\
	} while (0)

#endif /* _GDK_SEARCH_H_ */
