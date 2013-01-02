/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
gdk_export void HASHremove(BAT *b);
gdk_export void HASHdestroy(BAT *b);
gdk_export BUN HASHprobe(Hash *h, const void *v);
gdk_export BUN HASHlist(Hash *h, BUN i);

#define mix_sht(X)            (((X)>>7)^(X))
#define mix_int(X)            (((X)>>7)^((X)>>13)^((X)>>21)^(X))
#define hash_loc(H,V)         hash_any(H,V)
#define hash_var(H,V)         hash_any(H,V)
#define hash_any(H,V)         (ATOMhash((H)->type, (V)) & (H)->mask)
#define heap_hash_any(hp,H,V) ((hp) && (hp)->hashash ? ((BUN *) (V))[-1] & (H)->mask : hash_any(H,V))
#define hash_bte(H,V)         ((BUN) (*(const unsigned char*) (V)) & (H)->mask)
#define hash_sht(H,V)         ((BUN) mix_sht(*((const unsigned short*) (V))) & (H)->mask)
#define hash_int(H,V)         ((BUN) mix_int(*((const unsigned int*) (V))) & (H)->mask)
/* XXX return size_t-sized value for 8-byte oid? */
#define hash_lng(H,V)         ((BUN) mix_int((unsigned int) (*(const lng *)(V) ^ (*(lng *)(V) >> 32))) & (H)->mask)
#if SIZEOF_OID == SIZEOF_INT
#define hash_oid(H,V)         ((BUN) mix_int((unsigned int) *((const oid*) (V))) & (H)->mask)
#else
#define hash_oid(H,V)         ((BUN) mix_int((unsigned int) (*(const oid *)(V) ^ (*(const oid *)(V) >> 32))) & (H)->mask)
#endif
#if SIZEOF_WRD == SIZEOF_INT
#define hash_wrd(H,V)         ((BUN) mix_int((unsigned int) *((const wrd*) (V))) & (H)->mask)
#else
#define hash_wrd(H,V)         ((BUN) mix_int((unsigned int) (*(const wrd *)(V) ^ (*(const wrd *)(V) >> 32))) & (H)->mask)
#endif

#define hash_flt(H,V)         hash_int(H,V)
#define hash_dbl(H,V)         hash_lng(H,V)

#define HASHfnd_str(x,y,z)						\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if ((y).b->H->hash || BAThash((y).b, 0) ||		\
		    GDKfatal("HASHfnd_str: hash build failed on %s.\n",	\
			     BATgetId((y).b)))				\
			HASHloop_str((y), (y).b->H->hash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
	} while (0)
#define HASHfnd_str_hv(x,y,z)						\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if ((y).b->H->hash || BAThash((y).b, 0) ||		\
		    GDKfatal("HASHfnd_str_hv: hash build failed on %s.\n", \
			     BATgetId((y).b)))				\
			HASHloop_str_hv((y), (y).b->H->hash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
	} while (0)
#define HASHfnd(x,y,z)							\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if ((y).b->H->hash || BAThash((y).b, 0) ||		\
		    GDKfatal("HASHfnd: hash build failed on %s.\n",	\
			     BATgetId((y).b)))				\
			HASHloop((y), (y).b->H->hash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
	} while (0)
#define HASHfnd_TYPE(x,y,z,TYPE)					\
	do {								\
		BUN _i;							\
		(x) = BUN_NONE;						\
		if ((y).b->H->hash || BAThash((y).b, 0) ||		\
		    GDKfatal("HASHfnd_" #TYPE ": hash build failed on %s.\n", \
			     BATgetId((y).b)))				\
			HASHloop_##TYPE((y), (y).b->H->hash, _i, (z)) {	\
				(x) = _i;				\
				break;					\
			}						\
	} while (0)
#define HASHfnd_bte(x,y,z)	HASHfnd_TYPE(x,y,z,bte)
#define HASHfnd_sht(x,y,z)	HASHfnd_TYPE(x,y,z,sht)
#define HASHfnd_int(x,y,z)	HASHfnd_TYPE(x,y,z,int)
#define HASHfnd_lng(x,y,z)	HASHfnd_TYPE(x,y,z,lng)
#define HASHfnd_oid(x,y,z)	HASHfnd_TYPE(x,y,z,oid)
#define HASHfnd_wrd(x,y,z)	HASHfnd_TYPE(x,y,z,wrd)

#if SIZEOF_VOID_P == SIZEOF_INT
#define HASHfnd_ptr(x,y,z)	HASHfnd_int(x,y,z)
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
#define HASHfnd_ptr(x,y,z)	HASHfnd_lng(x,y,z)
#endif
#define HASHfnd_bit(x,y,z)	HASHfnd_bte(x,y,z)
#define HASHfnd_flt(x,y,z)	HASHfnd_int(x,y,z)
#define HASHfnd_dbl(x,y,z)	HASHfnd_lng(x,y,z)
#define HASHfnd_any(x,y,z)	HASHfnd(x,y,z)
/*
 * A new entry is added with HASHins using the BAT, the BUN index, and
 * a pointer to the value to be stored. An entry is removed by HASdel.
 */
#define HASHins_TYPE(h, i, v, TYPE)		\
	do {					\
		BUN _c = hash_##TYPE(h,v);	\
		h->link[i] = h->hash[_c];	\
		h->hash[_c] = i;		\
	} while (0)

#define HASHins_str(h,i,v)			\
	do {					\
		BUN _c;				\
		GDK_STRHASH(v,_c);		\
		_c &= (h)->mask;		\
		h->link[i] = h->hash[_c];	\
		h->hash[_c] = i;		\
	} while (0)
#define HASHins_str_hv(h,i,v)				\
	do {						\
		BUN _c = ((BUN *) v)[-1] & (h)->mask;	\
		h->link[i] = h->hash[_c];		\
		h->hash[_c] = i;			\
	} while (0)

#define HASHins_any(h,i,v)			\
	do {					\
		BUN _c = HASHprobe(h, v);	\
		h->link[i] = h->hash[_c];	\
		h->hash[_c] = i;		\
	} while (0)

/* HASHins now receives a BAT* param and has become adaptive; killing
 * wrongly configured hash tables */
/* use HASHins_any or HASHins_<tpe> instead if you know what you're
 * doing or want to keep the hash whatever */
#define HASHins(b,i,v)							\
	do {								\
		if (((i) & 1023) == 1023 && HASHgonebad((b),(v)))	\
			HASHremove(b);					\
		else							\
			HASHins_any((b)->H->hash,(i),(v));		\
	} while (0)

#if SIZEOF_VOID_P == SIZEOF_INT
#define HASHins_ptr(h,i,v)	HASHins_int(h,i,v)
#else /* SIZEOF_VOID_P == SIZEOF_LNG */
#define HASHins_ptr(h,i,v)	HASHins_lng(h,i,v)
#endif
#define HASHins_bit(h,i,v)	HASHins_bte(h,i,v)
#if SIZEOF_OID == SIZEOF_INT	/* OIDDEPEND */
#define HASHins_oid(h,i,v)	HASHins_int(h,i,v)
#else
#define HASHins_oid(h,i,v)	HASHins_lng(h,i,v)
#endif
#define HASHins_flt(h,i,v)	HASHins_int(h,i,v)
#define HASHins_dbl(h,i,v)	HASHins_lng(h,i,v)
#define HASHinsvar(h,i,v)	HASHins_any(h,i,v)
#define HASHinsloc(h,i,v)	HASHins_any(h,i,v)

#define HASHins_bte(h,i,v)	HASHins_TYPE(h,i,v,bte)
#define HASHins_sht(h,i,v)	HASHins_TYPE(h,i,v,sht)
#define HASHins_int(h,i,v)	HASHins_TYPE(h,i,v,int)
#define HASHins_lng(h,i,v)	HASHins_TYPE(h,i,v,lng)

#define HASHdel(h, i, v, next)						\
	do {								\
		if (next && h->link[i+1] == i) {			\
			h->link[i+1] = h->link[i];			\
		} else {						\
			BUN _c = HASHprobe(h, v);			\
			if (h->hash[_c] == i) {				\
				h->hash[_c] = h->link[i];		\
			} else {					\
				for(_c = h->hash[_c]; _c != BUN_NONE;	\
						_c = h->link[_c]){	\
					if (h->link[_c] == i) {		\
						h->link[_c] = h->link[i]; \
						break;			\
					}				\
				}					\
			}						\
		} h->link[i] = BUN_NONE;				\
	} while (0)

#define HASHmove(h, i, j, v, next)					\
	do {								\
		if (next && h->link[i+1] == i) {			\
			h->link[i+1] = j;				\
		} else {						\
			BUN _c = HASHprobe(h, v);			\
			if (h->hash[_c] == i) {				\
				h->hash[_c] = j;			\
			} else {					\
				for(_c = h->hash[_c]; _c != BUN_NONE;	\
						_c = h->link[_c]){	\
					if (h->link[_c] == i) {		\
						h->link[_c] = j;	\
						break;			\
					}				\
				}					\
			}						\
		} h->link[j] = h->link[i];				\
	} while (0)
/*
 * @+ Binary Search on a Sorted BAT
 * We have two main routines, SORTfndfirst(b,v) and
 * SORTfndlast(b,v), that search for a TAIL value 'v' in a sorted
 * BAT. If the value is present, the first routine returns a pointer
 * to its first occurrence, while the second routine returns a pointer
 * to the BUN just after the last occurrence of 'v'.  In case value
 * 'v' does not occur in the tail of BAT b, both routines return a
 * pointer to the first BUN with a tail value larger than 'v' (i.e.,
 * BUNfirst(b), in case all tail values are larger than 'v'); or
 * BUNlast(b), in case all tail values are smaller than 'v'.
 *
 * From the above routines we now also defined the SORTfnd function
 * that looks for a certain value in the HEAD and returns a (not
 * necessarily the first or last) reference to it, or BUN_NONE (if the
 * value does not exist).
 *
 * Note: of the SORTfnd, only SORTfndfirst(b,v) and
 * SORTfndlast(b,v) work on the tail of a bat!
 */

gdk_export BUN SORTfnd(BAT *b, const void *v);
gdk_export BUN SORTfndfirst(BAT *b, const void *v);
gdk_export BUN SORTfndlast(BAT *b, const void *v);

#endif /* _GDK_SEARCH_H_ */
