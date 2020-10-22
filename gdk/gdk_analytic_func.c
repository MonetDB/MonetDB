/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_analytic.h"
#include "gdk_calc_private.h"

#define NTILE_CALC(TPE, NEXT_VALUE, LNG_HGE, UPCAST)	\
	do {					\
		for (TPE i = 0; rb < rp; i++, rb++) {	\
			TPE val = NEXT_VALUE; \
			if (is_##TPE##_nil(val)) {	\
				has_nils = true;	\
				*rb = TPE##_nil;	\
			} else { \
				UPCAST nval = (UPCAST) LNG_HGE; \
				if (nval >= ncnt) { \
					*rb = i + 1;  \
				} else { \
					UPCAST bsize = ncnt / nval; \
					UPCAST top = ncnt - nval * bsize; \
					UPCAST small = top * (bsize + 1); \
					if ((UPCAST) i < small) \
						*rb = (TPE)(1 + i / (bsize + 1)); \
					else \
						*rb = (TPE)(1 + top + (i - small) / bsize); \
				} \
			} \
		} \
	} while (0)

#define ANALYTICAL_NTILE_IMP(TPE, NEXT_VALUE, LNG_HGE, UPCAST)	\
	do {							\
		TPE *rp, *rb;	\
		UPCAST ncnt; \
		rb = rp = (TPE*)Tloc(r, 0);		\
		if (p) {					\
			pnp = np = (bit*)Tloc(p, 0);	\
			end = np + cnt;				\
			for (; np < end; np++) {	\
				if (*np) {			\
					ncnt = np - pnp;	\
					rp += ncnt;		\
					NTILE_CALC(TPE, NEXT_VALUE, LNG_HGE, UPCAST);\
					pnp = np;	\
				}				\
			}					\
			ncnt = np - pnp;			\
			rp += ncnt;				\
			NTILE_CALC(TPE, NEXT_VALUE, LNG_HGE, UPCAST);	\
		} else {					\
			ncnt = (UPCAST) cnt; \
			rp += cnt;				\
			NTILE_CALC(TPE, NEXT_VALUE, LNG_HGE, UPCAST);	\
		}						\
	} while (0)

#define ANALYTICAL_NTILE_SINGLE_IMP(TPE, LNG_HGE, UPCAST) \
	do {	\
		TPE ntl = *(TPE*) ntile; \
		ANALYTICAL_NTILE_IMP(TPE, ntl, LNG_HGE, UPCAST); \
	} while (0)

#define ANALYTICAL_NTILE_MULTI_IMP(TPE, LNG_HGE, UPCAST) \
	do {	\
		BUN k = 0; \
		TPE *restrict nn = (TPE*)Tloc(n, 0);	\
		ANALYTICAL_NTILE_IMP(TPE, nn[k++], LNG_HGE, UPCAST); \
	} while (0)

gdk_return
GDKanalyticalntile(BAT *r, BAT *b, BAT *p, BAT *n, int tpe, const void *restrict ntile)
{
	BUN cnt = BATcount(b);
	bit *np, *pnp, *end;
	bool has_nils = false;

	assert((n && !ntile) || (!n && ntile));

	if (ntile) {
		switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_NTILE_SINGLE_IMP(bte, val, BUN);
			break;
		case TYPE_sht:
			ANALYTICAL_NTILE_SINGLE_IMP(sht, val, BUN);
			break;
		case TYPE_int:
			ANALYTICAL_NTILE_SINGLE_IMP(int, val, BUN);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_INT
			ANALYTICAL_NTILE_SINGLE_IMP(lng, val, lng);
#else
			ANALYTICAL_NTILE_SINGLE_IMP(lng, val, BUN);
#endif
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
#if SIZEOF_OID == SIZEOF_INT
			ANALYTICAL_NTILE_SINGLE_IMP(hge, (val > (hge) GDK_int_max) ? GDK_int_max : (lng) val, lng);
#else
			ANALYTICAL_NTILE_SINGLE_IMP(hge, (val > (hge) GDK_lng_max) ? GDK_lng_max : (lng) val, BUN);
#endif
#endif
		default:
			goto nosupport;
		}
	} else {
		switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_NTILE_MULTI_IMP(bte, val, BUN);
			break;
		case TYPE_sht:
			ANALYTICAL_NTILE_MULTI_IMP(sht, val, BUN);
			break;
		case TYPE_int:
			ANALYTICAL_NTILE_MULTI_IMP(int, val, BUN);
			break;
		case TYPE_lng:
#if SIZEOF_OID == SIZEOF_INT
			ANALYTICAL_NTILE_MULTI_IMP(lng, val, lng);
#else
			ANALYTICAL_NTILE_MULTI_IMP(lng, val, BUN);
#endif
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
#if SIZEOF_OID == SIZEOF_INT
			ANALYTICAL_NTILE_MULTI_IMP(hge, (val > (hge) GDK_int_max) ? GDK_int_max : (lng) val, lng);
#else
			ANALYTICAL_NTILE_MULTI_IMP(hge, (val > (hge) GDK_lng_max) ? GDK_lng_max : (lng) val, BUN);
#endif
		break;
#endif
		default:
			goto nosupport;
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
nosupport:
	GDKerror("type %s not supported for the ntile type.\n", ATOMname(tpe));
	return GDK_FAIL;
}

#define ANALYTICAL_FIRST_IMP(TPE)				\
	do {							\
		TPE *bp, *bs, *be, curval, *restrict rb;	\
		bp = (TPE*)Tloc(b, 0);				\
		rb = (TPE*)Tloc(r, 0);				\
		for (; i < cnt; i++, rb++) {			\
			bs = bp + start[i];			\
			be = bp + end[i];			\
			curval = (be > bs) ? *bs : TPE##_nil;	\
			*rb = curval;				\
			has_nils |= is_##TPE##_nil(curval);		\
		}						\
	} while (0)

gdk_return
GDKanalyticalfirst(BAT *r, BAT *b, BAT *s, BAT *e, int tpe)
{
	BUN i = 0, cnt = BATcount(b);
	lng *restrict start, *restrict end;
	bool has_nils = false;

	assert(s && e);
	start = (lng *) Tloc(s, 0);
	end = (lng *) Tloc(e, 0);

	switch (ATOMbasetype(tpe)) {
	case TYPE_bte:
		ANALYTICAL_FIRST_IMP(bte);
		break;
	case TYPE_sht:
		ANALYTICAL_FIRST_IMP(sht);
		break;
	case TYPE_int:
		ANALYTICAL_FIRST_IMP(int);
		break;
	case TYPE_lng:
		ANALYTICAL_FIRST_IMP(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ANALYTICAL_FIRST_IMP(hge);
		break;
#endif
	case TYPE_flt:
		ANALYTICAL_FIRST_IMP(flt);
		break;
	case TYPE_dbl:
		ANALYTICAL_FIRST_IMP(dbl);
		break;
	default:{
		const void *restrict nil = ATOMnilptr(tpe);
		int (*atomcmp) (const void *, const void *) = ATOMcompare(tpe);
		BATiter bpi = bat_iterator(b);
		void *curval;

		for (; i < cnt; i++) {
			curval = (end[i] > start[i]) ? BUNtail(bpi, (BUN) start[i]) : (void *) nil;
			if (BUNappend(r, curval, false) != GDK_SUCCEED)
				return GDK_FAIL;
			has_nils |= atomcmp(curval, nil) == 0;
		}
	}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#define ANALYTICAL_LAST_IMP(TPE)					\
	do {								\
		TPE *bp, *bs, *be, curval, *restrict rb;		\
		bp = (TPE*)Tloc(b, 0);					\
		rb = (TPE*)Tloc(r, 0);					\
		for (; i<cnt; i++, rb++) {				\
			bs = bp + start[i];				\
			be = bp + end[i];				\
			curval = (be > bs) ? *(be - 1) : TPE##_nil;	\
			*rb = curval;					\
			has_nils |= is_##TPE##_nil(curval);			\
		}							\
	} while (0)

gdk_return
GDKanalyticallast(BAT *r, BAT *b, BAT *s, BAT *e, int tpe)
{
	BUN i = 0, cnt = BATcount(b);
	lng *restrict start, *restrict end;
	bool has_nils = false;

	assert(s && e);
	start = (lng *) Tloc(s, 0);
	end = (lng *) Tloc(e, 0);

	switch (ATOMbasetype(tpe)) {
	case TYPE_bte:
		ANALYTICAL_LAST_IMP(bte);
		break;
	case TYPE_sht:
		ANALYTICAL_LAST_IMP(sht);
		break;
	case TYPE_int:
		ANALYTICAL_LAST_IMP(int);
		break;
	case TYPE_lng:
		ANALYTICAL_LAST_IMP(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ANALYTICAL_LAST_IMP(hge);
		break;
#endif
	case TYPE_flt:
		ANALYTICAL_LAST_IMP(flt);
		break;
	case TYPE_dbl:
		ANALYTICAL_LAST_IMP(dbl);
		break;
	default:{
		const void *restrict nil = ATOMnilptr(tpe);
		int (*atomcmp) (const void *, const void *) = ATOMcompare(tpe);
		BATiter bpi = bat_iterator(b);
		void *curval;

		for (; i < cnt; i++) {
			curval = (end[i] > start[i]) ? BUNtail(bpi, (BUN) (end[i] - 1)) : (void *) nil;
			if (BUNappend(r, curval, false) != GDK_SUCCEED)
				return GDK_FAIL;
			has_nils |= atomcmp(curval, nil) == 0;
		}
	}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#define ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(TPE1)			\
	do {								\
		TPE1 *bp = (TPE1*)Tloc(b, 0), *bs, *be, curval, *restrict rb = (TPE1*)Tloc(r, 0); \
		if (is_lng_nil(nth)) {					\
			has_nils = true;				\
			for (; i < cnt; i++, rb++)			\
				*rb = TPE1##_nil;			\
		} else {						\
			nth--;						\
			for (; i < cnt; i++, rb++) {			\
				bs = bp + start[i];			\
				be = bp + end[i];			\
				curval = (be > bs && nth < (end[i] - start[i])) ? *(bs + nth) : TPE1##_nil; \
				*rb = curval;				\
				has_nils |= is_##TPE1##_nil(curval);	\
			}						\
		}							\
	} while (0)

#define ANALYTICAL_NTHVALUE_IMP_MULTI_FIXED(TPE1, TPE2, TPE3)		\
	do {								\
		TPE2 *restrict lp = (TPE2*)Tloc(l, 0);			\
		for (; i < cnt; i++, rb++) {				\
			TPE2 lnth = lp[i];				\
			bs = bp + start[i];				\
			be = bp + end[i];				\
			if (is_##TPE2##_nil(lnth) || be <= bs || (TPE3)(lnth - 1) > (TPE3)(end[i] - start[i])) { \
				curval = TPE1##_nil;	\
				has_nils = true;	\
			} else {						\
				curval = *(bs + lnth - 1);		\
				has_nils |= is_##TPE1##_nil(curval);	\
			}	\
			*rb = curval;	\
		}							\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_NTHVALUE_CALC_FIXED_HGE(TPE1)			\
	case TYPE_hge:							\
		ANALYTICAL_NTHVALUE_IMP_MULTI_FIXED(TPE1, hge, hge);	\
		break;
#else
#define ANALYTICAL_NTHVALUE_CALC_FIXED_HGE(TPE1)
#endif

#define ANALYTICAL_NTHVALUE_CALC_FIXED(TPE1)				\
	do {								\
		TPE1 *bp = (TPE1*)Tloc(b, 0), *bs, *be, curval, *restrict rb = (TPE1*)Tloc(r, 0);	\
		switch (tp2) {						\
		case TYPE_bte:						\
			ANALYTICAL_NTHVALUE_IMP_MULTI_FIXED(TPE1, bte, lng); \
			break;						\
		case TYPE_sht:						\
			ANALYTICAL_NTHVALUE_IMP_MULTI_FIXED(TPE1, sht, lng); \
			break;						\
		case TYPE_int:						\
			ANALYTICAL_NTHVALUE_IMP_MULTI_FIXED(TPE1, int, lng); \
			break;						\
		case TYPE_lng:						\
			ANALYTICAL_NTHVALUE_IMP_MULTI_FIXED(TPE1, lng, lng); \
			break;						\
		ANALYTICAL_NTHVALUE_CALC_FIXED_HGE(TPE1)		\
		default:						\
			goto nosupport;					\
		}							\
	} while (0)

#define ANALYTICAL_NTHVALUE_IMP_MULTI_VARSIZED(TPE1, TPE2)		\
	do {								\
		TPE1 *restrict lp = (TPE1*)Tloc(l, 0);			\
		for (; i < cnt; i++) {					\
			TPE1 lnth = lp[i];				\
			if (is_##TPE1##_nil(lnth) || end[i] <= start[i] || (TPE2)(lnth - 1) > (TPE2)(end[i] - start[i])) {	\
				curval = (void *) nil;			\
				has_nils = true; \
			} else {	\
				curval = BUNtail(bpi, (BUN) (start[i] + lnth - 1)); \
				has_nils |= atomcmp(curval, nil) == 0;	\
			}	\
			if (BUNappend(r, curval, false) != GDK_SUCCEED) \
				return GDK_FAIL;			\
		}							\
	} while (0)

gdk_return
GDKanalyticalnthvalue(BAT *r, BAT *b, BAT *s, BAT *e, BAT *l, const void *restrict bound, int tp1, int tp2)
{
	BUN i = 0, cnt = BATcount(b);
	lng *restrict start, *restrict end, nth = 0;
	bool has_nils = false;
	const void *restrict nil = ATOMnilptr(tp1);
	int (*atomcmp) (const void *, const void *) = ATOMcompare(tp1);
	void *curval;

	assert(s && e && ((l && !bound) || (!l && bound)));
	start = (lng *) Tloc(s, 0);
	end = (lng *) Tloc(e, 0);

	if (bound) {
		switch (tp2) {
		case TYPE_bte:{
			bte val = *(bte *) bound;
			nth = !is_bte_nil(val) ? (lng) val : lng_nil;
		} break;
		case TYPE_sht:{
			sht val = *(sht *) bound;
			nth = !is_sht_nil(val) ? (lng) val : lng_nil;
		} break;
		case TYPE_int:{
			int val = *(int *) bound;
			nth = !is_int_nil(val) ? (lng) val : lng_nil;
		} break;
		case TYPE_lng:{
			nth = *(lng *) bound;
		} break;
#ifdef HAVE_HGE
		case TYPE_hge:{
			hge nval = *(hge *) bound;
			nth = is_hge_nil(nval) ? lng_nil : (nval > (hge) GDK_lng_max) ? GDK_lng_max : (lng) nval;
		} break;
#endif
		default:
			goto nosupport;
		}
		switch (ATOMbasetype(tp1)) {
		case TYPE_bte:
			ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(bte);
			break;
		case TYPE_sht:
			ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(sht);
			break;
		case TYPE_int:
			ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(int);
			break;
		case TYPE_lng:
			ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(hge);
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(flt);
			break;
		case TYPE_dbl:
			ANALYTICAL_NTHVALUE_IMP_SINGLE_FIXED(dbl);
			break;
		default:{
			BATiter bpi = bat_iterator(b);
			if (is_lng_nil(nth)) {
				has_nils = true;
				for (; i < cnt; i++)
					if (BUNappend(r, nil, false) != GDK_SUCCEED)
						return GDK_FAIL;
			} else {
				nth--;
				for (; i < cnt; i++) {
					curval = (end[i] > start[i] && nth < (end[i] - start[i])) ? BUNtail(bpi, (BUN) (start[i] + nth)) : (void *) nil;
					if (BUNappend(r, curval, false) != GDK_SUCCEED)
						return GDK_FAIL;
					has_nils |= atomcmp(curval, nil) == 0;
				}
			}
		}
		}
	} else {
		switch (ATOMbasetype(tp1)) {
		case TYPE_bte:
			ANALYTICAL_NTHVALUE_CALC_FIXED(bte);
			break;
		case TYPE_sht:
			ANALYTICAL_NTHVALUE_CALC_FIXED(sht);
			break;
		case TYPE_int:
			ANALYTICAL_NTHVALUE_CALC_FIXED(int);
			break;
		case TYPE_lng:
			ANALYTICAL_NTHVALUE_CALC_FIXED(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_NTHVALUE_CALC_FIXED(hge);
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_NTHVALUE_CALC_FIXED(flt);
			break;
		case TYPE_dbl:
			ANALYTICAL_NTHVALUE_CALC_FIXED(dbl);
			break;
		default:{
			BATiter bpi = bat_iterator(b);
			switch (tp2) {
			case TYPE_bte:
				ANALYTICAL_NTHVALUE_IMP_MULTI_VARSIZED(bte, lng);
				break;
			case TYPE_sht:
				ANALYTICAL_NTHVALUE_IMP_MULTI_VARSIZED(sht, lng);
				break;
			case TYPE_int:
				ANALYTICAL_NTHVALUE_IMP_MULTI_VARSIZED(int, lng);
				break;
			case TYPE_lng:
				ANALYTICAL_NTHVALUE_IMP_MULTI_VARSIZED(lng, lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				ANALYTICAL_NTHVALUE_IMP_MULTI_VARSIZED(hge, hge);
				break;
#endif
			default:
				goto nosupport;
			}
		}
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
      nosupport:
	GDKerror("type %s not supported for the nth_value.\n", ATOMname(tp2));
	return GDK_FAIL;
}

#define ANALYTICAL_LAG_CALC(TPE)				\
	do {							\
		for (i = 0; i < lag && rb < rp; i++, rb++)	\
			*rb = def;				\
		has_nils |= (lag > 0 && is_##TPE##_nil(def));	\
		for (; rb < rp; rb++, bp++) {			\
			next = *bp;				\
			*rb = next;				\
			has_nils |= is_##TPE##_nil(next);		\
		}						\
	} while (0)

#define ANALYTICAL_LAG_IMP(TPE)						\
	do {								\
		TPE *rp, *rb, *bp, *nbp, *rend,				\
			def = *((TPE *) default_value), next;		\
		bp = (TPE*)Tloc(b, 0);					\
		rb = rp = (TPE*)Tloc(r, 0);				\
		rend = rb + cnt;					\
		if (lag == BUN_NONE) {					\
			has_nils = true;				\
			for (; rb < rend; rb++)				\
				*rb = TPE##_nil;			\
		} else if (p) {						\
			pnp = np = (bit*)Tloc(p, 0);			\
			end = np + cnt;					\
			for (; np < end; np++) {			\
				if (*np) {				\
					ncnt = (np - pnp);		\
					rp += ncnt;			\
					nbp = bp + ncnt;		\
					ANALYTICAL_LAG_CALC(TPE);	\
					bp = nbp;			\
					pnp = np;			\
				}					\
			}						\
			rp += (np - pnp);				\
			ANALYTICAL_LAG_CALC(TPE);			\
		} else {						\
			rp += cnt;					\
			ANALYTICAL_LAG_CALC(TPE);			\
		}							\
	} while (0)

#define ANALYTICAL_LAG_OTHERS						\
	do {								\
		for (i = 0; i < lag && k < j; i++, k++) {		\
			if (BUNappend(r, default_value, false) != GDK_SUCCEED) \
				return GDK_FAIL;			\
		}							\
		has_nils |= (lag > 0 && atomcmp(default_value, nil) == 0);	\
		for (l = k - lag; k < j; k++, l++) {			\
			curval = BUNtail(bpi, l);			\
			if (BUNappend(r, curval, false) != GDK_SUCCEED)	\
				return GDK_FAIL;			\
			has_nils |= atomcmp(curval, nil) == 0;			\
		}	\
	} while (0)

gdk_return
GDKanalyticallag(BAT *r, BAT *b, BAT *p, BUN lag, const void *restrict default_value, int tpe)
{
	int (*atomcmp) (const void *, const void *);
	const void *restrict nil;
	BUN i = 0, j = 0, k = 0, l = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	bool has_nils = false;

	assert(default_value);

	switch (ATOMbasetype(tpe)) {
	case TYPE_bte:
		ANALYTICAL_LAG_IMP(bte);
		break;
	case TYPE_sht:
		ANALYTICAL_LAG_IMP(sht);
		break;
	case TYPE_int:
		ANALYTICAL_LAG_IMP(int);
		break;
	case TYPE_lng:
		ANALYTICAL_LAG_IMP(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ANALYTICAL_LAG_IMP(hge);
		break;
#endif
	case TYPE_flt:
		ANALYTICAL_LAG_IMP(flt);
		break;
	case TYPE_dbl:
		ANALYTICAL_LAG_IMP(dbl);
		break;
	default:{
		BATiter bpi = bat_iterator(b);
		const void *restrict curval;
		nil = ATOMnilptr(tpe);
		atomcmp = ATOMcompare(tpe);
		if (lag == BUN_NONE) {
			has_nils = true;
			for (j = 0; j < cnt; j++) {
				if (BUNappend(r, nil, false) != GDK_SUCCEED)
					return GDK_FAIL;
			}
		} else if (p) {
			pnp = np = (bit *) Tloc(p, 0);
			end = np + cnt;
			for (; np < end; np++) {
				if (*np) {
					j += (np - pnp);
					ANALYTICAL_LAG_OTHERS;
					pnp = np;
				}
			}
			j += (np - pnp);
			ANALYTICAL_LAG_OTHERS;
		} else {
			j += cnt;
			ANALYTICAL_LAG_OTHERS;
		}
	}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#define LEAD_CALC(TPE)						\
	do {							\
		if (lead < ncnt) {				\
			bp += lead;				\
			l = ncnt - lead;			\
			for (i = 0; i < l; i++, rb++, bp++) {	\
				next = *bp;			\
				*rb = next;			\
				has_nils |= is_##TPE##_nil(next);	\
			}					\
		} else {					\
			bp += ncnt;				\
		}						\
		for (;rb < rp; rb++)				\
			*rb = def;				\
		has_nils |= (lead > 0 && is_##TPE##_nil(def));	\
	} while (0)

#define ANALYTICAL_LEAD_IMP(TPE)				\
	do {							\
		TPE *rp, *rb, *bp, *rend,			\
			def = *((TPE *) default_value), next;	\
		bp = (TPE*)Tloc(b, 0);				\
		rb = rp = (TPE*)Tloc(r, 0);			\
		rend = rb + cnt;				\
		if (lead == BUN_NONE) {				\
			has_nils = true;			\
			for (; rb < rend; rb++)			\
				*rb = TPE##_nil;		\
		} else if (p) {					\
			pnp = np = (bit*)Tloc(p, 0);		\
			end = np + cnt;				\
			for (; np < end; np++) {		\
				if (*np) {			\
					ncnt = (np - pnp);	\
					rp += ncnt;		\
					LEAD_CALC(TPE);		\
					pnp = np;		\
				}				\
			}					\
			ncnt = (np - pnp);			\
			rp += ncnt;				\
			LEAD_CALC(TPE);				\
		} else {					\
			ncnt = cnt;				\
			rp += ncnt;				\
			LEAD_CALC(TPE);				\
		}						\
	} while (0)

#define ANALYTICAL_LEAD_OTHERS						\
	do {								\
		j += ncnt;						\
		if (lead < ncnt) {					\
			m = ncnt - lead;				\
			for (i = 0,n = k + lead; i < m; i++, n++) {	\
				curval = BUNtail(bpi, n);		\
				if (BUNappend(r, curval, false) != GDK_SUCCEED)	\
					return GDK_FAIL;		\
				has_nils |= atomcmp(curval, nil) == 0;		\
			}						\
			k += i;						\
		}							\
		for (; k < j; k++) {					\
			if (BUNappend(r, default_value, false) != GDK_SUCCEED) \
				return GDK_FAIL;			\
		}							\
		has_nils |= (lead > 0 && atomcmp(default_value, nil) == 0);	\
	} while (0)

gdk_return
GDKanalyticallead(BAT *r, BAT *b, BAT *p, BUN lead, const void *restrict default_value, int tpe)
{
	int (*atomcmp) (const void *, const void *);
	const void *restrict nil;
	BUN i = 0, j = 0, k = 0, l = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	bool has_nils = false;

	assert(default_value);

	switch (ATOMbasetype(tpe)) {
	case TYPE_bte:
		ANALYTICAL_LEAD_IMP(bte);
		break;
	case TYPE_sht:
		ANALYTICAL_LEAD_IMP(sht);
		break;
	case TYPE_int:
		ANALYTICAL_LEAD_IMP(int);
		break;
	case TYPE_lng:
		ANALYTICAL_LEAD_IMP(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ANALYTICAL_LEAD_IMP(hge);
		break;
#endif
	case TYPE_flt:
		ANALYTICAL_LEAD_IMP(flt);
		break;
	case TYPE_dbl:
		ANALYTICAL_LEAD_IMP(dbl);
		break;
	default:{
		BUN m = 0, n = 0;
		BATiter bpi = bat_iterator(b);
		const void *restrict curval;
		nil = ATOMnilptr(tpe);
		atomcmp = ATOMcompare(tpe);
		if (lead == BUN_NONE) {
			has_nils = true;
			for (j = 0; j < cnt; j++) {
				if (BUNappend(r, nil, false) != GDK_SUCCEED)
					return GDK_FAIL;
			}
		} else if (p) {
			pnp = np = (bit *) Tloc(p, 0);
			end = np + cnt;
			for (; np < end; np++) {
				if (*np) {
					ncnt = (np - pnp);
					ANALYTICAL_LEAD_OTHERS;
					pnp = np;
				}
			}
			ncnt = (np - pnp);
			ANALYTICAL_LEAD_OTHERS;
		} else {
			ncnt = cnt;
			ANALYTICAL_LEAD_OTHERS;
		}
	}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#define ANALYTICAL_MIN_MAX_CALC_FIXED_UNBOUNDED_TILL_CURRENT_ROW(TPE, MIN_MAX)	\
	do { \
		for (; k < i;) { \
			j = k++; \
			TPE curval = bp[j]; \
			while (k < i && !op[k]) { \
				if (!is_##TPE##_nil(bp[k])) {		\
					if (is_##TPE##_nil(curval))	\
						curval = bp[k];	\
					else				\
						curval = MIN_MAX(bp[k], curval); \
				}					\
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils |= is_##TPE##_nil(curval); \
		} \
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_CURRENT_ROW_TILL_UNBOUNDED(TPE, MIN_MAX)	\
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			TPE curval = bp[l]; \
			while (j >= k && !op[j]) { \
				if (!is_##TPE##_nil(bp[j])) {		\
					if (is_##TPE##_nil(curval))	\
						curval = bp[j];	\
					else				\
						curval = MIN_MAX(bp[j], curval); \
				}					\
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
			has_nils |= is_##TPE##_nil(curval); \
		}	\
		k = i; \
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_ALL_ROWS(TPE, MIN_MAX)	\
	do { \
		TPE curval = TPE##_nil; \
		for (j = k; j < i; j++) { \
			TPE v = bp[j];				\
			if (!is_##TPE##_nil(v)) {		\
				if (is_##TPE##_nil(curval))	\
					curval = v;	\
				else				\
					curval = MIN_MAX(v, curval); \
			}					\
		} \
		for (; k < i; k++) \
			rb[k] = curval; \
		has_nils |= is_##TPE##_nil(curval); \
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_CURRENT_ROW(TPE, MIN_MAX)	\
	do { \
		for (; k < i; k++) { \
			TPE v = bp[k]; \
			rb[k] = v; \
			has_nils |= is_##TPE##_nil(v); \
		} \
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_OTHERS(TPE, MIN_MAX)	\
	do { \
		TPE curval = TPE##_nil; \
		for (; k < i; k++) { \
			TPE *bs = bp + start[k];				\
			TPE *be = bp + end[k];				\
			for (; bs < be; bs++) {				\
				TPE v = *bs;				\
				if (!is_##TPE##_nil(v)) {		\
					if (is_##TPE##_nil(curval))	\
						curval = v;	\
					else				\
						curval = MIN_MAX(v, curval); \
				}					\
			}						\
			rb[k] = curval;					\
			if (is_##TPE##_nil(curval))			\
				has_nils = true;			\
			else						\
				curval = TPE##_nil;	/* For the next iteration */	\
		}		\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_VARSIZED_UNBOUNDED_TILL_CURRENT_ROW(GT_LT)	\
	do { \
		for (; k < i;) { \
			j = k++; \
			void *curval = BUNtail(bpi, j);	\
			while (k < i && !op[k]) { \
				void *next = BUNtail(bpi, k); \
				if (atomcmp(next, nil) != 0) {		\
					if (atomcmp(curval, nil) == 0)	\
						curval = next;		\
					else				\
						curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
				}					\
				k++; \
			} \
			for (; j < k; j++) \
				if (tfastins_nocheckVAR(r, j, curval, Tsize(r)) != GDK_SUCCEED) \
					return GDK_FAIL; \
			has_nils |= atomcmp(curval, nil) == 0;		\
		} \
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_VARSIZED_CURRENT_ROW_TILL_UNBOUNDED(GT_LT)	\
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			void *curval = BUNtail(bpi, l);	\
			while (j >= k && !op[j]) { \
				void *next = BUNtail(bpi, j); \
				if (atomcmp(next, nil) != 0) {		\
					if (atomcmp(curval, nil) == 0)	\
						curval = next;		\
					else				\
						curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
				}					\
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				if (tfastins_nocheckVAR(r, l, curval, Tsize(r)) != GDK_SUCCEED) \
					return GDK_FAIL; \
			has_nils |= atomcmp(curval, nil) == 0;		\
		}	\
		k = i; \
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_VARSIZED_ALL_ROWS(GT_LT)	\
	do { \
		void *curval = (void*) nil; \
		for (j = k; j < i; j++) { \
			void *next = BUNtail(bpi, j);	\
			if (atomcmp(next, nil) != 0) {		\
				if (atomcmp(curval, nil) == 0)	\
					curval = next;		\
				else				\
					curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
			}					\
		} \
		for (; k < i; k++) \
			if (tfastins_nocheckVAR(r, k, curval, Tsize(r)) != GDK_SUCCEED) \
				return GDK_FAIL; \
		has_nils |= atomcmp(curval, nil) == 0;		\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_VARSIZED_CURRENT_ROW(GT_LT)	\
	do { \
		for (; k < i; k++) { \
			void *next = BUNtail(bpi, k); \
			if (tfastins_nocheckVAR(r, k, next, Tsize(r)) != GDK_SUCCEED) \
				return GDK_FAIL; \
			has_nils |= atomcmp(next, nil) == 0;		\
		} \
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_VARSIZED_OTHERS(GT_LT)	\
	do { \
		void *curval = (void*) nil; \
		for (; k < i; k++) { \
			j = start[k];					\
			l = end[k];					\
			curval = (void*) nil;				\
			for (;j < l; j++) {				\
				void *next = BUNtail(bpi, j);	\
				if (atomcmp(next, nil) != 0) {		\
					if (atomcmp(curval, nil) == 0)	\
						curval = next;		\
					else				\
						curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
				}					\
			}						\
			if (tfastins_nocheckVAR(r, k, curval, Tsize(r)) != GDK_SUCCEED) \
				return GDK_FAIL; \
			has_nils |= atomcmp(curval, nil) == 0;		\
		}							\
	} while (0)

#define ANALYTICAL_MIN_MAX_PARTITIONS(TPE, MIN_MAX, IMP)		\
	do {					\
		TPE *bp = (TPE*)Tloc(b, 0), *restrict rb = (TPE*)Tloc(r, 0); \
		if (p) {					\
			for (; i < cnt; i++) {		\
				if (np[i]) 			\
					ANALYTICAL_MIN_MAX_CALC_FIXED_##IMP(TPE, MIN_MAX); \
			}						\
		}		\
		i = cnt;					\
		ANALYTICAL_MIN_MAX_CALC_FIXED_##IMP(TPE, MIN_MAX);	\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_MIN_MAX_LIMIT(MIN_MAX, IMP)			\
	case TYPE_hge:					\
		ANALYTICAL_MIN_MAX_PARTITIONS(hge, MIN_MAX, IMP);	\
	break;
#else
#define ANALYTICAL_MIN_MAX_LIMIT(MIN_MAX, IMP)
#endif

#define ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, IMP)		\
	do { \
		switch (ATOMbasetype(tpe)) {				\
		case TYPE_bte:							\
			ANALYTICAL_MIN_MAX_PARTITIONS(bte, MIN_MAX, IMP);			\
			break;							\
		case TYPE_sht:							\
			ANALYTICAL_MIN_MAX_PARTITIONS(sht, MIN_MAX, IMP);			\
			break;							\
		case TYPE_int:							\
			ANALYTICAL_MIN_MAX_PARTITIONS(int, MIN_MAX, IMP);			\
			break;							\
		case TYPE_lng:							\
			ANALYTICAL_MIN_MAX_PARTITIONS(lng, MIN_MAX, IMP);			\
			break;							\
			ANALYTICAL_MIN_MAX_LIMIT(MIN_MAX, IMP)				\
		case TYPE_flt:							\
			ANALYTICAL_MIN_MAX_PARTITIONS(flt, MIN_MAX, IMP);			\
			break;							\
		case TYPE_dbl:							\
			ANALYTICAL_MIN_MAX_PARTITIONS(dbl, MIN_MAX, IMP);			\
			break;							\
		default: {							\
			if (p) {						\
				for (; i < cnt; i++) {			\
					if (np[i]) 			\
						ANALYTICAL_MIN_MAX_CALC_VARSIZED_##IMP(GT_LT); \
				}						\
			} 					\
			i = cnt;					\
			ANALYTICAL_MIN_MAX_CALC_VARSIZED_##IMP(GT_LT);	\
		}								\
		}								\
	} while (0)

#define ANALYTICAL_MIN_MAX(OP, MIN_MAX, GT_LT)				\
gdk_return								\
GDKanalytical##OP(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type)		\
{									\
	bool has_nils = false;						\
	lng i = 0, j = 0, k = 0, l = 0, m = 0, cnt = (lng) BATcount(b);					\
	lng *restrict start = s ? (lng*)Tloc(s, 0) : NULL, *restrict end = e ? (lng*)Tloc(e, 0) : NULL;		\
	bit *restrict np = p ? Tloc(p, 0) : NULL, *restrict op = o ? Tloc(o, 0) : NULL; 	\
	BATiter bpi = bat_iterator(b);				\
	const void *nil = ATOMnilptr(tpe);			\
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe); \
									\
	switch (frame_type) {		\
	case 3: /* unbounded until current row */	{	\
		ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, UNBOUNDED_TILL_CURRENT_ROW); \
	} break;		\
	case 4: /* current row until unbounded */	{	\
		ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, CURRENT_ROW_TILL_UNBOUNDED); \
	} break;		\
	case 5: /* all rows */	{	\
		ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, ALL_ROWS); \
	} break;		\
	case 6: /* current row */ {	\
		ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, CURRENT_ROW); \
	} break;		\
	default: {		\
		ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, OTHERS); \
	}		\
	}	\
	BATsetcount(r, (BUN) cnt);						\
	r->tnonil = !has_nils;						\
	r->tnil = has_nils;						\
	return GDK_SUCCEED;						\
}

ANALYTICAL_MIN_MAX(min, MIN, >)
ANALYTICAL_MIN_MAX(max, MAX, <)

/* Counting no nils for fixed sizes */
#define ANALYTICAL_COUNT_FIXED_UNBOUNDED_TILL_CURRENT_ROW(TPE) \
	do { \
		curval = 0; \
		if (count_all) { \
			for (; k < i;) { \
				j = k++; \
				curval++; \
				while (k < i && !op[k]) { \
					curval++; \
					k++; \
				} \
				for (; j < k; j++) \
					rb[j] = curval; \
			} \
		} else { \
			for (; k < i;) { \
				j = k++; \
				curval += !is_##TPE##_nil(bp[j]); \
				while (k < i && !op[k]) { \
					curval += !is_##TPE##_nil(bp[k]); \
					k++; \
				} \
				for (; j < k; j++) \
					rb[j] = curval; \
			} \
		}	\
	} while (0)

#define ANALYTICAL_COUNT_FIXED_CURRENT_ROW_TILL_UNBOUNDED(TPE) \
	do { \
		curval = 0; \
		if (count_all) { \
			for (j = i - 1; j >= k; ) { \
				l = j--; \
				curval++; \
				while (j >= k && !op[j]) { \
					curval++; \
					j--; \
				} \
				m = MAX(k, j); \
				for (; l >= m; l--) \
					rb[l] = curval; \
			}	\
		} else { \
			for (j = i - 1; j >= k; ) { \
				l = j--; \
				curval += !is_##TPE##_nil(bp[l]); \
				while (j >= k && !op[j]) { \
					curval += !is_##TPE##_nil(bp[j]); \
					j--; \
				} \
				m = MAX(k, j); \
				for (; l >= m; l--) \
					rb[l] = curval; \
			}	\
		} \
		k = i; \
	} while (0)

#define ANALYTICAL_COUNT_FIXED_ALL_ROWS(TPE) \
	do { \
		if (count_all) { \
			curval = i - k; \
			for (; k < i; k++) \
				rb[k] = curval; \
		} else {	\
			curval = 0; \
			for (; j < i; j++) \
				curval += !is_##TPE##_nil(bp[j]); \
			for (; k < i; k++) \
				rb[k] = curval; \
		}	\
	} while (0)

#define ANALYTICAL_COUNT_FIXED_CURRENT_ROW(TPE) \
	do { \
		if (count_all) { \
			for (; k < i; k++) \
				rb[k] = 1; \
		} else { \
			for (; k < i; k++) \
				rb[k] = !is_##TPE##_nil(bp[k]); \
		} \
	} while (0)

#define ANALYTICAL_COUNT_FIXED_OTHERS(TPE) \
	do { \
		if (count_all) { \
			for (; k < i; k++) \
				rb[k] = (end[k] > start[k]) ? (end[k] - start[k]) : 0; \
		} else {	\
			curval = 0; \
			for (; k < i; k++) {			\
				TPE *bs = bp + start[k];		\
				TPE *be = bp + end[k];		\
				for (; bs < be; bs++)			\
					curval += !is_##TPE##_nil(*bs);	\
				rb[k] = curval;		\
				curval = 0;		\
			}						\
		}	\
	} while (0)

/* Counting no nils for other types */
#define ANALYTICAL_COUNT_OTHERS_UNBOUNDED_TILL_CURRENT_ROW \
	do { \
		curval = 0; \
		if (count_all) { \
			for (; k < i; ) { \
				j = k++; \
				curval++; \
				while (k < i && !op[k]) { \
					curval++; \
					k++; \
				} \
				for (; j < k; j++) \
					rb[j] = curval; \
			} \
		} else { \
			for (; k < i; ) { \
				j = k++; \
				const void *v = BUNtail(bpi, j); \
				curval += cmp(v, nil) != 0; \
				while (k < i && !op[k]) { \
					curval += cmp(BUNtail(bpi, k), nil) != 0; \
					k++; \
				} \
				for (; j < k; j++) \
					rb[j] = curval; \
			} \
		}	\
	} while (0)

#define ANALYTICAL_COUNT_OTHERS_CURRENT_ROW_TILL_UNBOUNDED \
	do { \
		curval = 0; \
		if (count_all) { \
			for (j = i - 1; j >= k; ) { \
				l = j--; \
				curval++; \
				while (j >= k && !op[j]) { \
					curval++; \
					j--; \
				} \
				m = MAX(k, j); \
				for (; l >= m; l--) \
					rb[l] = curval; \
			}	\
		} else { \
			for (j = i - 1; j >= k; ) { \
				l = j--; \
				const void *v = Tloc(b, l); \
				curval += cmp(v, nil) != 0; \
				while (j >= k && !op[j]) { \
					curval += cmp(BUNtail(bpi, j), nil) != 0; \
					j--; \
				} \
				m = MAX(k, j); \
				for (; l >= m; l--) \
					rb[l] = curval; \
			}	\
	} \
		k = i; \
	} while (0)

#define ANALYTICAL_COUNT_OTHERS_ALL_ROWS	\
	do { \
		curval = 0; \
		if (count_all) { \
			curval = i - k; \
		} else {	\
			for (; j < i; j++) \
				curval += cmp(BUNtail(bpi, j), nil) != 0; \
		}	\
		for (; k < i; k++) \
			rb[k] = curval; \
	} while (0)

#define ANALYTICAL_COUNT_OTHERS_CURRENT_ROW	\
	do { \
		if (count_all) { \
			for (; k < i; k++) \
				rb[k] = 1; \
		} else { \
			for (; k < i; k++) \
				rb[k] = cmp(BUNtail(bpi, k), nil) != 0; \
		} \
	} while (0)

#define ANALYTICAL_COUNT_OTHERS_OTHERS	\
	do { \
		if (count_all) { \
			for (; k < i; k++) \
				rb[k] = (end[k] > start[k]) ? (end[k] - start[k]) : 0; \
		} else {	\
			curval = 0; \
			for (; k < i; k++) { \
				j = start[k]; \
				l = end[k]; \
				for (; j < l; j++) \
					curval += cmp(BUNtail(bpi, j), nil) != 0; \
				rb[k] = curval; \
				curval = 0; \
			} \
		}	\
	} while (0)

/* Now do the count analytic function branches */
#define ANALYTICAL_COUNT_FIXED_PARTITIONS(TPE, IMP)		\
	do {					\
		TPE *bp = (TPE*) bheap; \
		if (p) {					\
			for (; i < cnt; i++) {		\
				if (np[i]) 			\
					ANALYTICAL_COUNT_FIXED_##IMP(TPE); \
			}						\
		}	\
		i = cnt;			\
		ANALYTICAL_COUNT_FIXED_##IMP(TPE);	\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_COUNT_LIMIT(IMP)			\
	case TYPE_hge:					\
		ANALYTICAL_COUNT_FIXED_PARTITIONS(hge, IMP);	\
	break;
#else
#define ANALYTICAL_COUNT_LIMIT(IMP)
#endif

#define ANALYTICAL_COUNT_BRANCHES(IMP)		\
	do { \
		switch (ATOMbasetype(tpe)) {		\
		case TYPE_bte:					\
			ANALYTICAL_COUNT_FIXED_PARTITIONS(bte, IMP);		\
			break;							\
		case TYPE_sht:							\
			ANALYTICAL_COUNT_FIXED_PARTITIONS(sht, IMP);		\
			break;							\
		case TYPE_int:							\
			ANALYTICAL_COUNT_FIXED_PARTITIONS(int, IMP);		\
			break;							\
		case TYPE_lng:							\
			ANALYTICAL_COUNT_FIXED_PARTITIONS(lng, IMP);		\
			break;							\
			ANALYTICAL_COUNT_LIMIT(IMP)			\
		case TYPE_flt:							\
			ANALYTICAL_COUNT_FIXED_PARTITIONS(flt, IMP);		\
			break;							\
		case TYPE_dbl:							\
			ANALYTICAL_COUNT_FIXED_PARTITIONS(dbl, IMP);		\
			break;							\
		default: {							\
			if (p) {						\
				for (; i < cnt; i++) {			\
					if (np[i]) 			\
						ANALYTICAL_COUNT_OTHERS_##IMP; \
				}						\
			}	\
			i = cnt;				\
			ANALYTICAL_COUNT_OTHERS_##IMP;	\
		}								\
		}								\
	} while (0)

gdk_return
GDKanalyticalcount(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, bit ignore_nils, int tpe, int frame_type)
{
	lng i = 0, j = 0, k = 0, l = 0, m = 0, curval = 0, cnt = (lng) BATcount(b);
	lng *restrict start = s ? (lng*)Tloc(s, 0) : NULL, *restrict end = e ? (lng*)Tloc(e, 0) : NULL, *restrict rb = (lng *) Tloc(r, 0);
	bit *restrict np = p ? Tloc(p, 0) : NULL, *restrict op = o ? Tloc(o, 0) : NULL;
	const void *restrict nil = ATOMnilptr(tpe);
	int (*cmp) (const void *, const void *) = ATOMcompare(tpe);
	const void *restrict bheap = Tloc(b, 0);
	bool count_all = !ignore_nils || b->tnonil;
	BATiter bpi = bat_iterator(b);

	switch (frame_type) {
	case 3: /* unbounded until current row */	{
		ANALYTICAL_COUNT_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW);
	} break;
	case 4: /* current row until unbounded */	{
		ANALYTICAL_COUNT_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED);
	} break;
	case 5: /* all rows */	{
		ANALYTICAL_COUNT_BRANCHES(ALL_ROWS);
	} break;
	case 6: /* current row */ {
		ANALYTICAL_COUNT_BRANCHES(CURRENT_ROW);
	} break;
	default: {
		ANALYTICAL_COUNT_BRANCHES(OTHERS);
	}
	}
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return GDK_SUCCEED;
}

/* sum on fixed size integers */
#define ANALYTICAL_SUM_IMP_NUM_UNBOUNDED_TILL_CURRENT_ROW(TPE1, TPE2) \
	do { \
		for (; k < i;) { \
			j = k++; \
			TPE2 curval = is_##TPE1##_nil(bp[j]) ? TPE2##_nil : (TPE2) bp[j];	\
			while (k < i && !op[k]) { \
				if (!is_##TPE1##_nil(bp[k])) {		\
					if (is_##TPE2##_nil(curval))	\
						curval = (TPE2) bp[k];	\
					else				\
						ADD_WITH_CHECK(bp[k], curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}					\
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		} \
	} while (0)

#define ANALYTICAL_SUM_IMP_NUM_CURRENT_ROW_TILL_UNBOUNDED(TPE1, TPE2) \
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			TPE2 curval = is_##TPE1##_nil(bp[l]) ? TPE2##_nil : (TPE2) bp[l];	\
			while (j >= k && !op[j]) { \
				if (!is_##TPE1##_nil(bp[j])) {		\
					if (is_##TPE2##_nil(curval))	\
						curval = (TPE2) bp[j];	\
					else				\
						ADD_WITH_CHECK(bp[j], curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}					\
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		}	\
		k = i; \
	} while (0)

#define ANALYTICAL_SUM_IMP_NUM_ALL_ROWS(TPE1, TPE2)	\
	do { \
		TPE2 curval = TPE2##_nil; \
		for (; j < i; j++) { \
			TPE1 v = bp[j]; \
			if (!is_##TPE1##_nil(v)) {		\
				if (is_##TPE2##_nil(curval))	\
					curval = (TPE2) v;	\
				else				\
					ADD_WITH_CHECK(v, curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
			}					\
		} \
		for (; k < i; k++) \
			rb[k] = curval; \
		has_nils |= is_##TPE2##_nil(curval);	\
	} while (0)

#define ANALYTICAL_SUM_IMP_NUM_CURRENT_ROW(TPE1, TPE2)	\
	do { \
		for (; k < i; k++) { \
			TPE1 v = bp[k]; \
			if (is_##TPE1##_nil(v)) {	\
				rb[k] = TPE2##_nil; \
				has_nils = true; \
			} else	{		\
				rb[k] = (TPE2) v; \
			} \
		} \
	} while (0)

#define ANALYTICAL_SUM_IMP_NUM_OTHERS(TPE1, TPE2)	\
	do { \
		TPE2 curval = TPE2##_nil; \
		for (; k < i; k++) {		\
			TPE1 *bs = bp + start[k];		\
			TPE1 *be = bp + end[k];		\
			for (; bs < be; bs++) {			\
				TPE1 v = *bs;				\
				if (!is_##TPE1##_nil(v)) {		\
					if (is_##TPE2##_nil(curval))	\
						curval = (TPE2) v;	\
					else				\
						ADD_WITH_CHECK(v, curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}					\
			}						\
			rb[k] = curval;					\
			if (is_##TPE2##_nil(curval))			\
				has_nils = true;			\
			else						\
				curval = TPE2##_nil;	/* For the next iteration */ \
		}							\
	} while (0)

/* sum on floating-points */
#define ANALYTICAL_SUM_IMP_FP_UNBOUNDED_TILL_CURRENT_ROW(TPE1, TPE2) /* TODO go through a version of dofsum which returns the current partials */ \
	do { \
		for (; k < i;) { \
			j = k++; \
			TPE2 curval = is_##TPE1##_nil(bp[j]) ? TPE2##_nil : (TPE2) bp[j];	\
			while (k < i && !op[k]) { \
				if (!is_##TPE1##_nil(bp[k])) {		\
					if (is_##TPE2##_nil(curval))	\
						curval = (TPE2) bp[k];	\
					else				\
						ADD_WITH_CHECK(bp[k], curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}					\
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		} \
	} while (0)

#define ANALYTICAL_SUM_IMP_FP_CURRENT_ROW_TILL_UNBOUNDED(TPE1, TPE2) /* TODO go through a version of dofsum which returns the current partials */ \
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			TPE2 curval = is_##TPE1##_nil(bp[l]) ? TPE2##_nil : (TPE2) bp[l];	\
			while (j >= k && !op[j]) { \
				if (!is_##TPE1##_nil(bp[j])) {		\
					if (is_##TPE2##_nil(curval))	\
						curval = (TPE2) bp[j];	\
					else				\
						ADD_WITH_CHECK(bp[j], curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}					\
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		}	\
		k = i; \
	} while (0)

#define ANALYTICAL_SUM_IMP_FP_ALL_ROWS(TPE1, TPE2)	\
	do { \
		TPE1 *bs = &(bp[k]);	\
		BUN parcel = (BUN)(i - k);	\
		TPE2 curval = TPE2##_nil; \
		if (dofsum(bs, 0,			\
				&(struct canditer){.tpe = cand_dense, .ncand = parcel,}, \
				parcel, &curval, 1, TYPE_##TPE1, \
				TYPE_##TPE2, NULL, 0, 0, true, \
				false, true) == BUN_NONE) {	\
			goto bailout;			\
		}	\
		for (; k < i; k++) \
			rb[k] = curval; \
		has_nils |= is_##TPE2##_nil(curval);	\
	} while (0)

#define ANALYTICAL_SUM_IMP_FP_CURRENT_ROW(TPE1, TPE2)	\
	do { \
		for (; k < i; k++) { \
			TPE1 v = bp[k]; \
			if (is_##TPE1##_nil(v)) {	\
				rb[k] = TPE2##_nil; \
				has_nils = true; \
			} else	{		\
				rb[k] = (TPE2) v; \
			} \
		} \
	} while (0)

#define ANALYTICAL_SUM_IMP_FP_OTHERS(TPE1, TPE2)				\
	do {								\
		TPE2 curval = TPE2##_nil; \
		for (; k < i; k++) {		\
			if (end[k] > start[k]) {			\
				TPE1 *bs = bp + start[k];			\
				BUN parcel = (BUN)(end[k] - start[k]);	\
				if (dofsum(bs, 0,			\
					   &(struct canditer){.tpe = cand_dense, .ncand = parcel,}, \
					   parcel, &curval, 1, TYPE_##TPE1, \
					   TYPE_##TPE2, NULL, 0, 0, true, \
					   false, true) == BUN_NONE) {	\
					goto bailout;			\
				}					\
			}						\
			rb[k] = curval;					\
			if (is_##TPE2##_nil(curval))			\
				has_nils = true;			\
			else						\
				curval = TPE2##_nil;	/* For the next iteration */ \
		}							\
	} while (0)

#define ANALYTICAL_SUM_CALC(TPE1, TPE2, IMP)		\
	do {						\
		TPE1 *bp = (TPE1*)Tloc(b, 0);	 \
		TPE2 *restrict rb = (TPE2*)Tloc(r, 0); \
		if (p) {					\
			for (; i < cnt; i++) {		\
				if (np[i]) 			\
					IMP(TPE1, TPE2);	\
			}						\
		}	\
		i = cnt;					\
		IMP(TPE1, TPE2);	\
	} while (0)

#if HAVE_HGE
#define ANALYTICAL_SUM_LIMIT(IMP)	\
	case TYPE_hge:{		\
		switch (tp1) {		\
		case TYPE_bte:		\
			ANALYTICAL_SUM_CALC(bte, hge, ANALYTICAL_SUM_IMP_NUM_##IMP);		\
			break;			\
		case TYPE_sht:		\
			ANALYTICAL_SUM_CALC(sht, hge, ANALYTICAL_SUM_IMP_NUM_##IMP);		\
			break;			\
		case TYPE_int:		\
			ANALYTICAL_SUM_CALC(int, hge, ANALYTICAL_SUM_IMP_NUM_##IMP);		\
			break;			\
		case TYPE_lng:		\
			ANALYTICAL_SUM_CALC(lng, hge, ANALYTICAL_SUM_IMP_NUM_##IMP);		\
			break;			\
		case TYPE_hge:		\
			ANALYTICAL_SUM_CALC(hge, hge, ANALYTICAL_SUM_IMP_NUM_##IMP);		\
			break;		\
		default:		\
			goto nosupport;		\
		}			\
		break;		\
	}
#else
#define ANALYTICAL_SUM_LIMIT(IMP)
#endif

#define ANALYTICAL_SUM_BRANCHES(IMP)		\
	do { \
		switch (tp2) {		\
		case TYPE_bte:{		\
			switch (tp1) {		\
			case TYPE_bte:		\
				ANALYTICAL_SUM_CALC(bte, bte, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			default:		\
				goto nosupport;		\
			}		\
			break;		\
		}		\
		case TYPE_sht:{		\
			switch (tp1) {		\
			case TYPE_bte:		\
				ANALYTICAL_SUM_CALC(bte, sht, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			case TYPE_sht:		\
				ANALYTICAL_SUM_CALC(sht, sht, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			default:		\
				goto nosupport;		\
			}		\
			break;		\
		}		\
		case TYPE_int:{		\
			switch (tp1) {		\
			case TYPE_bte:		\
				ANALYTICAL_SUM_CALC(bte, int, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			case TYPE_sht:		\
				ANALYTICAL_SUM_CALC(sht, int, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			case TYPE_int:		\
				ANALYTICAL_SUM_CALC(int, int, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			default:		\
				goto nosupport;		\
			}		\
			break;		\
		}		\
		case TYPE_lng:{		\
			switch (tp1) {		\
			case TYPE_bte:		\
				ANALYTICAL_SUM_CALC(bte, lng, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			case TYPE_sht:		\
				ANALYTICAL_SUM_CALC(sht, lng, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			case TYPE_int:		\
				ANALYTICAL_SUM_CALC(int, lng, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			case TYPE_lng:		\
				ANALYTICAL_SUM_CALC(lng, lng, ANALYTICAL_SUM_IMP_NUM_##IMP);	\
				break;		\
			default:		\
				goto nosupport;		\
			}		\
			break;		\
		}		\
		ANALYTICAL_SUM_LIMIT(IMP)		\
		case TYPE_flt:{		\
			switch (tp1) {		\
			case TYPE_flt:		\
				ANALYTICAL_SUM_CALC(flt, flt, ANALYTICAL_SUM_IMP_FP_##IMP);		\
				break;		\
			default:		\
				goto nosupport;		\
			}		\
			break;		\
		}		\
		case TYPE_dbl:{		\
			switch (tp1) {		\
			case TYPE_flt:		\
				ANALYTICAL_SUM_CALC(flt, dbl, ANALYTICAL_SUM_IMP_FP_##IMP);		\
				break;		\
			case TYPE_dbl:		\
				ANALYTICAL_SUM_CALC(dbl, dbl, ANALYTICAL_SUM_IMP_FP_##IMP);		\
				break;		\
			default:		\
				goto nosupport;		\
			}		\
			break;		\
		}		\
		default:		\
			goto nosupport;		\
		}		\
	} while (0)

gdk_return
GDKanalyticalsum(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tp1, int tp2, int frame_type)
{
	bool has_nils = false;
	lng i = 0, j = 0, k = 0, l = 0, m = 0, cnt = (lng) BATcount(b);
	lng *restrict start = s ? (lng*)Tloc(s, 0) : NULL, *restrict end = e ? (lng*)Tloc(e, 0) : NULL;
	bit *restrict np = p ? Tloc(p, 0) : NULL, *restrict op = o ? Tloc(o, 0) : NULL;
	int abort_on_error = 1;
	BUN nils = 0;

	switch (frame_type) {
	case 3: /* unbounded until current row */	{
		ANALYTICAL_SUM_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW);
	} break;
	case 4: /* current row until unbounded */	{
		ANALYTICAL_SUM_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED);
	} break;
	case 5: /* all rows */	{
		ANALYTICAL_SUM_BRANCHES(ALL_ROWS);
	} break;
	case 6: /* current row */ {
		ANALYTICAL_SUM_BRANCHES(CURRENT_ROW);
	} break;
	default: {
		ANALYTICAL_SUM_BRANCHES(OTHERS);
	}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
      bailout:
	GDKerror("error while calculating floating-point sum\n");
	return GDK_FAIL;
      nosupport:
	GDKerror("type combination (sum(%s)->%s) not supported.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
      calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}

/* product on integers */
#define PROD_NUM(TPE1, TPE2, TPE3, ARG) \
	do {	\
		if (!is_##TPE1##_nil(ARG)) {		\
			if (is_##TPE2##_nil(curval))	\
				curval = (TPE2) ARG;	\
			else				\
				MUL4_WITH_CHECK(ARG, curval, TPE2, curval, GDK_##TPE2##_max, TPE3, goto calc_overflow); \
		}				\
	} while(0)

#define ANALYTICAL_PROD_CALC_NUM_UNBOUNDED_TILL_CURRENT_ROW(TPE1, TPE2, TPE3) \
	do { \
		for (; k < i;) { \
			j = k++; \
			TPE2 curval = is_##TPE1##_nil(bp[j]) ? TPE2##_nil : (TPE2) bp[j];	\
			while (k < i && !op[k]) { \
				PROD_NUM(TPE1, TPE2, TPE3, bp[k]); \
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		} \
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_CURRENT_ROW_TILL_UNBOUNDED(TPE1, TPE2, TPE3) \
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			TPE2 curval = is_##TPE1##_nil(bp[l]) ? TPE2##_nil : (TPE2) bp[l];	\
			while (j >= k && !op[j]) { \
				PROD_NUM(TPE1, TPE2, TPE3, bp[j]); \
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		}	\
		k = i; \
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_ALL_ROWS(TPE1, TPE2, TPE3)	\
	do { \
		TPE2 curval = TPE2##_nil; \
		for (; j < i; j++) { \
			TPE1 v = bp[j]; \
			PROD_NUM(TPE1, TPE2, TPE3, v); \
		} \
		for (; k < i; k++) \
			rb[k] = curval; \
		has_nils |= is_##TPE2##_nil(curval);	\
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_CURRENT_ROW(TPE1, TPE2, TPE3)	\
	do {								\
		for (; k < i; k++) { \
			TPE1 v = bp[k]; \
			if (is_##TPE1##_nil(v)) {	\
				rb[k] = TPE2##_nil; \
				has_nils = true; \
			} else	{		\
				rb[k] = (TPE2) v; \
			} \
		} \
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_OTHERS(TPE1, TPE2, TPE3)	\
	do {								\
		TPE2 curval = TPE2##_nil;			\
		for (; k < i; k++) {				\
			TPE1 *bs = bp + start[k], *be = bp + end[k];				\
			for (; bs < be; bs++) {				\
				TPE1 v = *bs;				\
				PROD_NUM(TPE1, TPE2, TPE3, v); \
			}						\
			rb[k] = curval;					\
			if (is_##TPE2##_nil(curval))			\
				has_nils = true;			\
			else						\
				curval = TPE2##_nil;	/* For the next iteration */	\
		}							\
	} while (0)

/* product on integers while checking for overflows on the output  */
#define PROD_NUM_LIMIT(TPE1, TPE2, REAL_IMP, ARG) \
	do {	\
		if (!is_##TPE1##_nil(ARG)) {		\
			if (is_##TPE2##_nil(curval))	\
				curval = (TPE2) ARG;	\
			else				\
				REAL_IMP(ARG, curval, curval, GDK_##TPE2##_max, goto calc_overflow); \
		}					\
	} while(0)

#define ANALYTICAL_PROD_CALC_NUM_LIMIT_UNBOUNDED_TILL_CURRENT_ROW(TPE1, TPE2, REAL_IMP) \
	do { \
		for (; k < i;) { \
			j = k++; \
			TPE2 curval = is_##TPE1##_nil(bp[j]) ? TPE2##_nil : (TPE2) bp[j];	\
			while (k < i && !op[k]) { \
				PROD_NUM_LIMIT(TPE1, TPE2, REAL_IMP, bp[k]); \
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		} \
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_LIMIT_CURRENT_ROW_TILL_UNBOUNDED(TPE1, TPE2, REAL_IMP) \
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			TPE2 curval = is_##TPE1##_nil(bp[l]) ? TPE2##_nil : (TPE2) bp[l];	\
			while (j >= k && !op[j]) { \
				PROD_NUM_LIMIT(TPE1, TPE2, REAL_IMP, bp[j]); \
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		}	\
		k = i; \
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_LIMIT_ALL_ROWS(TPE1, TPE2, REAL_IMP)	\
	do { \
		TPE2 curval = TPE2##_nil; \
		for (; j < i; j++) { \
			TPE1 v = bp[j]; \
			PROD_NUM_LIMIT(TPE1, TPE2, REAL_IMP, v); \
		} \
		for (; k < i; k++) \
			rb[k] = curval; \
		has_nils |= is_##TPE2##_nil(curval);	\
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_LIMIT_CURRENT_ROW(TPE1, TPE2, REAL_IMP)	\
	do {								\
		for (; k < i; k++) { \
			TPE1 v = bp[k]; \
			if (is_##TPE1##_nil(v)) {	\
				rb[k] = TPE2##_nil; \
				has_nils = true; \
			} else	{		\
				rb[k] = (TPE2) v; \
			} \
		} \
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_LIMIT_OTHERS(TPE1, TPE2, REAL_IMP)	\
	do {								\
		TPE2 curval = TPE2##_nil;			\
		for (; k < i; k++) {				\
			TPE1 *bs = bp + start[k], *be = bp + end[k];				\
			for (; bs < be; bs++) {				\
				TPE1 v = *bs;				\
				PROD_NUM_LIMIT(TPE1, TPE2, REAL_IMP, v); \
			}						\
			rb[k] = curval;					\
			if (is_##TPE2##_nil(curval))			\
				has_nils = true;			\
			else						\
				curval = TPE2##_nil;	/* For the next iteration */	\
		}							\
	} while (0)

/* product on floating-points */
#define PROD_FP(TPE1, TPE2, ARG) \
	do {	\
		if (!is_##TPE1##_nil(ARG)) {		\
			if (is_##TPE2##_nil(curval)) {	\
				curval = (TPE2) ARG;	\
			} else if (ABSOLUTE(curval) > 1 && GDK_##TPE2##_max / ABSOLUTE(ARG) < ABSOLUTE(curval)) { \
				if (abort_on_error)	\
					goto calc_overflow; \
				curval = TPE2##_nil;	\
				nils++;			\
			} else {			\
				curval *= ARG;		\
			}			\
		}			\
	} while(0)

#define ANALYTICAL_PROD_CALC_FP_UNBOUNDED_TILL_CURRENT_ROW(TPE1, TPE2, ARG3)	/* ARG3 is ignored here */ \
	do { \
		for (; k < i;) { \
			j = k++; \
			TPE2 curval = is_##TPE1##_nil(bp[j]) ? TPE2##_nil : (TPE2) bp[j];	\
			while (k < i && !op[k]) { \
				PROD_FP(TPE1, TPE2, bp[k]);	\
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils |= is_##TPE2##_nil(curval);	\
		} \
	} while (0)

#define ANALYTICAL_PROD_CALC_FP_CURRENT_ROW_TILL_UNBOUNDED(TPE1, TPE2, ARG3)	/* ARG3 is ignored here */ \
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			TPE2 curval = is_##TPE1##_nil(bp[l]) ? TPE2##_nil : (TPE2) bp[l];	\
			while (j >= k && !op[j]) { \
				PROD_FP(TPE1, TPE2, bp[j]);	\
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
		}	\
		k = i; \
	} while (0)

#define ANALYTICAL_PROD_CALC_FP_ALL_ROWS(TPE1, TPE2, ARG3)	/* ARG3 is ignored here */	\
	do { \
		TPE2 curval = TPE2##_nil; \
		for (; j < i; j++) { \
			TPE1 v = bp[j]; \
			PROD_FP(TPE1, TPE2, v); \
		} \
		for (; k < i; k++) \
			rb[k] = curval; \
		has_nils |= is_##TPE2##_nil(curval);	\
	} while (0)

#define ANALYTICAL_PROD_CALC_FP_CURRENT_ROW(TPE1, TPE2, ARG3)	/* ARG3 is ignored here */	\
	do {								\
		for (; k < i; k++) { \
			TPE1 v = bp[k]; \
			if (is_##TPE1##_nil(v)) {	\
				rb[k] = TPE2##_nil; \
				has_nils = true; \
			} else	{		\
				rb[k] = (TPE2) v; \
			} \
		} \
	} while (0)

#define ANALYTICAL_PROD_CALC_FP_OTHERS(TPE1, TPE2, ARG3)	/* ARG3 is ignored here */		\
	do {								\
		TPE2 curval = TPE2##_nil;			\
		for (; k < i; k++) {				\
			TPE1 *bs = bp + start[k], *be = bp + end[k];	\
			for (; bs < be; bs++) {				\
				TPE1 v = *bs;				\
				PROD_FP(TPE1, TPE2, v); \
			}						\
			rb[k] = curval;					\
			if (is_##TPE2##_nil(curval))			\
				has_nils = true;			\
			else						\
				curval = TPE2##_nil;	/* For the next iteration */	\
		}							\
	} while (0)

#define ANALYTICAL_PROD_CALC_NUM_PARTITIONS(TPE1, TPE2, TPE3_OR_REAL_IMP, IMP)		\
	do {						\
		TPE1 *bp = (TPE1*)Tloc(b, 0);	 \
		TPE2 *restrict rb = (TPE2*)Tloc(r, 0); \
		if (p) {					\
			for (; i < cnt; i++) {		\
				if (np[i]) 			\
					IMP(TPE1, TPE2, TPE3_OR_REAL_IMP);	\
			}						\
		}	\
		i = cnt;					\
		IMP(TPE1, TPE2, TPE3_OR_REAL_IMP);	\
	} while (0)

#if HAVE_HGE
#define ANALYTICAL_PROD_LIMIT(IMP)	\
	case TYPE_lng:{	\
		switch (tp1) {	\
		case TYPE_bte:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(bte, lng, hge, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
			break;	\
		case TYPE_sht:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(sht, lng, hge, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
			break;	\
		case TYPE_int:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(int, lng, hge, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
			break;	\
		case TYPE_lng:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(lng, lng, hge, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
			break;	\
		default:	\
			goto nosupport;	\
		}	\
		break;	\
	}	\
	case TYPE_hge:{	\
		switch (tp1) {	\
		case TYPE_bte:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(bte, hge, HGEMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		case TYPE_sht:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(sht, hge, HGEMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		case TYPE_int:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(int, hge, HGEMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		case TYPE_lng:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(lng, hge, HGEMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		case TYPE_hge:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(hge, hge, HGEMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		default:	\
			goto nosupport;	\
		}	\
		break;	\
	}
#else
#define ANALYTICAL_PROD_LIMIT(IMP)
	case TYPE_lng:{	\
		switch (tp1) {	\
		case TYPE_bte:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(bte, lng, LNGMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		case TYPE_sht:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(sht, lng, LNGMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		case TYPE_int:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(int, lng, LNGMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		case TYPE_lng:	\
			ANALYTICAL_PROD_CALC_NUM_PARTITIONS(lng, lng, LNGMUL_CHECK, ANALYTICAL_PROD_CALC_NUM_LIMIT_##IMP);	\
			break;	\
		default:	\
			goto nosupport;	\
		}	\
		break;	\
	}
#endif

#define ANALYTICAL_PROD_BRANCHES(IMP)		\
	do { \
		switch (tp2) {	\
		case TYPE_bte:{	\
			switch (tp1) {	\
			case TYPE_bte:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(bte, bte, sht, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
				break;	\
			default:	\
				goto nosupport;	\
			}	\
			break;	\
		}	\
		case TYPE_sht:{	\
			switch (tp1) {	\
			case TYPE_bte:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(bte, sht, int, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
				break;	\
			case TYPE_sht:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(sht, sht, int, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
				break;	\
			default:	\
				goto nosupport;	\
			}	\
			break;	\
		}	\
		case TYPE_int:{	\
			switch (tp1) {	\
			case TYPE_bte:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(bte, int, lng, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
				break;	\
			case TYPE_sht:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(sht, int, lng, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
				break;	\
			case TYPE_int:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(int, int, lng, ANALYTICAL_PROD_CALC_NUM_##IMP);	\
				break;	\
			default:	\
				goto nosupport;	\
			}	\
			break;	\
		}	\
		ANALYTICAL_PROD_LIMIT(IMP)	\
		case TYPE_flt:{	\
			switch (tp1) {	\
			case TYPE_flt:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(flt, flt, ;, ANALYTICAL_PROD_CALC_FP_##IMP);	\
				break;	\
			default:	\
				goto nosupport;	\
			}	\
			break;	\
		}	\
		case TYPE_dbl:{	\
			switch (tp1) {	\
			case TYPE_flt:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(flt, dbl, ;, ANALYTICAL_PROD_CALC_FP_##IMP);	\
				break;	\
			case TYPE_dbl:	\
				ANALYTICAL_PROD_CALC_NUM_PARTITIONS(dbl, dbl, ;, ANALYTICAL_PROD_CALC_FP_##IMP);	\
				break;	\
			default:	\
				goto nosupport;	\
			}	\
			break;	\
		}	\
		default:	\
			goto nosupport;	\
		}	\
	} while (0)

gdk_return
GDKanalyticalprod(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tp1, int tp2, int frame_type)
{
	bool has_nils = false;
	lng i = 0, j = 0, k = 0, l = 0, m = 0, cnt = (lng) BATcount(b);
	lng *restrict start = s ? (lng*)Tloc(s, 0) : NULL, *restrict end = e ? (lng*)Tloc(e, 0) : NULL;
	bit *restrict np = p ? Tloc(p, 0) : NULL, *restrict op = o ? Tloc(o, 0) : NULL;
	int abort_on_error = 1;
	BUN nils = 0;

	switch (frame_type) {
	case 3: /* unbounded until current row */	{
		ANALYTICAL_PROD_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW);
	} break;
	case 4: /* current row until unbounded */	{
		ANALYTICAL_PROD_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED);
	} break;
	case 5: /* all rows */	{
		ANALYTICAL_PROD_BRANCHES(ALL_ROWS);
	} break;
	case 6: /* current row */ {
		ANALYTICAL_PROD_BRANCHES(CURRENT_ROW);
	} break;
	default: {
		ANALYTICAL_PROD_BRANCHES(OTHERS);
	}
	}

	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
      nosupport:
	GDKerror("type combination (prod(%s)->%s) not supported.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
      calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}

#ifdef HAVE_HGE
#define LNG_HGE hge
#else
#define LNG_HGE lng
#endif

/* average on integers */
#define ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, PART, LNG_HGE, ARG) \
	if (!is_##TPE##_nil(ARG)) {		\
		ADD_WITH_CHECK(ARG, sum, LNG_HGE, sum, GDK_##LNG_HGE##_max, goto avg_overflow##TPE##IMP##PART); \
		/* count only when no overflow occurs */ \
		n++;				\
	}

#define ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP, PART) \
			if (0) {					\
avg_overflow##TPE##IMP##PART:							\
				assert(n > 0);				\
				if (sum >= 0) {				\
					a = (TPE) (sum / n);		\
					rr = (lng) (sum % n);		\
				} else {				\
					sum = -sum;			\
					a = - (TPE) (sum / n);		\
					rr = (lng) (sum % n);		\
					if (r) {			\
						a--;			\
						rr = n - rr;		\
					}				\
				}

#define ANALYTICAL_AVERAGE_CALC_NUM_STEP3(TPE, IMP, PART) \
				curval = a + (dbl) rr / n;		\
				goto calc_done##TPE##IMP##PART;			\
			}						\
			curval = n > 0 ? (dbl) sum / n : dbl_nil;	\
calc_done##TPE##IMP##PART:

#define ANALYTICAL_AVG_IMP_NUM_UNBOUNDED_TILL_CURRENT_ROW(TPE, IMP, PART, LNG_HGE) \
	do { \
		for (; k < i;) { \
			dbl curval = is_##TPE##_nil(bp[k]) ? dbl_nil : (dbl) bp[k];	\
			j = k++; \
			while (k < i && !op[k]) { \
				ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, PART, LNG_HGE, bp[k]) \
				ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP, PART) \
				while (k < i && !op[k]) { \
					TPE v = bp[k++];			\
					if (is_##TPE##_nil(v))		\
						continue;		\
					AVERAGE_ITER(TPE, v, a, rr, n);	\
				}					\
				ANALYTICAL_AVERAGE_CALC_NUM_STEP3(TPE, IMP, PART) \
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils = has_nils || (n == 0);		\
		} \
		n = 0;				\
		sum = 0;			\
		a = 0; 			\
	} while (0)

#define ANALYTICAL_AVG_IMP_NUM_CURRENT_ROW_TILL_UNBOUNDED(TPE, IMP, PART, LNG_HGE) \
	do { \
		for (j = i - 1; j >= k; ) { \
			dbl curval = is_##TPE##_nil(bp[j]) ? dbl_nil : (dbl) bp[j];	\
			l = j--; \
			while (j >= k && !op[j]) { \
				ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, PART, LNG_HGE, bp[j]) \
				ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP, PART) \
				while (j >= k && !op[j]) { \
					TPE v = bp[j--];			\
					if (is_##TPE##_nil(v))		\
						continue;		\
					AVERAGE_ITER(TPE, v, a, rr, n);	\
				}					\
				ANALYTICAL_AVERAGE_CALC_NUM_STEP3(TPE, IMP, PART) \
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
			has_nils = has_nils || (n == 0);		\
		}	\
		n = 0;				\
		sum = 0;			\
		a = 0; 			\
		k = i; \
	} while (0)

#define ANALYTICAL_AVG_IMP_NUM_ALL_ROWS(TPE, IMP, PART, LNG_HGE)	\
	do { \
		dbl dr = 0; \
		for (; j < i; j++) { \
			TPE v = bp[j]; \
			ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, PART, LNG_HGE, v) \
			ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP, PART) \
			for (; j < i; j++) { \
				v = bp[j];			\
				if (is_##TPE##_nil(v))		\
					continue;		\
				AVERAGE_ITER(TPE, v, a, rr, n);	\
			}					\
			ANALYTICAL_AVERAGE_CALC_NUM_STEP3(TPE, IMP, PART) \
			dr = curval; \
		} \
		for (; k < i; k++) \
			rb[k] = dr; \
		has_nils = has_nils || (n == 0);		\
		n = 0;				\
		sum = 0;			\
		a = 0; 			\
	} while (0)

#define ANALYTICAL_AVG_IMP_NUM_CURRENT_ROW(TPE, IMP, PART, LNG_HGE)	\
	do { \
		for (; k < i; k++) { \
			TPE v = bp[k]; \
			if (is_##TPE##_nil(v)) {	\
				rb[k] = dbl_nil; \
				has_nils = true; \
			} else	{		\
				rb[k] = (dbl) v; \
			} \
		} \
	} while (0)

#define ANALYTICAL_AVG_IMP_NUM_OTHERS(TPE, IMP, PART, LNG_HGE)	\
	do {								\
		TPE *be = 0, *bs = 0;	 \
		for (; k < i; k++) {				\
			bs = bp + start[k];				\
			be = bp + end[k];				\
			for (; bs < be; bs++) {				\
				TPE v = *bs;				\
				ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, PART, LNG_HGE, v) \
			}						\
			ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP, PART) \
			for (; bs < be; bs++) {			\
				TPE v = *bs;			\
				if (is_##TPE##_nil(v))		\
					continue;		\
				AVERAGE_ITER(TPE, v, a, rr, n);	\
			}					\
			ANALYTICAL_AVERAGE_CALC_NUM_STEP3(TPE, IMP, PART) \
			rb[k] = curval;					\
			has_nils = has_nils || (n == 0);		\
			n = 0;				\
			sum = 0;			\
			a = 0; 			\
		}							\
	} while (0)

/* average on floating-points */
#define ANALYTICAL_AVG_IMP_FP_UNBOUNDED_TILL_CURRENT_ROW(TPE, IMP, PART, LNG_HGE) \
	do { \
		for (; k < i;) { \
			j = k++; \
			dbl curval = is_##TPE##_nil(bp[j]) ? dbl_nil : (dbl) bp[j];	\
			while (k < i && !op[k]) { \
				if (!is_##TPE##_nil(bp[k]))		\
					AVERAGE_ITER_FLOAT(TPE, bp[k], a, n); \
				k++; \
			} \
			for (; j < k; j++) \
				rb[j] = curval; \
			has_nils = has_nils || (n == 0);		\
		} \
		n = 0;			\
		a = 0; 		\
	} while (0)

#define ANALYTICAL_AVG_IMP_FP_CURRENT_ROW_TILL_UNBOUNDED(TPE, IMP, PART, LNG_HGE) \
	do { \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			dbl curval = is_##TPE##_nil(bp[l]) ? dbl_nil : (dbl) bp[l];	\
			while (j >= k && !op[j]) { \
				if (!is_##TPE##_nil(bp[j]))		\
					AVERAGE_ITER_FLOAT(TPE, bp[j], a, n); \
				j--; \
			} \
			m = MAX(k, j); \
			for (; l >= m; l--) \
				rb[l] = curval; \
			has_nils = has_nils || (n == 0);		\
		}	\
		n = 0;		\
		a = 0;		\
		k = i; \
	} while (0)

#define ANALYTICAL_AVG_IMP_FP_ALL_ROWS(TPE, IMP, PART, LNG_HGE)	\
	do { \
		for (; j < i; j++) { \
			TPE v = bp[j]; \
			if (!is_##TPE##_nil(v))		\
				AVERAGE_ITER_FLOAT(TPE, v, a, n); \
		} \
		curval = (n > 0) ? a : dbl_nil;			\
		for (; k < i; k++) \
			rb[k] = curval; \
		has_nils = has_nils || (n == 0);		\
		n = 0;		\
		a = 0; 		\
	} while (0)

#define ANALYTICAL_AVG_IMP_FP_CURRENT_ROW(TPE, IMP, PART, LNG_HGE)	 ANALYTICAL_AVG_IMP_NUM_CURRENT_ROW(TPE, IMP, PART, LNG_HGE)

#define ANALYTICAL_AVG_IMP_FP_OTHERS(TPE, IMP, PART, LNG_HGE)			\
	do {								\
		for (; k < i; k++) {				\
			TPE *bs = bp + start[k];				\
			TPE *be = bp + end[k];				\
			for (; bs < be; bs++) {				\
				TPE v = *bs;				\
				if (!is_##TPE##_nil(v))			\
					AVERAGE_ITER_FLOAT(TPE, v, a, n); \
			}						\
			curval = (n > 0) ? a : dbl_nil;			\
			rb[k] = curval;					\
			has_nils = has_nils || (n == 0);		\
			n = 0;			\
			a = 0; 		\
		}							\
	} while (0)

#define ANALYTICAL_AVG_PARTITIONS(TPE, IMP, LNG_HGE, REAL_IMP)		\
	do {						\
		TPE *bp = (TPE*)Tloc(b, 0); \
		if (p) {					\
			for (; i < cnt; i++) {		\
				if (np[i]) 			\
					REAL_IMP(TPE, IMP, P1, LNG_HGE);	\
			}						\
		}			\
		i = cnt;					\
		REAL_IMP(TPE, IMP, P3, LNG_HGE);	\
	} while (0)

#if HAVE_HGE
#define ANALYTICAL_AVG_LIMIT(IMP, LNG_HGE) \
	case TYPE_hge: \
		ANALYTICAL_AVG_PARTITIONS(hge, IMP, LNG_HGE, ANALYTICAL_AVG_IMP_NUM_##IMP); \
		break;
#else
#define ANALYTICAL_PROD_LIMIT(IMP, LNG_HGE)
#endif

#define ANALYTICAL_AVG_BRANCHES(IMP, LNG_HGE)		\
	do { \
		switch (tpe) {	\
		case TYPE_bte:	\
			ANALYTICAL_AVG_PARTITIONS(bte, IMP, LNG_HGE, ANALYTICAL_AVG_IMP_NUM_##IMP);	\
			break;	\
		case TYPE_sht:	\
			ANALYTICAL_AVG_PARTITIONS(sht, IMP, LNG_HGE, ANALYTICAL_AVG_IMP_NUM_##IMP);	\
			break;	\
		case TYPE_int:	\
			ANALYTICAL_AVG_PARTITIONS(int, IMP, LNG_HGE, ANALYTICAL_AVG_IMP_NUM_##IMP);	\
			break;	\
		case TYPE_lng:	\
			ANALYTICAL_AVG_PARTITIONS(lng, IMP, LNG_HGE, ANALYTICAL_AVG_IMP_NUM_##IMP);	\
			break;	\
		ANALYTICAL_AVG_LIMIT(IMP, LNG_HGE)	\
		case TYPE_flt:	\
			ANALYTICAL_AVG_PARTITIONS(flt, IMP, LNG_HGE, ANALYTICAL_AVG_IMP_FP_##IMP);	\
			break;	\
		case TYPE_dbl:	\
			ANALYTICAL_AVG_PARTITIONS(dbl, IMP, LNG_HGE, ANALYTICAL_AVG_IMP_FP_##IMP);	\
			break;	\
		default:	\
			goto nosupport;	\
		}	\
	} while (0)

gdk_return
GDKanalyticalavg(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type)
{
	bool has_nils = false;
	lng i = 0, j = 0, k = 0, l = 0, m = 0, cnt = (lng) BATcount(b), n = 0, rr = 0;
	lng *restrict start = s ? (lng*)Tloc(s, 0) : NULL, *restrict end = e ? (lng*)Tloc(e, 0) : NULL;
	dbl *restrict rb = (dbl *) Tloc(r, 0), curval = 0, a = 0;
	bit *restrict np = p ? Tloc(p, 0) : NULL, *restrict op = o ? Tloc(o, 0) : NULL;
	bool abort_on_error = true;
	BUN nils = 0;
#ifdef HAVE_HGE
	hge sum = 0;
#else
	lng sum = 0;
#endif

	switch (frame_type) {
	case 3: /* unbounded until current row */	{
		ANALYTICAL_AVG_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW, LNG_HGE);
	} break;
	case 4: /* current row until unbounded */	{
		ANALYTICAL_AVG_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED, LNG_HGE);
	} break;
	case 5: /* all rows */	{
		ANALYTICAL_AVG_BRANCHES(ALL_ROWS, LNG_HGE);
	} break;
	case 6: /* current row */ {
		ANALYTICAL_AVG_BRANCHES(CURRENT_ROW, LNG_HGE);
	} break;
	default: {
		ANALYTICAL_AVG_BRANCHES(OTHERS, LNG_HGE);
	}
	}

	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
      nosupport:
	GDKerror("average of type %s to dbl unsupported.\n", ATOMname(tpe));
	return GDK_FAIL;
}

#ifdef TRUNCATE_NUMBERS
#define ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avg, rem, ncnt) \
	do {
		if (rem > 0 && avg < 0) \
			avg++; \
	} while(0)
#else
#define ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avg, rem, ncnt) \
	do { \
		if (rem > 0) { \
			if (avg < 0) { \
				if (2*rem > ncnt) \
					avg++; \
			} else { \
				if (2*rem >= ncnt) \
					avg++; \
			} \
		} \
	} while(0)
#endif

#define ANALYTICAL_AVG_INT_UNBOUNDED_TILL_CURRENT_ROW(TPE) \
	do { \
		TPE avg = 0; \
		for (; k < i;) { \
			j = k++; \
			if (!is_##TPE##_nil(bp[j])) {	\
				avg = bp[j];	\
				ncnt++;	\
			}	\
			while (k < i && !op[k]) { \
				if (!is_##TPE##_nil(bp[k]))	\
					AVERAGE_ITER(TPE, bp[k], avg, rem, ncnt); \
				k++;	\
			} \
			if (ncnt == 0) {	\
				has_nils = true; \
				for (; j < k; j++) \
					rb[j] = TPE##_nil; \
			} else { \
				dbl avgfinal = avg; \
				ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avgfinal, rem, ncnt); \
				for (; j < k; j++) \
					rb[j] = avgfinal; \
			} \
		} \
		rem = 0; \
		ncnt = 0; \
	} while (0)

#define ANALYTICAL_AVG_INT_CURRENT_ROW_TILL_UNBOUNDED(TPE) \
	do { \
		TPE avg = 0; \
		for (j = i - 1; j >= k; ) { \
			l = j--; \
			if (!is_##TPE##_nil(bp[l])) {	\
				avg = bp[l];	\
				ncnt++;	\
			}	\
			while (j >= k && !op[j]) { \
				if (!is_##TPE##_nil(bp[j]))	\
					AVERAGE_ITER(TPE, bp[j], avg, rem, ncnt); \
				j--; \
			} \
			m = MAX(k, j); \
			if (ncnt == 0) {	\
				has_nils = true; \
				for (; l >= m; l--) \
					rb[l] = TPE##_nil; \
			} else { \
				dbl avgfinal = avg; \
				ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avgfinal, rem, ncnt); \
				for (; l >= m; l--) \
					rb[l] = avgfinal; \
			} \
		}	\
		rem = 0; \
		ncnt = 0; \
		k = i; \
	} while (0)

#define ANALYTICAL_AVG_INT_ALL_ROWS(TPE)	\
	do { \
		TPE avg = 0; \
		for (; j < i; j++) { \
			TPE v = bp[j]; \
			if (!is_##TPE##_nil(v))	\
				AVERAGE_ITER(TPE, v, avg, rem, ncnt); \
		} \
		if (ncnt == 0) {	\
			has_nils = true; \
			rb[k] = TPE##_nil; \
		} else { \
			ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avg, rem, ncnt); \
			rb[k] = avg; \
		} \
		rem = 0; \
		ncnt = 0; \
	} while (0)

#define ANALYTICAL_AVG_INT_CURRENT_ROW(TPE)	\
	do {								\
		for (; k < i; k++) { \
			TPE v = bp[k]; \
			rb[k] = bp[k]; \
			has_nils |= is_##TPE##_nil(v); \
		} \
	} while (0)

#define ANALYTICAL_AVG_INT_OTHERS(TPE)			\
	do {								\
		TPE avg = 0; \
		for (; k < i; k++) {			\
			TPE *bs = bp + start[k], *be = bp + end[k];				\
			for (; bs < be; bs++) {				\
				TPE v = *bs;				\
				if (!is_##TPE##_nil(v))	\
					AVERAGE_ITER(TPE, v, avg, rem, ncnt); \
			}	\
			if (ncnt == 0) {	\
				has_nils = true; \
				rb[k] = TPE##_nil; \
			} else { \
				ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avg, rem, ncnt); \
				rb[k] = avg; \
			} \
			rem = 0; \
			ncnt = 0; \
		}	\
	} while (0)

#define ANALYTICAL_AVG_INT_PARTITIONS(TPE, IMP)		\
	do {						\
		TPE *bp = (TPE*)Tloc(b, 0), *restrict rb = (TPE *) Tloc(r, 0); \
		if (p) {					\
			for (; i < cnt; i++) {		\
				if (np[i]) 			\
					IMP(TPE);	\
			}						\
		}	\
		i = cnt;			\
		IMP(TPE);	\
	} while (0)

#if HAVE_HGE
#define ANALYTICAL_AVG_INT_LIMIT(IMP) \
	case TYPE_hge: \
		ANALYTICAL_AVG_INT_PARTITIONS(hge, ANALYTICAL_AVG_INT_##IMP); \
		break;
#else
#define ANALYTICAL_AVG_INT_LIMIT(IMP)
#endif

#define ANALYTICAL_AVG_INT_BRANCHES(IMP)		\
	do { \
		switch (tpe) {	\
		case TYPE_bte:	\
			ANALYTICAL_AVG_INT_PARTITIONS(bte, ANALYTICAL_AVG_INT_##IMP);	\
			break;	\
		case TYPE_sht:	\
			ANALYTICAL_AVG_INT_PARTITIONS(sht, ANALYTICAL_AVG_INT_##IMP);	\
			break;	\
		case TYPE_int:	\
			ANALYTICAL_AVG_INT_PARTITIONS(int, ANALYTICAL_AVG_INT_##IMP);	\
			break;	\
		case TYPE_lng:	\
			ANALYTICAL_AVG_INT_PARTITIONS(lng, ANALYTICAL_AVG_INT_##IMP);	\
			break;	\
		ANALYTICAL_AVG_INT_LIMIT(IMP)	\
		default:	\
			goto nosupport;	\
		}	\
	} while (0)

gdk_return
GDKanalyticalavginteger(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type)
{
	bool has_nils = false;
	lng i = 0, j = 0, k = 0, l = 0, m = 0, cnt = (lng) BATcount(b), rem = 0, ncnt = 0;
	lng *restrict start = s ? (lng*)Tloc(s, 0) : NULL, *restrict end = e ? (lng*)Tloc(e, 0) : NULL;
	bit *restrict np = p ? Tloc(p, 0) : NULL, *restrict op = o ? Tloc(o, 0) : NULL;

	switch (frame_type) {
	case 3: /* unbounded until current row */	{
		ANALYTICAL_AVG_INT_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW);
	} break;
	case 4: /* current row until unbounded */	{
		ANALYTICAL_AVG_INT_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED);
	} break;
	case 5: /* all rows */	{
		ANALYTICAL_AVG_INT_BRANCHES(ALL_ROWS);
	} break;
	case 6: /* current row */ {
		ANALYTICAL_AVG_INT_BRANCHES(CURRENT_ROW);
	} break;
	default: {
		ANALYTICAL_AVG_INT_BRANCHES(OTHERS);
	}
	}

	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
      nosupport:
	GDKerror("average of type %s to %s unsupported.\n", ATOMname(tpe), ATOMname(tpe));
	return GDK_FAIL;
}

#define ANALYTICAL_STDEV_VARIANCE_CALC(TPE, SAMPLE, OP)	\
	do {								\
		TPE *bp = (TPE*)Tloc(b, 0), *bs, *be, v;		\
		for (; i < cnt; i++, rb++) {				\
			bs = bp + start[i];				\
			be = bp + end[i];				\
			for (; bs < be; bs++) {				\
				v = *bs;				\
				if (is_##TPE##_nil(v))		\
					continue;		\
				n++;				\
				delta = (dbl) v - mean;		\
				mean += delta / n;		\
				m2 += delta * ((dbl) v - mean);	\
			}						\
			if (isinf(m2)) {	\
				goto overflow;		\
			} else if (n > SAMPLE) { \
				*rb = OP; \
			} else { \
				*rb = dbl_nil; \
				nils++; \
			} \
			n = 0;	\
			mean = 0;	\
			m2 = 0; \
		}	\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_STDEV_VARIANCE_LIMIT(SAMPLE, OP) \
	case TYPE_hge: \
		ANALYTICAL_STDEV_VARIANCE_CALC(hge, SAMPLE, OP); \
	break;
#else
#define ANALYTICAL_STDEV_VARIANCE_LIMIT(SAMPLE, OP)
#endif

#define GDK_ANALYTICAL_STDEV_VARIANCE(NAME, SAMPLE, OP, DESC) \
gdk_return \
GDKanalytical_##NAME(BAT *r, BAT *b, BAT *s, BAT *e, int tpe) \
{ \
	BUN i = 0, cnt = BATcount(b), n = 0, nils = 0; \
	lng *restrict start, *restrict end; \
	dbl *restrict rb = (dbl *) Tloc(r, 0), mean = 0, m2 = 0, delta; \
 \
	assert(s && e); \
	start = (lng *) Tloc(s, 0); \
	end = (lng *) Tloc(e, 0); \
 \
	switch (tpe) { \
	case TYPE_bte: \
		ANALYTICAL_STDEV_VARIANCE_CALC(bte, SAMPLE, OP); \
		break; \
	case TYPE_sht: \
		ANALYTICAL_STDEV_VARIANCE_CALC(sht, SAMPLE, OP); \
		break; \
	case TYPE_int: \
		ANALYTICAL_STDEV_VARIANCE_CALC(int, SAMPLE, OP); \
		break; \
	case TYPE_lng: \
		ANALYTICAL_STDEV_VARIANCE_CALC(lng, SAMPLE, OP); \
		break; \
	ANALYTICAL_STDEV_VARIANCE_LIMIT(SAMPLE, OP) \
	case TYPE_flt:\
		ANALYTICAL_STDEV_VARIANCE_CALC(flt, SAMPLE, OP); \
		break; \
	case TYPE_dbl: \
		ANALYTICAL_STDEV_VARIANCE_CALC(dbl, SAMPLE, OP); \
		break; \
	default: \
		GDKerror("%s of type %s unsupported.\n", DESC, ATOMname(tpe)); \
		return GDK_FAIL; \
	} \
	BATsetcount(r, cnt); \
	r->tnonil = nils == 0; \
	r->tnil = nils > 0; \
	return GDK_SUCCEED; \
  overflow: \
	GDKerror("22003!overflow in calculation.\n"); \
	return GDK_FAIL; \
}

GDK_ANALYTICAL_STDEV_VARIANCE(stddev_samp, 1, sqrt(m2 / (n - 1)), "standard deviation")
GDK_ANALYTICAL_STDEV_VARIANCE(stddev_pop, 0, sqrt(m2 / n), "standard deviation")
GDK_ANALYTICAL_STDEV_VARIANCE(variance_samp, 1, m2 / (n - 1), "variance")
GDK_ANALYTICAL_STDEV_VARIANCE(variance_pop, 0, m2 / n, "variance")

#define ANALYTICAL_COVARIANCE_CALC(TPE, SAMPLE, OP)	\
	do {								\
		TPE *bp1 = (TPE*)Tloc(b1, 0), *bp2 = (TPE*)Tloc(b2, 0), *bs1, *be1, *bs2, v1, v2;	\
		for (; i < cnt; i++, rb++) {		\
			bs1 = bp1 + start[i];				\
			be1 = bp1 + end[i];				\
			bs2 = bp2 + start[i];		\
			for (; bs1 < be1; bs1++, bs2++) {	\
				v1 = *bs1;				\
				v2 = *bs2;				\
				if (is_##TPE##_nil(v1) || is_##TPE##_nil(v2))	\
					continue;		\
				n++;				\
				delta1 = (dbl) v1 - mean1;		\
				mean1 += delta1 / n;		\
				delta2 = (dbl) v2 - mean2;		\
				mean2 += delta2 / n;		\
				m2 += delta1 * ((dbl) v2 - mean2);	\
			}	\
			if (isinf(m2)) {	\
				goto overflow;		\
			} else if (n > SAMPLE) { \
				*rb = OP; \
			} else { \
				*rb = dbl_nil; \
				nils++; \
			} \
			n = 0;	\
			mean1 = 0;	\
			mean2 = 0;	\
			m2 = 0; \
		}	\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_COVARIANCE_LIMIT(SAMPLE, OP) \
	case TYPE_hge: \
		ANALYTICAL_COVARIANCE_CALC(hge, SAMPLE, OP); \
	break;
#else
#define ANALYTICAL_COVARIANCE_LIMIT(SAMPLE, OP)
#endif

#define GDK_ANALYTICAL_COVARIANCE(NAME, SAMPLE, OP) \
gdk_return \
GDKanalytical_##NAME(BAT *r, BAT *b1, BAT *b2, BAT *s, BAT *e, int tpe) \
{ \
	BUN i = 0, cnt = BATcount(b1), n = 0, nils = 0; \
	lng *restrict start, *restrict end; \
	dbl *restrict rb = (dbl *) Tloc(r, 0), mean1 = 0, mean2 = 0, m2 = 0, delta1, delta2; \
 \
	assert(s && e && BATcount(b1) == BATcount(b2)); \
	start = (lng *) Tloc(s, 0); \
	end = (lng *) Tloc(e, 0); \
 \
	switch (tpe) { \
	case TYPE_bte: \
		ANALYTICAL_COVARIANCE_CALC(bte, SAMPLE, OP); \
		break; \
	case TYPE_sht: \
		ANALYTICAL_COVARIANCE_CALC(sht, SAMPLE, OP); \
		break; \
	case TYPE_int: \
		ANALYTICAL_COVARIANCE_CALC(int, SAMPLE, OP); \
		break; \
	case TYPE_lng: \
		ANALYTICAL_COVARIANCE_CALC(lng, SAMPLE, OP); \
		break; \
	ANALYTICAL_COVARIANCE_LIMIT(SAMPLE, OP) \
	case TYPE_flt:\
		ANALYTICAL_COVARIANCE_CALC(flt, SAMPLE, OP); \
		break; \
	case TYPE_dbl: \
		ANALYTICAL_COVARIANCE_CALC(dbl, SAMPLE, OP); \
		break; \
	default: \
		GDKerror("covariance of type %s unsupported.\n", ATOMname(tpe)); \
		return GDK_FAIL; \
	} \
	BATsetcount(r, cnt); \
	r->tnonil = nils == 0; \
	r->tnil = nils > 0; \
	return GDK_SUCCEED; \
  overflow: \
	GDKerror("22003!overflow in calculation.\n"); \
	return GDK_FAIL; \
}

GDK_ANALYTICAL_COVARIANCE(covariance_samp, 1, m2 / (n - 1))
GDK_ANALYTICAL_COVARIANCE(covariance_pop, 0, m2 / n)

#define ANALYTICAL_CORRELATION_CALC(TPE)	\
	do {								\
		TPE *bp1 = (TPE*)Tloc(b1, 0), *bp2 = (TPE*)Tloc(b2, 0), *bs1, *be1, *bs2, v1, v2;	\
		for (; i < cnt; i++, rb++) {		\
			bs1 = bp1 + start[i];				\
			be1 = bp1 + end[i];				\
			bs2 = bp2 + start[i];		\
			for (; bs1 < be1; bs1++, bs2++) {	\
				v1 = *bs1;				\
				v2 = *bs2;				\
				if (is_##TPE##_nil(v1) || is_##TPE##_nil(v2))	\
					continue;		\
				n++;	\
				delta1 = (dbl) v1 - mean1;	\
				mean1 += delta1 / n;	\
				delta2 = (dbl) v2 - mean2;	\
				mean2 += delta2 / n;	\
				aux = (dbl) v2 - mean2; \
				up += delta1 * aux;	\
				down1 += delta1 * ((dbl) v1 - mean1);	\
				down2 += delta2 * aux;	\
			}	\
			if (isinf(up) || isinf(down1) || isinf(down2)) {	\
				goto overflow;	\
			} else if (n != 0 && down1 != 0 && down2 != 0) { \
				*rb = (up / n) / (sqrt(down1 / n) * sqrt(down2 / n)); \
				assert(!is_dbl_nil(*rb)); \
			} else { \
				*rb = dbl_nil; \
				nils++; \
			} \
			n = 0;	\
			mean1 = 0;	\
			mean2 = 0;	\
			up = 0; \
			down1 = 0;	\
			down2 = 0;	\
		}	\
	} while (0)

gdk_return
GDKanalytical_correlation(BAT *r, BAT *b1, BAT *b2, BAT *s, BAT *e, int tpe)
{
	BUN i = 0, cnt = BATcount(b1), n = 0, nils = 0;
	lng *restrict start, *restrict end;
	dbl *restrict rb = (dbl *) Tloc(r, 0), mean1 = 0, mean2 = 0, up = 0, down1 = 0, down2 = 0, delta1, delta2, aux;

	assert(s && e && BATcount(b1) == BATcount(b2));
	start = (lng *) Tloc(s, 0);
	end = (lng *) Tloc(e, 0);

	switch (tpe) {
	case TYPE_bte:
		ANALYTICAL_CORRELATION_CALC(bte);
		break;
	case TYPE_sht:
		ANALYTICAL_CORRELATION_CALC(sht);
		break;
	case TYPE_int:
		ANALYTICAL_CORRELATION_CALC(int);
		break;
	case TYPE_lng:
		ANALYTICAL_CORRELATION_CALC(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ANALYTICAL_CORRELATION_CALC(hge);
		break;
#endif
	case TYPE_flt:
		ANALYTICAL_CORRELATION_CALC(flt);
		break;
	case TYPE_dbl:
		ANALYTICAL_CORRELATION_CALC(dbl);
		break;
	default:
		GDKerror("correlation of type %s unsupported.\n", ATOMname(tpe));
		return GDK_FAIL;
	}
	BATsetcount(r, cnt);
	r->tnonil = nils == 0;
	r->tnil = nils > 0;
	return GDK_SUCCEED;
  overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}
