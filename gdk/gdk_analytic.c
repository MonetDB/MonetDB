/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_analytic.h"
#include "gdk_calc_private.h"

#define ANALYTICAL_DIFF_IMP(TPE)              \
	do {                                      \
		TPE *bp = (TPE*)Tloc(b, 0);           \
		TPE prev = *bp, *end = bp + cnt;      \
		if(rp) {                              \
			for(; bp<end; bp++, rb++, rp++) { \
				*rb = *rp;                    \
				if (*bp != prev) {            \
					*rb = TRUE;               \
					prev = *bp;               \
				}                             \
			}                                 \
		} else {                              \
			for(; bp<end; bp++, rb++) {       \
				*rb = FALSE;                  \
				if (*bp != prev) {            \
					*rb = TRUE;               \
					prev = *bp;               \
				}                             \
			}                                 \
		}                                     \
	} while(0);

gdk_return
GDKanalyticaldiff(BAT *r, BAT *b, BAT *c, int tpe)
{
	BUN i, cnt = BATcount(b);
	bit *restrict rb = (bit*)Tloc(r, 0), *restrict rp = c ? (bit*)Tloc(c, 0) : NULL;
	int (*atomcmp)(const void *, const void *);

	switch(tpe) {
		case TYPE_bit:
			ANALYTICAL_DIFF_IMP(bit)
			break;
		case TYPE_bte:
			ANALYTICAL_DIFF_IMP(bte)
			break;
		case TYPE_sht:
			ANALYTICAL_DIFF_IMP(sht)
			break;
		case TYPE_int:
			ANALYTICAL_DIFF_IMP(int)
			break;
		case TYPE_lng:
			ANALYTICAL_DIFF_IMP(lng)
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_DIFF_IMP(hge)
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_DIFF_IMP(flt)
			break;
		case TYPE_dbl:
			ANALYTICAL_DIFF_IMP(dbl)
			break;
		default: {
			BATiter it = bat_iterator(b);
			ptr v = BUNtail(it, 0), next;
			atomcmp = ATOMcompare(tpe);
			if(rp) {
				for (i=0; i<cnt; i++, rb++, rp++) {
					*rb = *rp;
					next = BUNtail(it, i);
					if (atomcmp(v, next) != 0) {
						*rb = TRUE;
						v = next;
					}
				}
			} else {
				for(i=0; i<cnt; i++, rb++) {
					*rb = FALSE;
					next = BUNtail(it, i);
					if (atomcmp(v, next) != 0) {
						*rb = TRUE;
						v = next;
					}
				}
			}
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_DIFF_IMP

#define NTILE_CALC(TPE)               \
	do {                              \
		if((BUN)val >= ncnt) {        \
			i = 1;                    \
			for(; rb<rp; i++, rb++)   \
				*rb = i;              \
		} else if(ncnt % val == 0) {  \
			buckets = ncnt / val;     \
			for(; rb<rp; i++, rb++) { \
				if(i == buckets) {    \
					j++;              \
					i = 0;            \
				}                     \
				*rb = j;              \
			}                         \
		} else {                      \
			buckets = ncnt / val;     \
			for(; rb<rp; i++, rb++) { \
				*rb = j;              \
				if(i == buckets) {    \
					j++;              \
					i = 0;            \
				}                     \
			}                         \
		}                             \
	} while(0);

#define ANALYTICAL_NTILE_IMP(TPE)            \
	do {                                     \
		TPE i = 0, j = 1, *rp, *rb, buckets; \
		TPE val =  *(TPE *)ntile;            \
		rb = rp = (TPE*)Tloc(r, 0);          \
		if(is_##TPE##_nil(val)) {            \
			TPE *end = rp + cnt;             \
			has_nils = true;                 \
			for(; rp<end; rp++)              \
				*rp = TPE##_nil;             \
		} else if(p) {                       \
			pnp = np = (bit*)Tloc(p, 0);     \
			end = np + cnt;                  \
			for(; np<end; np++) {            \
				if (*np) {                   \
					i = 0;                   \
					j = 1;                   \
					ncnt = np - pnp;         \
					rp += ncnt;              \
					NTILE_CALC(TPE)          \
					pnp = np;                \
				}                            \
			}                                \
			i = 0;                           \
			j = 1;                           \
			ncnt = np - pnp;                 \
			rp += ncnt;                      \
			NTILE_CALC(TPE)                  \
		} else {                             \
			rp += cnt;                       \
			NTILE_CALC(TPE)                  \
		}                                    \
		goto finish;                         \
	} while(0);

gdk_return
GDKanalyticalntile(BAT *r, BAT *b, BAT *p, BAT *o, int tpe, const void* restrict ntile)
{
	BUN cnt = BATcount(b), ncnt = cnt;
	bit *np, *pnp, *end;
	bool has_nils = false;
	gdk_return gdk_res = GDK_SUCCEED;

	assert(ntile);

	(void) o;
	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_NTILE_IMP(bte)
			break;
		case TYPE_sht:
			ANALYTICAL_NTILE_IMP(sht)
			break;
		case TYPE_int:
			ANALYTICAL_NTILE_IMP(int)
			break;
		case TYPE_lng:
			ANALYTICAL_NTILE_IMP(lng)
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_NTILE_IMP(hge)
			break;
#endif
		default:
			goto nosupport;
	}
nosupport:
	GDKerror("ntile: type %s not supported.\n", ATOMname(tpe));
	return GDK_FAIL;
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return gdk_res;
}

#undef ANALYTICAL_NTILE_IMP
#undef NTILE_CALC

#define FIRST_CALC(TPE)            \
	do {                           \
		for (;rb < rp; rb++)       \
			*rb = curval;          \
		if(is_##TPE##_nil(curval)) \
			has_nils = true;       \
	} while(0);

#define ANALYTICAL_FIRST_IMP(TPE)           \
	do {                                    \
		TPE *rp, *rb, *restrict bp, curval; \
		rb = rp = (TPE*)Tloc(r, 0);         \
		bp = (TPE*)Tloc(b, 0);              \
		curval = *bp;                       \
		if (p) {                            \
			pnp = np = (bit*)Tloc(p, 0);    \
			end = np + cnt;                 \
			for(; np<end; np++) {           \
				if (*np) {                  \
					ncnt = (np - pnp);      \
					rp += ncnt;             \
					bp += ncnt;             \
					FIRST_CALC(TPE)         \
					curval = *bp;           \
					pnp = np;               \
				}                           \
			}                               \
			ncnt = (np - pnp);              \
			rp += ncnt;                     \
			bp += ncnt;                     \
			FIRST_CALC(TPE)                 \
		} else {                            \
			rp += cnt;                      \
			FIRST_CALC(TPE)                 \
		}                                   \
	} while(0);

#define ANALYTICAL_FIRST_OTHERS                                         \
	do {                                                                \
		curval = BUNtail(bpi, j);                                       \
		if((*atomcmp)(curval, nil) == 0)                                \
			has_nils = true;                                            \
		for (;j < i; j++) {                                             \
			if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED) \
				goto finish;                                            \
		}                                                               \
	} while(0);

gdk_return
GDKanalyticalfirst(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i = 0, j = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;

	(void) o;
	switch(tpe) {
		case TYPE_bit:
			ANALYTICAL_FIRST_IMP(bit)
			break;
		case TYPE_bte:
			ANALYTICAL_FIRST_IMP(bte)
			break;
		case TYPE_sht:
			ANALYTICAL_FIRST_IMP(sht)
			break;
		case TYPE_int:
			ANALYTICAL_FIRST_IMP(int)
			break;
		case TYPE_lng:
			ANALYTICAL_FIRST_IMP(lng)
			break;
#ifdef HAVE_HUGE
		case TYPE_hge:
			ANALYTICAL_FIRST_IMP(hge)
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_FIRST_IMP(flt)
			break;
		case TYPE_dbl:
			ANALYTICAL_FIRST_IMP(dbl)
			break;
		default: {
			BATiter bpi = bat_iterator(b);
			void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if (p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
					if (*np) {
						i += (np - pnp);
						ANALYTICAL_FIRST_OTHERS
						pnp = np;
					}
				}
				i += (np - pnp);
				ANALYTICAL_FIRST_OTHERS
			} else { /* single value, ie no ordering */
				i += cnt;
				ANALYTICAL_FIRST_OTHERS
			}
		}
	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return gdk_res;
}

#undef ANALYTICAL_FIRST_IMP
#undef FIRST_CALC
#undef ANALYTICAL_FIRST_OTHERS

#define LAST_CALC(TPE)             \
	do {                           \
		curval = *(bp - 1);        \
		if(is_##TPE##_nil(curval)) \
			has_nils = true;       \
		for (;rb < rp; rb++)       \
			*rb = curval;          \
	} while(0);

#define ANALYTICAL_LAST_IMP(TPE)            \
	do {                                    \
		TPE *rp, *rb, *restrict bp, curval; \
		rb = rp = (TPE*)Tloc(r, 0);         \
		bp = (TPE*)Tloc(b, 0);              \
		if (p) {                            \
			pnp = np = (bit*)Tloc(p, 0);    \
			end = np + cnt;                 \
			for(; np<end; np++) {           \
				if (*np) {                  \
					ncnt = (np - pnp);      \
					rp += ncnt;             \
					bp += ncnt;             \
					LAST_CALC(TPE)          \
					pnp = np;               \
				}                           \
			}                               \
			ncnt = (np - pnp);              \
			rp += ncnt;                     \
			bp += ncnt;                     \
			LAST_CALC(TPE)                  \
		} else {                            \
			rp += cnt;                      \
			bp += cnt;                      \
			LAST_CALC(TPE)                  \
		}                                   \
	} while(0);

#define ANALYTICAL_LAST_OTHERS                                          \
	do {                                                                \
		curval = BUNtail(bpi, i - 1);                                   \
		if((*atomcmp)(curval, nil) == 0)                                \
			has_nils = true;                                            \
		for (;j < i; j++) {                                             \
			if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED) \
				goto finish;                                            \
		}                                                               \
	} while(0);

gdk_return
GDKanalyticallast(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i = 0, j = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;

	(void) o;
	switch(tpe) {
		case TYPE_bit:
			ANALYTICAL_LAST_IMP(bit)
			break;
		case TYPE_bte:
			ANALYTICAL_LAST_IMP(bte)
			break;
		case TYPE_sht:
			ANALYTICAL_LAST_IMP(sht)
			break;
		case TYPE_int:
			ANALYTICAL_LAST_IMP(int)
			break;
		case TYPE_lng:
			ANALYTICAL_LAST_IMP(lng)
			break;
#ifdef HAVE_HUGE
		case TYPE_hge:
			ANALYTICAL_LAST_IMP(hge)
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_LAST_IMP(flt)
			break;
		case TYPE_dbl:
			ANALYTICAL_LAST_IMP(dbl)
			break;
		default: {
			BATiter bpi = bat_iterator(b);
			void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if (p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
					if (*np) {
						i += (np - pnp);
						ANALYTICAL_LAST_OTHERS
						pnp = np;
					}
				}
				i += (np - pnp);
				ANALYTICAL_LAST_OTHERS
			} else { /* single value, ie no ordering */
				i += cnt;
				ANALYTICAL_LAST_OTHERS
			}
		}
	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return gdk_res;
}

#undef ANALYTICAL_LAST_IMP
#undef LAST_CALC
#undef ANALYTICAL_LAST_OTHERS

#define NTHVALUE_CALC(TPE)         \
	do {                           \
		if(nth > (BUN) (bp - pbp)) \
			curval = TPE##_nil;    \
		else                       \
			curval = *(pbp + nth); \
		if(is_##TPE##_nil(curval)) \
			has_nils = true;       \
		for(; rb<rp; rb++)         \
			*rb = curval;          \
	} while(0);

#define ANALYTICAL_NTHVALUE_IMP(TPE)     \
	do {                                 \
		TPE *rp, *rb, *pbp, *bp, curval; \
		pbp = bp = (TPE*)Tloc(b, 0);     \
		rb = rp = (TPE*)Tloc(r, 0);      \
		if(nth == BUN_NONE) {            \
			TPE* rend = rp + cnt;        \
			has_nils = true;             \
			for(; rp<rend; rp++)         \
				*rp = TPE##_nil;         \
		} else if(p) {                   \
			pnp = np = (bit*)Tloc(p, 0); \
			end = np + cnt;              \
			for(; np<end; np++) {        \
				if (*np) {               \
					ncnt = (np - pnp);   \
					rp += ncnt;          \
					bp += ncnt;          \
					NTHVALUE_CALC(TPE)   \
					pbp = bp;            \
					pnp = np;            \
				}                        \
			}                            \
			ncnt = (np - pnp);           \
			rp += ncnt;                  \
			bp += ncnt;                  \
			NTHVALUE_CALC(TPE)           \
		} else {                         \
			rp += cnt;                   \
			bp += cnt;                   \
			NTHVALUE_CALC(TPE)           \
		}                                \
		goto finish;                     \
	} while(0);

#define ANALYTICAL_NTHVALUE_OTHERS                                      \
	do {                                                                \
		if(nth > (i - j))                                               \
			curval = nil;                                               \
		else                                                            \
			curval = BUNtail(bpi, nth);                                 \
		if((*atomcmp)(curval, nil) == 0)                                \
			has_nils = true;                                            \
		for (;j < i; j++) {                                             \
			if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED) \
				goto finish;                                            \
		}                                                               \
	} while(0);

gdk_return
GDKanalyticalnthvalue(BAT *r, BAT *b, BAT *p, BAT *o, BUN nth, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	BUN i = 0, j = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;
	bool has_nils = false;

	(void) o;
	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_NTHVALUE_IMP(bte)
			break;
		case TYPE_sht:
			ANALYTICAL_NTHVALUE_IMP(sht)
			break;
		case TYPE_int:
			ANALYTICAL_NTHVALUE_IMP(int)
			break;
		case TYPE_lng:
			ANALYTICAL_NTHVALUE_IMP(lng)
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_NTHVALUE_IMP(hge)
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_NTHVALUE_IMP(flt)
			break;
		case TYPE_dbl:
			ANALYTICAL_NTHVALUE_IMP(dbl)
			break;
		default: {
			BATiter bpi = bat_iterator(b);
			const void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if(nth == BUN_NONE) {
				has_nils = true;
				for(i=0; i<cnt; i++) {
					if ((gdk_res = BUNappend(r, nil, false)) != GDK_SUCCEED)
						goto finish;
				}
			} else if (p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
					if (*np) {
						i += (np - pnp);
						ANALYTICAL_NTHVALUE_OTHERS
						pnp = np;
					}
				}
				i += (np - pnp);
				ANALYTICAL_NTHVALUE_OTHERS
			} else { /* single value, ie no ordering */
				i += cnt;
				ANALYTICAL_NTHVALUE_OTHERS
			}
		}
	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return gdk_res;
}

#undef ANALYTICAL_NTHVALUE_IMP
#undef NTHVALUE_CALC
#undef ANALYTICAL_NTHVALUE_OTHERS

#define ANALYTICAL_LAG_CALC(TPE)            \
	do {                                    \
		for(i=0; i<lag && rb<rp; i++, rb++) \
			*rb = def;                      \
		if(lag > 0 && is_##TPE##_nil(def))  \
			has_nils = true;                \
		for(;rb<rp; rb++, bp++) {           \
			next = *bp;                     \
			*rb = next;                     \
			if(is_##TPE##_nil(next))        \
				has_nils = true;            \
		}                                   \
	} while(0);

#define ANALYTICAL_LAG_IMP(TPE)                   \
	do {                                          \
		TPE *rp, *rb, *bp, *rend,                 \
			def = *((TPE *) default_value), next; \
		bp = (TPE*)Tloc(b, 0);                    \
		rb = rp = (TPE*)Tloc(r, 0);               \
		rend = rb + cnt;                          \
		if(lag == BUN_NONE) {                     \
			has_nils = true;                      \
			for(; rb<rend; rb++)                  \
				*rb = TPE##_nil;                  \
		} else if(p) {                            \
			pnp = np = (bit*)Tloc(p, 0);          \
			end = np + cnt;                       \
			for(; np<end; np++) {                 \
				if (*np) {                        \
					ncnt = (np - pnp);            \
					rp += ncnt;                   \
					ANALYTICAL_LAG_CALC(TPE)      \
					bp += (lag < ncnt) ? lag : 0; \
					pnp = np;                     \
				}                                 \
			}                                     \
			rp += (np - pnp);                     \
			ANALYTICAL_LAG_CALC(TPE)              \
		} else {                                  \
			rp += cnt;                            \
			ANALYTICAL_LAG_CALC(TPE)              \
		}                                         \
		goto finish;                              \
	} while(0);

#define ANALYTICAL_LAG_OTHERS                                                  \
	do {                                                                       \
		for(i=0; i<lag && k<j; i++, k++) {                                     \
			if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED) \
				goto finish;                                                   \
		}                                                                      \
		if(lag > 0 && (*atomcmp)(default_value, nil) == 0)                     \
			has_nils = true;                                                   \
		for(l=k-lag; k<j; k++, l++) {                                          \
			curval = BUNtail(bpi, l);                                          \
			if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)        \
				goto finish;                                                   \
			if((*atomcmp)(curval, nil) == 0)                                   \
				has_nils = true;                                               \
		}                                                                      \
	} while (0);

gdk_return
GDKanalyticallag(BAT *r, BAT *b, BAT *p, BAT *o, BUN lag, const void* restrict default_value, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void *restrict nil;
	BUN i = 0, j = 0, k = 0, l = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;
	bool has_nils = false;

	assert(default_value);

	(void) o;
	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_LAG_IMP(bte)
			break;
		case TYPE_sht:
			ANALYTICAL_LAG_IMP(sht)
			break;
		case TYPE_int:
			ANALYTICAL_LAG_IMP(int)
			break;
		case TYPE_lng:
			ANALYTICAL_LAG_IMP(lng)
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_LAG_IMP(hge)
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_LAG_IMP(flt)
			break;
		case TYPE_dbl:
			ANALYTICAL_LAG_IMP(dbl)
			break;
		default: {
			BATiter bpi = bat_iterator(b);
			const void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if(lag == BUN_NONE) {
				has_nils = true;
				for (j=0;j < cnt; j++) {
					if ((gdk_res = BUNappend(r, nil, false)) != GDK_SUCCEED)
						goto finish;
				}
			} else if(p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
					if (*np) {
						j += (np - pnp);
						ANALYTICAL_LAG_OTHERS
						pnp = np;
					}
				}
				j += (np - pnp);
				ANALYTICAL_LAG_OTHERS
			} else {
				j += cnt;
				ANALYTICAL_LAG_OTHERS
			}
		}
	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return gdk_res;
}

#undef ANALYTICAL_LAG_IMP
#undef ANALYTICAL_LAG_CALC
#undef ANALYTICAL_LAG_OTHERS

#define LEAD_CALC(TPE)                       \
	do {                                     \
		if(lead < ncnt) {                    \
			bp += lead;                      \
			l = ncnt - lead;                 \
			for(i=0; i<l; i++, rb++, bp++) { \
				next = *bp;                  \
				*rb = next;                  \
				if(is_##TPE##_nil(next))     \
					has_nils = true;         \
			}                                \
		} else {                             \
			bp += ncnt;                      \
		}                                    \
		for(;rb<rp; rb++)                    \
			*rb = def;                       \
		if(lead > 0 && is_##TPE##_nil(def))  \
			has_nils = true;                 \
	} while(0);

#define ANALYTICAL_LEAD_IMP(TPE)                  \
	do {                                          \
		TPE *rp, *rb, *bp, *rend,                 \
			def = *((TPE *) default_value), next; \
		bp = (TPE*)Tloc(b, 0);                    \
		rb = rp = (TPE*)Tloc(r, 0);               \
		rend = rb + cnt;                          \
		if(lead == BUN_NONE) {                    \
			has_nils = true;                      \
			for(; rb<rend; rb++)                  \
				*rb = TPE##_nil;                  \
		} else if(p) {                            \
			pnp = np = (bit*)Tloc(p, 0);          \
			end = np + cnt;                       \
			for(; np<end; np++) {                 \
				if (*np) {                        \
					ncnt = (np - pnp);            \
					rp += ncnt;                   \
					LEAD_CALC(TPE)                \
					pnp = np;                     \
				}                                 \
			}                                     \
			ncnt = (np - pnp);                    \
			rp += ncnt;                           \
			LEAD_CALC(TPE)                        \
		} else {                                  \
			ncnt = cnt;                           \
			rp += ncnt;                           \
			LEAD_CALC(TPE)                        \
		}                                         \
		goto finish;                              \
	} while(0);

#define ANALYTICAL_LEAD_OTHERS                                                 \
	do {                                                                       \
		j += ncnt;                                                             \
		if(lead < ncnt) {                                                      \
			m = ncnt - lead;                                                   \
			for(i=0,n=k+lead; i<m; i++, n++) {                                 \
				curval = BUNtail(bpi, n);                                      \
				if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)    \
					goto finish;                                               \
				if((*atomcmp)(curval, nil) == 0)                               \
					has_nils = true;                                           \
			}                                                                  \
			k += i;                                                            \
		}                                                                      \
		for(; k<j; k++) {                                                      \
			if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED) \
				goto finish;                                                   \
		}                                                                      \
		if(lead > 0 && (*atomcmp)(default_value, nil) == 0)                    \
			has_nils = true;                                                   \
	} while(0);

gdk_return
GDKanalyticallead(BAT *r, BAT *b, BAT *p, BAT *o, BUN lead, const void* restrict default_value, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	BUN i = 0, j = 0, k = 0, l = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;
	bool has_nils = false;

	assert(default_value);

	(void) o;
	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_LEAD_IMP(bte)
			break;
		case TYPE_sht:
			ANALYTICAL_LEAD_IMP(sht)
			break;
		case TYPE_int:
			ANALYTICAL_LEAD_IMP(int)
			break;
		case TYPE_lng:
			ANALYTICAL_LEAD_IMP(lng)
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_LEAD_IMP(hge)
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_LEAD_IMP(flt)
			break;
		case TYPE_dbl:
			ANALYTICAL_LEAD_IMP(dbl)
			break;
		default: {
			BUN m = 0, n = 0;
			BATiter bpi = bat_iterator(b);
			const void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if(lead == BUN_NONE) {
				has_nils = true;
				for (j=0;j < cnt; j++) {
					if ((gdk_res = BUNappend(r, nil, false)) != GDK_SUCCEED)
						goto finish;
				}
			} else if(p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
					if (*np) {
						ncnt = (np - pnp);
						ANALYTICAL_LEAD_OTHERS
						pnp = np;
					}
				}
				ncnt = (np - pnp);
				ANALYTICAL_LEAD_OTHERS
			} else {
				ncnt = cnt;
				ANALYTICAL_LEAD_OTHERS
			}
		}
	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return gdk_res;
}

#undef ANALYTICAL_LEAD_IMP
#undef LEAD_CALC
#undef ANALYTICAL_LEAD_OTHERS

#define ANALYTICAL_MIN_MAX_IMP_ROWS(TPE, IMP)         \
	do {                                              \
		TPE *bl = pbp, *bs, *be;                      \
		for(; pbp<bp;pbp++) {                         \
			bs = (pbp > bl+start) ? pbp - start : bl; \
			be = (pbp+end < bp) ? pbp + end + 1 : bp; \
			curval = *bs;                             \
			bs++;                                     \
			for(; bs<be; bs++) {                      \
				v = *bs;                              \
				if(!is_##TPE##_nil(v)) {              \
					if(is_##TPE##_nil(curval))        \
						curval = v;                   \
					else                              \
						curval = IMP(v, curval);      \
				}                                     \
			}                                         \
			*rb = curval;                             \
			rb++;                                     \
			if(is_##TPE##_nil(curval))                \
				has_nils = true;                      \
		}                                             \
	} while(0);

#define ANALYTICAL_MIN_MAX_CALC_ROWS(TPE, IMP, REAL) \
	do {                                           \
		TPE *rp, *rb, *pbp, *bp, curval, v;        \
		rb = rp = (TPE*)Tloc(r, 0);                \
		pbp = bp = (TPE*)Tloc(b, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					rp += ncnt;                    \
					bp += ncnt;                    \
					REAL(TPE, IMP)                 \
					pnp = np;                      \
					pbp = bp;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			rp += ncnt;                            \
			bp += ncnt;                            \
			REAL(TPE, IMP)                         \
		} else {                                   \
			rp += cnt;                             \
			bp += cnt;                             \
			REAL(TPE, IMP)                         \
		}                                          \
	} while(0);

#define ANALYTICAL_MIN_MAX_IMP_RANGE_ALL(TPE, IMP) \
	do {                                          \
		TPE v;                                    \
		curval = *pbp;                            \
		pbp++;                                    \
		for(; pbp<bp; pbp++) {                    \
			v = *pbp;                             \
			if(!is_##TPE##_nil(v)) {              \
				if(is_##TPE##_nil(curval))        \
					curval = v;                   \
				else                              \
					curval = IMP(v, curval);      \
			}                                     \
		}                                         \
		for (;rb < rp; rb++)                      \
			*rb = curval;                         \
		if(is_##TPE##_nil(curval))                \
			has_nils = true;                      \
	} while(0);

#define ANALYTICAL_MIN_MAX_IMP_RANGE_PART(TPE, IMP) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		TPE *bs, *be, v;                        \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			bs = bp + (ns - nl);                \
			be = bs + parcel;                   \
			curval = *bs;                       \
			bs++;                               \
			for(; bs<be; bs++) {                \
				v = *bs;                        \
				if(!is_##TPE##_nil(v)) {        \
					if(is_##TPE##_nil(curval))  \
						curval = v;             \
					else                        \
						curval = IMP(v, curval);\
				}                               \
			}                                   \
			*rb = curval;                       \
			rb++;                               \
			if(is_##TPE##_nil(curval))          \
				has_nils = true;                \
		}                                       \
	} while(0);

#define ANALYTICAL_MIN_MAX_CALC_RANGE(TPE, IMP, REAL) \
	do {                                           \
		TPE *rp, *rb, *bp, curval;                 \
		rb = rp = (TPE*)Tloc(r, 0);                \
		bp = (TPE*)Tloc(b, 0);                     \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					REAL##_PART(TPE, IMP)          \
					bp += ncnt;                    \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			REAL##_PART(TPE, IMP)                  \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			REAL##_PART(TPE, IMP)                  \
		} else {                                   \
			TPE *pbp = bp;                         \
			bp += cnt;                             \
			rp += cnt;                             \
			REAL##_ALL(TPE, IMP)                   \
		}                                          \
	} while(0);

#ifdef HAVE_HUGE
#define ANALYTICAL_MIN_MAX_LIMIT(FRAME, IMP, REAL) \
	case TYPE_hge: \
		ANALYTICAL_MIN_MAX_CALC##FRAME(hge, IMP, REAL) \
	break;
#else
#define ANALYTICAL_MIN_MAX_LIMIT(FRAME, IMP, REAL)
#endif

#define ANALYTICAL_MIN_MAX_OTHERS_IMP_ROWS(SIGN_OP)                               \
	do {                                                                          \
		m = k;                                                                    \
		for(;k<i;k++) {                                                           \
			j = (k > m+start) ? k - start : m;                                    \
			l = (k+end < i) ? k + end + 1 : i;                                    \
			curval = BUNtail(bpi, j);                                             \
			j++;                                                                  \
			for (;j < l; j++) {                                                   \
				void *next = BUNtail(bpi, j);                                     \
				if((*atomcmp)(next, nil) != 0) {                                  \
					if((*atomcmp)(curval, nil) == 0)                              \
						curval = next;                                            \
					else                                                          \
						curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next; \
				}                                                                 \
			}                                                                     \
			if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)           \
				goto finish;                                                      \
			if((*atomcmp)(curval, nil) == 0)                                      \
				has_nils = true;                                                  \
		}                                                                         \
	} while(0);

#define ANALYTICAL_MIN_MAX_OTHERS_CALC_ROWS(SIGN_OP, REAL) \
	do {                                                   \
		if (p) {                                           \
			pnp = np = (bit*)Tloc(p, 0);                   \
			nend = np + cnt;                               \
			for(; np<nend; np++) {                         \
				if (*np) {                                 \
					i += (np - pnp);                       \
					REAL(SIGN_OP)                          \
					pnp = np;                              \
				}                                          \
			}                                              \
			i += (np - pnp);                               \
			REAL(SIGN_OP)                                  \
		} else {                                           \
			i += cnt;                                      \
			REAL(SIGN_OP)                                  \
		}                                                  \
	} while(0);

#define ANALYTICAL_MIN_MAX_OTHERS_IMP_RANGE_ALL(SIGN_OP)                      \
	do {                                                                      \
		l = j;                                                                \
		curval = BUNtail(bpi, j);                                             \
		j++;                                                                  \
		for (;j < i; j++) {                                                   \
			void *next = BUNtail(bpi, j);                                     \
			if((*atomcmp)(next, nil) != 0) {                                  \
				if((*atomcmp)(curval, nil) == 0)                              \
					curval = next;                                            \
				else                                                          \
					curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next; \
			}                                                                 \
		}                                                                     \
		j = l;                                                                \
		for (;j < i; j++) {                                                   \
			if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)       \
				goto finish;                                                  \
		}                                                                     \
		if((*atomcmp)(curval, nil) == 0)                                      \
			has_nils = true;                                                  \
	} while(0);

#define ANALYTICAL_MIN_MAX_OTHERS_IMP_RANGE_PART(SIGN_OP) \
	do {                                                  \
		bit *nl = lp, *ns, *ne;                           \
		BUN rstart, rend, parcel;                         \
		for(; lp<lend;lp++) {                             \
			rstart = start;                               \
			for(ns=lp; ns>nl; ns--) {                     \
				if(*ns) {                                 \
					if(rstart == 0)                       \
						break;                            \
					rstart--;                             \
				}                                         \
			}                                             \
			rend = end;                                   \
			for(ne=lp+1; ne<lend; ne++) {                 \
				if(*ne) {                                 \
					if(rend == 0)                         \
						break;                            \
					rend--;                               \
				}                                         \
			}                                             \
			parcel = (ne - ns);                           \
			j = i + (ns - nl);                            \
			l = j + parcel;                               \
			curval = BUNtail(bpi, j++);                   \
			for (;j < l; j++) {                           \
				void *next = BUNtail(bpi, j);             \
				if((*atomcmp)(next, nil) != 0) {          \
					if((*atomcmp)(curval, nil) == 0)      \
						curval = next;                    \
					else                                  \
						curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next; \
				}                                         \
			}                                             \
			if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED) \
				goto finish;                              \
		}                                                 \
	} while(0);

#define ANALYTICAL_MIN_MAX_OTHERS_CALC_RANGE(SIGN_OP, REAL) \
	do {                                                    \
		if (p) {                                            \
			pnp = np = (bit*)Tloc(p, 0);                    \
			lend = lp = o ? (bit*)Tloc(o, 0) : np;          \
			nend = np + cnt;                                \
			for(; np<nend; np++) {                          \
				if (*np) {                                  \
					ncnt = (np - pnp);                      \
					lend += ncnt;                           \
					REAL##_PART(SIGN_OP)                    \
					i += ncnt;                              \
					pnp = np;                               \
				}                                           \
			}                                               \
			ncnt += (np - pnp);                             \
			REAL##_PART(SIGN_OP)                            \
		} else if (o) {                                     \
			lend = lp = (bit*)Tloc(o, 0);                   \
			lend += cnt;                                    \
			REAL##_PART(SIGN_OP)                            \
		} else {                                            \
			i += cnt;                                       \
			REAL##_ALL(SIGN_OP)                             \
		}                                                   \
	} while(0);

#define ANALYTICAL_MIN_MAX_BRANCHES(OP, IMP, SIGN_OP, FRAME) \
	switch(tpe) { \
		case TYPE_bit: \
			ANALYTICAL_MIN_MAX_CALC##FRAME(bit, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
			break; \
		case TYPE_bte: \
			ANALYTICAL_MIN_MAX_CALC##FRAME(bte, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
			break; \
		case TYPE_sht: \
			ANALYTICAL_MIN_MAX_CALC##FRAME(sht, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
			break; \
		case TYPE_int: \
			ANALYTICAL_MIN_MAX_CALC##FRAME(int, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
			break; \
		case TYPE_lng: \
			ANALYTICAL_MIN_MAX_CALC##FRAME(lng, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
			break; \
		ANALYTICAL_MIN_MAX_LIMIT(FRAME, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
		case TYPE_flt: \
			ANALYTICAL_MIN_MAX_CALC##FRAME(flt, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
			break; \
		case TYPE_dbl: \
			ANALYTICAL_MIN_MAX_CALC##FRAME(dbl, IMP, ANALYTICAL_MIN_MAX_IMP##FRAME) \
			break; \
		default: { \
			BATiter bpi = bat_iterator(b); \
			void *restrict curval = BUNtail(bpi, 0); \
			nil = ATOMnilptr(tpe); \
			atomcmp = ATOMcompare(tpe); \
			ANALYTICAL_MIN_MAX_OTHERS_CALC##FRAME(SIGN_OP, ANALYTICAL_MIN_MAX_OTHERS_IMP##FRAME) \
		}\
	}

#define ANALYTICAL_MIN_MAX(OP, IMP, SIGN_OP) \
gdk_return \
GDKanalytical##OP(BAT *r, BAT *b, BAT *p, BAT *o, int tpe, int unit, BUN start, BUN end) \
{ \
	int (*atomcmp)(const void *, const void *); \
	const void* restrict nil; \
	bool has_nils = false; \
	BUN i = 0, j = 0, l = 0, k = 0, m = 0, ncnt, cnt = BATcount(b); \
	bit *np, *pnp, *nend, *lp, *lend; \
	gdk_return gdk_res = GDK_SUCCEED; \
 \
	assert(unit >= 0 && unit <= 2); \
 \
	if(unit == 0) { \
		ANALYTICAL_MIN_MAX_BRANCHES(OP, IMP, SIGN_OP, _ROWS) \
	} else { \
		ANALYTICAL_MIN_MAX_BRANCHES(OP, IMP, SIGN_OP, _RANGE) \
	} \
finish: \
	BATsetcount(r, cnt); \
	r->tnonil = !has_nils; \
	r->tnil = has_nils; \
	return gdk_res; \
}

ANALYTICAL_MIN_MAX(min, MIN, >)
ANALYTICAL_MIN_MAX(max, MAX, <)

#undef ANALYTICAL_MIN_MAX_CALC_ROWS
#undef ANALYTICAL_MIN_MAX_CALC_RANGE
#undef ANALYTICAL_MIN_MAX_IMP_ROWS
#undef ANALYTICAL_MIN_MAX_IMP_RANGE_PART
#undef ANALYTICAL_MIN_MAX_IMP_RANGE_ALL
#undef ANALYTICAL_MIN_MAX_OTHERS_CALC_ROWS
#undef ANALYTICAL_MIN_MAX_OTHERS_CALC_RANGE
#undef ANALYTICAL_MIN_MAX_OTHERS_IMP_ROWS
#undef ANALYTICAL_MIN_MAX_OTHERS_IMP_RANGE_PART
#undef ANALYTICAL_MIN_MAX_OTHERS_IMP_RANGE_ALL
#undef ANALYTICAL_MIN_MAX
#undef ANALYTICAL_MIN_MAX_BRANCHES
#undef ANALYTICAL_MIN_MAX_LIMIT

#define ANALYTICAL_COUNT_IGNORE_NILS_IMP_ROWS       \
	do {                                            \
		lng *rs = rb, *fs, *fe;                     \
		for(; rb<rp;rb++) {                         \
			fs = (rb > rs+start) ? rb - start : rs; \
			fe = (rb+end < rp) ? rb + end + 1 : rp; \
			*rb = (fe - fs);                        \
		}                                           \
	} while(0);

#define ANALYTICAL_COUNT_IGNORE_NILS_CALC_ROWS            \
	do {                                                  \
		lng *rp, *rb, curval = 0;                         \
		rb = rp = (lng*)Tloc(r, 0);                       \
		if (p) {                                          \
			np = pnp = (bit*)Tloc(p, 0);                  \
			nend = np + cnt;                              \
			for(; np < nend; np++) {                      \
				if (*np) {                                \
					curval = np - pnp;                    \
					rp += curval;                         \
					ANALYTICAL_COUNT_IGNORE_NILS_IMP_ROWS \
					pnp = np;                             \
				}                                         \
			}                                             \
			curval = np - pnp;                            \
			rp += curval;                                 \
			ANALYTICAL_COUNT_IGNORE_NILS_IMP_ROWS         \
		} else {                                          \
			curval = cnt;                                 \
			rp += curval;                                 \
			ANALYTICAL_COUNT_IGNORE_NILS_IMP_ROWS         \
		}                                                 \
	} while(0);

#define ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_ALL \
	do {                                           \
		for (;rb < rp; rb++)                       \
			*rb = curval;                          \
	} while(0);

#define ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_PART \
	do {                                  \
		bit *nl = lp, *ns, *ne;           \
		BUN rstart, rend;                 \
		for(; lp<lend;lp++) {             \
			rstart = start;               \
			for(ns=lp; ns>nl; ns--) {     \
				if(*ns) {                 \
					if(rstart == 0)       \
						break;            \
					rstart--;             \
				}                         \
			}                             \
			rend = end;                   \
			for(ne=lp+1; ne<lend; ne++) { \
				if(*ne) {                 \
					if(rend == 0)         \
						break;            \
					rend--;               \
				}                         \
			}                             \
			curval = (lng)(ne - ns);      \
			*rb = curval;                 \
			rb++;                         \
		}                                 \
	} while(0);

#define ANALYTICAL_COUNT_IGNORE_NILS_CALC_RANGE    \
	do {                                           \
		lng *rb, *rp, curval = 0;                  \
		rp = rb = (lng*)Tloc(r, 0);                \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_PART \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_PART \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_PART \
		} else {                                   \
			curval = cnt;                          \
			rp += curval;                          \
			ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_ALL \
		}                                          \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP_ROWS(TPE)\
	do {                                                \
		TPE *bl = pbp, *bs, *be;                        \
		for(; pbp<bp;pbp++) {                           \
			bs = (pbp > bl+start) ? pbp - start : bl;   \
			be = (pbp+end < bp) ? pbp + end + 1 : bp;   \
			for(; bs<be; bs++)                          \
				curval += !is_##TPE##_nil(*bs);         \
			*rb = curval;                               \
			rb++;                                       \
			curval = 0;                                 \
		}                                               \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC_ROWS(TPE, IMP) \
	do {                                                 \
		TPE *pbp, *bp = (TPE*)Tloc(b, 0);                \
		lng *rp, *rb, curval = 0;                        \
		rb = rp = (lng*)Tloc(r, 0);                      \
		pbp = bp;                                        \
		if (p) {                                         \
			pnp = np = (bit*)Tloc(p, 0);                 \
			nend = np + cnt;                             \
			for(; np<nend; np++) {                       \
				if (*np) {                               \
					ncnt = np - pnp;                     \
					bp += ncnt;                          \
					rp += ncnt;                          \
					IMP(TPE)                             \
					pnp = np;                            \
					pbp = bp;                            \
				}                                        \
			}                                            \
			ncnt = np - pnp;                             \
			bp += ncnt;                                  \
			rp += ncnt;                                  \
			IMP(TPE)                                     \
		} else {                                         \
			bp += cnt;                                   \
			rp += cnt;                                   \
			IMP(TPE)                                     \
		}                                                \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP_RANGE_ALL(TPE) \
	do {                                                   \
		for (;pbp < bp; pbp++)                             \
			curval += !is_##TPE##_nil(*pbp);               \
		for (;rb < rp; rb++)                               \
			*rb = curval;                                  \
		curval = 0;                                        \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP_RANGE_PART(TPE) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		TPE *bs, *be;                           \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			bs = bp + (ns - nl);                \
			be = bs + parcel;                   \
			for(; bs<be; bs++)                  \
				curval += !is_##TPE##_nil(*bs); \
			*rb = curval;                       \
			curval = 0;                         \
			rb++;                               \
		}                                       \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC_RANGE(TPE, IMP) \
	do {                                           \
		TPE *bp = (TPE*)Tloc(b, 0);                \
		lng *rb, *rp, curval = 0;                  \
		rp = rb = (lng*)Tloc(r, 0);                \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					IMP##_PART(TPE)                \
					bp += ncnt;                    \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			IMP##_PART(TPE)                        \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			IMP##_PART(TPE)                        \
		} else {                                   \
			TPE *pbp = bp;                         \
			rp += cnt;                             \
			bp += cnt;                             \
			IMP##_ALL(TPE)                         \
		}                                          \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_STR_IMP_ROWS(TPE_CAST, OFFSET)            \
	do {                                                                  \
		m = k;                                                            \
		for(;k<i;k++) {                                                   \
			j = (k > m+start) ? k - start : m;                            \
			l = (k+end < i) ? k + end + 1 : i;                            \
			for(; j<l; j++)                                               \
				curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200'; \
			*rb = curval;                                                 \
			rb++;                                                         \
			curval = 0;                                                   \
		}                                                                 \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_STR_CALC_ROWS(TPE_CAST, OFFSET, IMP) \
	do {                                      \
		const void *restrict bp = Tloc(b, 0); \
		lng *rp, *rb, curval = 0;             \
		rb = rp = (lng*)Tloc(r, 0);           \
		if (p) {                              \
			pnp = np = (bit*)Tloc(p, 0);      \
			nend = np + cnt;                  \
			for(; np<nend; np++) {            \
				if (*np) {                    \
				    ncnt = (np - pnp);        \
					rp += ncnt;               \
					i += ncnt;                \
					IMP(TPE_CAST, OFFSET)     \
					pnp = np;                 \
				}                             \
			}                                 \
			ncnt = (np - pnp);                \
			rp += ncnt;                       \
			i += ncnt;                        \
			IMP(TPE_CAST, OFFSET)             \
		} else {                              \
			rp += cnt;                        \
			i += cnt;                         \
			IMP(TPE_CAST, OFFSET)             \
		}                                     \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_STR_IMP_RANGE_ALL(TPE_CAST, OFFSET)   \
	do {                                                              \
		for(;j<i;j++)                                                 \
			curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200'; \
		for (;rb < rp; rb++)                                          \
			*rb = curval;                                             \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_STR_IMP_RANGE_PART(TPE_CAST, OFFSET) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			j = i + (ns - nl);                  \
			l = j + parcel;                     \
			for(; j<l; j++)                     \
				curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200';\
			*rb = curval;                       \
			curval = 0;                         \
			rb++;                               \
		}                                       \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_STR_CALC_RANGE(TPE_CAST, OFFSET, IMP) \
	do {                                           \
		const void *restrict bp = Tloc(b, 0);      \
		lng *rp, *rb, curval = 0;                  \
		rb = rp = (lng*)Tloc(r, 0);                \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					IMP##_PART(TPE_CAST, OFFSET)   \
					i += ncnt;                     \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			IMP##_PART(TPE_CAST, OFFSET)           \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			IMP##_PART(TPE_CAST, OFFSET)           \
		} else {                                   \
			rp += cnt;                             \
			i += cnt;                              \
			IMP##_ALL(TPE_CAST, OFFSET)            \
		}                                          \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_ROWS                         \
	do {                                                                    \
		m = k;                                                              \
		for(;k<i;k++) {                                                     \
			j = (k > m+start) ? k - start : m;                              \
			l = (k+end < i) ? k + end + 1 : i;                              \
			for(; j<l; j++)                                                 \
				curval += (*cmp)(nil, base + ((const var_t *) bp)[j]) != 0; \
			*rb = curval;                                                   \
			rb++;                                                           \
			curval = 0;                                                     \
		}                                                                   \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_ROWS    \
	do {                                                \
		m = k;                                          \
		for(;k<i;k++) {                                 \
			j = (k > m+start) ? k - start : m;          \
			l = (k+end < i) ? k + end + 1 : i;          \
			for(; j<l; j++)                             \
				curval += (*cmp)(Tloc(b, j), nil) != 0; \
			*rb = curval;                               \
			rb++;                                       \
			curval = 0;                                 \
		}                                               \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES_CALC_ROWS(IMP_VARSIZED, IMP_FIXEDSIZE) \
	do {                                                           \
		lng *rp, *rb, curval = 0;                                  \
		rb = rp = (lng*)Tloc(r, 0);                                \
		if (b->tvarsized) {                                        \
			const char *restrict base = b->tvheap->base;           \
			const void *restrict bp = Tloc(b, 0);                  \
			if (p) {                                               \
				pnp = np = (bit*)Tloc(p, 0);                       \
				nend = np + cnt;                                   \
				for(; np < nend; np++) {                           \
					if (*np) {                                     \
						ncnt = (np - pnp);                         \
						rp += ncnt;                                \
						i += ncnt;                                 \
						IMP_VARSIZED                               \
						pnp = np;                                  \
					}                                              \
				}                                                  \
				ncnt = (np - pnp);                                 \
				rp += ncnt;                                        \
				i += ncnt;                                         \
				IMP_VARSIZED                                       \
			} else {                                               \
				rp += cnt;                                         \
				i += cnt;                                          \
				IMP_VARSIZED                                       \
			}                                                      \
		} else {                                                   \
			if (p) {                                               \
				pnp = np = (bit*)Tloc(p, 0);                       \
				nend = np + cnt;                                   \
				for(; np < nend; np++) {                           \
					if (*np) {                                     \
						ncnt = (np - pnp);                         \
						rp += ncnt;                                \
						i += ncnt;                                 \
						IMP_FIXEDSIZE                              \
						pnp = np;                                  \
					}                                              \
				}                                                  \
				ncnt = (np - pnp);                                 \
				rp += ncnt;                                        \
				i += ncnt;                                         \
				IMP_FIXEDSIZE                                      \
			} else {                                               \
				rp += cnt;                                         \
				i += cnt;                                          \
				IMP_FIXEDSIZE                                      \
			}                                                      \
		}                                                          \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_RANGE_ALL                \
	do {                                                                \
		for(; j<i; j++)                                                 \
			curval += (*cmp)(nil, base + ((const var_t *) bp)[j]) != 0; \
		for (;rb < rp; rb++)                                            \
			*rb = curval;                                               \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_RANGE_PART \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			j = i + (ns - nl);                  \
			l = j + parcel;                     \
			for(; j<l; j++)                     \
				curval += (*cmp)(nil, base + ((const var_t *) bp)[j]) != 0; \
			*rb = curval;                       \
			curval = 0;                         \
			rb++;                               \
		}                                       \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_RANGE_ALL \
	do {                                                  \
		for(; j<i; j++)                                   \
			curval += (*cmp)(Tloc(b, j), nil) != 0;       \
		for (;rb < rp; rb++)                              \
			*rb = curval;                                 \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_RANGE_PART \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			j = i + (ns - nl);                  \
			l = j + parcel;                     \
			for(; j<l; j++)                     \
				curval += (*cmp)(Tloc(b, j), nil) != 0; \
			*rb = curval;                       \
			curval = 0;                         \
			rb++;                               \
		}                                       \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES_CALC_RANGE(IMP_VARSIZED, IMP_FIXEDSIZE) \
	do {                                                           \
		lng *rp, *rb, curval = 0;                                  \
		rb = rp = (lng*)Tloc(r, 0);                                \
		if (b->tvarsized) {                                        \
			const char *restrict base = b->tvheap->base;           \
			const void *restrict bp = Tloc(b, 0);                  \
			if (p) {                                               \
				pnp = np = (bit*)Tloc(p, 0);                       \
				lend = lp = o ? (bit*)Tloc(o, 0) : np;             \
				nend = np + cnt;                                   \
				for(; np<nend; np++) {                             \
					if (*np) {                                     \
						ncnt = (np - pnp);                         \
						lend += ncnt;                              \
						IMP_VARSIZED##_PART                        \
						i += ncnt;                                 \
						pnp = np;                                  \
					}                                              \
				}                                                  \
				ncnt = (np - pnp);                                 \
				lend += ncnt;                                      \
				IMP_VARSIZED##_PART                                \
			} else {                                               \
				rp += cnt;                                         \
				i += cnt;                                          \
				IMP_VARSIZED##_ALL                                 \
			}                                                      \
		} else {                                                   \
			if (p) {                                               \
				pnp = np = (bit*)Tloc(p, 0);                       \
				lend = lp = o ? (bit*)Tloc(o, 0) : np;             \
				nend = np + cnt;                                   \
				for(; np<nend; np++) {                             \
					if (*np) {                                     \
						ncnt = (np - pnp);                         \
						lend += ncnt;                              \
						IMP_FIXEDSIZE##_PART                       \
						i += ncnt;                                 \
						pnp = np;                                  \
					}                                              \
				}                                                  \
				ncnt = (np - pnp);                                 \
				lend += ncnt;                                      \
				IMP_FIXEDSIZE##_PART                               \
			} else {                                               \
				rp += cnt;                                         \
				i += cnt;                                          \
				IMP_FIXEDSIZE##_ALL                                \
			}                                                      \
		}                                                          \
	} while(0);

#ifdef HAVE_HGE
#define ANALYTICAL_COUNT_LIMIT(FRAME) \
	case TYPE_hge: \
		ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(hge, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
		break;
#else
#define ANALYTICAL_COUNT_LIMIT(FRAME)
#endif

#if SIZEOF_VAR_T != SIZEOF_INT
#define ANALYTICAL_COUNT_STR_LIMIT(FRAME) \
	case 4: \
		ANALYTICAL_COUNT_NO_NIL_STR_CALC##FRAME(const unsigned int *, [j], ANALYTICAL_COUNT_NO_NIL_STR_IMP##FRAME) \
		break;
#else
#define ANALYTICAL_COUNT_STR_LIMIT(FRAME)
#endif

#define ANALYTICAL_COUNT_BRANCHES(FRAME) \
	switch (tpe) { \
		case TYPE_bit: \
			ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(bit, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
			break; \
		case TYPE_bte: \
			ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(bte, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
			break; \
		case TYPE_sht: \
			ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(sht, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
			break; \
		case TYPE_int: \
			ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(int, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
			break; \
		case TYPE_lng: \
			ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(lng, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
			break; \
		ANALYTICAL_COUNT_LIMIT(FRAME) \
		case TYPE_flt: \
			ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(flt, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
			break; \
		case TYPE_dbl: \
			ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC##FRAME(dbl, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP##FRAME) \
			break; \
		case TYPE_str: { \
			const char *restrict base = b->tvheap->base; \
			switch (b->twidth) { \
				case 1: \
					ANALYTICAL_COUNT_NO_NIL_STR_CALC##FRAME(const unsigned char *, [j] + GDK_VAROFFSET, ANALYTICAL_COUNT_NO_NIL_STR_IMP##FRAME) \
					break; \
				case 2: \
					ANALYTICAL_COUNT_NO_NIL_STR_CALC##FRAME(const unsigned short *, [j] + GDK_VAROFFSET, ANALYTICAL_COUNT_NO_NIL_STR_IMP##FRAME) \
					break; \
				ANALYTICAL_COUNT_STR_LIMIT(FRAME) \
				default: \
					ANALYTICAL_COUNT_NO_NIL_STR_CALC##FRAME(const var_t *, [j], ANALYTICAL_COUNT_NO_NIL_STR_IMP##FRAME) \
					break; \
			} \
			break; \
		} \
		default: { \
			const void *restrict nil = ATOMnilptr(tpe); \
			int (*cmp)(const void *, const void *) = ATOMcompare(tpe); \
			ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES_CALC##FRAME(ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES##FRAME, ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES##FRAME) \
		} \
	}

gdk_return
GDKanalyticalcount(BAT *r, BAT *b, BAT *p, BAT *o, const bit *ignore_nils, int tpe, int unit, BUN start, BUN end)
{
	BUN i = 0, j = 0, k = 0, l = 0, m = 0, ncnt, cnt = BATcount(b);
	bit *pnp, *np, *nend, *lp, *lend;
	gdk_return gdk_res = GDK_SUCCEED;

	assert(ignore_nils);
	assert(unit >= 0 && unit <= 2);

	if(!*ignore_nils || b->T.nonil) {
		if(unit == 0) {
			ANALYTICAL_COUNT_IGNORE_NILS_CALC_ROWS
		} else {
			ANALYTICAL_COUNT_IGNORE_NILS_CALC_RANGE
		}
	} else if(unit == 0) {
		ANALYTICAL_COUNT_BRANCHES(_ROWS)
	} else {
		ANALYTICAL_COUNT_BRANCHES(_RANGE)
	}
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return gdk_res;
}

#undef ANALYTICAL_COUNT_IGNORE_NILS_CALC_ROWS
#undef ANALYTICAL_COUNT_IGNORE_NILS_IMP_ROWS
#undef ANALYTICAL_COUNT_IGNORE_NILS_CALC_RANGE
#undef ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_ALL
#undef ANALYTICAL_COUNT_IGNORE_NILS_IMP_RANGE_PART
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC_ROWS
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP_ROWS
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_CALC_RANGE
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP_RANGE_PART
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP_RANGE_ALL
#undef ANALYTICAL_COUNT_NO_NIL_STR_CALC_ROWS
#undef ANALYTICAL_COUNT_NO_NIL_STR_IMP_ROWS
#undef ANALYTICAL_COUNT_NO_NIL_STR_CALC_RANGE
#undef ANALYTICAL_COUNT_NO_NIL_STR_IMP_RANGE_PART
#undef ANALYTICAL_COUNT_NO_NIL_STR_IMP_RANGE_ALL
#undef ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES_CALC_ROWS
#undef ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_ROWS
#undef ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_ROWS
#undef ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES_CALC_RANGE
#undef ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_RANGE_PART
#undef ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_RANGE_PART
#undef ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_RANGE_ALL
#undef ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_RANGE_ALL
#undef ANALYTICAL_COUNT_LIMIT
#undef ANALYTICAL_COUNT_STR_LIMIT
#undef ANALYTICAL_COUNT_BRANCHES

#define ANALYTICAL_SUM_IMP_ROWS(TPE1, TPE2)           \
	do {                                              \
		TPE1 *bl = pbp, *bs, *be, v;                  \
		for(; pbp<bp;pbp++) {                         \
			bs = (pbp > bl+start) ? pbp - start : bl; \
			be = (pbp+end < bp) ? pbp + end + 1 : bp; \
			for(; bs<be; bs++) {                      \
				v = *bs;                              \
				if (!is_##TPE1##_nil(v)) {            \
					if(is_##TPE2##_nil(curval))       \
						curval = (TPE2) v;            \
					else                              \
						ADD_WITH_CHECK(TPE1, v, TPE2, curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}                                     \
			}                                         \
			*rb = curval;                             \
			rb++;                                     \
			if(is_##TPE2##_nil(curval))               \
				has_nils = true;                      \
			else                                      \
				curval = TPE2##_nil;                  \
		}                                             \
	} while(0);

#define ANALYTICAL_SUM_FP_IMP_ROWS(TPE1, TPE2)           \
	do {                                                 \
		TPE1 *bl = pbp, *bs, *be;                        \
		(void) curval;                                   \
		for(; pbp<bp; pbp++) {                           \
			bs = (pbp > bl+start) ? pbp - start : bl;    \
			be = (pbp+end < bp) ? pbp + end + 1 : bp;    \
			if(dofsum(bs, 0, 0, (be - bs), rb, 1, TYPE_##TPE1, TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true, false, true, \
					  "GDKanalyticalsum") == BUN_NONE) { \
				goto bailout;                            \
			}                                            \
			if(is_##TPE2##_nil(*rb))                     \
				has_nils = true;                         \
			rb++;                                        \
		}                                                \
	} while(0);

#define ANALYTICAL_SUM_CALC_ROWS(TPE1, TPE2, IMP)  \
	do {                                           \
		TPE1 *pbp, *bp;                            \
		TPE2 *rp, *rb, curval = TPE2##_nil;        \
		pbp = bp = (TPE1*)Tloc(b, 0);              \
		rb = rp = (TPE2*)Tloc(r, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					bp += ncnt;                    \
					rp += ncnt;                    \
					IMP(TPE1, TPE2)                \
					pnp = np;                      \
					pbp = bp;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2)                        \
		} else {                                   \
			ncnt = cnt;                            \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2)                        \
		}                                          \
		goto finish;                               \
	} while(0);

#define ANALYTICAL_SUM_IMP_RANGE_PART(TPE1, TPE2) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		TPE1 *bs, *be, v;                       \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			bs = bp + (ns - nl);                \
			be = bs + parcel;                   \
			for(; bs<be; bs++) {                \
				v = *bs;                        \
				if (!is_##TPE1##_nil(v)) {      \
					if(is_##TPE2##_nil(curval)) \
						curval = (TPE2) v;      \
					else                        \
						ADD_WITH_CHECK(TPE1, v, TPE2, curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}                               \
			}                                   \
			*rb = curval;                       \
			rb++;                               \
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0);

#define ANALYTICAL_SUM_IMP_RANGE_ALL(TPE1, TPE2) \
	do {                                      \
		TPE1 v;                               \
		for(; pbp<bp; pbp++) {                \
			v = *pbp;                         \
			if (!is_##TPE1##_nil(v)) {        \
				if(is_##TPE2##_nil(curval))   \
					curval = (TPE2) v;        \
				else                          \
					ADD_WITH_CHECK(TPE1, v, TPE2, curval, TPE2, curval, GDK_##TPE2##_max, goto calc_overflow); \
			}                                 \
		}                                     \
		for (;rb < rp; rb++)                  \
			*rb = curval;                     \
		if(is_##TPE2##_nil(curval))           \
			has_nils = true;                  \
	} while(0);                               \

#define ANALYTICAL_SUM_FP_IMP_RANGE_PART(TPE1, TPE2) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		TPE1 *bs;                               \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			bs = bp + (ns - nl);                \
			if(dofsum(bs, 0, 0, parcel, &curval, 1, TYPE_##TPE1, TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true, false, \
					  true, "GDKanalyticalsum") == BUN_NONE) { \
				goto bailout;                   \
			}                                   \
			*rb = curval;                       \
			rb++;                               \
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0);

#define ANALYTICAL_SUM_FP_IMP_RANGE_ALL(TPE1, TPE2)  \
	do {                                             \
		if(dofsum(pbp, 0, 0, cnt, &curval, 1, TYPE_##TPE1, TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true, false, true, \
				  "GDKanalyticalsum") == BUN_NONE) { \
			goto bailout;                            \
		}                                            \
		for (;rb < rp; rb++)                         \
			*rb = curval;                            \
		if(is_##TPE2##_nil(curval))                  \
			has_nils = true;                         \
	} while(0);

#define ANALYTICAL_SUM_CALC_RANGE(TPE1, TPE2, IMP) \
	do {                                           \
		TPE1 *bp;                                  \
		TPE2 *rb, *rp, curval = TPE2##_nil;        \
		bp = (TPE1*)Tloc(b, 0);                    \
		rp = rb = (TPE2*)Tloc(r, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					IMP##_PART(TPE1, TPE2)         \
					bp += ncnt;                    \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			IMP##_PART(TPE1, TPE2)                 \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			IMP##_PART(TPE1, TPE2)                 \
		} else {                                   \
			TPE1 *pbp = bp;                        \
			rp += cnt;                             \
			bp += cnt;                             \
			IMP##_ALL(TPE1, TPE2)                  \
		}                                          \
		goto finish;                               \
	} while(0);

#ifdef HAVE_HGE
#define ANALYTICAL_SUM_LIMIT(FRAME) \
	case TYPE_hge: { \
		switch (tp1) { \
			case TYPE_bte: \
				ANALYTICAL_SUM_CALC##FRAME(bte, hge, ANALYTICAL_SUM_IMP##FRAME); \
				break; \
			case TYPE_sht: \
				ANALYTICAL_SUM_CALC##FRAME(sht, hge, ANALYTICAL_SUM_IMP##FRAME); \
				break; \
			case TYPE_int: \
				ANALYTICAL_SUM_CALC##FRAME(int, hge, ANALYTICAL_SUM_IMP##FRAME); \
				break; \
			case TYPE_lng: \
				ANALYTICAL_SUM_CALC##FRAME(lng, hge, ANALYTICAL_SUM_IMP##FRAME); \
				break; \
			case TYPE_hge: \
				ANALYTICAL_SUM_CALC##FRAME(hge, hge, ANALYTICAL_SUM_IMP##FRAME); \
				break; \
			default: \
				goto nosupport; \
		} \
		break; \
	}
#else
#define ANALYTICAL_SUM_LIMIT(FRAME)
#endif

#define ANALYTICAL_SUM_BRANCHES(FRAME) \
	switch (tp2) { \
		case TYPE_bte: { \
			switch (tp1) { \
				case TYPE_bte: \
					ANALYTICAL_SUM_CALC##FRAME(bte, bte, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				default: \
					goto nosupport; \
			} \
			break; \
		} \
		case TYPE_sht: { \
			switch (tp1) { \
				case TYPE_bte: \
					ANALYTICAL_SUM_CALC##FRAME(bte, sht, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				case TYPE_sht: \
					ANALYTICAL_SUM_CALC##FRAME(sht, sht, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				default: \
					goto nosupport; \
			} \
			break; \
		} \
		case TYPE_int: { \
			switch (tp1) { \
				case TYPE_bte: \
					ANALYTICAL_SUM_CALC##FRAME(bte, int, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				case TYPE_sht: \
					ANALYTICAL_SUM_CALC##FRAME(sht, int, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				case TYPE_int: \
					ANALYTICAL_SUM_CALC##FRAME(int, int, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				default: \
					goto nosupport; \
			} \
			break; \
		} \
		case TYPE_lng: { \
			switch (tp1) { \
				case TYPE_bte: \
					ANALYTICAL_SUM_CALC##FRAME(bte, lng, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				case TYPE_sht: \
					ANALYTICAL_SUM_CALC##FRAME(sht, lng, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				case TYPE_int: \
					ANALYTICAL_SUM_CALC##FRAME(int, lng, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				case TYPE_lng: \
					ANALYTICAL_SUM_CALC##FRAME(lng, lng, ANALYTICAL_SUM_IMP##FRAME); \
					break; \
				default: \
					goto nosupport; \
			} \
			break; \
		} \
		ANALYTICAL_SUM_LIMIT(FRAME) \
		case TYPE_flt: { \
			switch (tp1) { \
				case TYPE_flt: \
					ANALYTICAL_SUM_CALC##FRAME(flt, flt, ANALYTICAL_SUM_FP_IMP##FRAME); \
					break; \
				default: \
					goto nosupport; \
					break; \
			} \
		} \
		case TYPE_dbl: { \
			switch (tp1) { \
				case TYPE_flt: \
					ANALYTICAL_SUM_CALC##FRAME(flt, dbl, ANALYTICAL_SUM_FP_IMP##FRAME); \
					break; \
				case TYPE_dbl: \
					ANALYTICAL_SUM_CALC##FRAME(dbl, dbl, ANALYTICAL_SUM_FP_IMP##FRAME); \
					break; \
				default: \
					goto nosupport; \
					break; \
			} \
		} \
		default: \
			goto nosupport; \
	}

gdk_return
GDKanalyticalsum(BAT *r, BAT *b, BAT *p, BAT *o, int tp1, int tp2, int unit, BUN start, BUN end)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0;
	bit *np, *pnp, *nend, *lp, *lend;
	int abort_on_error = 1;

	assert(unit >= 0 && unit <= 2);

	if(unit == 0) {
		ANALYTICAL_SUM_BRANCHES(_ROWS)
	} else {
		ANALYTICAL_SUM_BRANCHES(_RANGE)
	}
bailout:
	GDKerror("error while calculating floating-point sum\n");
	return GDK_FAIL;
nosupport:
	GDKerror("sum: type combination (sum(%s)->%s) not supported.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_SUM_IMP_ROWS
#undef ANALYTICAL_SUM_IMP_RANGE_PART
#undef ANALYTICAL_SUM_IMP_RANGE_ALL
#undef ANALYTICAL_SUM_FP_IMP_ROWS
#undef ANALYTICAL_SUM_FP_IMP_RANGE_PART
#undef ANALYTICAL_SUM_FP_IMP_RANGE_ALL
#undef ANALYTICAL_SUM_CALC_ROWS
#undef ANALYTICAL_SUM_CALC_RANGE
#undef ANALYTICAL_SUM_BRANCHES
#undef ANALYTICAL_SUM_LIMIT

#define ANALYTICAL_PROD_IMP_NUM_ROWS(TPE1, TPE2, TPE3) \
	do {                                              \
		TPE1 *bl = pbp, *bs, *be;                     \
		for(; pbp<bp;pbp++) {                         \
			bs = (pbp > bl+start) ? pbp - start : bl; \
			be = (pbp+end < bp) ? pbp + end + 1 : bp; \
			for(; bs<be; bs++) {                      \
				v = *bs;                              \
				if (!is_##TPE1##_nil(v)) {            \
					if(is_##TPE2##_nil(curval))       \
						curval = (TPE2) v;            \
					else                              \
						MUL4_WITH_CHECK(TPE1, v, TPE2, curval, TPE2, curval, GDK_##TPE2##_max, TPE3, \
										goto calc_overflow); \
				}                                     \
			}                                         \
			*rb = curval;                             \
			rb++;                                     \
			if(is_##TPE2##_nil(curval))               \
				has_nils = true;                      \
			else                                      \
				curval = TPE2##_nil;                  \
		}                                             \
	} while(0);

#define ANALYTICAL_PROD_CALC_NUM_ROWS(TPE1, TPE2, TPE3, IMP) \
	do {                                           \
		TPE1 *pbp, *bp, v;                         \
		TPE2 *rp, *rb, curval = TPE2##_nil;        \
		pbp = bp = (TPE1*)Tloc(b, 0);              \
		rb = rp = (TPE2*)Tloc(r, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = np - pnp;               \
					bp += ncnt;                    \
					rp += ncnt;                    \
					IMP(TPE1, TPE2, TPE3)          \
					pnp = np;                      \
					pbp = bp;                      \
				}                                  \
			}                                      \
			ncnt = np - pnp;                       \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2, TPE3)                  \
		} else {                                   \
			ncnt = cnt;                            \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2, TPE3)                  \
		}                                          \
		goto finish;                               \
	} while(0);

#define ANALYTICAL_PROD_IMP_NUM_LIMIT_ROWS(TPE1, TPE2, REAL_IMP) \
	do {                                                        \
		TPE1 *bl = pbp, *bs, *be;                               \
		for(; pbp<bp;pbp++) {                                   \
			bs = (pbp > bl+start) ? pbp - start : bl;           \
			be = (pbp+end < bp) ? pbp + end + 1 : bp;           \
			for(; bs<be; bs++) {                                \
				v = *bs;                                        \
				if (!is_##TPE1##_nil(v)) {                      \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) v;                      \
					else                                        \
						REAL_IMP(TPE1, v, TPE2, curval, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}                                               \
			}                                                   \
			*rb = curval;                                       \
			rb++;                                               \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
			else                                                \
				curval = TPE2##_nil;                            \
		}                                                       \
	} while(0);

#define ANALYTICAL_PROD_CALC_NUM_LIMIT_ROWS(TPE1, TPE2, IMP, REAL_IMP)\
	do {                                                     \
		TPE1 *pbp, *bp, v;                                   \
		TPE2 *rp, *rb, curval = TPE2##_nil;                  \
		pbp = bp = (TPE1*)Tloc(b, 0);                        \
		rb = rp = (TPE2*)Tloc(r, 0);                         \
		if (p) {                                             \
			pnp = np = (bit*)Tloc(p, 0);                     \
			nend = np + cnt;                                 \
			for(; np<nend; np++) {                           \
				if (*np) {                                   \
					ncnt = np - pnp;                         \
					bp += ncnt;                              \
					rp += ncnt;                              \
					IMP(TPE1, TPE2, REAL_IMP)                \
					pbp = bp;                                \
					pnp = np;                                \
				}                                            \
			}                                                \
			ncnt = np - pnp;                                 \
			bp += ncnt;                                      \
			rp += ncnt;                                      \
			IMP(TPE1, TPE2, REAL_IMP)                        \
		} else {                                             \
			ncnt = cnt;                                      \
			bp += ncnt;                                      \
			rp += ncnt;                                      \
			IMP(TPE1, TPE2, REAL_IMP)                        \
		}                                                    \
		goto finish;                                         \
	} while(0);

#define ANALYTICAL_PROD_IMP_NUM_RANGE_ALL(TPE1, TPE2, TPE3) \
	do {                                                 \
		TPE1 v;                                          \
		for(; pbp<bp; pbp++) {                           \
			v = *pbp;                                    \
			if (!is_##TPE1##_nil(v)) {                   \
				if(is_##TPE2##_nil(curval))              \
					curval = (TPE2) v;                   \
				else                                     \
					MUL4_WITH_CHECK(TPE1, v, TPE2, curval, TPE2, curval, GDK_##TPE2##_max, TPE3, goto calc_overflow); \
			}                                            \
		}                                                \
		for (;rb < rp; rb++)                             \
			*rb = curval;                                \
		if(is_##TPE2##_nil(curval))                      \
			has_nils = true;                             \
		else                                             \
			curval = TPE2##_nil;                         \
	} while (0);

#define ANALYTICAL_PROD_IMP_NUM_RANGE_PART(TPE1, TPE2, TPE3) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		TPE1 *bs, *be, v;                       \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			bs = bp + (ns - nl);                \
			be = bs + parcel;                   \
			for(; bs<be; bs++) {                \
				v = *bs;                        \
				if (!is_##TPE1##_nil(v)) {      \
					if(is_##TPE2##_nil(curval)) \
						curval = (TPE2) v;      \
					else                        \
						MUL4_WITH_CHECK(TPE1, v, TPE2, curval, TPE2, curval, GDK_##TPE2##_max, TPE3, \
										goto calc_overflow); \
				}                               \
			}                                   \
			*rb = curval;                       \
			rb++;                               \
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0);

#define ANALYTICAL_PROD_CALC_NUM_RANGE(TPE1, TPE2, TPE3, IMP)   \
	do {                                           \
		TPE1 *bp;                                  \
		TPE2 *rb, *rp, curval = TPE2##_nil;        \
		bp = (TPE1*)Tloc(b, 0);                    \
		rp = rb = (TPE2*)Tloc(r, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					IMP##_PART(TPE1, TPE2, TPE3)   \
					bp += ncnt;                    \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			IMP##_PART(TPE1, TPE2, TPE3)           \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			IMP##_PART(TPE1, TPE2, TPE3)           \
		} else {                                   \
			TPE1 *pbp = bp;                        \
			rp += cnt;                             \
			bp += cnt;                             \
			IMP##_ALL(TPE1, TPE2, TPE3)            \
		}                                          \
		goto finish;                               \
	} while(0);

#define ANALYTICAL_PROD_IMP_NUM_LIMIT_RANGE_ALL(TPE1, TPE2, REAL_IMP)  \
	do {                                                           \
		TPE1 v;                                                    \
		for(; pbp<bp; pbp++) {                                     \
			v = *pbp;                                              \
			if (!is_##TPE1##_nil(v)) {                             \
				if(is_##TPE2##_nil(curval))                        \
					curval = (TPE2) v;                             \
				else                                               \
					REAL_IMP(TPE1, v, TPE2, curval, curval, GDK_##TPE2##_max, goto calc_overflow); \
			}                                                      \
		}                                                          \
		for (;rb < rp; rb++)                                       \
			*rb = curval;                                          \
		if(is_##TPE2##_nil(curval))                                \
			has_nils = true;                                       \
		else                                                       \
			curval = TPE2##_nil;                                   \
	} while (0);

#define ANALYTICAL_PROD_IMP_NUM_LIMIT_RANGE_PART(TPE1, TPE2, REAL_IMP) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		TPE1 *bs, *be, v;                       \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			bs = bp + (ns - nl);                \
			be = bs + parcel;                   \
			for(; bs<be; bs++) {                \
				v = *bs;                        \
				if (!is_##TPE1##_nil(v)) {      \
					if(is_##TPE2##_nil(curval)) \
						curval = (TPE2) v;      \
					else                        \
						REAL_IMP(TPE1, v, TPE2, curval, curval, GDK_##TPE2##_max, goto calc_overflow); \
				}                               \
			}                                   \
			*rb = curval;                       \
			rb++;                               \
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0);

#define ANALYTICAL_PROD_CALC_NUM_LIMIT_RANGE(TPE1, TPE2, IMP, REAL_IMP) \
	do {                                           \
		TPE1 *bp;                                  \
		TPE2 *rb, *rp, curval = TPE2##_nil;        \
		bp = (TPE1*)Tloc(b, 0);                    \
		rp = rb = (TPE2*)Tloc(r, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					IMP##_PART(TPE1, TPE2, REAL_IMP) \
					bp += ncnt;                    \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			IMP##_PART(TPE1, TPE2, REAL_IMP)       \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			IMP##_PART(TPE1, TPE2, REAL_IMP)       \
		} else {                                   \
			TPE1 *pbp = bp;                        \
			rp += cnt;                             \
			bp += cnt;                             \
			IMP##_ALL(TPE1, TPE2, REAL_IMP)        \
		}                                          \
		goto finish;                               \
	} while(0);

#define ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2)                                          \
	do {                                                                                 \
		if (ABSOLUTE(curval) > 1 && GDK_##TPE2##_max / ABSOLUTE(v) < ABSOLUTE(curval)) { \
			if (abort_on_error)                                                          \
				goto calc_overflow;                                                      \
			curval = TPE2##_nil;                                                         \
			nils++;                                                                      \
		} else {                                                                         \
			curval *= v;                                                                 \
		}                                                                                \
	} while(0);

#define ANALYTICAL_PROD_IMP_FP_ROWS(TPE1, TPE2)                 \
	do {                                                        \
		TPE1 *bl = pbp, *bs, *be;                               \
		for(; pbp<bp;pbp++) {                                   \
			bs = (pbp > bl+start) ? pbp - start : bl;           \
			be = (pbp+end < bp) ? pbp + end + 1 : bp;           \
			for(; bs<be; bs++) {                                \
				v = *bs;                                        \
				if (!is_##TPE1##_nil(v)) {                      \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) v;                      \
					else                                        \
						ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
				}                                               \
			}                                                   \
			*rb = curval;                                       \
			rb++;                                               \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
			else                                                \
				curval = TPE2##_nil;                            \
		}                                                       \
	} while(0);

#define ANALYTICAL_PROD_CALC_FP_ROWS(TPE1, TPE2, IMP) \
	do {                                           \
		TPE1 *pbp, *bp, v;                         \
		TPE2 *rp, *rb, curval = TPE2##_nil;        \
		pbp = bp = (TPE1*)Tloc(b, 0);              \
		rb = rp = (TPE2*)Tloc(r, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = np - pnp;               \
					bp += ncnt;                    \
					rp += ncnt;                    \
					IMP(TPE1, TPE2)                \
					pbp = bp;                      \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = np - pnp;                       \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2)                        \
		} else {                                   \
			ncnt = cnt;                            \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2)                        \
		}                                          \
		goto finish;                               \
	} while(0);

#define ANALYTICAL_PROD_IMP_FP_RANGE_ALL(TPE1, TPE2)        \
	do {                                                    \
		TPE1 v;                                             \
		for(; pbp<bp; pbp++) {                              \
			v = *pbp;                                       \
			 if (!is_##TPE1##_nil(v)) {                     \
				if(is_##TPE2##_nil(curval))                 \
					curval = (TPE2) v;                      \
				else                                        \
					ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
			}                                               \
		}                                                   \
		for (;rb < rp; rb++)                                \
			*rb = curval;                                   \
		if(is_##TPE2##_nil(curval))                         \
			has_nils = true;                                \
		else                                                \
			curval = TPE2##_nil;                            \
	} while (0);

#define ANALYTICAL_PROD_IMP_FP_RANGE_PART(TPE1, TPE2) \
	do {                                        \
		bit *nl = lp, *ns, *ne;                 \
		TPE1 *bs, *be, v;                       \
		BUN rstart, rend, parcel;               \
		for(; lp<lend;lp++) {                   \
			rstart = start;                     \
			for(ns=lp; ns>nl; ns--) {           \
				if(*ns) {                       \
					if(rstart == 0)             \
						break;                  \
					rstart--;                   \
				}                               \
			}                                   \
			rend = end;                         \
			for(ne=lp+1; ne<lend; ne++) {       \
				if(*ne) {                       \
					if(rend == 0)               \
						break;                  \
					rend--;                     \
				}                               \
			}                                   \
			parcel = (ne - ns);                 \
			bs = bp + (ns - nl);                \
			be = bs + parcel;                   \
			for(; bs<be; bs++) {                \
				v = *bs;                        \
				if (!is_##TPE1##_nil(v)) {      \
					if(is_##TPE2##_nil(curval)) \
						curval = (TPE2) v;      \
					else                        \
						ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
				}                               \
			}                                   \
			*rb = curval;                       \
			rb++;                               \
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0);

#define ANALYTICAL_PROD_CALC_FP_RANGE(TPE1, TPE2, IMP) \
	do {                                           \
		TPE1 *bp;                                  \
		TPE2 *rb, *rp, curval = TPE2##_nil;        \
		bp = (TPE1*)Tloc(b, 0);                    \
		rp = rb = (TPE2*)Tloc(r, 0);               \
		if (p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);           \
			lend = lp = o ? (bit*)Tloc(o, 0) : np; \
			nend = np + cnt;                       \
			for(; np<nend; np++) {                 \
				if (*np) {                         \
					ncnt = (np - pnp);             \
					lend += ncnt;                  \
					IMP##_PART(TPE1, TPE2)         \
					bp += ncnt;                    \
					pnp = np;                      \
				}                                  \
			}                                      \
			ncnt = (np - pnp);                     \
			lend += ncnt;                          \
			IMP##_PART(TPE1, TPE2)                 \
		} else if (o) {                            \
			lend = lp = (bit*)Tloc(o, 0);          \
			lend += cnt;                           \
			IMP##_PART(TPE1, TPE2)                 \
		} else {                                   \
			TPE1 *pbp = bp;                        \
			rp += cnt;                             \
			bp += cnt;                             \
			IMP##_ALL(TPE1, TPE2)                  \
		}                                          \
		goto finish;                               \
	} while(0);

#ifdef HAVE_HGE
#define ANALYTICAL_PROD_LIMIT(FRAME) \
	case TYPE_lng: { \
		switch (tp1) { \
			case TYPE_bte: \
				ANALYTICAL_PROD_CALC_NUM##FRAME(bte, lng, hge, ANALYTICAL_PROD_IMP_NUM##FRAME); \
				break; \
			case TYPE_sht: \
				ANALYTICAL_PROD_CALC_NUM##FRAME(sht, lng, hge, ANALYTICAL_PROD_IMP_NUM##FRAME); \
				break; \
			case TYPE_int: \
				ANALYTICAL_PROD_CALC_NUM##FRAME(int, lng, hge, ANALYTICAL_PROD_IMP_NUM##FRAME); \
				break; \
			case TYPE_lng: \
				ANALYTICAL_PROD_CALC_NUM##FRAME(lng, lng, hge, ANALYTICAL_PROD_IMP_NUM##FRAME); \
				break; \
			default: \
				goto nosupport; \
		} \
		break; \
	} \
	case TYPE_hge: { \
		switch (tp1) { \
			case TYPE_bte: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(bte, hge, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, HGEMUL_CHECK); \
				break; \
			case TYPE_sht: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(sht, hge, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, HGEMUL_CHECK); \
				break; \
			case TYPE_int: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(int, hge, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, HGEMUL_CHECK); \
				break; \
			case TYPE_lng: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(lng, hge, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, HGEMUL_CHECK); \
				break; \
			case TYPE_hge: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(hge, hge, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, HGEMUL_CHECK); \
				break; \
			default: \
				goto nosupport; \
		} \
		break; \
	}
#else
#define ANALYTICAL_PROD_LIMIT(FRAME) \
	case TYPE_lng: { \
		switch (tp1) { \
			case TYPE_bte: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(bte, lng, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, LNGMUL_CHECK); \
				break; \
			case TYPE_sht: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(sht, lng, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, LNGMUL_CHECK); \
				break; \
			case TYPE_int: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(int, lng, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, LNGMUL_CHECK); \
				break; \
			case TYPE_lng: \
				ANALYTICAL_PROD_CALC_NUM_LIMIT##FRAME(lng, lng, ANALYTICAL_PROD_IMP_NUM_LIMIT##FRAME, LNGMUL_CHECK); \
				break; \
			default: \
				goto nosupport; \
		} \
		break; \
	}
#endif

#define ANALYTICAL_PROD_BRANCHES(FRAME) \
		switch (tp2) { \
			case TYPE_bte: { \
				switch (tp1) { \
					case TYPE_bte: \
						ANALYTICAL_PROD_CALC_NUM##FRAME(bte, bte, sht, ANALYTICAL_PROD_IMP_NUM##FRAME); \
						break; \
					default: \
						goto nosupport; \
				} \
				break; \
			} \
			case TYPE_sht: { \
				switch (tp1) { \
					case TYPE_bte: \
						ANALYTICAL_PROD_CALC_NUM##FRAME(bte, sht, int, ANALYTICAL_PROD_IMP_NUM##FRAME); \
						break; \
					case TYPE_sht: \
						ANALYTICAL_PROD_CALC_NUM##FRAME(sht, sht, int, ANALYTICAL_PROD_IMP_NUM##FRAME); \
						break; \
					default: \
						goto nosupport; \
				} \
				break; \
			} \
			case TYPE_int: { \
				switch (tp1) { \
					case TYPE_bte: \
						ANALYTICAL_PROD_CALC_NUM##FRAME(bte, int, lng, ANALYTICAL_PROD_IMP_NUM##FRAME); \
						break; \
					case TYPE_sht: \
						ANALYTICAL_PROD_CALC_NUM##FRAME(sht, int, lng, ANALYTICAL_PROD_IMP_NUM##FRAME); \
						break; \
					case TYPE_int: \
						ANALYTICAL_PROD_CALC_NUM##FRAME(int, int, lng, ANALYTICAL_PROD_IMP_NUM##FRAME); \
						break; \
					default: \
						goto nosupport; \
				} \
				break; \
			} \
			ANALYTICAL_PROD_LIMIT(FRAME) \
			case TYPE_flt: { \
				switch (tp1) { \
					case TYPE_flt: \
						ANALYTICAL_PROD_CALC_FP##FRAME(flt, flt, ANALYTICAL_PROD_IMP_FP##FRAME); \
						break; \
					default: \
						goto nosupport; \
						break; \
				} \
			} \
			case TYPE_dbl: { \
				switch (tp1) { \
					case TYPE_flt: \
						ANALYTICAL_PROD_CALC_FP##FRAME(flt, dbl, ANALYTICAL_PROD_IMP_FP##FRAME); \
						break; \
					case TYPE_dbl: \
						ANALYTICAL_PROD_CALC_FP##FRAME(dbl, dbl, ANALYTICAL_PROD_IMP_FP##FRAME); \
						break; \
					default: \
						goto nosupport; \
						break; \
				} \
			} \
			default: \
				goto nosupport; \
		}

gdk_return
GDKanalyticalprod(BAT *r, BAT *b, BAT *p, BAT *o, int tp1, int tp2, int unit, BUN start, BUN end)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0;
	bit *pnp, *np, *nend, *lp, *lend;
	int abort_on_error = 1;

	assert(unit >= 0 && unit <= 2);

	if(unit == 0) {
		ANALYTICAL_PROD_BRANCHES(_ROWS)
	} else {
		ANALYTICAL_PROD_BRANCHES(_RANGE)
	}
nosupport:
	GDKerror("prod: type combination (prod(%s)->%s) not supported.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_PROD_IMP_NUM_ROWS
#undef ANALYTICAL_PROD_IMP_NUM_RANGE_PART
#undef ANALYTICAL_PROD_IMP_NUM_RANGE_ALL
#undef ANALYTICAL_PROD_CALC_NUM_ROWS
#undef ANALYTICAL_PROD_CALC_NUM_RANGE
#undef ANALYTICAL_PROD_IMP_NUM_LIMIT_ROWS
#undef ANALYTICAL_PROD_IMP_NUM_LIMIT_RANGE_PART
#undef ANALYTICAL_PROD_IMP_NUM_LIMIT_RANGE_ALL
#undef ANALYTICAL_PROD_CALC_NUM_LIMIT_ROWS
#undef ANALYTICAL_PROD_CALC_NUM_LIMIT_RANGE
#undef ANALYTICAL_PROD_CALC_FP_ROWS
#undef ANALYTICAL_PROD_CALC_FP_RANGE
#undef ANALYTICAL_PROD_IMP_FP_ROWS
#undef ANALYTICAL_PROD_IMP_FP_RANGE_PART
#undef ANALYTICAL_PROD_IMP_FP_RANGE_ALL
#undef ANALYTICAL_PROD_IMP_FP_REAL
#undef ANALYTICAL_PROD_BRANCHES
#undef ANALYTICAL_PROD_LIMIT

#define ANALYTICAL_AVERAGE_IMP_NO_OVERLAP(TPE,lng_hge,LABEL)  \
	do {                                                      \
		for(; pbp<bp; pbp++) {                                \
			v = *pbp;                                         \
			if (!is_##TPE##_nil(v)) {                         \
				ADD_WITH_CHECK(TPE, v, lng_hge, sum, lng_hge, sum, GDK_##lng_hge##_max, \
							   goto avg_overflow##TPE##LABEL##no_overlap); \
				/* count only when no overflow occurs */      \
				n++;                                          \
			}                                                 \
		}                                                     \
		if(0) {                                               \
avg_overflow##TPE##LABEL##no_overlap:                         \
			assert(n > 0);                                    \
			if (sum >= 0) {                                   \
				a = (TPE) (sum / (lng_hge) n);                \
				rr = (BUN) (sum % (SBUN) n);                  \
			} else {                                          \
				sum = -sum;                                   \
				a = - (TPE) (sum / (lng_hge) n);              \
				rr = (BUN) (sum % (SBUN) n);                  \
				if (r) {                                      \
					a--;                                      \
					rr = n - rr;                              \
				}                                             \
			}                                                 \
			for(; pbp<bp; pbp++) {                            \
				v = *pbp;                                     \
				if (is_##TPE##_nil(v))                        \
					continue;                                 \
				AVERAGE_ITER(TPE, v, a, rr, n);               \
			}                                                 \
			curval = a + (dbl) rr / n;                        \
			goto calc_done##TPE##LABEL##no_overlap;           \
		}                                                     \
		curval = n > 0 ? (dbl) sum / n : dbl_nil;             \
calc_done##TPE##LABEL##no_overlap:                            \
		has_nils = has_nils || (n == 0);                      \
		for (;rb < rp; rb++)                                  \
			*rb = curval;                                     \
		n = 0;                                                \
		sum = 0;                                              \
	} while(0);

#define ANALYTICAL_AVERAGE_IMP_ROWS(TPE,lng_hge,LABEL)            \
	do {                                                          \
		TPE *bl = pbp, *bs, *be;                                  \
		for(; pbp<bp;pbp++) {                                     \
			bs = (pbp > bl+start) ? pbp - start : bl;             \
			be = (pbp+end < bp) ? pbp + end + 1 : bp;             \
			for(; bs<be; bs++) {                                  \
				v = *bs;                                          \
				if (!is_##TPE##_nil(v)) {                         \
					ADD_WITH_CHECK(TPE, v, lng_hge, sum, lng_hge, sum, GDK_##lng_hge##_max, \
								   goto avg_overflow##TPE##LABEL##rows); \
					/* count only when no overflow occurs */      \
					n++;                                          \
				}                                                 \
			}                                                     \
			if(0) {                                               \
avg_overflow##TPE##LABEL##rows:                                   \
				assert(n > 0);                                    \
				if (sum >= 0) {                                   \
					a = (TPE) (sum / (lng_hge) n);                \
					rr = (BUN) (sum % (SBUN) n);                  \
				} else {                                          \
					sum = -sum;                                   \
					a = - (TPE) (sum / (lng_hge) n);              \
					rr = (BUN) (sum % (SBUN) n);                  \
					if (r) {                                      \
						a--;                                      \
						rr = n - rr;                              \
					}                                             \
				}                                                 \
				for(; bs<be; bs++) {                              \
					v = *bs;                                      \
					if (is_##TPE##_nil(v))                        \
						continue;                                 \
					AVERAGE_ITER(TPE, v, a, rr, n);               \
				}                                                 \
				curval = a + (dbl) rr / n;                        \
				goto calc_done##TPE##LABEL##rows;                 \
			}                                                     \
			curval = n > 0 ? (dbl) sum / n : dbl_nil;             \
calc_done##TPE##LABEL##rows:                                      \
			has_nils = has_nils || (n == 0);                      \
			*rb = curval;                                         \
			rb++;                                                 \
			n = 0;                                                \
			sum = 0;                                              \
		}                                                         \
	} while(0);

#define ANALYTICAL_AVERAGE_LNG_HGE(TPE,lng_hge,IMP)   \
	do {                                              \
		TPE *pbp, *bp, a, v;                          \
		dbl *rp, *rb;                                 \
		pbp = bp = (TPE*)Tloc(b, 0);                  \
		rb = rp = (dbl*)Tloc(r, 0);                   \
		if (p) {                                      \
			pnp = np = (bit*)Tloc(p, 0);              \
			nend = np + cnt;                          \
			for(; np<nend; np++) {                    \
				if (*np) {                            \
					ncnt = np - pnp;                  \
					bp += ncnt;                       \
					rp += ncnt;                       \
					IMP(TPE,lng_hge,middle_partition) \
					pnp = np;                         \
					pbp = bp;                         \
				}                                     \
			}                                         \
			ncnt = np - pnp;                          \
			bp += ncnt;                               \
			rp += ncnt;                               \
			IMP(TPE,lng_hge,final_partition)          \
		} else if (o || force_order) {                \
			bp += cnt;                                \
			rp += cnt;                                \
			IMP(TPE,lng_hge,single_partition)         \
		} else {                                      \
			dbl* rend = rp + cnt;                     \
			for(; rp<rend; rp++, bp++) {              \
				v = *bp;                              \
				if(is_##TPE##_nil(v)) {               \
					*rp = dbl_nil;                    \
					has_nils = true;                  \
				} else {                              \
					*rp = (dbl) v;                    \
				}                                     \
			}                                         \
		}                                             \
		goto finish;                                  \
	} while(0);

#ifdef HAVE_HGE
#define ANALYTICAL_AVERAGE_CALC(TYPE,IMP) ANALYTICAL_AVERAGE_LNG_HGE(TYPE,hge,IMP)
#else
#define ANALYTICAL_AVERAGE_CALC(TYPE,IMP) ANALYTICAL_AVERAGE_LNG_HGE(TYPE,lng,IMP)
#endif

#define ANALYTICAL_AVERAGE_FLOAT_IMP_NO_OVERLAP(TPE) \
	do {                                          \
		for(; pbp<bp; pbp++) {                    \
			v = *pbp;                             \
			if (!is_##TPE##_nil(v))               \
				AVERAGE_ITER_FLOAT(TPE, v, a, n); \
		}                                         \
		curval = n > 0 ? a : dbl_nil;             \
		has_nils = has_nils || (n == 0);          \
		for (;rb < rp; rb++)                      \
			*rb = curval;                         \
		n = 0;                                    \
		a = 0;                                    \
	} while(0);

#define ANALYTICAL_AVERAGE_FLOAT_IMP_ROWS(TPE)        \
	do {                                              \
		TPE *bl = pbp, *bs, *be;                      \
		for(; pbp<bp;pbp++) {                         \
			bs = (pbp > bl+start) ? pbp - start : bl; \
			be = (pbp+end < bp) ? pbp + end + 1 : bp; \
			for(; bs<be; bs++) {                      \
				v = *bs;                              \
				if (!is_##TPE##_nil(v))               \
					AVERAGE_ITER_FLOAT(TPE, v, a, n); \
			}                                         \
			curval = n > 0 ? a : dbl_nil;             \
			has_nils = has_nils || (n == 0);          \
			*rb = curval;                             \
			rb++;                                     \
			n = 0;                                    \
			a = 0;                                    \
		}                                             \
	} while(0);

#define ANALYTICAL_AVERAGE_CALC_FLOAT(TPE, IMP) \
	do {                                   \
		TPE *pbp, *bp, v;                  \
		dbl *rp, *rb, a = 0;               \
		pbp = bp = (TPE*)Tloc(b, 0);       \
		rb = rp = (dbl*)Tloc(r, 0);        \
		if (p) {                           \
			pnp = np = (bit*)Tloc(p, 0);   \
			nend = np + cnt;               \
			for(; np<nend; np++) {         \
				if (*np) {                 \
					ncnt = np - pnp;       \
					bp += ncnt;            \
					rp += ncnt;            \
					IMP(TPE)               \
					pbp = bp;              \
					pnp = np;              \
				}                          \
			}                              \
			ncnt = np - pnp;               \
			bp += ncnt;                    \
			rp += ncnt;                    \
			IMP(TPE)                       \
		} else if (o || force_order) {     \
			bp += cnt;                     \
			rp += cnt;                     \
			IMP(TPE)                       \
		} else {                           \
			dbl *rend = rp + cnt;          \
			for(; rp<rend; rp++, bp++) {   \
				if(is_##TPE##_nil(*bp)) {  \
					*rp = dbl_nil;         \
					has_nils = true;       \
				} else {                   \
					*rp = (dbl) *bp;       \
				}                          \
			}                              \
		}                                  \
		goto finish;                       \
	} while(0);

#ifdef HAVE_HGE
#define ANALYTICAL_AVERAGE_LIMIT(FRAME) \
	case TYPE_hge: \
		ANALYTICAL_AVERAGE_CALC(hge, ANALYTICAL_AVERAGE_IMP##FRAME); \
		break;
#else
#define ANALYTICAL_AVERAGE_LIMIT(FRAME)
#endif

#define ANALYTICAL_AVERAGE_BRANCHES(FRAME) \
	switch (tpe) { \
		case TYPE_bte: \
			ANALYTICAL_AVERAGE_CALC(bte, ANALYTICAL_AVERAGE_IMP##FRAME); \
			break; \
		case TYPE_sht: \
			ANALYTICAL_AVERAGE_CALC(sht, ANALYTICAL_AVERAGE_IMP##FRAME); \
			break; \
		case TYPE_int: \
			ANALYTICAL_AVERAGE_CALC(int, ANALYTICAL_AVERAGE_IMP##FRAME); \
			break; \
		case TYPE_lng: \
			ANALYTICAL_AVERAGE_CALC(lng, ANALYTICAL_AVERAGE_IMP##FRAME); \
			break; \
		ANALYTICAL_AVERAGE_LIMIT(FRAME) \
		case TYPE_flt: \
			ANALYTICAL_AVERAGE_CALC_FLOAT(flt, ANALYTICAL_AVERAGE_FLOAT_IMP##FRAME); \
			break; \
		case TYPE_dbl: \
			ANALYTICAL_AVERAGE_CALC_FLOAT(dbl, ANALYTICAL_AVERAGE_FLOAT_IMP##FRAME); \
			break; \
		default: \
			GDKerror("GDKanalyticalavg: average of type %s unsupported.\n", ATOMname(tpe)); \
			return GDK_FAIL; \
	}

gdk_return
GDKanalyticalavg(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tpe, int unit, BUN start, BUN end)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0, n = 0, rr;
	bit *np, *pnp, *nend;
	bool abort_on_error = true;
	dbl curval;
#ifdef HAVE_HGE
	hge sum = 0;
#else
	lng sum = 0;
#endif

	assert(unit >= 0 && unit <= 2);

	if(unit == 0 && start == 0 && end == 0) {
		ANALYTICAL_AVERAGE_BRANCHES(_NO_OVERLAP)
	} else if(unit == 0) {
		ANALYTICAL_AVERAGE_BRANCHES(_ROWS)
	} else {

	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_AVERAGE_CALC
#undef ANALYTICAL_AVERAGE_LNG_HGE
#undef ANALYTICAL_AVERAGE_IMP_ROWS
#undef ANALYTICAL_AVERAGE_IMP_NO_OVERLAP
#undef ANALYTICAL_AVERAGE_CALC_FLOAT
#undef ANALYTICAL_AVERAGE_FLOAT_IMP_ROWS
#undef ANALYTICAL_AVERAGE_FLOAT_IMP_NO_OVERLAP
#undef ANALYTICAL_AVERAGE_BRANCHES
#undef ANALYTICAL_AVERAGE_LIMIT
