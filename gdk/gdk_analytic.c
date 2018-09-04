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
	bit *rb = (bit*)Tloc(r, 0), *rp = c ? (bit*)Tloc(c, 0) : NULL;
	int (*atomcmp)(const void *, const void *);

	switch(ATOMstorage(tpe)) {
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
			rp = rb + cnt;                   \
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
		default: {
			goto nosupport;
		}
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

#define ANALYTICAL_FIRST_IMP(TPE)                 \
	do {                                          \
		TPE *rp, *rb, *restrict bp, curval;       \
		rb = rp = (TPE*)Tloc(r, 0);               \
		bp = (TPE*)Tloc(b, 0);                    \
		curval = *bp;                             \
		if (p) {                                  \
			pnp = np = (bit*)Tloc(p, 0);          \
			end = np + cnt;                       \
			for(; np<end; np++) {                 \
				if (*np) {                        \
					ncnt = (np - pnp);            \
					rp += ncnt;                   \
					bp += ncnt;                   \
					if(is_##TPE##_nil(curval))    \
						has_nils = true;          \
					for (;rb < rp; rb++)          \
						*rb = curval;             \
					curval = *bp;                 \
					pnp = np;                     \
				}                                 \
			}                                     \
			ncnt = (np - pnp);                    \
			rp += ncnt;                           \
			bp += ncnt;                           \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for (;rb < rp; rb++)                  \
				*rb = curval;                     \
		} else {                                  \
			TPE *rend = rp + cnt;                 \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rp<rend; rp++)                  \
				*rp = curval;                     \
		}                                         \
	} while(0);

gdk_return
GDKanalyticalfirst(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i, j, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;

	(void)o;
	switch(ATOMstorage(tpe)) {
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
			void *restrict curval = BUNtail(bpi, 0);
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if (p) {
				np = (bit*)Tloc(p, 0);
				for(i=0,j=0; i<cnt; i++, np++) {
					if (*np) {
						if((*atomcmp)(curval, nil) == 0)
							has_nils = true;
						for (;j < i; j++) {
							if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
								goto finish;
						}
						curval = BUNtail(bpi, i);
					}
				}
				if((*atomcmp)(curval, nil) == 0)
					has_nils = true;
				for (;j < i; j++) {
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
					   goto finish;
				}
			} else { /* single value, ie no ordering */
				if((*atomcmp)(curval, nil) == 0)
					has_nils = true;
				for(i=0; i<cnt; i++) {
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
				}
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

#define ANALYTICAL_LAST_IMP(TPE)                  \
	do {                                          \
		TPE *rp, *rb, *restrict bp, curval;       \
		rb = rp = (TPE*)Tloc(r, 0);               \
		bp = (TPE*)Tloc(b, 0);                    \
		if (p) {                                  \
			pnp = np = (bit*)Tloc(p, 0);          \
			end = np + cnt;                       \
			for(; np<end; np++) {                 \
				if (*np) {                        \
					ncnt = (np - pnp);            \
					rp += ncnt;                   \
					bp += ncnt;                   \
					curval = *(bp - 1);           \
					if(is_##TPE##_nil(curval))    \
						has_nils = true;          \
					for (;rb < rp; rb++)          \
						*rb = curval;             \
					pnp = np;                     \
				}                                 \
			}                                     \
			ncnt = (np - pnp);                    \
			rp += ncnt;                           \
			bp += ncnt;                           \
			curval = *(bp - 1);                   \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for (;rb < rp; rb++)                  \
				*rb = curval;                     \
		} else {                                  \
			TPE *rend = rp + cnt;                 \
			curval = *(bp + cnt - 1);             \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rp<rend; rp++)                  \
				*rp = curval;                     \
		}                                         \
	} while(0);

gdk_return
GDKanalyticallast(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i, j, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;

	(void)o;
	switch(ATOMstorage(tpe)) {
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
				np = (bit*)Tloc(p, 0);
				for(i=0,j=0; i<cnt; i++, np++) {
					if (*np) {
						curval = BUNtail(bpi, i - 1);
						if((*atomcmp)(curval, nil) == 0)
							has_nils = true;
						for (;j < i; j++) {
							if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
								goto finish;
						}
					}
				}
				curval = BUNtail(bpi, cnt - 1);
				if((*atomcmp)(curval, nil) == 0)
					has_nils = true;
				for (;j < i; j++) {
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
				}
			} else { /* single value, ie no ordering */
				curval = BUNtail(bpi, cnt - 1);
				if((*atomcmp)(curval, nil) == 0)
					has_nils = true;
				for(i=0; i<cnt; i++) {
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
				}
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

#define ANALYTICAL_NTHVALUE_IMP(TPE)              \
	do {                                          \
		TPE *rp, *rb, *pbp, *bp, curval;          \
		pbp = bp = (TPE*)Tloc(b, 0);              \
		rb = rp = (TPE*)Tloc(r, 0);               \
		if(nth == BUN_NONE) {                     \
			TPE* rend = rp + cnt;                 \
			has_nils = true;                      \
			for(; rp<rend; rp++)                  \
				*rp = TPE##_nil;                  \
		} else if(p) {                            \
			pnp = np = (bit*)Tloc(p, 0);          \
			end = np + cnt;                       \
			for(; np<end; np++) {                 \
				if (*np) {                        \
					ncnt = (np - pnp);            \
					rp += ncnt;                   \
					bp += ncnt;                   \
					if(nth > (BUN) (bp - pbp)) {  \
						curval = TPE##_nil;       \
					} else {                      \
						curval = *(pbp + nth);    \
					}                             \
					if(is_##TPE##_nil(curval))    \
						has_nils = true;          \
					for(; rb<rp; rb++)            \
						*rb = curval;             \
					pbp = bp;                     \
					pnp = np;                     \
				}                                 \
			}                                     \
			ncnt = (np - pnp);                    \
			rp += ncnt;                           \
			bp += ncnt;                           \
			if(nth > (BUN) (bp - pbp)) {          \
				curval = TPE##_nil;               \
			} else {                              \
				curval = *(pbp + nth);            \
			}                                     \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rb<rp; rb++)                    \
				*rb = curval;                     \
		} else {                                  \
			TPE* rend = rp + cnt;                 \
			if(nth > cnt) {                       \
				curval = TPE##_nil;               \
			} else {                              \
				curval = *(bp + nth);             \
			}                                     \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rp<rend; rp++)                  \
				*rp = curval;                     \
		}                                         \
		goto finish;                              \
	} while(0);

gdk_return
GDKanalyticalnthvalue(BAT *r, BAT *b, BAT *p, BAT *o, BUN nth, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	BUN i, j, ncnt, cnt = BATcount(b);
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
				np = (bit*)Tloc(p, 0);
				for(i=0,j=0; i<cnt; i++, np++) {
					if (*np) {
						if(nth > (i - j)) {
							curval = nil;
						} else {
							curval = BUNtail(bpi, nth);
						}
						if((*atomcmp)(curval, nil) == 0)
							has_nils = true;
						for (;j < i; j++) {
							if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
								goto finish;
						}
					}
				}
				if(nth > (i - j)) {
					curval = nil;
				} else {
					curval = BUNtail(bpi, nth);
				}
				if((*atomcmp)(curval, nil) == 0)
					has_nils = true;
				for (;j < i; j++) {
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
				}
			} else { /* single value, ie no ordering */
				if(nth > cnt) {
					curval = nil;
				} else {
					curval = BUNtail(bpi, nth);
				}
				if((*atomcmp)(curval, nil) == 0)
					has_nils = true;
				for(i=0; i<cnt; i++) {
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
				}
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

#define ANALYTICAL_LAG_IMP(TPE)                         \
	do {                                                \
		TPE *rp, *rb, *bp, *rend,                       \
			def = *((TPE *) default_value), next;       \
		bp = (TPE*)Tloc(b, 0);                          \
		rb = rp = (TPE*)Tloc(r, 0);                     \
		rend = rb + cnt;                                \
		if(lag == BUN_NONE) {                           \
			has_nils = true;                            \
			for(; rb<rend; rb++)                        \
				*rb = TPE##_nil;                        \
		} else if(p) {                                  \
			pnp = np = (bit*)Tloc(p, 0);                \
			end = np + cnt;                             \
			for(; np<end; np++) {                       \
				if (*np) {                              \
					ncnt = (np - pnp);                  \
					rp += ncnt;                         \
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
					bp += (lag < ncnt) ? lag : 0;       \
					pnp = np;                           \
				}                                       \
			}                                           \
			for(i=0; i<lag && rb<rend; i++, rb++)       \
				*rb = def;                              \
			if(lag > 0 && is_##TPE##_nil(def))          \
				has_nils = true;                        \
			for(;rb<rend; rb++, bp++) {                 \
				next = *bp;                             \
				*rb = next;                             \
				if(is_##TPE##_nil(next))                \
					has_nils = true;                    \
			}                                           \
		} else {                                        \
			for(i=0; i<lag && rb<rend; i++, rb++)       \
				*rb = def;                              \
			if(lag > 0 && is_##TPE##_nil(def))          \
				has_nils = true;                        \
			for(;rb<rend; rb++, bp++) {                 \
				next = *bp;                             \
				*rb = next;                             \
				if(is_##TPE##_nil(next))                \
					has_nils = true;                    \
			}                                           \
		}                                               \
		goto finish;                                    \
	} while(0);

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
				np = (bit*)Tloc(p, 0);
				for(j=0,k=0; j<cnt; j++, np++) {
					if (*np) {
						for(i=0; i<lag && k<j; i++, k++) {
							if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED)
								goto finish;
						}
						if(lag > 0 && (*atomcmp)(default_value, nil) == 0)
							has_nils = true;
						for(l=k-lag; k<j; k++, l++) {
							curval = BUNtail(bpi, l);
							if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
								goto finish;
							if((*atomcmp)(curval, nil) == 0)
								has_nils = true;
						}
					}
				}
				for(i=0; i<lag && k<cnt; i++, k++) {
					if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED)
						goto finish;
				}
				if(lag > 0 && (*atomcmp)(default_value, nil) == 0)
					has_nils = true;
				for(l=k-lag; k<cnt; k++, l++) {
					curval = BUNtail(bpi, l);
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
					if((*atomcmp)(curval, nil) == 0)
						has_nils = true;
				}
			} else {
				for(i=0; i<lag && i<cnt; i++) {
					if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED)
						goto finish;
				}
				if(lag > 0 && (*atomcmp)(default_value, nil) == 0)
					has_nils = true;
				for(l=0, k=(BUN)lag; k<cnt; k++, l++) {
					curval = BUNtail(bpi, l);
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
					if((*atomcmp)(curval, nil) == 0)
						has_nils = true;
				}
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

#define ANALYTICAL_LEAD_IMP(TPE)                         \
	do {                                                 \
		TPE *rp, *rb, *bp, *rend,                        \
			def = *((TPE *) default_value), next;        \
		bp = (TPE*)Tloc(b, 0);                           \
		rb = rp = (TPE*)Tloc(r, 0);                      \
		rend = rb + cnt;                                 \
		if(lead == BUN_NONE) {                           \
			has_nils = true;                             \
			for(; rb<rend; rb++)                         \
				*rb = TPE##_nil;                         \
		} else if(p) {                                   \
			pnp = np = (bit*)Tloc(p, 0);                 \
			end = np + cnt;                              \
			for(; np<end; np++) {                        \
				if (*np) {                               \
					ncnt = (np - pnp);                   \
					rp += ncnt;                          \
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
					pnp = np;                            \
				}                                        \
			}                                            \
			ncnt = (np - pnp);                           \
			if(lead < ncnt) {                            \
				bp += lead;                              \
				l = ncnt - lead;                         \
				for(i=0; i<l; i++, rb++, bp++) {         \
					next = *bp;                          \
					*rb = next;                          \
					if(is_##TPE##_nil(next))             \
						has_nils = true;                 \
				}                                        \
			}                                            \
			for(;rb<rend; rb++)                          \
				*rb = def;                               \
			if(lead > 0 && is_##TPE##_nil(def))          \
				has_nils = true;                         \
		} else {                                         \
			if(lead < cnt) {                             \
				bp += lead;                              \
				l = cnt - lead;                          \
				for(i=0; i<l; i++, rb++, bp++) {         \
					next = *bp;                          \
					*rb = next;                          \
					if(is_##TPE##_nil(next))             \
						has_nils = true;                 \
				}                                        \
			}                                            \
			for(;rb<rend; rb++)                          \
				*rb = def;                               \
			if(lead > 0 && is_##TPE##_nil(def))          \
				has_nils = true;                         \
		}                                                \
		goto finish;                                     \
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
				np = (bit*)Tloc(p, 0);
				for(j=0,k=0; j<cnt; j++, np++) {
					if (*np) {
						l = (j - k);
						if(lead < l) {
							m = l - lead;
							for(i=0,n=k+lead; i<m; i++, n++) {
								curval = BUNtail(bpi, n);
								if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
									goto finish;
								if((*atomcmp)(curval, nil) == 0)
									has_nils = true;
							}
							k += i;
						}
						for(; k<j; k++) {
							if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED)
								goto finish;
						}
						if(lead > 0 && (*atomcmp)(default_value, nil) == 0)
							has_nils = true;
					}
				}
				l = (j - k);
				if(lead < l) {
					m = l - lead;
					for(i=0,n=k+lead; i<m; i++, n++) {
						curval = BUNtail(bpi, n);
						if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
							goto finish;
						if((*atomcmp)(curval, nil) == 0)
							has_nils = true;
					}
					k += i;
				}
				for(; k<j; k++) {
					if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED)
						goto finish;
				}
				if(lead > 0 && (*atomcmp)(default_value, nil) == 0)
					has_nils = true;
			} else {
				if(lead < cnt) {
					m = cnt - lead;
					for(i=0,n=lead; i<m; i++, n++) {
						curval = BUNtail(bpi, n);
						if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
							goto finish;
						if((*atomcmp)(curval, nil) == 0)
							has_nils = true;
					}
					k += i;
				}
				for(; k<cnt; k++) {
					if ((gdk_res = BUNappend(r, default_value, false)) != GDK_SUCCEED)
						goto finish;
				}
				if(lead > 0 && (*atomcmp)(default_value, nil) == 0)
					has_nils = true;
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

#define ANALYTICAL_LIMIT_IMP_NO_OVERLAP(TPE, IMP) \
	do {                                          \
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

#define ANALYTICAL_LIMIT_IMP_OVERLAP(TPE, IMP)        \
	do {                                              \
		TPE *bs, *bl, *be;                            \
		bl = pbp;                                     \
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

#define ANALYTICAL_LIMIT_IMP(TPE, IMP, REAL)       \
	do {                                           \
		TPE *rp, *rb, *pbp, *bp, *rend, curval, v; \
		rb = rp = (TPE*)Tloc(r, 0);                \
		pbp = bp = (TPE*)Tloc(b, 0);               \
		rend = rp + cnt;                           \
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
		} else if (o || force_order) {             \
			rp += cnt;                             \
			bp += cnt;                             \
			REAL(TPE, IMP)                         \
		} else {                                   \
			for(; rp<rend; rp++, bp++) {           \
				v = *bp;                           \
				if(is_##TPE##_nil(v))              \
					has_nils = true;               \
				*rp = v;                           \
			}                                      \
		}                                          \
	} while(0);

#ifdef HAVE_HUGE
#define ANALYTICAL_LIMIT_IMP_HUGE(IMP, REAL) \
	case TYPE_hge:                           \
		ANALYTICAL_LIMIT_IMP(hge, IMP, REAL) \
	break;
#else
#define ANALYTICAL_LIMIT_IMP_HUGE(IMP, REAL)
#endif

#define ANALYTICAL_LIMIT_IMP_OTHERS_NO_OVERLAP(SIGN_OP)                       \
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

#define ANALYTICAL_LIMIT_IMP_OTHERS_OVERLAP(SIGN_OP)                              \
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

#define ANALYTICAL_LIMIT_IMP_OTHERS(SIGN_OP, REAL)                        \
	do {                                                                  \
		BATiter bpi = bat_iterator(b);                                    \
		void *restrict curval = BUNtail(bpi, 0);                          \
		nil = ATOMnilptr(tpe);                                            \
		atomcmp = ATOMcompare(tpe);                                       \
		if (p) {                                                          \
			pnp = np = (bit*)Tloc(p, 0);                                  \
			nend = np + cnt;                                              \
			for(; np<nend; np++) {                                        \
				if (*np) {                                                \
					i += (np - pnp);                                      \
					REAL(SIGN_OP)                                         \
					pnp = np;                                             \
				}                                                         \
			}                                                             \
			i += (np - pnp);                                              \
			REAL(SIGN_OP)                                                 \
		} else if (o || force_order) {                                    \
			i += cnt;                                                     \
			REAL(SIGN_OP)                                                 \
		} else {                                                          \
			for(i=0; i<cnt; i++) {                                        \
				void *next = BUNtail(bpi, i);                             \
				if((*atomcmp)(next, nil) == 0)                            \
					has_nils = true;                                      \
				if ((gdk_res = BUNappend(r, next, false)) != GDK_SUCCEED) \
					goto finish;                                          \
			}                                                             \
		}                                                                 \
	} while(0);

#define ANALYTICAL_LIMIT(OP, IMP, SIGN_OP)                                                   \
gdk_return                                                                                   \
GDKanalytical##OP(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tpe, BUN start, BUN end) \
{                                                                                            \
	int (*atomcmp)(const void *, const void *);                                              \
	const void* restrict nil;                                                                \
	bool has_nils = false;                                                                   \
	BUN i = 0, j = 0, l = 0, k = 0, m = 0, ncnt, cnt = BATcount(b);                          \
	bit *np, *pnp, *nend;                                                                    \
	gdk_return gdk_res = GDK_SUCCEED;                                                        \
                                                                                             \
	if(start == 0 && end == 0) {                                                             \
		switch(ATOMstorage(tpe)) {                                                           \
			case TYPE_bit:                                                                   \
				ANALYTICAL_LIMIT_IMP(bit, IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)              \
				break;                                                                       \
			case TYPE_bte:                                                                   \
				ANALYTICAL_LIMIT_IMP(bte, IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)              \
				break;                                                                       \
			case TYPE_sht:                                                                   \
				ANALYTICAL_LIMIT_IMP(sht, IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)              \
				break;                                                                       \
			case TYPE_int:                                                                   \
				ANALYTICAL_LIMIT_IMP(int, IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)              \
				break;                                                                       \
			case TYPE_lng:                                                                   \
				ANALYTICAL_LIMIT_IMP(lng, IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)              \
				break;                                                                       \
			ANALYTICAL_LIMIT_IMP_HUGE(IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)                  \
			case TYPE_flt:                                                                   \
				ANALYTICAL_LIMIT_IMP(flt, IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)              \
				break;                                                                       \
			case TYPE_dbl:                                                                   \
				ANALYTICAL_LIMIT_IMP(dbl, IMP, ANALYTICAL_LIMIT_IMP_NO_OVERLAP)              \
				break;                                                                       \
			default:                                                                         \
				ANALYTICAL_LIMIT_IMP_OTHERS(SIGN_OP, ANALYTICAL_LIMIT_IMP_OTHERS_NO_OVERLAP) \
		}                                                                                    \
	} else {                                                                                 \
		switch(ATOMstorage(tpe)) {                                                           \
			case TYPE_bit:                                                                   \
				ANALYTICAL_LIMIT_IMP(bit, IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                 \
				break;                                                                       \
			case TYPE_bte:                                                                   \
				ANALYTICAL_LIMIT_IMP(bte, IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                 \
				break;                                                                       \
			case TYPE_sht:                                                                   \
				ANALYTICAL_LIMIT_IMP(sht, IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                 \
				break;                                                                       \
			case TYPE_int:                                                                   \
				ANALYTICAL_LIMIT_IMP(int, IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                 \
				break;                                                                       \
			case TYPE_lng:                                                                   \
				ANALYTICAL_LIMIT_IMP(lng, IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                 \
				break;                                                                       \
			ANALYTICAL_LIMIT_IMP_HUGE(IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                     \
			case TYPE_flt:                                                                   \
				ANALYTICAL_LIMIT_IMP(flt, IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                 \
				break;                                                                       \
			case TYPE_dbl:                                                                   \
				ANALYTICAL_LIMIT_IMP(dbl, IMP, ANALYTICAL_LIMIT_IMP_OVERLAP)                 \
				break;                                                                       \
			default:                                                                         \
				ANALYTICAL_LIMIT_IMP_OTHERS(SIGN_OP, ANALYTICAL_LIMIT_IMP_OTHERS_OVERLAP)    \
		}                                                                                    \
	}                                                                                        \
finish:                                                                                      \
	BATsetcount(r, cnt);                                                                     \
	r->tnonil = !has_nils;                                                                   \
	r->tnil = has_nils;                                                                      \
	return gdk_res;                                                                          \
}

ANALYTICAL_LIMIT(min, MIN, >)
ANALYTICAL_LIMIT(max, MAX, <)

#undef ANALYTICAL_LIMIT
#undef ANALYTICAL_LIMIT_IMP_HUGE
#undef ANALYTICAL_LIMIT_IMP
#undef ANALYTICAL_LIMIT_IMP_OVERLAP
#undef ANALYTICAL_LIMIT_IMP_NO_OVERLAP
#undef ANALYTICAL_LIMIT_IMP_OTHERS
#undef ANALYTICAL_LIMIT_IMP_OTHERS_OVERLAP
#undef ANALYTICAL_LIMIT_IMP_OTHERS_NO_OVERLAP

#define ANALYTICAL_COUNT_IGNORE_NILS_NO_OVERLAP \
	do {                                        \
		for (;rb < rp; rb++)                    \
			*rb = curval;                       \
	} while(0);

#define ANALYTICAL_COUNT_IGNORE_NILS_OVERLAP        \
	do {                                            \
		lng *rs = rb, *fs, *fe;                     \
		for(; rb<rp;rb++) {                         \
			fs = (rb > rs + start) ? rb - start : rs; \
			fe = (rb+end < rp) ? rb + end + 1 : rp; \
			*rb = (fe - fs);                        \
		}                                           \
	} while(0);

#define ANALYTICAL_COUNT_IGNORE_NILS_IMP(IMP) \
	do {                                      \
		lng *rp, *rb, curval = 0;             \
		rb = rp = (lng*)Tloc(r, 0);           \
		if (p) {                              \
			np = pnp = (bit*)Tloc(p, 0);      \
			nend = np + cnt;                  \
			for(; np < nend; np++) {          \
				if (*np) {                    \
					curval = np - pnp;        \
					rp += curval;             \
					IMP                       \
					pnp = np;                 \
				}                             \
			}                                 \
			curval = np - pnp;                \
			rp += curval;                     \
			IMP                               \
		} else {                              \
			curval = cnt;                     \
			rp += curval;                     \
			IMP                               \
		}                                     \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP(TPE) \
	do {                                                   \
		for (;pbp < bp; pbp++)                             \
			curval += !is_##TPE##_nil(*pbp);               \
		for (;rb < rp; rb++)                               \
			*rb = curval;                                  \
		curval = 0;                                        \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP(TPE) \
	do {                                                \
		TPE *bs, *bl, *be;                              \
		bl = pbp;                                       \
		for(; pbp<bp;pbp++) {                           \
			bs = (pbp > bl+start) ? pbp - start : bl; \
			be = (pbp+end < bp) ? pbp + end + 1 : bp;   \
			for(; bs<be; bs++)                          \
				curval += !is_##TPE##_nil(*bs);         \
			*rb = curval;                               \
			rb++;                                       \
			curval = 0;                                 \
		}                                               \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(TPE, IMP) \
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

#define ANALYTICAL_COUNT_NO_NIL_STR_IMP_NO_OVERLAP(TPE_CAST, OFFSET)  \
	do {                                                              \
		for(;j<i;j++)                                                 \
			curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200'; \
		for (;rb < rp; rb++)                                          \
			*rb = curval;                                             \
		curval = 0;                                                   \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_STR_IMP_OVERLAP(TPE_CAST, OFFSET)         \
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

#define ANALYTICAL_COUNT_NO_NIL_STR_IMP(TPE_CAST, OFFSET, IMP) \
	do {                                                       \
		const void *restrict bp = Tloc(b, 0);                  \
		lng *rp, *rb, curval = 0;                              \
		rb = rp = (lng*)Tloc(r, 0);                            \
		if (p) {                                               \
			pnp = np = (bit*)Tloc(p, 0);                       \
			nend = np + cnt;                                   \
			for(; np<nend; np++) {                             \
				if (*np) {                                     \
				    ncnt = (np - pnp);                         \
					rp += ncnt;                                \
					i += ncnt;                                 \
					IMP(TPE_CAST, OFFSET)                      \
					pnp = np;                                  \
				}                                              \
			}                                                  \
			ncnt = (np - pnp);                                 \
			rp += ncnt;                                        \
			i += ncnt;                                         \
			IMP(TPE_CAST, OFFSET)                              \
		} else {                                               \
			rp += cnt;                                         \
			i += cnt;                                          \
			IMP(TPE_CAST, OFFSET)                              \
		}                                                      \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_NO_OVERLAP               \
	do {                                                                \
		for(; j<i; j++)                                                 \
			curval += (*cmp)(nil, base + ((const var_t *) bp)[j]) != 0; \
		for (;rb < rp; rb++)                                            \
			*rb = curval;                                               \
		curval = 0;                                                     \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_NO_OVERLAP \
	do {                                                   \
		for(; j<i; j++)                                    \
			curval += (*cmp)(Tloc(b, j), nil) != 0;        \
		for (;rb < rp; rb++)                               \
			*rb = curval;                                  \
		curval = 0;                                        \
	} while(0);

#define ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_OVERLAP                      \
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

#define ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_OVERLAP \
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

#define ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES(IMP_VARSIZED, IMP_FIXEDSIZE) \
	do {                                                                 \
		const void *restrict nil = ATOMnilptr(tpe);                      \
		int (*cmp)(const void *, const void *) = ATOMcompare(tpe);       \
		lng *rp, *rb, curval = 0;                                        \
		rb = rp = (lng*)Tloc(r, 0);                                      \
		if (b->tvarsized) {                                              \
			const char *restrict base = b->tvheap->base;                 \
			const void *restrict bp = Tloc(b, 0);                        \
			if (p) {                                                     \
				pnp = np = (bit*)Tloc(p, 0);                             \
				nend = np + cnt;                                         \
				for(; np < nend; np++) {                                 \
					if (*np) {                                           \
						ncnt = (np - pnp);                               \
						rp += ncnt;                                      \
						i += ncnt;                                       \
						IMP_VARSIZED                                     \
						pnp = np;                                        \
					}                                                    \
				}                                                        \
				ncnt = (np - pnp);                                       \
				rp += ncnt;                                              \
				i += ncnt;                                               \
				IMP_VARSIZED                                             \
			} else {                                                     \
				rp += cnt;                                               \
				i += cnt;                                                \
				IMP_VARSIZED                                             \
			}                                                            \
		} else {                                                         \
			if (p) {                                                     \
				pnp = np = (bit*)Tloc(p, 0);                             \
				nend = np + cnt;                                         \
				for(; np < nend; np++) {                                 \
					if (*np) {                                           \
						ncnt = (np - pnp);                               \
						rp += ncnt;                                      \
						i += ncnt;                                       \
						IMP_FIXEDSIZE                                    \
						pnp = np;                                        \
					}                                                    \
				}                                                        \
				ncnt = (np - pnp);                                       \
				rp += ncnt;                                              \
				i += ncnt;                                               \
				IMP_FIXEDSIZE                                            \
			} else {                                                     \
				rp += cnt;                                               \
				i += cnt;                                                \
				IMP_FIXEDSIZE                                            \
			}                                                            \
		}                                                                \
	} while(0);

gdk_return
GDKanalyticalcount(BAT *r, BAT *b, BAT *p, BAT *o, const bit *ignore_nils, int tpe, BUN start, BUN end)
{
	BUN i = 0, j = 0, k = 0, l = 0, m = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *nend;
	gdk_return gdk_res = GDK_SUCCEED;

	assert(ignore_nils);
	(void) o;
	if(!*ignore_nils || b->T.nonil) {
		if(start == 0 && end == 0) {
			ANALYTICAL_COUNT_IGNORE_NILS_IMP(ANALYTICAL_COUNT_IGNORE_NILS_NO_OVERLAP)
		} else {
			ANALYTICAL_COUNT_IGNORE_NILS_IMP(ANALYTICAL_COUNT_IGNORE_NILS_OVERLAP)
		}
	} else if(start == 0 && end == 0) {
		switch (tpe) {
			case TYPE_bit:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(bit, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
			case TYPE_bte:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(bte, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
			case TYPE_sht:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(sht, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
			case TYPE_int:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(int, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
			case TYPE_lng:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(lng, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(hge, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
#endif
			case TYPE_flt:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(flt, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
			case TYPE_dbl:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(dbl, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP)
				break;
			case TYPE_str: {
				const char *restrict base = b->tvheap->base;
				switch (b->twidth) {
					case 1:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned char *, [j] + GDK_VAROFFSET, ANALYTICAL_COUNT_NO_NIL_STR_IMP_NO_OVERLAP)
						break;
					case 2:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned short *, [j] + GDK_VAROFFSET, ANALYTICAL_COUNT_NO_NIL_STR_IMP_NO_OVERLAP)
						break;
#if SIZEOF_VAR_T != SIZEOF_INT
					case 4:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned int *, [j], ANALYTICAL_COUNT_NO_NIL_STR_IMP_NO_OVERLAP)
						break;
#endif
					default:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const var_t *, [j], ANALYTICAL_COUNT_NO_NIL_STR_IMP_NO_OVERLAP)
						break;
				}
				break;
			}
			default:
				ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES(ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_NO_OVERLAP, ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_NO_OVERLAP)
		}
	} else {
		switch (tpe) {
			case TYPE_bit:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(bit, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
			case TYPE_bte:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(bte, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
			case TYPE_sht:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(sht, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
			case TYPE_int:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(int, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
			case TYPE_lng:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(lng, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(hge, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
#endif
			case TYPE_flt:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(flt, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
			case TYPE_dbl:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(dbl, ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP)
				break;
			case TYPE_str: {
				const char *restrict base = b->tvheap->base;
				switch (b->twidth) {
					case 1:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned char *, [j] + GDK_VAROFFSET, ANALYTICAL_COUNT_NO_NIL_STR_IMP_OVERLAP)
						break;
					case 2:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned short *, [j] + GDK_VAROFFSET, ANALYTICAL_COUNT_NO_NIL_STR_IMP_OVERLAP)
						break;
#if SIZEOF_VAR_T != SIZEOF_INT
					case 4:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned int *, [j], ANALYTICAL_COUNT_NO_NIL_STR_IMP_OVERLAP)
						break;
#endif
					default:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const var_t *, [j], ANALYTICAL_COUNT_NO_NIL_STR_IMP_OVERLAP)
						break;
				}
				break;
			}
			default:
				ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES(ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_OVERLAP, ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_OVERLAP)
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return gdk_res;
}

#undef ANALYTICAL_COUNT_IGNORE_NILS_IMP
#undef ANALYTICAL_COUNT_IGNORE_NILS_OVERLAP
#undef ANALYTICAL_COUNT_IGNORE_NILS_NO_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_NO_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP
#undef ANALYTICAL_COUNT_NO_NIL_STR_IMP_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_STR_IMP_NO_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_STR_IMP
#undef ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_NO_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_NO_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_VARSIZED_TYPES_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_FIXEDSIZE_TYPES_OVERLAP
#undef ANALYTICAL_COUNT_NO_NIL_OTHER_TYPES

#define ANALYTICAL_SUM_NO_OVERLAP(TPE1, TPE2) \
	do {                                      \
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
		else                                  \
			curval = TPE2##_nil;              \
	} while(0);                               \

#define ANALYTICAL_SUM_OVERLAP(TPE1, TPE2)            \
	do {                                              \
		TPE1 *bs, *bl, *be;                           \
		bl = pbp;                                     \
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

#define ANALYTICAL_SUM_IMP(TPE1, TPE2, IMP)        \
	do {                                           \
		TPE1 *pbp, *bp, v;                         \
		TPE2 *rp, *rb, *rend, curval = TPE2##_nil; \
		pbp = bp = (TPE1*)Tloc(b, 0);              \
		rb = rp = (TPE2*)Tloc(r, 0);               \
		rend = rp + cnt;                           \
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
		} else if (o || force_order) {             \
			ncnt = cnt;                            \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2)                        \
		} else {                                   \
			for(; rb<rend; rb++, bp++) {           \
				v = *bp;                           \
				if(is_##TPE1##_nil(v)) {           \
					*rb = TPE2##_nil;              \
					has_nils = true;               \
				} else {                           \
					*rb = (TPE2) v;                \
				}                                  \
			}                                      \
		}                                          \
		goto finish;                               \
	} while(0);

#define ANALYTICAL_SUM_FP_NO_OVERLAP(TPE1, TPE2)     \
	do {                                             \
		if(dofsum(pbp, 0, 0, ncnt, rb, 1, TYPE_##TPE1, TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true, false, true, \
				  "GDKanalyticalsum") == BUN_NONE) { \
			goto bailout;                            \
		}                                            \
		curval = *rb;                                \
		for (;rb < rp; rb++)                         \
			*rb = curval;                            \
		if(is_##TPE2##_nil(curval))                  \
			has_nils = true;                         \
	} while(0);

#define ANALYTICAL_SUM_FP_OVERLAP(TPE1, TPE2)            \
	do {                                                 \
		TPE1 *bs, *bl, *be;                              \
		bl = pbp;                                        \
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

gdk_return
GDKanalyticalsum(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tp1, int tp2, BUN start, BUN end)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0;
	bit *np, *pnp, *nend;
	int abort_on_error = 1;

	if(start == 0 && end == 0) {
		switch (tp2) {
			case TYPE_bte: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, bte, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_sht: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, sht, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, sht, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_int: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, int, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, int, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_SUM_IMP(int, int, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_lng: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, lng, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, lng, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_SUM_IMP(int, lng, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_lng:
						ANALYTICAL_SUM_IMP(lng, lng, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#ifdef HAVE_HGE
			case TYPE_hge: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, hge, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, hge, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_SUM_IMP(int, hge, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_lng:
						ANALYTICAL_SUM_IMP(lng, hge, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					case TYPE_hge:
						ANALYTICAL_SUM_IMP(hge, hge, ANALYTICAL_SUM_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#endif
			case TYPE_flt: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_SUM_IMP(flt, flt, ANALYTICAL_SUM_FP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			case TYPE_dbl: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_SUM_IMP(flt, dbl, ANALYTICAL_SUM_FP_NO_OVERLAP);
						break;
					case TYPE_dbl:
						ANALYTICAL_SUM_IMP(dbl, dbl, ANALYTICAL_SUM_FP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			default:
				goto nosupport;
		}
	} else {
		switch (tp2) {
			case TYPE_bte: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, bte, ANALYTICAL_SUM_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_sht: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, sht, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, sht, ANALYTICAL_SUM_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_int: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, int, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, int, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_SUM_IMP(int, int, ANALYTICAL_SUM_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_lng: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, lng, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, lng, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_SUM_IMP(int, lng, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_lng:
						ANALYTICAL_SUM_IMP(lng, lng, ANALYTICAL_SUM_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#ifdef HAVE_HGE
			case TYPE_hge: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_SUM_IMP(bte, hge, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_SUM_IMP(sht, hge, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_SUM_IMP(int, hge, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_lng:
						ANALYTICAL_SUM_IMP(lng, hge, ANALYTICAL_SUM_OVERLAP);
						break;
					case TYPE_hge:
						ANALYTICAL_SUM_IMP(hge, hge, ANALYTICAL_SUM_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#endif
			case TYPE_flt: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_SUM_IMP(flt, flt, ANALYTICAL_SUM_FP_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			case TYPE_dbl: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_SUM_IMP(flt, dbl, ANALYTICAL_SUM_FP_OVERLAP);
						break;
					case TYPE_dbl:
						ANALYTICAL_SUM_IMP(dbl, dbl, ANALYTICAL_SUM_FP_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			default:
				goto nosupport;
		}
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

#undef ANALYTICAL_SUM_IMP
#undef ANALYTICAL_SUM_NO_OVERLAP
#undef ANALYTICAL_SUM_OVERLAP
#undef ANALYTICAL_SUM_FP_NO_OVERLAP
#undef ANALYTICAL_SUM_FP_OVERLAP

#define ANALYTICAL_PROD_IMP_NO_OVERLAP(TPE1, TPE2, TPE3) \
	do {                                                 \
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

#define ANALYTICAL_PROD_IMP_OVERLAP(TPE1, TPE2, TPE3) \
	do {                                              \
		TPE1 *bs, *bl, *be;                           \
		bl = pbp;                                     \
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

#define ANALYTICAL_PROD_IMP(TPE1, TPE2, TPE3, IMP) \
	do {                                           \
		TPE1 *pbp, *bp, v;                         \
		TPE2 *rp, *rb, *rend, curval = TPE2##_nil; \
		pbp = bp = (TPE1*)Tloc(b, 0);              \
		rb = rp = (TPE2*)Tloc(r, 0);               \
		rend = rb + cnt;                           \
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
		} else if (o || force_order) {             \
			ncnt = cnt;                            \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2, TPE3)                  \
		} else {                                   \
			for(; rb<rend; rb++, bp++)  {          \
				v = *bp;                           \
				if(is_##TPE1##_nil(v)) {           \
					*rb = TPE2##_nil;              \
					has_nils = true;               \
				} else {                           \
					*rb = (TPE2) v;                \
				}                                  \
			}                                      \
		}                                          \
		goto finish;                               \
	} while(0);

#define ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP(TPE1, TPE2, REAL_IMP) \
	do {                                                           \
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

#define ANALYTICAL_PROD_IMP_LIMIT_OVERLAP(TPE1, TPE2, REAL_IMP) \
	do {                                                        \
		TPE1 *bs, *bl, *be;                                     \
		bl = pbp;                                               \
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

#define ANALYTICAL_PROD_IMP_LIMIT(TPE1, TPE2, IMP, REAL_IMP) \
	do {                                                     \
		TPE1 *pbp, *bp, v;                                   \
		TPE2 *rp, *rb, *rend, curval = TPE2##_nil;           \
		pbp = bp = (TPE1*)Tloc(b, 0);                        \
		rb = rp = (TPE2*)Tloc(r, 0);                         \
		rend = rp + cnt;                                     \
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
		} else if (o || force_order) {                       \
			ncnt = cnt;                                      \
			bp += ncnt;                                      \
			rp += ncnt;                                      \
			IMP(TPE1, TPE2, REAL_IMP)                        \
		} else {                                             \
			for(; rp<rend; rp++, bp++)  {                    \
				v = *bp;                                     \
				if(is_##TPE1##_nil(v)) {                     \
					*rp = TPE2##_nil;                        \
					has_nils = true;                         \
				} else {                                     \
					*rp = (TPE2) v;                          \
				}                                            \
			}                                                \
		}                                                    \
		goto finish;                                         \
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

#define ANALYTICAL_PROD_IMP_FP_NO_OVERLAP(TPE1, TPE2)       \
	do {                                                    \
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

#define ANALYTICAL_PROD_IMP_FP_OVERLAP(TPE1, TPE2)              \
	do {                                                        \
		TPE1 *bs, *bl, *be;                                     \
		bl = pbp;                                               \
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

#define ANALYTICAL_PROD_IMP_FP(TPE1, TPE2, IMP)    \
	do {                                           \
		TPE1 *pbp, *bp, v;                         \
		TPE2 *rp, *rb, *rend, curval = TPE2##_nil; \
		pbp = bp = (TPE1*)Tloc(b, 0);              \
		rb = rp = (TPE2*)Tloc(r, 0);               \
		rend = rp + cnt;                           \
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
		} else if (o || force_order) {             \
			ncnt = cnt;                            \
			bp += ncnt;                            \
			rp += ncnt;                            \
			IMP(TPE1, TPE2)                        \
		} else {                                   \
			for(; rp<rend; rp++, bp++) {           \
				v = *bp;                           \
				if(is_##TPE1##_nil(v)) {           \
					*rp = TPE2##_nil;              \
					has_nils = true;               \
				} else {                           \
					*rp = (TPE2) v;                \
				}                                  \
			}                                      \
		}                                          \
		goto finish;                               \
	} while(0);

gdk_return
GDKanalyticalprod(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tp1, int tp2, BUN start, BUN end)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0;
	bit *pnp, *np, *nend;
	int abort_on_error = 1;

	if(start == 0 && end == 0) {
		switch (tp2) {
			case TYPE_bte: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, bte, sht, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_sht: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, sht, int, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP(sht, sht, int, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_int: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, int, lng, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP(sht, int, lng, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP(int, int, lng, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#ifdef HAVE_HGE
			case TYPE_lng: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, lng, hge, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP(sht, lng, hge, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP(int, lng, hge, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					case TYPE_lng:
						ANALYTICAL_PROD_IMP(lng, lng, hge, ANALYTICAL_PROD_IMP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_hge: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP_LIMIT(bte, hge, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP_LIMIT(sht, hge, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP_LIMIT(int, hge, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_lng:
						ANALYTICAL_PROD_IMP_LIMIT(lng, hge, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_hge:
						ANALYTICAL_PROD_IMP_LIMIT(hge, hge, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, HGEMUL_CHECK);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#else
			case TYPE_lng: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP_LIMIT(bte, lng, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, LNGMUL_CHECK);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP_LIMIT(sht, lng, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, LNGMUL_CHECK);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP_LIMIT(int, lng, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, LNGMUL_CHECK);
						break;
					case TYPE_lng:
						ANALYTICAL_PROD_IMP_LIMIT(lng, lng, ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP, LNGMUL_CHECK);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#endif
			case TYPE_flt: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_PROD_IMP_FP(flt, flt, ANALYTICAL_PROD_IMP_FP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			case TYPE_dbl: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_PROD_IMP_FP(flt, dbl, ANALYTICAL_PROD_IMP_FP_NO_OVERLAP);
						break;
					case TYPE_dbl:
						ANALYTICAL_PROD_IMP_FP(dbl, dbl, ANALYTICAL_PROD_IMP_FP_NO_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			default:
				goto nosupport;
		}
	} else {
		switch (tp2) {
			case TYPE_bte: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, bte, sht, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_sht: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, sht, int, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP(sht, sht, int, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_int: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, int, lng, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP(sht, int, lng, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP(int, int, lng, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#ifdef HAVE_HGE
			case TYPE_lng: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP(bte, lng, hge, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP(sht, lng, hge, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP(int, lng, hge, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					case TYPE_lng:
						ANALYTICAL_PROD_IMP(lng, lng, hge, ANALYTICAL_PROD_IMP_OVERLAP);
						break;
					default:
						goto nosupport;
				}
				break;
			}
			case TYPE_hge: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP_LIMIT(bte, hge, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP_LIMIT(sht, hge, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP_LIMIT(int, hge, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_lng:
						ANALYTICAL_PROD_IMP_LIMIT(lng, hge, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, HGEMUL_CHECK);
						break;
					case TYPE_hge:
						ANALYTICAL_PROD_IMP_LIMIT(hge, hge, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, HGEMUL_CHECK);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#else
			case TYPE_lng: {
				switch (tp1) {
					case TYPE_bte:
						ANALYTICAL_PROD_IMP_LIMIT(bte, lng, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, LNGMUL_CHECK);
						break;
					case TYPE_sht:
						ANALYTICAL_PROD_IMP_LIMIT(sht, lng, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, LNGMUL_CHECK);
						break;
					case TYPE_int:
						ANALYTICAL_PROD_IMP_LIMIT(int, lng, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, LNGMUL_CHECK);
						break;
					case TYPE_lng:
						ANALYTICAL_PROD_IMP_LIMIT(lng, lng, ANALYTICAL_PROD_IMP_LIMIT_OVERLAP, LNGMUL_CHECK);
						break;
					default:
						goto nosupport;
				}
				break;
			}
#endif
			case TYPE_flt: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_PROD_IMP_FP(flt, flt, ANALYTICAL_PROD_IMP_FP_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			case TYPE_dbl: {
				switch (tp1) {
					case TYPE_flt:
						ANALYTICAL_PROD_IMP_FP(flt, dbl, ANALYTICAL_PROD_IMP_FP_OVERLAP);
						break;
					case TYPE_dbl:
						ANALYTICAL_PROD_IMP_FP(dbl, dbl, ANALYTICAL_PROD_IMP_FP_OVERLAP);
						break;
					default:
						goto nosupport;
						break;
				}
			}
			default:
				goto nosupport;
		}
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

#undef ANALYTICAL_PROD_IMP_OVERLAP
#undef ANALYTICAL_PROD_IMP_NO_OVERLAP
#undef ANALYTICAL_PROD_IMP
#undef ANALYTICAL_PROD_IMP_LIMIT_OVERLAP
#undef ANALYTICAL_PROD_IMP_LIMIT_NO_OVERLAP
#undef ANALYTICAL_PROD_IMP_LIMIT
#undef ANALYTICAL_PROD_IMP_FP_OVERLAP
#undef ANALYTICAL_PROD_IMP_FP_NO_OVERLAP
#undef ANALYTICAL_PROD_IMP_FP
#undef ANALYTICAL_PROD_IMP_FP_REAL

#define ANALYTICAL_AVERAGE_NO_OVERLAP(TPE,lng_hge,LABEL)      \
	do {                                                      \
		for(; pbp<bp; pbp++) {                                \
			v = *pbp;                                         \
			if (!is_##TPE##_nil(v)) {                         \
				ADD_WITH_CHECK(TPE, v, lng_hge, sum, lng_hge, \
							   sum, GDK_##lng_hge##_max,      \
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

#define ANALYTICAL_AVERAGE_OVERLAP(TPE,lng_hge,LABEL)             \
	do {                                                          \
		TPE *bs, *bl, *be;                                        \
		bl = pbp;                                                 \
		for(; pbp<bp;pbp++) {                                     \
			bs = (pbp > bl+start) ? pbp - start : bl;             \
			be = (pbp+end < bp) ? pbp + end + 1 : bp;             \
			for(; bs<be; bs++) {                                  \
				v = *bs;                                          \
				if (!is_##TPE##_nil(v)) {                         \
					ADD_WITH_CHECK(TPE, v, lng_hge, sum, lng_hge, \
								   sum, GDK_##lng_hge##_max,      \
								   goto avg_overflow##TPE##LABEL##overlap); \
					/* count only when no overflow occurs */      \
					n++;                                          \
				}                                                 \
			}                                                     \
			if(0) {                                               \
avg_overflow##TPE##LABEL##overlap:                                \
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
				goto calc_done##TPE##LABEL##overlap;              \
			}                                                     \
			curval = n > 0 ? (dbl) sum / n : dbl_nil;             \
calc_done##TPE##LABEL##overlap:                                   \
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
#define ANALYTICAL_AVERAGE(TYPE,IMP) ANALYTICAL_AVERAGE_LNG_HGE(TYPE,hge,IMP)
#else
#define ANALYTICAL_AVERAGE(TYPE,IMP) ANALYTICAL_AVERAGE_LNG_HGE(TYPE,lng,IMP)
#endif

#define ANALYTICAL_AVERAGE_FLOAT_NO_OVERLAP(TPE)  \
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

#define ANALYTICAL_AVERAGE_FLOAT_OVERLAP(TPE)         \
	do {                                              \
		TPE *bs, *bl, *be;                            \
		bl = pbp;                                     \
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

#define ANALYTICAL_AVERAGE_FLOAT(TPE, IMP) \
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

gdk_return
GDKanalyticalavg(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tpe, BUN start, BUN end)
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

	if(start == 0 && end == 0) {
		switch (tpe) {
			case TYPE_bte:
				ANALYTICAL_AVERAGE(bte, ANALYTICAL_AVERAGE_NO_OVERLAP);
				break;
			case TYPE_sht:
				ANALYTICAL_AVERAGE(sht, ANALYTICAL_AVERAGE_NO_OVERLAP);
				break;
			case TYPE_int:
				ANALYTICAL_AVERAGE(int, ANALYTICAL_AVERAGE_NO_OVERLAP);
				break;
			case TYPE_lng:
				ANALYTICAL_AVERAGE(lng, ANALYTICAL_AVERAGE_NO_OVERLAP);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				ANALYTICAL_AVERAGE(hge, ANALYTICAL_AVERAGE_NO_OVERLAP);
				break;
#endif
			case TYPE_flt:
				ANALYTICAL_AVERAGE_FLOAT(flt, ANALYTICAL_AVERAGE_FLOAT_NO_OVERLAP);
				break;
			case TYPE_dbl:
				ANALYTICAL_AVERAGE_FLOAT(dbl, ANALYTICAL_AVERAGE_FLOAT_NO_OVERLAP);
				break;
			default:
				GDKerror("GDKanalyticalavg: average of type %s unsupported.\n", ATOMname(tpe));
				return GDK_FAIL;
		}
	} else {
		switch (tpe) {
			case TYPE_bte:
				ANALYTICAL_AVERAGE(bte, ANALYTICAL_AVERAGE_OVERLAP);
				break;
			case TYPE_sht:
				ANALYTICAL_AVERAGE(sht, ANALYTICAL_AVERAGE_OVERLAP);
				break;
			case TYPE_int:
				ANALYTICAL_AVERAGE(int, ANALYTICAL_AVERAGE_OVERLAP);
				break;
			case TYPE_lng:
				ANALYTICAL_AVERAGE(lng, ANALYTICAL_AVERAGE_OVERLAP);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				ANALYTICAL_AVERAGE(hge, ANALYTICAL_AVERAGE_OVERLAP);
				break;
#endif
			case TYPE_flt:
				ANALYTICAL_AVERAGE_FLOAT(flt, ANALYTICAL_AVERAGE_FLOAT_OVERLAP);
				break;
			case TYPE_dbl:
				ANALYTICAL_AVERAGE_FLOAT(dbl, ANALYTICAL_AVERAGE_FLOAT_OVERLAP);
				break;
			default:
				GDKerror("GDKanalyticalavg: average of type %s unsupported.\n", ATOMname(tpe));
				return GDK_FAIL;
		}
	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_AVERAGE
#undef ANALYTICAL_AVERAGE_LNG_HGE
#undef ANALYTICAL_AVERAGE_OVERLAP
#undef ANALYTICAL_AVERAGE_NO_OVERLAP
#undef ANALYTICAL_AVERAGE_FLOAT
#undef ANALYTICAL_AVERAGE_FLOAT_OVERLAP
#undef ANALYTICAL_AVERAGE_FLOAT_NO_OVERLAP
