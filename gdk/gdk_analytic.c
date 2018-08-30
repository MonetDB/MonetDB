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

#define ANALYTICAL_LIMIT_IMP(TPE, OP)                        \
	do {                                                     \
		TPE *rp, *rb, *pbp, *bp, *rend, curval, v;           \
		rb = rp = (TPE*)Tloc(r, 0);                          \
		pbp = bp = (TPE*)Tloc(b, 0);                         \
		curval = *bp;                                        \
		rend = rp + cnt;                                     \
		if (p) {                                             \
			pnp = np = (bit*)Tloc(p, 0);                     \
			end = np + cnt;                                  \
			for(; np<end; np++) {                            \
				if (*np) {                                   \
					ncnt = (np - pnp);                       \
					rp += ncnt;                              \
					bp += ncnt;                              \
					for(; pbp<bp; pbp++) {                   \
						v = *pbp;                            \
						if(!is_##TPE##_nil(v)) {             \
							if(is_##TPE##_nil(curval))       \
								curval = v;                  \
							else                             \
								curval = OP(v, curval);      \
						}                                    \
					}                                        \
					if(is_##TPE##_nil(curval))               \
						has_nils = true;                     \
					for (;rb < rp; rb++)                     \
						*rb = curval;                        \
					curval = *bp;                            \
					pnp = np;                                \
					pbp = bp;                                \
				}                                            \
			}                                                \
			ncnt = (np - pnp);                               \
			rp += ncnt;                                      \
			bp += ncnt;                                      \
			for(; pbp<bp; pbp++) {                           \
				v = *pbp;                                    \
				if(!is_##TPE##_nil(v)) {                     \
					if(is_##TPE##_nil(curval))               \
						curval = v;                          \
					else                                     \
						curval = OP(v, curval);              \
				}                                            \
			}                                                \
			if(is_##TPE##_nil(curval))                       \
				has_nils = true;                             \
			for (;rb < rp; rb++)                             \
				*rb = curval;                                \
		} else if (o || force_order) {                       \
			TPE *bend = bp + cnt;                            \
			for(; bp<bend; bp++) {                           \
				v = *bp;                                     \
				if(!is_##TPE##_nil(v)) {                     \
					if(is_##TPE##_nil(curval))               \
						curval = v;                          \
					else                                     \
						curval = OP(v, curval);              \
				}                                            \
			}                                                \
			if(is_##TPE##_nil(curval))                       \
				has_nils = true;                             \
			for(;rb<rend; rb++)                              \
				*rb = curval;                                \
		} else { /* single value, ie no ordering */          \
			for(; rp<rend; rp++, bp++) {                     \
				v = *bp;                                     \
				if(is_##TPE##_nil(v))                        \
					has_nils = true;                         \
				*rp = v;                                     \
			}                                                \
		}                                                    \
	} while(0);

#ifdef HAVE_HUGE
#define ANALYTICAL_LIMIT_IMP_HUGE(IMP) \
	case TYPE_hge:                     \
		ANALYTICAL_LIMIT_IMP(hge, IMP) \
	break;
#else
#define ANALYTICAL_LIMIT_IMP_HUGE(IMP)
#endif

#define ANALYTICAL_LIMIT(OP, IMP, SIGN_OP)                                                \
gdk_return                                                                                \
GDKanalytical##OP(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tpe)               \
{                                                                                         \
	int (*atomcmp)(const void *, const void *);                                           \
	const void* restrict nil;                                                             \
	bool has_nils = false;                                                                \
	BUN i, j, ncnt, cnt = BATcount(b);                                                    \
	bit *np, *pnp, *end;                                                                  \
	gdk_return gdk_res = GDK_SUCCEED;                                                     \
                                                                                          \
	switch(ATOMstorage(tpe)) {                                                            \
		case TYPE_bit:                                                                    \
			ANALYTICAL_LIMIT_IMP(bit, IMP)                                                \
			break;                                                                        \
		case TYPE_bte:                                                                    \
			ANALYTICAL_LIMIT_IMP(bte, IMP)                                                \
			break;                                                                        \
		case TYPE_sht:                                                                    \
			ANALYTICAL_LIMIT_IMP(sht, IMP)                                                \
			break;                                                                        \
		case TYPE_int:                                                                    \
			ANALYTICAL_LIMIT_IMP(int, IMP)                                                \
			break;                                                                        \
		case TYPE_lng:                                                                    \
			ANALYTICAL_LIMIT_IMP(lng, IMP)                                                \
			break;                                                                        \
		ANALYTICAL_LIMIT_IMP_HUGE(IMP)                                                    \
		case TYPE_flt:                                                                    \
			ANALYTICAL_LIMIT_IMP(flt, IMP)                                                \
			break;                                                                        \
		case TYPE_dbl:                                                                    \
			ANALYTICAL_LIMIT_IMP(dbl, IMP)                                                \
			break;                                                                        \
		default: {                                                                        \
			BATiter bpi = bat_iterator(b);                                                \
			void *restrict curval = BUNtail(bpi, 0);                                      \
			nil = ATOMnilptr(tpe);                                                        \
			atomcmp = ATOMcompare(tpe);                                                   \
			if (p) {                                                                      \
				np = (bit*)Tloc(p, 0);                                                    \
				for(i=0,j=0; i<cnt; i++, np++) {                                          \
					if (*np) {                                                            \
						if((*atomcmp)(curval, nil) == 0)                                  \
							has_nils = true;                                              \
						for (;j < i; j++) {                                               \
							if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)   \
								goto finish;                                              \
						}                                                                 \
						curval = BUNtail(bpi, i);                                         \
					}                                                                     \
					void *next = BUNtail(bpi, i);                                         \
					if((*atomcmp)(next, nil) != 0) {                                      \
						if((*atomcmp)(curval, nil) == 0)                                  \
							curval = next;                                                \
						else                                                              \
							curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next;     \
					}                                                                     \
				}                                                                         \
				if((*atomcmp)(curval, nil) == 0)                                          \
					has_nils = true;                                                      \
				for (;j < i; j++) {                                                       \
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)           \
					   goto finish;                                                       \
				}                                                                         \
			} else if (o || force_order) {                                                \
				for(i=0; i<cnt; i++) {                                                    \
					void *next = BUNtail(bpi, i);                                         \
						if((*atomcmp)(next, nil) != 0) {                                  \
							if((*atomcmp)(curval, nil) == 0)                              \
								curval = next;                                            \
							else                                                          \
								curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next; \
						}                                                                 \
				}                                                                         \
				if((*atomcmp)(curval, nil) == 0)                                          \
					has_nils = true;                                                      \
				for (j=0; j < i; j++) {                                                   \
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)           \
						goto finish;                                                      \
				}                                                                         \
			} else { /* single value, ie no ordering */                                   \
				for(i=0; i<cnt; i++) {                                                    \
					void *next = BUNtail(bpi, i);                                         \
					if((*atomcmp)(next, nil) == 0)                                        \
						has_nils = true;                                                  \
					if ((gdk_res = BUNappend(r, next, false)) != GDK_SUCCEED)             \
						goto finish;                                                      \
				}                                                                         \
			}                                                                             \
		}                                                                                 \
	}                                                                                     \
finish:                                                                                   \
	BATsetcount(r, cnt);                                                                  \
	r->tnonil = !has_nils;                                                                \
	r->tnil = has_nils;                                                                   \
	return gdk_res;                                                                       \
}

ANALYTICAL_LIMIT(min, MIN, >)
ANALYTICAL_LIMIT(max, MAX, <)

#undef ANALYTICAL_LIMIT
#undef ANALYTICAL_LIMIT_IMP_HUGE
#undef ANALYTICAL_LIMIT_IMP

#define ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(TPE)    \
	do {                                                 \
		TPE *pnb, *bp = (TPE*)Tloc(b, 0);                \
		lng *rp, *rb, *rend, curval = 0;                 \
		rb = rp = (lng*)Tloc(r, 0);                      \
		rend = rp + cnt;                                 \
		pnb = bp;                                        \
		if (p) {                                         \
			pnp = np = (bit*)Tloc(p, 0);                 \
			end = np + cnt;                              \
			for(; np<end; np++) {                        \
				if (*np) {                               \
					BUN ncnt = np - pnp;                 \
					bp += ncnt;                          \
					for (;pnb < bp; pnb++)               \
						curval += !is_##TPE##_nil(*pnb); \
					rp += ncnt;                          \
					for (;rb < rp; rb++)                 \
						*rb = curval;                    \
					curval = 0;                          \
					pnp = np;                            \
					pnb = bp;                            \
				}                                        \
			}                                            \
			bp += (np - pnp);                            \
			for (;pnb < bp; pnb++)                       \
				curval += !is_##TPE##_nil(*pnb);         \
			for (;rb < rend; rb++)                       \
				*rb = curval;                            \
		} else { /* single value, ie no partitions */    \
			for(; rp<rend; rp++, bp++)                   \
				curval += !is_##TPE##_nil(*bp);          \
			for(;rb < rp; rb++)                          \
				*rb = curval;                            \
		}                                                \
	} while(0);

#define ANALYTICAL_COUNT_WITH_NIL_STR_IMP(TPE_CAST, OFFSET)               \
	do {                                                                  \
		const void *restrict bp = Tloc(b, 0);                             \
		lng *rp, *rb, curval = 0;                                         \
		rb = rp = (lng*)Tloc(r, 0);                                       \
		if (p) {                                                          \
			pnp = np = (bit*)Tloc(p, 0);                                  \
			end = np + cnt;                                               \
			for(i = 0; i < cnt; i++, np++) {                              \
				if (*np) {                                                \
					rp += (np - pnp);                                     \
					for (;rb < rp; rb++)                                  \
						*rb = curval;                                     \
					curval = 0;                                           \
					pnp = np;                                             \
				}                                                         \
				curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200'; \
			}                                                             \
			rp += (np - pnp);                                             \
			for (;rb < rp; rb++)                                          \
				*rb = curval;                                             \
		} else { /* single value, ie no partitions */                     \
			for(i = 0; i < cnt; i++)                                      \
				curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200'; \
			rp += cnt;                                                    \
			for(;rb < rp; rb++)                                           \
				*rb = curval;                                             \
		}                                                                 \
	} while(0);

gdk_return
GDKanalyticalcount(BAT *r, BAT *b, BAT *p, BAT *o, const bit *ignore_nils, int tpe)
{
	BUN i, cnt = BATcount(b);
	bit *np, *pnp, *end;
	gdk_return gdk_res = GDK_SUCCEED;

	assert(ignore_nils);
	(void) o;
	if(!*ignore_nils || b->T.nonil) {
		lng *rp, *rb, curval = 0;
		rb = rp = (lng*)Tloc(r, 0);
		if (p) {
			np = pnp = (bit*)Tloc(p, 0);
			end = np + cnt;
			for(; np < end; np++) {
				if (*np) {
					curval = np - pnp;
					rp += curval;
					for (;rb < rp; rb++)
						*rb = curval;
					pnp = np;
				}
			}
			curval = np - pnp;
			rp += curval;
			for (;rb < rp; rb++)
				*rb = curval;
		} else { /* single value */
			lng* lend = rp + cnt;
			for(; rp < lend; rp++)
				*rp = cnt;
		}
	} else {
		switch (tpe) {
			case TYPE_bit:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(bit)
				break;
			case TYPE_bte:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(bte)
				break;
			case TYPE_sht:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(sht)
				break;
			case TYPE_int:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(int)
				break;
			case TYPE_lng:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(lng)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(hge)
				break;
#endif
			case TYPE_flt:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(flt)
				break;
			case TYPE_dbl:
				ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(dbl)
				break;
			case TYPE_str: {
				const char *restrict base = b->tvheap->base;
				switch (b->twidth) {
					case 1:
						ANALYTICAL_COUNT_WITH_NIL_STR_IMP(const unsigned char *, [i] + GDK_VAROFFSET)
						break;
					case 2:
						ANALYTICAL_COUNT_WITH_NIL_STR_IMP(const unsigned short *, [i] + GDK_VAROFFSET)
						break;
#if SIZEOF_VAR_T != SIZEOF_INT
					case 4:
						ANALYTICAL_COUNT_WITH_NIL_STR_IMP(const unsigned int *, [i])
						break;
#endif
					default:
						ANALYTICAL_COUNT_WITH_NIL_STR_IMP(const var_t *, [i])
						break;
				}
				break;
			}
			default: {
				const void *restrict nil = ATOMnilptr(tpe);
				int (*cmp)(const void *, const void *) = ATOMcompare(tpe);
				lng *rp, *rb, curval = 0;
				rb = rp = (lng*)Tloc(r, 0);
				if (b->tvarsized) {
					const char *restrict base = b->tvheap->base;
					const void *restrict bp = Tloc(b, 0);
					if (p) {
						pnp = np = (bit*)Tloc(p, 0);
						for(i = 0; i < cnt; i++, np++) {
							if (*np) {
								rp += (np - pnp);
								for (;rb < rp; rb++)
									*rb = curval;
								curval = 0;
								pnp = np;
							}
							curval += (*cmp)(nil, base + ((const var_t *) bp)[i]) != 0;
						}
						for (;rb < rp; rb++)
							*rb = curval;
					} else { /* single value, ie no partitions */
						for(i = 0; i < cnt; i++)
							curval += (*cmp)(nil, base + ((const var_t *) bp)[i]) != 0;
						rp += cnt;
						for(;rb < rp; rb++)
							*rb = curval;
					}
				} else {
					if (p) {
						pnp = np = (bit*)Tloc(p, 0);
						for(i = 0; i < cnt; i++, np++) {
							if (*np) {
								rp += (np - pnp);
								for (;rb < rp; rb++)
									*rb = curval;
								curval = 0;
								pnp = np;
							}
							curval += (*cmp)(Tloc(b, i), nil) != 0;
						}
						for (;rb < rp; rb++)
							*rb = curval;
					} else { /* single value, ie no partitions */
						for(i = 0; i < cnt; i++)
							curval += (*cmp)(Tloc(b, i), nil) != 0;
						rp += cnt;
						for(;rb < rp; rb++)
							*rb = curval;
					}
				}
			}
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return gdk_res;
}

#undef ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP

#define ANALYTICAL_SUM_IMP(TPE1, TPE2)                          \
	do {                                                        \
		TPE1 *pbp, *bp, v;                                      \
		TPE2 *rp, *rb, *rend, curval = TPE2##_nil;              \
		pbp = bp = (TPE1*)Tloc(b, 0);                           \
		rb = rp = (TPE2*)Tloc(r, 0);                            \
		rend = rp + cnt;                                        \
		if (p) {                                                \
			pnp = np = (bit*)Tloc(p, 0);                        \
			end = np + cnt;                                     \
			for(; np<end; np++) {                               \
				if (*np) {                                      \
					ncnt = (np - pnp);                          \
					bp += ncnt;                                 \
					for(; pbp<bp;pbp++) {                       \
						v = *pbp;                               \
						if (!is_##TPE1##_nil(v)) {              \
							if(is_##TPE2##_nil(curval))         \
								curval = (TPE2) v;              \
							else                                \
								ADD_WITH_CHECK(TPE1, v, TPE2,   \
									curval, TPE2, curval,       \
									GDK_##TPE2##_max,           \
									goto calc_overflow);        \
						}                                       \
					}                                           \
					rp += ncnt;                                 \
					for (;rb < rp; rb++)                        \
						*rb = curval;                           \
					if(is_##TPE2##_nil(curval))                 \
						has_nils = true;                        \
					else                                        \
						curval = TPE2##_nil;                    \
					pnp = np;                                   \
					pbp = bp;                                   \
				}                                               \
			}                                                   \
			ncnt = (np - pnp);                                  \
			bp += ncnt;                                         \
			for(; pbp<bp;pbp++) {                               \
				v = *pbp;                                       \
				if (!is_##TPE1##_nil(v)) {                      \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) v;                      \
					else                                        \
						ADD_WITH_CHECK(TPE1, v, TPE2, curval,   \
							TPE2, curval, GDK_##TPE2##_max,     \
							goto calc_overflow);                \
				}                                               \
			}                                                   \
			rp += ncnt;                                         \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
			for (;rb < rp; rb++)                                \
				*rb = curval;                                   \
		} else if (o || force_order) {                          \
			TPE1 *bend = bp + cnt;                              \
			rp += cnt;                                          \
			for(; bp<bend; bp++) {                              \
				v = *bp;                                        \
				if(!is_##TPE1##_nil(v)) {                       \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) v;                      \
					else                                        \
						ADD_WITH_CHECK(TPE1, v, TPE2, curval,   \
										TPE2, curval,           \
										GDK_##TPE2##_max,       \
										goto calc_overflow);    \
				}                                               \
			}                                                   \
			for(;rb < rp; rb++)                                 \
				*rb = curval;                                   \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
		} else { /* single value, ie no ordering */             \
			for(; rp<rend; rp++, bp++) {                        \
				v = *bp;                                        \
				if(is_##TPE1##_nil(v)) {                        \
					*rp = TPE2##_nil;                           \
					has_nils = true;                            \
				} else {                                        \
					*rp = (TPE2) v;                             \
				}                                               \
			}                                                   \
		}                                                       \
		goto finish;                                            \
	} while(0);

#define ANALYTICAL_SUM_FP_IMP(TPE1, TPE2)                             \
	do {                                                              \
		TPE1 *bp, *pbp;                                               \
		TPE2 *rp, *rb, curval = TPE2##_nil;                           \
		bp = pbp = (TPE1*)Tloc(b, 0);                                 \
		rb = rp = (TPE2*)Tloc(r, 0);                                  \
		if (p) {                                                      \
			pnp = np = (bit*)Tloc(p, 0);                              \
			end = np + cnt;                                           \
			for(; np<end; np++) {                                     \
				if (*np) {                                            \
					ncnt = np - pnp;                                  \
					bp += ncnt;                                       \
					rp += ncnt;                                       \
					if(dofsum(pbp, 0, 0, ncnt, rb, 1, TYPE_##TPE1, TYPE_##TPE2, \
							  NULL, NULL, NULL, 0, 0, true, false, true,        \
							  "GDKanalyticalsum") == BUN_NONE) {                \
						goto bailout;                                 \
					}                                                 \
					curval = *rb;                                     \
					for (;rb < rp; rb++)                              \
						*rb = curval;                                 \
					if(is_##TPE2##_nil(curval))                       \
						has_nils = true;                              \
					pnp = np;                                         \
					pbp = bp;                                         \
				}                                                     \
			}                                                         \
			ncnt = np - pnp;                                          \
			rp += ncnt;                                               \
			if(dofsum(pbp, 0, 0, ncnt, rb, 1, TYPE_##TPE1,            \
					  TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true,      \
					  false, true, "GDKanalyticalsum") == BUN_NONE) { \
				goto bailout;                                         \
			}                                                         \
			curval = *rb;                                             \
			if(is_##TPE2##_nil(curval))                               \
				has_nils = true;                                      \
			for (;rb < rp; rb++)                                      \
				*rb = curval;                                         \
		} else if (o || force_order) {                                \
			TPE2 *rend = rb + cnt;                                    \
			if(dofsum(bp, 0, 0, cnt, rb, 1, TYPE_##TPE1, TYPE_##TPE2, \
					  NULL, NULL, NULL, 0, 0, true, false, true,      \
					  "GDKanalyticalsum") == BUN_NONE) {              \
				goto bailout;                                         \
			}                                                         \
			curval = *rb;                                             \
			for(; rb<rend; rb++)                                      \
				*rb = curval;                                         \
			if(is_##TPE2##_nil(curval))                               \
				has_nils = true;                                      \
		} else { /* single value, ie no ordering */                   \
			TPE2 *rend = rp + cnt;                                    \
			for(; rp<rend; rp++, bp++) {                              \
				if(is_##TPE1##_nil(*bp)) {                            \
					*rp = TPE2##_nil;                                 \
					has_nils = true;                                  \
				} else {                                              \
					*rp = (TPE2) *bp;                                 \
				}                                                     \
			}                                                         \
		}                                                             \
		goto finish;                                                  \
	} while(0);

gdk_return
GDKanalyticalsum(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tp1, int tp2)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0;
	bit *np, *pnp, *end;
	int abort_on_error = 1;

	switch (tp2) {
		case TYPE_bte: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_IMP(bte, bte);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_sht: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_IMP(bte, sht);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_IMP(sht, sht);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_int: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_IMP(bte, int);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_IMP(sht, int);
					break;
				case TYPE_int:
					ANALYTICAL_SUM_IMP(int, int);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_lng: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_IMP(bte, lng);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_IMP(sht, lng);
					break;
				case TYPE_int:
					ANALYTICAL_SUM_IMP(int, lng);
					break;
				case TYPE_lng:
					ANALYTICAL_SUM_IMP(lng, lng);
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
					ANALYTICAL_SUM_IMP(bte, hge);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_IMP(sht, hge);
					break;
				case TYPE_int:
					ANALYTICAL_SUM_IMP(int, hge);
					break;
				case TYPE_lng:
					ANALYTICAL_SUM_IMP(lng, hge);
					break;
				case TYPE_hge:
					ANALYTICAL_SUM_IMP(hge, hge);
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
					ANALYTICAL_SUM_FP_IMP(flt, flt);
					break;
				default:
					goto nosupport;
					break;
			}
		}
		case TYPE_dbl: {
			switch (tp1) {
				case TYPE_flt:
					ANALYTICAL_SUM_FP_IMP(flt, dbl);
					break;
				case TYPE_dbl:
					ANALYTICAL_SUM_FP_IMP(dbl, dbl);
					break;
				default:
					goto nosupport;
					break;
			}
		}
		default:
			goto nosupport;
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
#undef ANALYTICAL_SUM_FP_IMP

#define ANALYTICAL_PROD_IMP_NORMAL(TPE1, TPE2, TPE3)             \
	do {                                                         \
		TPE1 *pbp, *bp, v;                                       \
		TPE2 *rp, *rb, curval = TPE2##_nil;                      \
		pbp = bp = (TPE1*)Tloc(b, 0);                            \
		rb = rp = (TPE2*)Tloc(r, 0);                             \
		if (p) {                                                 \
			pnp = np = (bit*)Tloc(p, 0);                         \
			end = np + cnt;                                      \
			for(; np<end; np++) {                                \
				if (*np) {                                       \
					ncnt = np - pnp;                             \
					bp += ncnt;                                  \
					rp += ncnt;                                  \
					for(; pbp<bp; pbp++) {                       \
						v = *pbp;                                \
						if (!is_##TPE1##_nil(v)) {               \
							if(is_##TPE2##_nil(curval))          \
								curval = (TPE2) v;               \
							else                                 \
								MUL4_WITH_CHECK(TPE1, v, TPE2,   \
									curval, TPE2, curval,        \
									GDK_##TPE2##_max, TPE3,      \
									goto calc_overflow);         \
						}                                        \
					}                                            \
					for (;rb < rp; rb++)                         \
						*rb = curval;                            \
					if(is_##TPE2##_nil(curval))                  \
						has_nils = true;                         \
					else                                         \
						curval = TPE2##_nil;                     \
					pnp = np;                                    \
					pbp = bp;                                    \
				}                                                \
			}                                                    \
			ncnt = np - pnp;                                     \
			bp += ncnt;                                          \
			rp += ncnt;                                          \
			for(; pbp<bp; pbp++) {                               \
				v = *pbp;                                        \
				if (!is_##TPE1##_nil(v)) {                       \
					if(is_##TPE2##_nil(curval))                  \
						curval = (TPE2) v;                       \
					else                                         \
						MUL4_WITH_CHECK(TPE1, v, TPE2, curval,   \
							TPE2, curval, GDK_##TPE2##_max, TPE3,\
							goto calc_overflow);                 \
				}                                                \
			}                                                    \
			if(is_##TPE2##_nil(curval))                          \
				has_nils = true;                                 \
			for (;rb < rp; rb++)                                 \
				*rb = curval;                                    \
		} else if (o || force_order) {                           \
			TPE1 *bend = bp + cnt;                               \
			for(; bp<bend; bp++) {                               \
				v = *bp;                                         \
				if(!is_##TPE1##_nil(v)) {                        \
					if(is_##TPE2##_nil(curval))                  \
						curval = (TPE2) v;                       \
					else                                         \
						MUL4_WITH_CHECK(TPE1, v, TPE2, curval,   \
							TPE2, curval, GDK_##TPE2##_max,      \
							TPE3, goto calc_overflow);           \
				}                                                \
			}                                                    \
			rp += cnt;                                           \
			for(;rb < rp; rb++)                                  \
				*rb = curval;                                    \
			if(is_##TPE2##_nil(curval))                          \
				has_nils = true;                                 \
		} else { /* single value, ie no ordering */              \
			TPE2 *rend = rp + cnt;                               \
			for(; rp<rend; rp++, bp++)  {                        \
				v = *bp;                                         \
				if(is_##TPE1##_nil(v)) {                         \
					*rp = TPE2##_nil;                            \
					has_nils = true;                             \
				} else {                                         \
					*rp = (TPE2) v;                              \
				}                                                \
			}                                                    \
		}                                                        \
		goto finish;                                             \
	} while(0);

#define ANALYTICAL_PROD_IMP_LIMIT(TPE1, TPE2, REAL_IMP)    \
	do {                                                   \
		TPE1 *pbp, *bp, v;                                 \
		TPE2 *rp, *rb, curval = TPE2##_nil;                \
		pbp = bp = (TPE1*)Tloc(b, 0);                      \
		rb = rp = (TPE2*)Tloc(r, 0);                       \
		if (p) {                                           \
			pnp = np = (bit*)Tloc(p, 0);                   \
			end = np + cnt;                                \
			for(; np<end; np++) {                          \
				if (*np) {                                 \
					ncnt = np - pnp;                       \
					bp += ncnt;                            \
					rp += ncnt;                            \
					for(; pbp<bp; pbp++) {                 \
						v = *pbp;                          \
						if (!is_##TPE1##_nil(v)) {         \
							if(is_##TPE2##_nil(curval))    \
								curval = (TPE2) v;         \
							else                           \
								REAL_IMP(TPE1, v, TPE2,    \
									curval, curval,        \
									GDK_##TPE2##_max,      \
									goto calc_overflow);   \
						}                                  \
					}                                      \
					for (;rb < rp; rb++)                   \
						*rb = curval;                      \
					if(is_##TPE2##_nil(curval))            \
						has_nils = true;                   \
					else                                   \
						curval = TPE2##_nil;               \
					pbp = bp;                              \
					pnp = np;                              \
				}                                          \
			}                                              \
			ncnt = np - pnp;                               \
			bp += ncnt;                                    \
			rp += ncnt;                                    \
			for(; pbp<bp; pbp++) {                         \
				v = *pbp;                                  \
				if (!is_##TPE1##_nil(v)) {                 \
					if(is_##TPE2##_nil(curval))            \
						curval = (TPE2) v;                 \
					else                                   \
						REAL_IMP(TPE1, v, TPE2, curval,    \
								 curval, GDK_##TPE2##_max, \
								 goto calc_overflow);      \
				}                                          \
			}                                              \
			if(is_##TPE2##_nil(curval))                    \
				has_nils = true;                           \
			for (;rb < rp; rb++)                           \
				*rb = curval;                              \
		} else if (o || force_order) {                     \
			TPE1 *bend = bp + cnt;                         \
			for(; bp<bend; bp++) {                         \
				v = *bp;                                   \
				if(!is_##TPE1##_nil(v)) {                  \
					if(is_##TPE2##_nil(curval))            \
						curval = (TPE2) v;                 \
					else                                   \
						REAL_IMP(TPE1, v, TPE2, curval,    \
								 curval, GDK_##TPE2##_max, \
								 goto calc_overflow);      \
				}                                          \
			}                                              \
			rp += cnt;                                     \
			for(;rb < rp; rb++)                            \
				*rb = curval;                              \
			if(is_##TPE2##_nil(curval))                    \
				has_nils = true;                           \
		} else { /* single value, ie no ordering */        \
			TPE2 *rend = rp + cnt;                         \
			for(; rp<rend; rp++, bp++)  {                  \
				v = *bp;                                   \
				if(is_##TPE1##_nil(v)) {                   \
					*rp = TPE2##_nil;                      \
					has_nils = true;                       \
				} else {                                   \
					*rp = (TPE2) v;                        \
				}                                          \
			}                                              \
		}                                                  \
		goto finish;                                       \
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

#define ANALYTICAL_PROD_IMP_FP(TPE1, TPE2)                      \
	do {                                                        \
		TPE1 *pbp, *bp, v;                                      \
		TPE2 *rp, *rb, curval = TPE2##_nil;                     \
		pbp = bp = (TPE1*)Tloc(b, 0);                           \
		rb = rp = (TPE2*)Tloc(r, 0);                            \
		if (p) {                                                \
			pnp = np = (bit*)Tloc(p, 0);                        \
			end = np + cnt;                                     \
			for(; np<end; np++) {                               \
				if (*np) {                                      \
					ncnt = np - pnp;                            \
					bp += ncnt;                                 \
					rp += ncnt;                                 \
					for(; pbp<bp; pbp++) {                      \
						v = *pbp;                               \
						 if (!is_##TPE1##_nil(v)) {             \
							if(is_##TPE2##_nil(curval))         \
								curval = (TPE2) v;              \
							else                                \
								ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
						}                                       \
					}                                           \
					for (;rb < rp; rb++)                        \
						*rb = curval;                           \
					if(is_##TPE2##_nil(curval))                 \
						has_nils = true;                        \
					else                                        \
						curval = TPE2##_nil;                    \
					pbp = bp;                                   \
					pnp = np;                                   \
				}                                               \
			}                                                   \
			ncnt = np - pnp;                                    \
			bp += ncnt;                                         \
			rp += ncnt;                                         \
			for(; pbp<bp; pbp++) {                              \
				v = *pbp;                                       \
				if (!is_##TPE1##_nil(v)) {                      \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) v;                      \
					else                                        \
						ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
				}                                               \
			}                                                   \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
			for (;rb < rp; rb++)                                \
				*rb = curval;                                   \
		} else if (o || force_order) {                          \
			TPE1 *bend = bp + cnt;                              \
			for(; pbp<bend; pbp++) {                            \
				v = *pbp;                                       \
				if(!is_##TPE1##_nil(v)) {                       \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) v;                      \
					else                                        \
						ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
				}                                               \
			}                                                   \
			rp += cnt;                                          \
			for(;rb < rp; rb++)                                 \
				*rb = curval;                                   \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
		} else { /* single value, ie no ordering */             \
			TPE2 *rend = rp + cnt;                              \
			for(; rp<rend; rp++, bp++) {                        \
				v = *bp;                                        \
				if(is_##TPE1##_nil(v)) {                        \
					*rp = TPE2##_nil;                           \
					has_nils = true;                            \
				} else {                                        \
					*rp = (TPE2) v;                             \
				}                                               \
			}                                                   \
		}                                                       \
		goto finish;                                            \
	} while(0);

gdk_return
GDKanalyticalprod(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tp1, int tp2)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0;
	bit *pnp, *np, *end;
	int abort_on_error = 1;

	switch (tp2) {
		case TYPE_bte: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_IMP_NORMAL(bte, bte, sht);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_sht: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_IMP_NORMAL(bte, sht, int);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_IMP_NORMAL(sht, sht, int);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_int: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_IMP_NORMAL(bte, int, lng);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_IMP_NORMAL(sht, int, lng);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_IMP_NORMAL(int, int, lng);
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
					ANALYTICAL_PROD_IMP_NORMAL(bte, lng, hge);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_IMP_NORMAL(sht, lng, hge);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_IMP_NORMAL(int, lng, hge);
					break;
				case TYPE_lng:
					ANALYTICAL_PROD_IMP_NORMAL(lng, lng, hge);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_hge: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_IMP_LIMIT(bte, hge, HGEMUL_CHECK);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_IMP_LIMIT(sht, hge, HGEMUL_CHECK);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_IMP_LIMIT(int, hge, HGEMUL_CHECK);
					break;
				case TYPE_lng:
					ANALYTICAL_PROD_IMP_LIMIT(lng, hge, HGEMUL_CHECK);
					break;
				case TYPE_hge:
					ANALYTICAL_PROD_IMP_LIMIT(hge, hge, HGEMUL_CHECK);
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
					ANALYTICAL_PROD_IMP_LIMIT(bte, lng, LNGMUL_CHECK);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_IMP_LIMIT(sht, lng, LNGMUL_CHECK);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_IMP_LIMIT(int, lng, LNGMUL_CHECK);
					break;
				case TYPE_lng:
					ANALYTICAL_PROD_IMP_LIMIT(lng, lng, LNGMUL_CHECK);
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
					ANALYTICAL_PROD_IMP_FP(flt, flt);
					break;
				default:
					goto nosupport;
					break;
			}
		}
		case TYPE_dbl: {
			switch (tp1) {
				case TYPE_flt:
					ANALYTICAL_PROD_IMP_FP(flt, dbl);
					break;
				case TYPE_dbl:
					ANALYTICAL_PROD_IMP_FP(dbl, dbl);
					break;
				default:
					goto nosupport;
					break;
			}
		}
		default:
			goto nosupport;
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

#undef ANALYTICAL_PROD_IMP_NORMAL
#undef ANALYTICAL_PROD_IMP_LIMIT
#undef ANALYTICAL_PROD_IMP_FP
#undef ANALYTICAL_PROD_IMP_FP_REAL

#define ANALYTICAL_AVERAGE_TYPE_LNG_HGE(TPE,lng_hge)                \
	do {                                                            \
		BUN rr;                                                     \
		TPE *restrict bp = (TPE*)Tloc(b, 0), a;                     \
		dbl *rp, *rb, *end;                                         \
		rb = rp = (dbl*)Tloc(r, 0);                                 \
		end = rp + cnt;                                             \
		if (p) {                                                    \
			np = (bit*)Tloc(p, 0);                                  \
			for(; rp<end; np++, rp++, bp++) {                       \
				if (*np) {                                          \
					curval = n > 0 ? (dbl) sum / n : dbl_nil;       \
					has_nils = has_nils || (n == 0);                \
back_up##TPE:                                                       \
					for (;rb < rp; rb++)                            \
						*rb = curval;                               \
					n = 0;                                          \
					sum = 0;                                        \
				}                                                   \
				if (!is_##TPE##_nil(*bp)) {                         \
					ADD_WITH_CHECK(TPE, *bp, lng_hge, sum,          \
								   lng_hge, sum,                    \
								   GDK_##lng_hge##_max,             \
								   goto avg_overflow##TPE);         \
				/* don't count value until after overflow check */  \
					n++;                                            \
				}                                                   \
				if(0) {                                             \
avg_overflow##TPE:                                                  \
					assert(n > 0);                                  \
					if (sum >= 0) {                                 \
						a = (TPE) (sum / (lng_hge) n);              \
						rr = (BUN) (sum % (SBUN) n);                \
					} else {                                        \
						sum = -sum;                                 \
						a = - (TPE) (sum / (lng_hge) n);            \
						rr = (BUN) (sum % (SBUN) n);                \
						if (r) {                                    \
							a--;                                    \
							rr = n - rr;                            \
						}                                           \
					}                                               \
					for(; rp<end; np++, rp++, bp++) {               \
						if (*np) {                                  \
							curval = a + (dbl) rr / n;              \
							goto back_up##TPE;                      \
						}                                           \
						if (is_##TPE##_nil(*bp))                    \
							continue;                               \
						AVERAGE_ITER(TPE, *bp, a, rr, n);           \
					}                                               \
					curval = a + (dbl) rr / n;                      \
					goto calc_done##TPE;                            \
				}                                                   \
			}                                                       \
			curval = n > 0 ? (dbl) sum / n : dbl_nil;               \
			has_nils = has_nils || (n == 0);                        \
calc_done##TPE:                                                     \
			for (;rb < rp; rb++)                                    \
				*rb = curval;                                       \
		} else if (o || force_order) {                              \
			for(; rp<end; rp++, bp++) {                             \
				if (!is_##TPE##_nil(*bp)) {                         \
					ADD_WITH_CHECK(TPE, *bp, lng_hge, sum, lng_hge, \
								   sum, GDK_##lng_hge##_max,        \
								   goto single_overflow##TPE);      \
				/* don't count value until after overflow check */  \
					n++;                                            \
				}                                                   \
			}                                                       \
			if(0) {                                                 \
single_overflow##TPE:                                               \
				assert(n > 0);                                      \
				if (sum >= 0) {                                     \
					a = (TPE) (sum / (lng_hge) n);                  \
					rr = (BUN) (sum % (SBUN) n);                    \
				} else {                                            \
					sum = -sum;                                     \
					a = - (TPE) (sum / (lng_hge) n);                \
					rr = (BUN) (sum % (SBUN) n);                    \
					if (r) {                                        \
						a--;                                        \
						rr = n - rr;                                \
					}                                               \
				}                                                   \
				for(; rp<end; rp++, bp++) {                         \
					if (is_##TPE##_nil(*bp))                        \
						continue;                                   \
					AVERAGE_ITER(TPE, *bp, a, rr, n);               \
				}                                                   \
				curval = a + (dbl) rr / n;                          \
				goto single_calc_done##TPE;                         \
			}                                                       \
			curval = n > 0 ? (dbl) sum / n : dbl_nil;               \
			has_nils = (n == 0);                                    \
single_calc_done##TPE:                                              \
			for (;rb < rp; rb++)                                    \
				*rb = curval;                                       \
		} else { /* single value, ie no ordering */                 \
			for(; rp<end; rp++, bp++) {                             \
				if(is_##TPE##_nil(*bp)) {                           \
					*rp = dbl_nil;                                  \
					has_nils = true;                                \
				} else {                                            \
					*rp = (dbl) *bp;                                \
				}                                                   \
			}                                                       \
		}                                                           \
		goto finish;                                                \
	} while(0);

#ifdef HAVE_HGE
#define ANALYTICAL_AVERAGE_TYPE(TYPE) ANALYTICAL_AVERAGE_TYPE_LNG_HGE(TYPE,hge)
#else
#define ANALYTICAL_AVERAGE_TYPE(TYPE) ANALYTICAL_AVERAGE_TYPE_LNG_HGE(TYPE,lng)
#endif

#define ANALYTICAL_AVERAGE_FLOAT_TYPE(TPE)                   \
	do {                                                     \
		TPE *pbp, *bp, v;                                    \
		dbl *rp, *rb, a = 0;                                 \
		pbp = bp = (TPE*)Tloc(b, 0);                         \
		rb = rp = (dbl*)Tloc(r, 0);                          \
		if (p) {                                             \
			pnp = np = (bit*)Tloc(p, 0);                     \
			end = np + cnt;                                  \
			for(; np<end; np++) {                            \
				if (*np) {                                   \
					ncnt = np - pnp;                         \
					bp += ncnt;                              \
					rp += ncnt;                              \
					for(; pbp<bp; pbp++) {                   \
						v = *pbp;                            \
						if (!is_##TPE##_nil(v))              \
							AVERAGE_ITER_FLOAT(TPE, v, a, n);\
					}                                        \
					curval = n > 0 ? a : dbl_nil;            \
					has_nils = has_nils || (n == 0);         \
					for (;rb < rp; rb++)                     \
						*rb = curval;                        \
					n = 0;                                   \
					a = 0;                                   \
					pbp = bp;                                \
					pnp = np;                                \
				}                                            \
			}                                                \
			ncnt = np - pnp;                                 \
			bp += ncnt;                                      \
			rp += ncnt;                                      \
			for(; pbp<bp; pbp++) {                           \
				v = *pbp;                                    \
				if (!is_##TPE##_nil(v))                      \
					AVERAGE_ITER_FLOAT(TPE, v, a, n);        \
			}                                                \
			curval = n > 0 ? a : dbl_nil;                    \
			has_nils = has_nils || (n == 0);                 \
			for (;rb < rp; rb++)                             \
				*rb = curval;                                \
		} else if (o || force_order) {                       \
			TPE *bend = bp + cnt;                            \
			for(; bp<bend; bp++) {                           \
				v = *bp;                                     \
				if (!is_##TPE##_nil(v))                      \
					AVERAGE_ITER_FLOAT(TPE, v, a, n);        \
			}                                                \
			rp += cnt;                                       \
			curval = n > 0 ? a : dbl_nil;                    \
			has_nils = (n == 0);                             \
			for (;rb < rp; rb++)                             \
				*rb = curval;                                \
		} else { /* single value, ie no ordering */          \
		    dbl *rend = rp + cnt;                            \
			for(; rp<rend; rp++, bp++) {                     \
				if(is_##TPE##_nil(*bp)) {                    \
					*rp = dbl_nil;                           \
					has_nils = true;                         \
				} else {                                     \
					*rp = (dbl) *bp;                         \
				}                                            \
			}                                                \
		}                                                    \
		goto finish;                                         \
	} while(0);

gdk_return
GDKanalyticalavg(BAT *r, BAT *b, BAT *p, BAT *o, bit force_order, int tpe)
{
	bool has_nils = false;
	BUN ncnt, cnt = BATcount(b), nils = 0, n = 0;
	bit *np, *pnp, *end;
	bool abort_on_error = true;
	dbl curval;
#ifdef HAVE_HGE
	hge sum = 0;
#else
	lng sum = 0;
#endif

	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_AVERAGE_TYPE(bte);
			break;
		case TYPE_sht:
			ANALYTICAL_AVERAGE_TYPE(sht);
			break;
		case TYPE_int:
			ANALYTICAL_AVERAGE_TYPE(int);
			break;
		case TYPE_lng:
			ANALYTICAL_AVERAGE_TYPE(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_AVERAGE_TYPE(hge);
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_AVERAGE_FLOAT_TYPE(flt);
			break;
		case TYPE_dbl:
			ANALYTICAL_AVERAGE_FLOAT_TYPE(dbl);
			break;
		default:
			GDKerror("GDKanalyticalavg: average of type %s unsupported.\n", ATOMname(tpe));
			return GDK_FAIL;
	}
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_AVERAGE_TYPE
#undef ANALYTICAL_AVERAGE_TYPE_LNG_HGE
#undef ANALYTICAL_AVERAGE_FLOAT_TYPE
