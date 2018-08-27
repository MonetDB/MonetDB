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
				for (i=0; i<cnt; i++, rp++) {
					*rb = *rp;
					next = BUNtail(it, i);
					if (atomcmp(v, next) != 0) {
						*rb = TRUE;
						v = next;
					}
				}
			} else {
				for(i=0; i<cnt; i++, rp++) {
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
		if((BUN)val >= cnt) {         \
			i = 1;                    \
			for(; rb<rp; i++, rb++)   \
				*rb = i;              \
		} else if(cnt % val == 0) {   \
			buckets = cnt / val;      \
			for(; rb<rp; i++, rb++) { \
				if(i == buckets) {    \
					j++;              \
					i = 0;            \
				}                     \
				*rb = j;              \
			}                         \
		} else {                      \
			buckets = cnt / val;      \
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
			TPE *end = rp + cnt;             \
			for(; rp<end; np++, rp++) {      \
				if (*np) {                   \
					i = 0;                   \
					j = 1;                   \
					cnt = np - pnp;          \
					pnp = np;                \
					NTILE_CALC(TPE)          \
				}                            \
			}                                \
			i = 0;                           \
			j = 1;                           \
			cnt = np - pnp;                  \
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
	BUN cnt = BATcount(b);
	bit *np, *pnp;
	bool has_nils = false;
	gdk_return gdk_res = GDK_SUCCEED;

	assert(ntile);

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
	if(o) {
		r->tsorted = o->tsorted;
		r->trevsorted = o->trevsorted;
	} else if(!p) {
		r->tsorted = true;
		r->trevsorted = false;
	}
	return gdk_res;
}

#undef ANALYTICAL_NTILE_IMP
#undef NTILE_CALC

#define ANALYTICAL_FIRST_IMP(TPE)                 \
	do {                                          \
		TPE *rp, *rb, *restrict bp, *end, curval; \
		rb = rp = (TPE*)Tloc(r, 0);               \
		bp = (TPE*)Tloc(b, 0);                    \
		curval = *bp;                             \
		end = rp + cnt;                           \
		if (p) {                                  \
			np = (bit*)Tloc(p, 0);                \
			for(; rp<end; np++, rp++, bp++) {     \
				if (*np) {                        \
					if(is_##TPE##_nil(curval))    \
						has_nils = true;          \
					for (;rb < rp; rb++)          \
						*rb = curval;             \
					curval = *bp;                 \
				}                                 \
			}                                     \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for (;rb < rp; rb++)                  \
				*rb = curval;                     \
		} else {                                  \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rp<end; rp++)                   \
				*rp = curval;                     \
		}                                         \
	} while(0);

gdk_return
GDKanalyticalfirst(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i, j, cnt = BATcount(b);
	bit *restrict np;
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
		TPE *rp, *rb, *restrict bp, *end, curval; \
		rb = rp = (TPE*)Tloc(r, 0);               \
		bp = (TPE*)Tloc(b, 0);                    \
		end = rp + cnt;                           \
		if (p) {                                  \
			np = (bit*)Tloc(p, 0);                \
			for(; rp<end; np++, rp++, bp++) {     \
				if (*np) {                        \
					curval = *(bp - 1);           \
					if(is_##TPE##_nil(curval))    \
						has_nils = true;          \
					for (;rb < rp; rb++)          \
						*rb = curval;             \
				}                                 \
			}                                     \
			curval = *(bp - 1);                   \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for (;rb < rp; rb++)                  \
				*rb = curval;                     \
		} else {                                  \
			curval = *(bp + cnt - 1);             \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rp<end; rp++)                   \
				*rp = curval;                     \
		}                                         \
	} while(0);

gdk_return
GDKanalyticallast(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i, j, cnt = BATcount(b);
	bit *restrict np;
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
		TPE *rp, *rb, *pbp, *bp, *end, curval;    \
		pbp = bp = (TPE*)Tloc(b, 0);              \
		rb = rp = (TPE*)Tloc(r, 0);               \
		end = rp + cnt;                           \
		if(is_lng_nil(nth)) {                     \
			has_nils = true;                      \
			for(; rp<end; rp++)                   \
				*rp = TPE##_nil;                  \
		} else if(p) {                            \
			np = (bit*)Tloc(p, 0);                \
			for(; rp<end; np++, rp++, bp++) {     \
				if (*np) {                        \
					if(nth > (TPE) (bp - pbp)) {  \
						curval = TPE##_nil;       \
					} else {                      \
						curval = *(pbp + nth);    \
					}                             \
					if(is_##TPE##_nil(curval))    \
						has_nils = true;          \
					for(; rb<rp; rb++)            \
						*rb = curval;             \
					pbp = bp;                     \
				}                                 \
			}                                     \
			if(nth > (TPE) (bp - pbp)) {          \
				curval = TPE##_nil;               \
			} else {                              \
				curval = *(pbp + nth);            \
			}                                     \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rb<rp; rb++)                    \
				*rb = curval;                     \
		} else {                                  \
			TPE* end = rp + cnt;                  \
			if(nth > (TPE) cnt) {                 \
				curval = TPE##_nil;               \
			} else {                              \
				curval = *(bp + nth);             \
			}                                     \
			if(is_##TPE##_nil(curval))            \
				has_nils = true;                  \
			for(; rp<end; rp++)                   \
				*rp = curval;                     \
		}                                         \
		goto finish;                              \
	} while(0);

gdk_return
GDKanalyticalnthvalue(BAT *r, BAT *b, BAT *p, BAT *o, lng nth, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	BUN i, j, cnt = BATcount(b);
	bit *np;
	gdk_return gdk_res = GDK_SUCCEED;
	bool has_nils = false;

	assert(is_lng_nil(nth) || nth >= 0);
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
			if(is_lng_nil(nth)) {
				has_nils = true;
				for(i=0; i<cnt; i++) {
					if ((gdk_res = BUNappend(r, nil, false)) != GDK_SUCCEED)
						goto finish;
				}
			} else if (p) {
				np = (bit*)Tloc(p, 0);
				for(i=0,j=0; i<cnt; i++, np++) {
					if (*np) {
						if(nth > (lng)(i - j)) {
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
				if(nth > (lng)(i - j)) {
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
				if(nth > (lng)cnt) {
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
		TPE *rp, *rb, *bp, *end,                        \
			def = *((TPE *) default_value), next;       \
		bp = (TPE*)Tloc(b, 0);                          \
		rb = rp = (TPE*)Tloc(r, 0);                     \
		if(is_lng_nil(lag)) {                           \
			has_nils = true;                            \
			end = rb + cnt;                             \
			for(; rb<end; rb++)                         \
				*rb = TPE##_nil;                        \
		} else if(p) {                                  \
			end = rp + cnt;                             \
			np = (bit*)Tloc(p, 0);                      \
			for(; rp<end; np++, rp++) {                 \
				if (*np) {                              \
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
					bp += lag;                          \
				}                                       \
			}                                           \
			for(i=0; i<lag && rb<end; i++, rb++)        \
				*rb = def;                              \
			if(lag > 0 && is_##TPE##_nil(def))          \
				has_nils = true;                        \
			for(;rb<end; rb++, bp++) {                  \
				next = *bp;                             \
				*rb = next;                             \
				if(is_##TPE##_nil(next))                \
					has_nils = true;                    \
			}                                           \
		} else {                                        \
			end = rb + cnt;                             \
			for(i=0; i<lag && rb<end; i++, rb++)        \
				*rb = def;                              \
			if(lag > 0 && is_##TPE##_nil(def))          \
				has_nils = true;                        \
			for(;rb<end; rb++, bp++) {                  \
				next = *bp;                             \
				*rb = next;                             \
				if(is_##TPE##_nil(next))                \
					has_nils = true;                    \
			}                                           \
		}                                               \
		goto finish;                                    \
	} while(0);

gdk_return
GDKanalyticallag(BAT *r, BAT *b, BAT *p, BAT *o, lng lag, const void* restrict default_value, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void *restrict nil;
	lng i = 0;
	BUN j = 0, k = 0, l, cnt = BATcount(b);
	bit *restrict np;
	gdk_return gdk_res = GDK_SUCCEED;
	bool has_nils = false;

	assert(default_value);
	assert(is_lng_nil(lag) || lag >= 0);

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
			if(is_lng_nil(lag)) {
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
						for(l=0; k<j; k++, l++) {
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
				for(l=0; k<cnt; k++, l++) {
					curval = BUNtail(bpi, l);
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)
						goto finish;
					if((*atomcmp)(curval, nil) == 0)
						has_nils = true;
				}
			} else {
				lng lcnt = (lng) cnt;
				for(i=0; i<lag && i<lcnt; i++) {
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

gdk_return
GDKanalyticallead(BAT *r, BAT *b, BAT *p, BAT *o, lng lead, const void* restrict default_value, int tpe)
{
	//int (*atomcmp)(const void *, const void *);
	//const void* restrict nil;
	BUN /*i, j,*/ cnt = BATcount(b);
	//bit *restrict np;
	gdk_return gdk_res = GDK_SUCCEED;
	bool has_nils = false;

	assert(default_value);
	assert(is_lng_nil(lead) || lead <= 0);

	(void) o;
	(void) p;
	(void) tpe;
//finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return gdk_res;
}

#define ANALYTICAL_LIMIT_IMP(TPE, OP)                        \
	do {                                                     \
		TPE *rp, *rb, *restrict bp, *end, curval;            \
		rb = rp = (TPE*)Tloc(r, 0);                          \
		bp = (TPE*)Tloc(b, 0);                               \
		curval = *bp;                                        \
		end = rp + cnt;                                      \
		if (p) {                                             \
			np = (bit*)Tloc(p, 0);                           \
			for(; rp<end; np++, rp++, bp++) {                \
				if (*np) {                                   \
					if(is_##TPE##_nil(curval))               \
						has_nils = true;                     \
					for (;rb < rp; rb++)                     \
						*rb = curval;                        \
					curval = *bp;                            \
				}                                            \
				if(!is_##TPE##_nil(*bp)) {                   \
					if(is_##TPE##_nil(curval))               \
						curval = *bp;                        \
					else                                     \
						curval = OP(*bp, curval);            \
				}                                            \
			}                                                \
			if(is_##TPE##_nil(curval))                       \
				has_nils = true;                             \
			for (;rb < rp; rb++)                             \
				*rb = curval;                                \
		} else if (o) { /* single value, ie no partitions */ \
			for(; rp<end; rp++, bp++) {                      \
				if(!is_##TPE##_nil(*bp)) {                   \
					if(is_##TPE##_nil(curval))               \
						curval = *bp;                        \
					else                                     \
						curval = OP(*bp, curval);            \
				}                                            \
			}                                                \
			if(is_##TPE##_nil(curval))                       \
				has_nils = true;                             \
			for(;rb < rp; rb++)                              \
				*rb = curval;                                \
		} else { /* single value, ie no ordering */          \
			for(; rp<end; rp++, bp++) {                      \
				if(is_##TPE##_nil(*bp))                      \
					has_nils = true;                         \
				*rp = *bp;                                   \
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
GDKanalytical##OP(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)                                \
{                                                                                         \
	int (*atomcmp)(const void *, const void *);                                           \
	const void* restrict nil;                                                             \
	bool has_nils = false;                                                                \
	BUN i, j, cnt = BATcount(b);                                                          \
	bit *restrict np;                                                                     \
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
			} else if (o) { /* single value, ie no partitions */                          \
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

#define ANALYTICAL_COUNT_WITH_NIL_FIXED_SIZE_IMP(TPE) \
	do {                                              \
		TPE *restrict bp = (TPE*)Tloc(b, 0);          \
		lng *rp, *rb, *end, curval = 0;               \
		rb = rp = (lng*)Tloc(r, 0);                   \
		end = rp + cnt;                               \
		if (p) {                                      \
			np = (bit*)Tloc(p, 0);                    \
			for(; rp<end; np++, rp++, bp++) {         \
				if (*np) {                            \
					for (;rb < rp; rb++)              \
						*rb = curval;                 \
					curval = 0;                       \
				}                                     \
				curval += !is_##TPE##_nil(*bp);       \
			}                                         \
			for (;rb < rp; rb++)                      \
				*rb = curval;                         \
		} else { /* single value, ie no partitions */ \
			for(; rp<end; rp++, bp++)                 \
				curval += !is_##TPE##_nil(*bp);       \
			for(;rb < rp; rb++)                       \
				*rb = curval;                         \
		}                                             \
	} while(0);

#define ANALYTICAL_COUNT_WITH_NIL_STR_IMP(TPE_CAST, OFFSET)               \
	do {                                                                  \
		const void *restrict bp = Tloc(b, 0);                             \
		lng *rp, *rb, curval = 0;                                         \
		rb = rp = (lng*)Tloc(r, 0);                                       \
		if (p) {                                                          \
			np = (bit*)Tloc(p, 0);                                        \
			for(i = 0; i < cnt; i++, np++, rp++) {                        \
				if (*np) {                                                \
					for (;rb < rp; rb++)                                  \
						*rb = curval;                                     \
					curval = 0;                                           \
				}                                                         \
				curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200'; \
			}                                                             \
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
	gdk_return gdk_res = GDK_SUCCEED;

	assert(ignore_nils);
	(void) o;
	if(!*ignore_nils || b->T.nonil) {
		bit *np, *pnp;
		lng *rp, *rb, curval = 0;
		rb = rp = (lng*)Tloc(r, 0);
		if (p) {
			np = pnp = (bit*)Tloc(p, 0);
			bit* end = np + cnt;
			for(; np < end; np++, rp++) {
				if (*np) {
					curval = np - pnp;
					pnp = np;
					for (;rb < rp; rb++)
						*rb = curval;
				}
			}
			curval = np - pnp;
			for (;rb < rp; rb++)
				*rb = curval;
		} else { /* single value */
			lng* end = rp + cnt;
			for(; rp < end; rp++)
				*rp = cnt;
		}
	} else {
		bit *restrict np;
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
						np = (bit*)Tloc(p, 0);
						for(i = 0; i < cnt; i++, np++, rp++) {
							if (*np) {
								for (;rb < rp; rb++)
									*rb = curval;
								curval = 0;
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
						np = (bit*)Tloc(p, 0);
						for(i = 0; i < cnt; i++, np++, rp++) {
							if (*np) {
								for (;rb < rp; rb++)
									*rb = curval;
								curval = 0;
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
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0);                  \
		TPE2 *rp, *rb, *end, curval = TPE2##_nil;               \
		rb = rp = (TPE2*)Tloc(r, 0);                            \
		end = rp + cnt;                                         \
		if (p) {                                                \
			np = (bit*)Tloc(p, 0);                              \
			for(; rp<end; np++, rp++, bp++) {                   \
				if (*np) {                                      \
					for (;rb < rp; rb++)                        \
						*rb = curval;                           \
					if(is_##TPE2##_nil(curval))                 \
						has_nils = true;                        \
					else                                        \
						curval = TPE2##_nil;                    \
				}                                               \
				if (!is_##TPE1##_nil(*bp)) {                    \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) *bp;                    \
					else                                        \
						ADD_WITH_CHECK(TPE1, *bp, TPE2, curval, \
										TPE2, curval,           \
										GDK_##TPE2##_max,       \
										goto calc_overflow);    \
				}                                               \
			}                                                   \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
			for (;rb < rp; rb++)                                \
				*rb = curval;                                   \
		} else if (o) { /* single value, ie no partitions */    \
			for(; rp<end; rp++, bp++) {                         \
				if(!is_##TPE1##_nil(*bp)) {                     \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) *bp;                    \
					else                                        \
						ADD_WITH_CHECK(TPE1, *bp, TPE2, curval, \
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
			for(; rp<end; rp++, bp++) {                         \
				if(is_##TPE1##_nil(*bp)) {                      \
					*rp = TPE2##_nil;                           \
					has_nils = true;                            \
				} else {                                        \
					*rp = (TPE2) *bp;                           \
				}                                               \
			}                                                   \
		}                                                       \
		goto finish;                                            \
	} while(0);

#define ANALYTICAL_SUM_FP_IMP(TPE1, TPE2)                             \
	do {                                                              \
		TPE1 *bp, *bprev;                                             \
		TPE2 *rp, *rb, curval = TPE2##_nil;                           \
		bp = bprev = (TPE1*)Tloc(b, 0);                               \
		rb = rp = (TPE2*)Tloc(r, 0);                                  \
		if (p) {                                                      \
			np = (bit*)Tloc(p, 0);                                    \
			for(i=0,j=0; i<cnt; i++, np++, rp++, bp++) {              \
				if (*np) {                                            \
					if(dofsum(bprev, 0, 0, i - j, rb, 1, TYPE_##TPE1, TYPE_##TPE2, \
							  NULL, NULL, NULL, 0, 0, true, false, true,           \
							  "GDKanalyticalsum") == BUN_NONE) {                   \
						goto bailout;                                 \
					}                                                 \
					curval = *rb;                                     \
					bprev = bp;                                       \
					j = i;                                            \
					for (;rb < rp; rb++)                              \
						*rb = curval;                                 \
					if(is_##TPE2##_nil(curval))                       \
						has_nils = true;                              \
				}                                                     \
			}                                                         \
			if(dofsum(bprev, 0, 0, i - j, rb, 1, TYPE_##TPE1,         \
					  TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true,      \
					  false, true, "GDKanalyticalsum") == BUN_NONE) { \
				goto bailout;                                         \
			}                                                         \
			curval = *rb;                                             \
			if(is_##TPE2##_nil(curval))                               \
				has_nils = true;                                      \
			for (;rb < rp; rb++)                                      \
				*rb = curval;                                         \
		} else if (o) { /* single value, ie no partitions */          \
			TPE2 *end = rb + cnt;                                     \
			if(dofsum(bp, 0, 0, cnt, rb, 1, TYPE_##TPE1, TYPE_##TPE2, \
					  NULL, NULL, NULL, 0, 0, true, false, true,      \
					  "GDKanalyticalsum") == BUN_NONE) {              \
				goto bailout;                                         \
			}                                                         \
			curval = *rb;                                             \
			for(; rb<end; rb++)                                       \
				*rb = curval;                                         \
			if(is_##TPE2##_nil(curval))                               \
				has_nils = true;                                      \
		} else { /* single value, ie no ordering */                   \
			TPE2 *end = rp + cnt;                                     \
			for(; rp<end; rp++, bp++) {                               \
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
GDKanalyticalsum(BAT *r, BAT *b, BAT *p, BAT *o, int tp1, int tp2)
{
	bool has_nils = false;
	BUN i, j, cnt = BATcount(b), nils = 0;
	bit *restrict np;
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
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0);                   \
		TPE2 *rp, *rb, *end, curval = TPE2##_nil;                \
		rb = rp = (TPE2*)Tloc(r, 0);                             \
		end = rp + cnt;                                          \
		if (p) {                                                 \
			np = (bit*)Tloc(p, 0);                               \
			for(; rp<end; np++, rp++, bp++) {                    \
				if (*np) {                                       \
					for (;rb < rp; rb++)                         \
						*rb = curval;                            \
					if(is_##TPE2##_nil(curval))                  \
						has_nils = true;                         \
					else                                         \
						curval = TPE2##_nil;                     \
				}                                                \
				if (!is_##TPE1##_nil(*bp)) {                     \
					if(is_##TPE2##_nil(curval))                  \
						curval = (TPE2) *bp;                     \
					else                                         \
						MUL4_WITH_CHECK(TPE1, *bp, TPE2, curval, \
										TPE2, curval,            \
										GDK_##TPE2##_max, TPE3,  \
										goto calc_overflow);     \
				}                                                \
			}                                                    \
			if(is_##TPE2##_nil(curval))                          \
				has_nils = true;                                 \
			for (;rb < rp; rb++)                                 \
				*rb = curval;                                    \
		} else if (o) { /* single value, ie no partitions */     \
			for(; rp<end; rp++, bp++) {                          \
				if(!is_##TPE1##_nil(*bp)) {                      \
					if(is_##TPE2##_nil(curval))                  \
						curval = (TPE2) *bp;                     \
					else                                         \
						MUL4_WITH_CHECK(TPE1, *bp, TPE2, curval, \
										TPE2, curval,            \
										GDK_##TPE2##_max, TPE3,  \
										goto calc_overflow);     \
				}                                                \
			}                                                    \
			for(;rb < rp; rb++)                                  \
				*rb = curval;                                    \
			if(is_##TPE2##_nil(curval))                          \
				has_nils = true;                                 \
		} else { /* single value, ie no ordering */              \
			for(; rp<end; rp++, bp++)  {                         \
				if(is_##TPE1##_nil(*bp)) {                       \
					*rp = TPE2##_nil;                            \
					has_nils = true;                             \
				} else {                                         \
					*rp = (TPE2) *bp;                            \
				}                                                \
			}                                                    \
		}                                                        \
		goto finish;                                             \
	} while(0);

#define ANALYTICAL_PROD_IMP_LIMIT(TPE1, TPE2, REAL_IMP)      \
	do {                                                     \
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0);               \
		TPE2 *rp, *rb, *end, curval = TPE2##_nil;            \
		rb = rp = (TPE2*)Tloc(r, 0);                         \
		end = rp + cnt;                                      \
		if (p) {                                             \
			np = (bit*)Tloc(p, 0);                           \
			for(; rp<end; np++, rp++, bp++) {                \
				if (*np) {                                   \
					for (;rb < rp; rb++)                     \
						*rb = curval;                        \
					if(is_##TPE2##_nil(curval))              \
						has_nils = true;                     \
					else                                     \
						curval = TPE2##_nil;                 \
				}                                            \
				if (!is_##TPE1##_nil(*bp)) {                 \
					if(is_##TPE2##_nil(curval))              \
						curval = (TPE2) *bp;                 \
					else                                     \
						REAL_IMP(TPE1, *bp, TPE2, curval,    \
								 curval, GDK_##TPE2##_max,   \
								 goto calc_overflow);        \
				}                                            \
			}                                                \
			if(is_##TPE2##_nil(curval))                      \
				has_nils = true;                             \
			for (;rb < rp; rb++)                             \
				*rb = curval;                                \
		} else if (o) { /* single value, ie no partitions */ \
			for(; rp<end; rp++, bp++) {                      \
				if(!is_##TPE1##_nil(*bp)) {                  \
					if(is_##TPE2##_nil(curval))              \
						curval = (TPE2) *bp;                 \
					else                                     \
						REAL_IMP(TPE1, *bp, TPE2, curval,    \
								 curval, GDK_##TPE2##_max,   \
								 goto calc_overflow);        \
				}                                            \
			}                                                \
			for(;rb < rp; rb++)                              \
				*rb = curval;                                \
			if(is_##TPE2##_nil(curval))                      \
				has_nils = true;                             \
		} else { /* single value, ie no ordering */          \
			for(; rp<end; rp++, bp++)  {                     \
				if(is_##TPE1##_nil(*bp)) {                   \
					*rp = TPE2##_nil;                        \
					has_nils = true;                         \
				} else {                                     \
					*rp = (TPE2) *bp;                        \
				}                                            \
			}                                                \
		}                                                    \
		goto finish;                                         \
	} while(0);

#define ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2)                                            \
	do {                                                                                   \
		if (ABSOLUTE(curval) > 1 && GDK_##TPE2##_max / ABSOLUTE(*bp) < ABSOLUTE(curval)) { \
			if (abort_on_error)                                                            \
				goto calc_overflow;                                                        \
			curval = TPE2##_nil;                                                           \
			nils++;                                                                        \
		} else {                                                                           \
			curval *= *bp;                                                                 \
		}                                                                                  \
	} while(0);

#define ANALYTICAL_PROD_IMP_FP(TPE1, TPE2)                      \
	do {                                                        \
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0);                  \
		TPE2 *rp, *rb, *end, curval = TPE2##_nil;               \
		rb = rp = (TPE2*)Tloc(r, 0);                            \
		end = rp + cnt;                                         \
		if (p) {                                                \
			np = (bit*)Tloc(p, 0);                              \
			for(; rp<end; np++, rp++, bp++) {                   \
				if (*np) {                                      \
					for (;rb < rp; rb++)                        \
						*rb = curval;                           \
					if(is_##TPE2##_nil(curval))                 \
						has_nils = true;                        \
					else                                        \
						curval = TPE2##_nil;                    \
				}                                               \
				if (!is_##TPE1##_nil(*bp)) {                    \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) *bp;                    \
					else                                        \
						ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
				}                                               \
			}                                                   \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
			for (;rb < rp; rb++)                                \
				*rb = curval;                                   \
		} else if (o) { /* single value, ie no partitions */    \
			for(; rp<end; rp++, bp++) {                         \
				if(!is_##TPE1##_nil(*bp)) {                     \
					if(is_##TPE2##_nil(curval))                 \
						curval = (TPE2) *bp;                    \
					else                                        \
						ANALYTICAL_PROD_IMP_FP_REAL(TPE1, TPE2) \
				}                                               \
			}                                                   \
			for(;rb < rp; rb++)                                 \
				*rb = curval;                                   \
			if(is_##TPE2##_nil(curval))                         \
				has_nils = true;                                \
		} else { /* single value, ie no ordering */             \
			for(; rp<end; rp++, bp++) {                         \
				if(is_##TPE1##_nil(*bp)) {                      \
					*rp = TPE2##_nil;                           \
					has_nils = true;                            \
				} else {                                        \
					*rp = (TPE2) *bp;                           \
				}                                               \
			}                                                   \
		}                                                       \
		goto finish;                                            \
	} while(0);

gdk_return
GDKanalyticalprod(BAT *r, BAT *b, BAT *p, BAT *o, int tp1, int tp2)
{
	bool has_nils = false;
	BUN cnt = BATcount(b), nils = 0;
	bit *restrict np;
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
		} else if (o) { /* single value, ie no partitions */        \
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
		TPE *restrict bp = (TPE*)Tloc(b, 0);                 \
		dbl *rp, *rb, *end, a = 0;                           \
		rb = rp = (dbl*)Tloc(r, 0);                          \
		end = rp + cnt;                                      \
		if (p) {                                             \
			np = (bit*)Tloc(p, 0);                           \
			for(; rp<end; np++, rp++, bp++) {                \
				if (*np) {                                   \
					curval = n > 0 ? a : dbl_nil;            \
					has_nils = has_nils || (n == 0);         \
					for (;rb < rp; rb++)                     \
						*rb = curval;                        \
					n = 0;                                   \
					a = 0;                                   \
				}                                            \
				if (!is_##TPE##_nil(*bp))                    \
					AVERAGE_ITER_FLOAT(TPE, *bp, a, n);      \
			}                                                \
			curval = n > 0 ? a : dbl_nil;                    \
			has_nils = has_nils || (n == 0);                 \
			for (;rb < rp; rb++)                             \
				*rb = curval;                                \
		} else if (o) { /* single value, ie no partitions */ \
			for(; rp<end; rp++, bp++) {                      \
				if (!is_##TPE##_nil(*bp))                    \
					AVERAGE_ITER_FLOAT(TPE, *bp, a, n);      \
			}                                                \
			curval = n > 0 ? a : dbl_nil;                    \
			has_nils = (n == 0);                             \
			for (;rb < rp; rb++)                             \
				*rb = curval;                                \
		} else { /* single value, ie no ordering */          \
			for(; rp<end; rp++, bp++) {                      \
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
GDKanalyticalavg(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)
{
	bool has_nils = false;
	BUN cnt = BATcount(b), nils = 0, n = 0;
	bit *restrict np;
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
