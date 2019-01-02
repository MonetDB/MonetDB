/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_analytic.h"
#include "gdk_calc_private.h"

#define ANALYTICAL_WINDOW_BOUNDS_ROWS_PRECEDING(LIMIT) \
	do {                                            \
		lng calc1, calc2;                           \
		j = k;                                      \
		for(; k<i; k++, rb++) {                     \
			lng rlimit = (lng) LIMIT;               \
			SUB_WITH_CHECK(k, rlimit, lng, calc1, GDK_lng_max, goto calc_overflow); \
			ADD_WITH_CHECK(calc1, !first_half, lng, calc2, GDK_lng_max, goto calc_overflow); \
			*rb = MAX(calc2, j);                    \
		}                                           \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_ROWS_FOLLOWING(LIMIT) \
	do {                                            \
		lng calc1, calc2;                           \
		for(; k<i; k++, rb++) {                     \
			lng rlimit = (lng) LIMIT;               \
			ADD_WITH_CHECK(rlimit, k, lng, calc1, GDK_lng_max, goto calc_overflow); \
			ADD_WITH_CHECK(calc1, !first_half, lng, calc2, GDK_lng_max, goto calc_overflow); \
			*rb = MIN(calc2, i);                    \
		}                                           \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_PRECEDING(TPE1, LIMIT, TPE2) \
	do {                                           \
		lng m = k - 1;                             \
		TPE1 v, calc;                              \
		TPE2 rlimit;                               \
		for(; k<i; k++, rb++) {                    \
			rlimit = (TPE2) LIMIT;                 \
			v = bp[k];                             \
			if(is_##TPE1##_nil(v)) {               \
				for(j=k; ; j--) {                  \
					if(j == m)                     \
						break;                     \
					if(!is_##TPE1##_nil(bp[j]))    \
						break;                     \
				}                                  \
			} else {                               \
				for(j=k; ; j--) {                  \
					if(j == m)                     \
						break;                     \
					if(is_##TPE1##_nil(bp[j]))     \
						break;                     \
					SUB_WITH_CHECK(v, bp[j], TPE1, calc, GDK_##TPE1##_max, goto calc_overflow); \
					if ((TPE2)(ABSOLUTE(calc)) > rlimit) \
						break;                     \
				}                                  \
			}                                      \
			j++;                                   \
			*rb = j;                               \
		}                                          \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_FOLLOWING(TPE1, LIMIT, TPE2) \
	do {                                         \
		TPE1 v, calc;                            \
		TPE2 rlimit;                             \
		for(; k<i; k++, rb++) {                  \
			rlimit = (TPE2) LIMIT;               \
			v = bp[k];                           \
			if(is_##TPE1##_nil(v)) {             \
				for(j=k+1; j<i; j++) {           \
					if(!is_##TPE1##_nil(bp[j]))  \
						break;                   \
				}                                \
			} else {                             \
				for(j=k+1; j<i; j++) {           \
					if(is_##TPE1##_nil(bp[j]))   \
						break;                   \
					SUB_WITH_CHECK(v, bp[j], TPE1, calc, GDK_##TPE1##_max, goto calc_overflow); \
					if ((TPE2)(ABSOLUTE(calc)) > rlimit) \
						break;                   \
				}                                \
			}                                    \
			*rb = j;                             \
		}                                        \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_PRECEDING(TPE1, LIMIT, TPE2) \
	do {                                  \
		lng m = k - 1;                    \
		for(; k<i; k++, rb++) {           \
			TPE2 rlimit = (TPE2) LIMIT;   \
			TPE1 v = bp[k];               \
			if(is_##TPE1##_nil(v)) {      \
				for(j=k; ; j--) {         \
					if(j == m)            \
						break;            \
					if(!is_##TPE1##_nil(bp[j])) \
						break;            \
				}                         \
			} else {                      \
				for(j=k; ; j--) {         \
					if(j == m)            \
						break;            \
					if(is_##TPE1##_nil(bp[j])) \
						break;            \
					if(v != bp[j]) {      \
						if(rlimit == 0)   \
							break;        \
						rlimit--;         \
						v = bp[j];        \
					}                     \
				}                         \
			}                             \
			j++;                          \
			*rb = j;                      \
		}                                 \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_FOLLOWING(TPE1, LIMIT, TPE2) \
	do {                                \
		for(; k<i; k++, rb++) {         \
			TPE2 rlimit = (TPE2) LIMIT; \
			TPE1 v = bp[k];             \
			if(is_##TPE1##_nil(v)) {    \
				for(j=k+1; j<i; j++) {  \
					if(!is_##TPE1##_nil(bp[j])) \
						break;          \
				}                       \
			} else {                    \
				for(j=k+1; j<i; j++) {  \
					if(is_##TPE1##_nil(bp[j])) \
						break;           \
					if(v != bp[j]) {    \
						if(rlimit == 0) \
							break;      \
						rlimit--;       \
						v = bp[j];      \
					}                   \
				}                       \
			}                           \
			*rb = j;                    \
		}                               \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(TPE1, IMP, LIMIT, TPE2) \
	do {                                        \
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0);  \
		if(np) {                                \
			nend += cnt;                        \
			for(; np<nend; np++) {              \
				if (*np) {                      \
					i += (np - pnp);            \
					IMP(TPE1, LIMIT, TPE2);     \
					pnp = np;                   \
				}                               \
			}                                   \
			i += (np - pnp);                    \
			IMP(TPE1, LIMIT, TPE2);             \
		} else {                                \
			i += (lng) cnt;                     \
			IMP(TPE1, LIMIT, TPE2);             \
		}                                       \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_PRECEDING(LIMIT) \
	do {                                          \
		lng m = k - 1;                            \
		for(; k<i; k++, rb++) {                   \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0) {            \
				for(j=k; ; j--) {                 \
					if(j == m)                    \
						break;                    \
					if(atomcmp(BUNtail(bpi, (BUN) j), nil) != 0) \
						break;                    \
				}                                 \
			} else {                              \
				for(j=k; ; j--) {                 \
					void *next;                   \
					if(j == m)                    \
						break;                    \
					next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						break;                    \
					if(ABSOLUTE(atomcmp(v, next)) > (int) LIMIT) \
						break;                    \
				}                                 \
			}                                     \
			j++;                                  \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_FOLLOWING(LIMIT) \
	do {                                          \
		for(; k<i; k++, rb++) {                   \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0) {            \
				for(j=k+1; j<i; j++) {            \
					if(atomcmp(BUNtail(bpi, (BUN) j), nil) != 0) \
						break;                    \
				}                                 \
			} else {                              \
				for(j=k+1; j<i; j++) {            \
					void *next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						break;                    \
					if(ABSOLUTE(atomcmp(v, next)) > (int) LIMIT) \
						break;                    \
				}                                 \
			}                                     \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_PRECEDING(LIMIT) \
	do {                                          \
		lng m = k - 1;                            \
		for(; k<i; k++, rb++) {                   \
			lng rlimit = (lng) LIMIT;             \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0) {            \
				for(j=k; ; j--) {                 \
					if(j == m)                    \
						break;                    \
					if(atomcmp(BUNtail(bpi, (BUN) j), nil) != 0) \
						break;                    \
				}                                 \
			} else {                              \
				for(j=k; ; j--) {                 \
					void *next;                   \
					if(j == m)                    \
						break;                    \
					next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						break;                    \
					if(atomcmp(v, next)) {        \
						if(rlimit == 0)           \
							break;                \
						rlimit--;                 \
						v = next;                 \
					}                             \
				}                                 \
			}                                     \
			j++;                                  \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_FOLLOWING(LIMIT) \
	do {                                          \
		for(; k<i; k++, rb++) {                   \
			lng rlimit = (lng) LIMIT;             \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0) {            \
				for(j=k+1; j<i; j++) {            \
					if(atomcmp(BUNtail(bpi, (BUN) j), nil) != 0) \
						break;                    \
				}                                 \
			} else {                              \
				for(j=k+1; j<i; j++) {            \
					void *next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						break;                    \
					if(atomcmp(v, next)) {        \
						if(rlimit == 0)           \
							break;                \
						rlimit--;                 \
						v = next;                 \
					}                             \
				}                                 \
			}                                     \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(IMP, LIMIT) \
	if (p) { \
		pnp = np = (bit*)Tloc(p, 0); \
		nend = np + cnt; \
		for(; np<nend; np++) { \
			if (*np) { \
				i += (np - pnp); \
				ANALYTICAL_WINDOW_BOUNDS_ROWS##IMP(LIMIT); \
				pnp = np; \
			} \
		} \
		i += (np - pnp); \
		ANALYTICAL_WINDOW_BOUNDS_ROWS##IMP(LIMIT); \
	} else { \
		i += (lng) cnt; \
		ANALYTICAL_WINDOW_BOUNDS_ROWS##IMP(LIMIT); \
	} \

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(IMP, LIMIT) \
	do { \
		switch(tp1) { \
			case TYPE_bit: \
				goto type_not_supported; \
			case TYPE_bte: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(bte, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE##IMP, LIMIT, lng); \
				break; \
			case TYPE_sht: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(sht, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE##IMP, LIMIT, lng); \
				break; \
			case TYPE_int: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(int, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE##IMP, LIMIT, lng); \
				break; \
			case TYPE_lng: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(lng, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE##IMP, LIMIT, lng); \
				break; \
			default: { \
				if (p) { \
					pnp = np = (bit*)Tloc(p, 0); \
					nend = np + cnt; \
					for(; np<nend; np++) { \
						if (*np) { \
							i += (np - pnp); \
							ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE##IMP(LIMIT); \
							pnp = np; \
						} \
					} \
					i += (np - pnp); \
					ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE##IMP(LIMIT); \
				} else { \
					i += (lng) cnt; \
					ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE##IMP(LIMIT); \
				} \
			} \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(IMP, LIMIT) \
	do { \
		switch(tp1) { \
			case TYPE_flt: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(flt, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE##IMP, LIMIT, flt); \
				break; \
			default: \
				goto type_not_supported; \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(IMP, LIMIT) \
	do { \
		switch(tp1) { \
			case TYPE_dbl: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(dbl, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE##IMP, LIMIT, dbl); \
				break; \
			default: \
				goto type_not_supported; \
		} \
	} while(0)

#ifdef HAVE_HGE
#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(IMP, LIMIT) \
	do { \
		switch(tp1) { \
			case TYPE_hge: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(hge, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE##IMP, LIMIT, hge); \
				break; \
			default: \
				goto type_not_supported; \
		} \
	} while(0)
#endif

#ifdef HAVE_HGE
#define ANALYTICAL_WINDOW_BOUNDS_GROUPS_HGE(IMP, LIMIT) \
	case TYPE_hge: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(hge, ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS##IMP, LIMIT, hge); \
		break;
#else
#define ANALYTICAL_WINDOW_BOUNDS_GROUPS_HGE(IMP, LIMIT)
#endif

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(IMP, LIMIT) \
	do { \
		switch(tp1) { \
			case TYPE_bit: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(bit, ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS##IMP, LIMIT, bit); \
				break; \
			case TYPE_bte: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(bte, ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS##IMP, LIMIT, bte); \
				break; \
			case TYPE_sht: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(sht, ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS##IMP, LIMIT, sht); \
				break; \
			case TYPE_int: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(int, ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS##IMP, LIMIT, int); \
				break; \
			case TYPE_lng: \
				ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(lng, ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS##IMP, LIMIT, lng); \
				break; \
			ANALYTICAL_WINDOW_BOUNDS_GROUPS_HGE(IMP, LIMIT); \
			default: { \
				if (p) { \
					pnp = np = (bit*)Tloc(p, 0); \
					nend = np + cnt; \
					for(; np<nend; np++) { \
						if (*np) { \
							i += (np - pnp); \
							ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS##IMP(LIMIT); \
							pnp = np; \
						} \
					} \
					i += (np - pnp); \
					ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS##IMP(LIMIT); \
				} else { \
					i += (lng) cnt; \
					ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS##IMP(LIMIT); \
				} \
			} \
		} \
	} while(0)

static gdk_return
GDKanalyticalallbounds(BAT *r, BAT *b, BAT *p, bool preceding)
{
	BUN cnt = BATcount(b);
	lng *restrict rb = (lng*) Tloc(r, 0), i = 0, k = 0, j = 0;
	bit *np = p ? (bit*) Tloc(p, 0) : NULL, *pnp = np, *nend = np;

	if(preceding) {
		if(np) {
			nend += cnt;
			for(; np<nend; np++) {
				if (*np) {
					i += (np - pnp);
					j = k;
					for (; k<i; k++)
						rb[k] = j;
					pnp = np;
				}
			}
			i += (np - pnp);
			j = k;
			for(; k<i; k++)
				rb[k] = j;
		} else {
			i += (lng) cnt;
			j = k;
			for(; k<i; k++)
				rb[k] = j;
		}
	} else if(np) { /* following */
		nend += cnt;
		for(; np<nend; np++) {
			if (*np) {
				i += (np - pnp);
				for(; k<i; k++)
					rb[k] = i;
				pnp = np;
			}
		}
		i += (np - pnp);
		for(; k<i; k++)
			rb[k] = i;
	} else {
		i += (lng) cnt;
		for(; k<i; k++)
			rb[k] = i;
	}

	BATsetcount(r, cnt);
	r->tnonil = false;
	r->tnil = false;
	return GDK_SUCCEED;
}

static gdk_return
GDKanalyticalrowbounds(BAT *r, BAT *b, BAT *p, BAT *l, const void* restrict bound, int tp2, bool preceding, lng first_half)
{
	BUN cnt = BATcount(b), nils = 0;
	lng *restrict rb = (lng*) Tloc(r, 0), i = 0, k = 0, j = 0;
	bit *np, *pnp, *nend;
	int abort_on_error = 1;

	if (l) { /* dynamic bounds */
		switch(tp2) {
			case TYPE_bte: {
				bte *restrict limit = (bte*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_sht: {
				sht *restrict limit = (sht*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_int: {
				int *restrict limit = (int*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_lng: {
				lng *restrict limit = (lng*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, limit[k]);
				}
			} break;
			default:
				goto bound_not_supported;
		}
	} else { /* static bounds */
		lng limit;
		switch(tp2) {
			case TYPE_bte:
				limit = (lng)(*(bte*)bound);
				break;
			case TYPE_sht:
				limit = (lng)(*(sht*)bound);
				break;
			case TYPE_int:
				limit = (lng)(*(int*)bound);
				break;
			case TYPE_lng:
				limit = (lng)(*(lng*)bound);
				break;
			default:
				goto bound_not_supported;
		}
		if (limit == GDK_lng_max) {
			return GDKanalyticalallbounds(r, b, p, preceding);
		} else if(preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, limit);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, limit);
		}
	}

	BATsetcount(r, cnt);
	r->tnonil = (nils == 0);
	r->tnil = (nils > 0);
	return GDK_SUCCEED;
bound_not_supported:
	GDKerror("GDKanalyticalrowbounds: rows frame bound type %s not supported.\n", ATOMname(tp2));
	return GDK_FAIL;
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}

static gdk_return
GDKanalyticalrangebounds(BAT *r, BAT *b, BAT *p, BAT *l, const void* restrict bound, int tp1, int tp2, bool preceding)
{
	BUN cnt = BATcount(b), nils = 0;
	lng *restrict rb = (lng*) Tloc(r, 0), i = 0, k = 0, j = 0;
	bit *np = p ? (bit*) Tloc(p, 0) : NULL, *pnp = np, *nend = np;
	BATiter bpi = bat_iterator(b);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tp1);
	const void* restrict nil = ATOMnilptr(tp1);
	int abort_on_error = 1;

	if (l) { /* dynamic bounds */
		switch(tp2) {
			case TYPE_bte: {
				bte *restrict limit = (bte*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_sht: {
				sht *restrict limit = (sht*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_int: {
				int *restrict limit = (int*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_lng: {
				lng *restrict limit = (lng*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_flt: {
				flt *restrict limit = (flt*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_dbl: {
				dbl *restrict limit = (dbl*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_FOLLOWING, limit[k]);
				}
			} break;
#ifdef HAVE_HGE
			case TYPE_hge: {
				hge *restrict limit = (hge*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_FOLLOWING, limit[k]);
				}
			} break;
#endif
			default:
				goto bound_not_supported;
		}
	} else { /* static bounds */
		switch (tp2) {
			case TYPE_bte:
			case TYPE_sht:
			case TYPE_int:
			case TYPE_lng: {
				lng limit = 0;
				switch(tp2) {
					case TYPE_bte: {
						bte ll = (*(bte*) bound);
						if (ll == GDK_bte_max) /* UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING cases, avoid overflow */
							return GDKanalyticalallbounds(r, b, p, preceding);
						else
							limit = (lng) ll;
					} break;
					case TYPE_sht: {
						sht ll = (*(sht*)bound);
						if (ll == GDK_sht_max)
							return GDKanalyticalallbounds(r, b, p, preceding);
						else
							limit = (lng) ll;
					} break;
					case TYPE_int: {
						int ll = (*(int*)bound);
						if (ll == GDK_int_max)
							return GDKanalyticalallbounds(r, b, p, preceding);
						else
							limit = (lng) ll;
					} break;
					case TYPE_lng: {
						lng ll = (*(lng*)bound);
						if (ll == GDK_lng_max)
							return GDKanalyticalallbounds(r, b, p, preceding);
						else
							limit = (lng) ll;
					} break;
					default:
						assert(0);
				}
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, limit);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, limit);
				}
			} break;
			case TYPE_flt: {
				flt limit = (*(flt*)bound);
				if (limit == GDK_flt_max) {
					return GDKanalyticalallbounds(r, b, p, preceding);
				} else if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_PRECEDING, limit);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_FOLLOWING, limit);
				}
			} break;
			case TYPE_dbl: {
				dbl limit = (*(dbl*)bound);
				if (limit == GDK_dbl_max) {
					return GDKanalyticalallbounds(r, b, p, preceding);
				} else if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_PRECEDING, limit);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_FOLLOWING, limit);
				}
			} break;
#ifdef HAVE_HGE
			case TYPE_hge: {
				hge limit = (*(hge*)bound);
				if (limit == GDK_hge_max) {
					return GDKanalyticalallbounds(r, b, p, preceding);
				} else if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_PRECEDING, limit);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_FOLLOWING, limit);
				}
			} break;
#endif
			default:
				goto bound_not_supported;
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = (nils == 0);
	r->tnil = (nils > 0);
	return GDK_SUCCEED;
bound_not_supported:
	GDKerror("GDKanalyticalrangebounds: range frame bound type %s not supported.\n", ATOMname(tp2));
	return GDK_FAIL;
type_not_supported:
	GDKerror("GDKanalyticalrangebounds: type %s not supported for %s frame bound type.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}

static gdk_return
GDKanalyticalgroupsbounds(BAT *r, BAT *b, BAT *p, BAT *l, const void* restrict bound, int tp1, int tp2, bool preceding)
{
	BUN cnt = BATcount(b), nils = 0;
	lng *restrict rb = (lng*) Tloc(r, 0), i = 0, k = 0, j = 0;
	bit *np = p ? (bit*) Tloc(p, 0) : NULL, *pnp = np, *nend = np;
	BATiter bpi = bat_iterator(b);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tp1);
	const void* restrict nil = ATOMnilptr(tp1);

	if (l) { /* dynamic bounds */
		switch(tp2) {
			case TYPE_bte: {
				bte *restrict limit = (bte*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_sht: {
				sht *restrict limit = (sht*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_int: {
				int *restrict limit = (int*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_lng: {
				lng *restrict limit = (lng*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, limit[k]);
				}
			} break;
			default:
				goto bound_not_supported;
		}
	} else { /* static bounds */
		lng limit;
		switch(tp2) {
			case TYPE_bte:
				limit = (lng)(*(bte*)bound);
				break;
			case TYPE_sht:
				limit = (lng)(*(sht*)bound);
				break;
			case TYPE_int:
				limit = (lng)(*(int*)bound);
				break;
			case TYPE_lng:
				limit = (lng)(*(lng*)bound);
				break;
			default:
				goto bound_not_supported;
		}
		if (limit == GDK_lng_max) {
			return GDKanalyticalallbounds(r, b, p, preceding);
		} else if(preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, limit);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, limit);
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = (nils == 0);
	r->tnil = (nils > 0);
	return GDK_SUCCEED;
bound_not_supported:
	GDKerror("GDKanalyticalgroupsbounds: groups frame bound type %s not supported.\n", ATOMname(tp2));
	return GDK_FAIL;
}

gdk_return
GDKanalyticalwindowbounds(BAT *r, BAT *b, BAT *p, BAT *l, const void* restrict bound, int tp1, int tp2, int unit,
						  bool preceding, lng first_half)
{
	assert((l && !bound) || (!l && bound));

	switch(unit) {
		case 0:
			return GDKanalyticalrowbounds(r, b, p, l, bound, tp2, preceding, first_half);
		case 1:
			return GDKanalyticalrangebounds(r, b, p, l, bound, tp1, tp2, preceding);
		case 2:
			return GDKanalyticalgroupsbounds(r, b, p, l, bound, tp1, tp2, preceding);
		case 3:
			return GDKanalyticalallbounds(r, b, p, preceding);
		default:
			assert(0);
	}
	GDKerror("GDKanalyticalwindowbounds: unit type %d not supported (this is a bug).\n", unit);
	return GDK_FAIL;
}
