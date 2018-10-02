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
		if(np) {                              \
			for(; bp<end; bp++, rb++, np++) { \
				*rb = *np;                    \
				if (*bp != prev) {            \
					*rb = TRUE;               \
					prev = *bp;               \
				}                             \
			}                                 \
		} else {                              \
			for(; bp<end; bp++, rb++) {       \
				if (*bp != prev) {            \
					*rb = TRUE;               \
					prev = *bp;               \
				} else {                      \
					*rb = FALSE;              \
				}                             \
			}                                 \
		}                                     \
	} while(0)

gdk_return
GDKanalyticaldiff(BAT *r, BAT *b, BAT *p, int tpe)
{
	BUN i, cnt = BATcount(b);
	bit *restrict rb = (bit*)Tloc(r, 0), *restrict np = p ? (bit*)Tloc(p, 0) : NULL;

	switch(tpe) {
		case TYPE_bit:
			ANALYTICAL_DIFF_IMP(bit);
			break;
		case TYPE_bte:
			ANALYTICAL_DIFF_IMP(bte);
			break;
		case TYPE_sht:
			ANALYTICAL_DIFF_IMP(sht);
			break;
		case TYPE_int:
			ANALYTICAL_DIFF_IMP(int);
			break;
		case TYPE_lng:
			ANALYTICAL_DIFF_IMP(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_DIFF_IMP(hge);
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_DIFF_IMP(flt);
			break;
		case TYPE_dbl:
			ANALYTICAL_DIFF_IMP(dbl);
			break;
		default: {
			BATiter it = bat_iterator(b);
			ptr v = BUNtail(it, 0), next;
			int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe);
			if(np) {
				for (i=0; i<cnt; i++, rb++, np++) {
					*rb = *np;
					next = BUNtail(it, i);
					if (atomcmp(v, next) != 0) {
						*rb = TRUE;
						v = next;
					}
				}
			} else {
				for(i=0; i<cnt; i++, rb++) {
					next = BUNtail(it, i);
					if (atomcmp(v, next) != 0) {
						*rb = TRUE;
						v = next;
					} else {
						*rb = FALSE;
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

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_PRECEDING(TPE, LIMIT) /* TPE is ignored in this case */ \
	do {                                            \
		lng calc1, calc2;                           \
		j = k;                                      \
		for(; k<i; k++, rb++) {                     \
			lng rlimit = (lng) LIMIT;               \
			SUB_WITH_CHECK(lng, k, lng, rlimit, lng, calc1, GDK_lng_max, goto calc_overflow); \
			ADD_WITH_CHECK(lng, calc1, lng, !first_half, lng, calc2, GDK_lng_max, goto calc_overflow); \
			*rb = MAX(calc2, j);                    \
		}                                           \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_FOLLOWING(TPE, LIMIT) /* TPE is ignored in this case */ \
	do {                                            \
		lng calc1, calc2;                           \
		for(; k<i; k++, rb++) {                     \
			lng rlimit = (lng) LIMIT;               \
			ADD_WITH_CHECK(lng, rlimit, lng, k, lng, calc1, GDK_lng_max, goto calc_overflow); \
			ADD_WITH_CHECK(lng, calc1, lng, !first_half, lng, calc2, GDK_lng_max, goto calc_overflow); \
			*rb = MIN(calc2, i);                    \
		}                                           \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_SECOND_PRECEDING(TPE, LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_PRECEDING(TPE, LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_SECOND_FOLLOWING(TPE, LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_FOLLOWING(TPE, LIMIT)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_FIRST_PRECEDING(TPE, LIMIT) \
	do {                                           \
		lng m = k;                                 \
		TPE v, rlimit, calc;                       \
		for(; k<i; k++, rb++) {                    \
			rlimit = (TPE) LIMIT;                  \
			v = bp[k];                             \
			if(is_##TPE##_nil(v))                  \
				j = m;                             \
			else                                   \
				for(j=k; j>m; j--) {               \
					if(is_##TPE##_nil(bp[j]))      \
						continue;                  \
					SUB_WITH_CHECK(TPE, v, TPE, bp[j], TPE, calc, GDK_##TPE##_max, goto calc_overflow); \
					if (ABSOLUTE(calc) > rlimit) { \
						j++;                       \
						break;                     \
					}                              \
				}                                  \
			*rb = j;                               \
		}                                          \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_FIRST_FOLLOWING(TPE, LIMIT) \
	do {                                         \
		TPE v, rlimit, calc;                     \
		for(; k<i; k++, rb++) {                  \
			rlimit = (TPE) LIMIT;                \
			v = bp[k];                           \
			if(is_##TPE##_nil(v))                \
				j = i;                           \
			else                                 \
				for(j=k+1; j<i; j++) {           \
					if(is_##TPE##_nil(bp[j]))    \
						continue;                \
					SUB_WITH_CHECK(TPE, v, TPE, bp[j], TPE, calc, GDK_##TPE##_max, goto calc_overflow); \
					if (ABSOLUTE(calc) > rlimit) \
						break;                   \
				}                                \
			*rb = j;                             \
		}                                        \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_SECOND_FOLLOWING(TPE, LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_FIRST_FOLLOWING(TPE, LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_SECOND_PRECEDING(TPE, LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_FIRST_PRECEDING(TPE, LIMIT)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_FIRST_PRECEDING(TPE, LIMIT) \
	do {                                  \
		lng m = k;                        \
		for(; k<i; k++, rb++) {           \
			lng rlimit = (lng) LIMIT;     \
			TPE v = bp[k];                \
			if(is_##TPE##_nil(v))         \
				j = m;                    \
			else                          \
				for(j=k; j>m; j--) {      \
					if(is_##TPE##_nil(bp[j])) \
						continue;         \
					if(v != bp[j]) {      \
						if(rlimit == 0) { \
							j++;          \
							break;        \
						}                 \
						rlimit--;         \
						v = bp[j];        \
					}                     \
				}                         \
			*rb = j;                      \
		}                                 \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_FIRST_FOLLOWING(TPE, LIMIT) \
	do {                                \
		for(; k<i; k++, rb++) {         \
			lng rlimit = (lng) LIMIT;   \
			TPE v = bp[k];              \
			if(is_##TPE##_nil(v))       \
				j = i;                  \
			else                        \
				for(j=k+1; j<i; j++) {  \
					if(is_##TPE##_nil(bp[j])) \
						continue;       \
					if(v != bp[j]) {    \
						if(rlimit == 0) \
							break;      \
						rlimit--;       \
						v = bp[j];      \
					}                   \
				}                       \
			*rb = j;                    \
		}                               \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_SECOND_FOLLOWING(TPE, LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_FIRST_FOLLOWING(TPE, LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_SECOND_PRECEDING(TPE, LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_FIRST_PRECEDING(TPE, LIMIT)
	
#define ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(TPE, IMP, LIMIT) \
	do {                                            \
		TPE *restrict bp = (TPE*)Tloc(b, 0);        \
		(void) bp;                                  \
		if(preceding) {                             \
			if(np) {                                \
				nend += cnt;                        \
				for(; np<nend; np++) {              \
					if (*np) {                      \
						i += (np - pnp);            \
						IMP##_PRECEDING(TPE, LIMIT); \
						pnp = np;                   \
					}                               \
				}                                   \
				i += (np - pnp);                    \
				IMP##_PRECEDING(TPE, LIMIT);	    \
			} else {                                \
				i += (lng) cnt;                     \
				IMP##_PRECEDING(TPE, LIMIT);	    \
			}                                       \
		} else if(np) {                             \
			nend += cnt;                            \
			for(; np<nend; np++) {                  \
				if (*np) {                          \
					i += (np - pnp);                \
					IMP##_FOLLOWING(TPE, LIMIT);	\
					pnp = np;                       \
				}                                   \
			}                                       \
			i += (np - pnp);                        \
			IMP##_FOLLOWING(TPE, LIMIT);		\
		} else {                                    \
			i += (lng) cnt;                         \
			IMP##_FOLLOWING(TPE, LIMIT);		\
		}                                           \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_FIRST_PRECEDING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_PRECEDING(lng, LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_FIRST_FOLLOWING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_FOLLOWING(lng, LIMIT)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_SECOND_PRECEDING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_FIRST_PRECEDING(LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_SECOND_FOLLOWING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_FIRST_FOLLOWING(LIMIT)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_FIRST_PRECEDING(LIMIT) \
	do {                                          \
		lng m = k;                                \
		for(; k<i; k++, rb++) {                   \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0)              \
				j = m;                            \
			else                                  \
				for(j=k; j>m; j--) {              \
					void *next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						continue;                 \
					if(ABSOLUTE(atomcmp(v, next)) > (int) LIMIT) { \
						j++;                      \
						break;                    \
					}                             \
				}                                 \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_SECOND_PRECEDING(LIMIT) \
	do {                                          \
		for(; k<i; k++, rb++) {                   \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0)              \
				j = i;                            \
			else                                  \
				for(j=k+1; j<i; j++) {            \
					void *next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						continue;                 \
					if(ABSOLUTE(atomcmp(v, next)) > (int) LIMIT) \
						break;                    \
				}                                 \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_FIRST_FOLLOWING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_FIRST_PRECEDING(LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_SECOND_FOLLOWING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_SECOND_PRECEDING(LIMIT)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_FIRST_PRECEDING(LIMIT) \
	do {                                          \
		lng m = k;                                \
		for(; k<i; k++, rb++) {                   \
			lng rlimit = (lng) LIMIT;             \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0)              \
				j = m;                            \
			else                                  \
				for(j=k; j>m; j--) {              \
					void *next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						continue;                 \
					if(atomcmp(v, next)) {        \
						if(rlimit == 0) {         \
							j++;                  \
							break;                \
						}                         \
						rlimit--;                 \
						v = next;                 \
					}                             \
				}                                 \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_SECOND_PRECEDING(LIMIT) \
	do {                                          \
		for(; k<i; k++, rb++) {                   \
			lng rlimit = (lng) LIMIT;             \
			void *v = BUNtail(bpi, (BUN) k);      \
			if(atomcmp(v, nil) == 0)              \
				j = i;                            \
			else                                  \
				for(j=k+1; j<i; j++) {            \
					void *next = BUNtail(bpi, (BUN) j); \
					if(atomcmp(next, nil) == 0)   \
						continue;                 \
					if(atomcmp(v, next)) {        \
						if(rlimit == 0)           \
							break;                \
						rlimit--;                 \
						v = next;                 \
					}                             \
				}                                 \
			*rb = j;                              \
		}                                         \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_FIRST_FOLLOWING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_FIRST_PRECEDING(LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_SECOND_FOLLOWING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_SECOND_PRECEDING(LIMIT)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_ALL_ALL_PRECEDING(TPE, LIMIT) \
	do {                      \
		j = k;                \
		for(; k<i; k++, rb++) \
			*rb = j;          \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_ALL_ALL_FOLLOWING(TPE, LIMIT) \
	do {                      \
		for(; k<i; k++, rb++) \
			*rb = i;          \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ALL_ALL_PRECEDING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_ALL_ALL_PRECEDING(LIMIT, LIMIT)
#define ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ALL_ALL_FOLLOWING(LIMIT) ANALYTICAL_WINDOW_BOUNDS_FIXED_ALL_ALL_FOLLOWING(LIMIT, LIMIT)

#define ANALYTICAL_WINDOW_BOUNDS_NUM(FRAME, HALF, LIMIT) \
	case TYPE_bit: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(bit, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break; \
	case TYPE_bte: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(bte, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break; \
	case TYPE_sht: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(sht, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break; \
	case TYPE_int: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(int, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break; \
	case TYPE_lng: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(lng, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break; \

#ifdef HAVE_HGE
#define ANALYTICAL_WINDOW_BOUNDS_HGE(FRAME, HALF, LIMIT) \
	case TYPE_hge: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(hge, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break;
#else
#define ANALYTICAL_WINDOW_BOUNDS_HGE(FRAME, HALF, LIMIT)
#endif

#define ANALYTICAL_WINDOW_BOUNDS_FP(FRAME, HALF, LIMIT) \
	case TYPE_flt: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(flt, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break; \
	case TYPE_dbl: \
		ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED(dbl, ANALYTICAL_WINDOW_BOUNDS_FIXED##FRAME##HALF, LIMIT); \
		break; \

#define ANALYTICAL_WINDOW_BOUNDS_OTHERS(FRAME, HALF, LIMIT) \
	if(preceding) { \
		if (p) { \
			pnp = np = (bit*)Tloc(p, 0); \
			nend = np + cnt; \
			for(; np<nend; np++) { \
				if (*np) { \
					i += (np - pnp); \
					ANALYTICAL_WINDOW_BOUNDS_VARSIZED##FRAME##HALF##_PRECEDING(LIMIT); \
					pnp = np; \
				} \
			} \
			i += (np - pnp); \
			ANALYTICAL_WINDOW_BOUNDS_VARSIZED##FRAME##HALF##_PRECEDING(LIMIT); \
		} else { \
			i += (lng) cnt; \
			ANALYTICAL_WINDOW_BOUNDS_VARSIZED##FRAME##HALF##_PRECEDING(LIMIT); \
		} \
	} else if (p) { \
		pnp = np = (bit*)Tloc(p, 0); \
		nend = np + cnt; \
		for(; np<nend; np++) { \
			if (*np) { \
				i += (np - pnp); \
				ANALYTICAL_WINDOW_BOUNDS_VARSIZED##FRAME##HALF##_FOLLOWING(LIMIT); \
				pnp = np; \
			} \
		} \
		i += (np - pnp); \
		ANALYTICAL_WINDOW_BOUNDS_VARSIZED##FRAME##HALF##_FOLLOWING(LIMIT); \
	} else { \
		i += (lng) cnt; \
		ANALYTICAL_WINDOW_BOUNDS_VARSIZED##FRAME##HALF##_FOLLOWING(LIMIT); \
	}

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(FRAME, HALF, LIMIT) \
	do { \
		switch(tp1) { \
			ANALYTICAL_WINDOW_BOUNDS_NUM(FRAME, HALF, LIMIT); \
			ANALYTICAL_WINDOW_BOUNDS_HGE(FRAME, HALF, LIMIT); \
			ANALYTICAL_WINDOW_BOUNDS_FP(FRAME, HALF, LIMIT); \
			default: { \
				ANALYTICAL_WINDOW_BOUNDS_OTHERS(FRAME, HALF, LIMIT); \
			} \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_NUM(FRAME, HALF, LIMIT) \
	do { \
		switch(tp1) { \
			ANALYTICAL_WINDOW_BOUNDS_NUM(FRAME, HALF, LIMIT); \
			ANALYTICAL_WINDOW_BOUNDS_HGE(FRAME, HALF, LIMIT); \
			default: { \
				ANALYTICAL_WINDOW_BOUNDS_OTHERS(FRAME, HALF, LIMIT); \
			} \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_FP(FRAME, HALF, LIMIT) \
	do { \
		switch(tp1) { \
			ANALYTICAL_WINDOW_BOUNDS_FP(FRAME, HALF, LIMIT); \
			default: { \
				goto logical_bound_not_supported; \
			} \
		} \
	} while(0)

#define NO_LIMIT ;

#define ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(TPE, IMP) \
	do { \
		if (l) { /* dynamic bounds */ \
			TPE *restrict limit = (TPE*) Tloc(l, 0); \
			if(first_half) { \
				IMP(_RANGE, _FIRST, limit[k]); \
			} else { \
				IMP(_RANGE, _SECOND, limit[k]); \
			} \
		} else { /* static bounds */ \
			TPE limit = *((TPE*)bound); \
			if (limit == GDK_##TPE##_max) { /* UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING cases, avoid overflow */ \
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_ALL, _ALL, NO_LIMIT); \
			} else if(first_half) { \
				IMP(_RANGE, _FIRST, limit);	\
			} else { \
				IMP(_RANGE, _SECOND, limit); \
			} \
		} \
	} while(0)

#define ANALYTICAL_BOUNDS_BRANCHES_PHYSICAL(TPE) \
	do { \
		if (l) { /* dynamic bounds */ \
			TPE *restrict limit = (TPE*) Tloc(l, 0); \
			if (unit == 0) { \
				if(first_half) { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_ROWS, _FIRST, limit[k]); \
				} else { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_ROWS, _SECOND, limit[k]); \
				} \
			} else if (unit == 2) { \
				if(first_half) { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_GROUPS, _FIRST, limit[k]); \
				} else { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_GROUPS, _SECOND, limit[k]); \
				} \
			} else { \
				assert(0); \
			} \
		} else { /* static bounds */ \
			TPE limit = *((TPE*)bound); \
			if (unit == 0) { \
				if(first_half) { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_ROWS, _FIRST, limit); \
				} else { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_ROWS, _SECOND, limit); \
				} \
			} else if (unit == 2) { \
				if(first_half) { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_GROUPS, _FIRST, limit); \
				} else { \
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_GROUPS, _SECOND, limit); \
				} \
			} else { \
				assert(0); \
			} \
		} \
	} while(0)

gdk_return
GDKanalyticalwindowbounds(BAT *r, BAT *b, BAT *p, BAT *l, const void* restrict bound, int tp1, int tp2, int unit, bool preceding, lng first_half)
{
	BUN cnt = BATcount(b), nils = 0;
	lng *restrict rb = (lng*) Tloc(r, 0), i = 0, k = 0, j = 0;
	bit *np = p ? (bit*) Tloc(p, 0) : NULL, *pnp = np, *nend = np;
	BATiter bpi = bat_iterator(b);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tp1);
	const void* restrict nil = ATOMnilptr(tp1);
	int abort_on_error = 1;

	assert(unit >= 0 && unit <= 3);

	if (unit == 3) { //special case, there are no boundaries
		ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PHYSICAL(_ALL, _ALL, NO_LIMIT);
	} else {
		assert((!l && bound) || (l && !bound));
		if(unit == 1) { /* on range frame, floating-point bounds are acceptable */
			switch(tp2) {
				case TYPE_bte:
					ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(bte, ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_NUM);
					break;
				case TYPE_sht:
					ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(sht, ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_NUM);
					break;
				case TYPE_int:
					ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(int, ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_NUM);
					break;
				case TYPE_lng:
					ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(lng, ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_NUM);
					break;
				case TYPE_flt:
					ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(flt, ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_FP); 
					break;
				case TYPE_dbl:
					ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(dbl, ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_FP);
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					ANALYTICAL_BOUNDS_BRANCHES_LOGICAL(hge, ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_NUM);
					break;
#endif
				default:
					goto physical_bound_not_supported;
			}
		} else {
			switch(tp2) {
				case TYPE_bte:
					ANALYTICAL_BOUNDS_BRANCHES_PHYSICAL(bte);
					break;
				case TYPE_sht:
					ANALYTICAL_BOUNDS_BRANCHES_PHYSICAL(sht);
					break;
				case TYPE_int:
					ANALYTICAL_BOUNDS_BRANCHES_PHYSICAL(int);
					break;
				case TYPE_lng:
					ANALYTICAL_BOUNDS_BRANCHES_PHYSICAL(lng);
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					ANALYTICAL_BOUNDS_BRANCHES_PHYSICAL(hge);
					break;
#endif
				default:
					goto physical_bound_not_supported;
			}
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = (nils == 0);
	r->tnil = (nils > 0);
	return GDK_SUCCEED;
logical_bound_not_supported:
	GDKerror("GDKanalyticalwindowbounds: range frame bound type %s not supported.\n", ATOMname(tp1));
	return GDK_FAIL;
physical_bound_not_supported:
	assert(unit == 0 || unit == 2);
	GDKerror("GDKanalyticalwindowbounds: %s frame bound type %s not supported.\n", (unit == 0) ? "rows" : "groups", ATOMname(tp2));
	return GDK_FAIL;
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}

#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_FIRST_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_SECOND_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_ROWS_SECOND_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_FIRST_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_SECOND_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_FIRST_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_SECOND_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_FIRST_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_SECOND_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_FIRST_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_GROUPS_SECOND_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_FIRST_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_FIRST_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_SECOND_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ROWS_SECOND_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_FIRST_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_SECOND_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_FIRST_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_RANGE_SECOND_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_FIRST_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_SECOND_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_FIRST_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_GROUPS_SECOND_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_ALL_ALL_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ALL_ALL_PRECEDING
#undef ANALYTICAL_WINDOW_BOUNDS_FIXED_ALL_ALL_FOLLOWING
#undef ANALYTICAL_WINDOW_BOUNDS_VARSIZED_ALL_ALL_FOLLOWING
#undef ANALYTICAL_BOUNDS_BRANCHES_LOGICAL
#undef ANALYTICAL_BOUNDS_BRANCHES_PHYSICAL
#undef ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_NUM
#undef ANALYTICAL_WINDOW_BOUNDS_BRANCHES_LOGICAL_FP
#undef ANALYTICAL_WINDOW_BOUNDS_BRANCHES
#undef ANALYTICAL_WINDOW_BOUNDS_NUM
#undef ANALYTICAL_WINDOW_BOUNDS_HGE
#undef ANALYTICAL_WINDOW_BOUNDS_FP
#undef ANALYTICAL_WINDOW_BOUNDS_OTHERS
#undef ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED
#undef NO_LIMIT

#define NTILE_CALC                    \
	do {                              \
		if(bval >= ncnt) {            \
			j = 1;                    \
			for(; rb<rp; j++, rb++)   \
				*rb = j;              \
		} else if(ncnt % bval == 0) { \
			buckets = ncnt / bval;    \
			for(; rb<rp; i++, rb++) { \
				if(i == buckets) {    \
					j++;              \
					i = 0;            \
				}                     \
				*rb = j;              \
			}                         \
		} else {                      \
			buckets = ncnt / bval;    \
			for(; rb<rp; i++, rb++) { \
				*rb = j;              \
				if(i == buckets) {    \
					j++;              \
					i = 0;            \
				}                     \
			}                         \
		}                             \
	} while(0)

#define ANALYTICAL_NTILE_IMP(TPE)            \
	do {                                     \
		TPE j = 1, *rp, *rb, val = *(TPE*) ntile; \
		BUN bval = (BUN) val;                \
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
					NTILE_CALC;		 \
					pnp = np;                \
				}                            \
			}                                \
			i = 0;                           \
			j = 1;                           \
			ncnt = np - pnp;                 \
			rp += ncnt;                      \
			NTILE_CALC;			 \
		} else {                             \
			rp += cnt;                       \
			NTILE_CALC;			 \
		}                                    \
	} while(0)

gdk_return
GDKanalyticalntile(BAT *r, BAT *b, BAT *p, int tpe, const void* restrict ntile)
{
	BUN cnt = BATcount(b), ncnt = cnt, buckets, i = 0;
	bit *np, *pnp, *end;
	bool has_nils = false;

	assert(ntile);

	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_NTILE_IMP(bte);
			break;
		case TYPE_sht:
			ANALYTICAL_NTILE_IMP(sht);
			break;
		case TYPE_int:
			ANALYTICAL_NTILE_IMP(int);
			break;
		case TYPE_lng:
			ANALYTICAL_NTILE_IMP(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_NTILE_IMP(hge);
			break;
#endif
		default:
			GDKerror("GDKanalyticalntile: type %s not supported.\n", ATOMname(tpe));
			return GDK_FAIL;
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_NTILE_IMP
#undef NTILE_CALC

#define FIRST_CALC(TPE)            \
	do {                           \
		for (;rb < rp; rb++)       \
			*rb = curval;          \
		if(is_##TPE##_nil(curval)) \
			has_nils = true;       \
	} while(0)

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
					FIRST_CALC(TPE);	\
					curval = *bp;           \
					pnp = np;               \
				}                           \
			}                               \
			ncnt = (np - pnp);              \
			rp += ncnt;                     \
			bp += ncnt;                     \
			FIRST_CALC(TPE);		\
		} else {                            \
			rp += cnt;                      \
			FIRST_CALC(TPE);		\
		}                                   \
	} while(0)

#define ANALYTICAL_FIRST_OTHERS                                         \
	do {                                                                \
		curval = BUNtail(bpi, j);                                       \
		if (atomcmp(curval, nil) == 0)                                  \
			has_nils = true;                                            \
		for (;j < i; j++) {                                             \
			if (BUNappend(r, curval, false) != GDK_SUCCEED)             \
				goto allocation_error;                                  \
		}                                                               \
	} while(0)

gdk_return
GDKanalyticalfirst(BAT *r, BAT *b, BAT *p, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i = 0, j = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;

	switch(tpe) {
		case TYPE_bit:
			ANALYTICAL_FIRST_IMP(bit);
			break;
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
#ifdef HAVE_HUGE
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
						ANALYTICAL_FIRST_OTHERS;
						pnp = np;
					}
				}
				i += (np - pnp);
				ANALYTICAL_FIRST_OTHERS;
			} else {
				i += cnt;
				ANALYTICAL_FIRST_OTHERS;
			}
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
allocation_error:
	GDKerror("GDKanalyticalfirst: malloc failure\n");
	return GDK_FAIL;
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
	} while(0)

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
					LAST_CALC(TPE);		\
					pnp = np;               \
				}                           \
			}                               \
			ncnt = (np - pnp);              \
			rp += ncnt;                     \
			bp += ncnt;                     \
			LAST_CALC(TPE);			\
		} else {                            \
			rp += cnt;                      \
			bp += cnt;                      \
			LAST_CALC(TPE);			\
		}                                   \
	} while(0)

#define ANALYTICAL_LAST_OTHERS                                          \
	do {                                                                \
		curval = BUNtail(bpi, i - 1);                                   \
		if (atomcmp(curval, nil) == 0)                                  \
			has_nils = true;                                            \
		for (;j < i; j++) {                                             \
			if (BUNappend(r, curval, false) != GDK_SUCCEED)             \
				goto allocation_error;                                  \
		}                                                               \
	} while(0)

gdk_return
GDKanalyticallast(BAT *r, BAT *b, BAT *p, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	bool has_nils = false;
	BUN i = 0, j = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;

	switch(tpe) {
		case TYPE_bit:
			ANALYTICAL_LAST_IMP(bit);
			break;
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
#ifdef HAVE_HUGE
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
						ANALYTICAL_LAST_OTHERS;
						pnp = np;
					}
				}
				i += (np - pnp);
				ANALYTICAL_LAST_OTHERS;
			} else {
				i += cnt;
				ANALYTICAL_LAST_OTHERS;
			}
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
allocation_error:
	GDKerror("GDKanalyticallast: malloc failure\n");
	return GDK_FAIL;
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
	} while(0)

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
					NTHVALUE_CALC(TPE);  \
					pbp = bp;            \
					pnp = np;            \
				}                        \
			}                            \
			ncnt = (np - pnp);           \
			rp += ncnt;                  \
			bp += ncnt;                  \
			NTHVALUE_CALC(TPE);	     \
		} else {                         \
			rp += cnt;                   \
			bp += cnt;                   \
			NTHVALUE_CALC(TPE);	     \
		}                                \
	} while(0)

#define ANALYTICAL_NTHVALUE_OTHERS                                      \
	do {                                                                \
		if(nth > (i - j)) /*i should be always at least at value of j */\
			curval = nil;                                               \
		else                                                            \
			curval = BUNtail(bpi, nth);                                 \
		if (atomcmp(curval, nil) == 0)                                  \
			has_nils = true;                                            \
		for (;j < i; j++) {                                             \
			if (BUNappend(r, curval, false) != GDK_SUCCEED)             \
				goto allocation_error;                                  \
		}                                                               \
	} while(0)

gdk_return
GDKanalyticalnthvalue(BAT *r, BAT *b, BAT *p, BUN nth, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	BUN i = 0, j = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	bool has_nils = false;

	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_NTHVALUE_IMP(bte);
			break;
		case TYPE_sht:
			ANALYTICAL_NTHVALUE_IMP(sht);
			break;
		case TYPE_int:
			ANALYTICAL_NTHVALUE_IMP(int);
			break;
		case TYPE_lng:
			ANALYTICAL_NTHVALUE_IMP(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_NTHVALUE_IMP(hge);
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_NTHVALUE_IMP(flt);
			break;
		case TYPE_dbl:
			ANALYTICAL_NTHVALUE_IMP(dbl);
			break;
		default: {
			BATiter bpi = bat_iterator(b);
			const void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if(nth == BUN_NONE) {
				has_nils = true;
				for(i=0; i<cnt; i++) {
					if (BUNappend(r, nil, false) != GDK_SUCCEED)
						goto allocation_error;
				}
			} else if (p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
					if (*np) {
						i += (np - pnp);
						ANALYTICAL_NTHVALUE_OTHERS;
						pnp = np;
					}
				}
				i += (np - pnp);
				ANALYTICAL_NTHVALUE_OTHERS;
			} else {
				i += cnt;
				ANALYTICAL_NTHVALUE_OTHERS;
			}
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
allocation_error:
	GDKerror("GDKanalyticalnthvalue: malloc failure\n");
	return GDK_FAIL;
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
	} while(0)

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
					ANALYTICAL_LAG_CALC(TPE);      \
					bp += (lag < ncnt) ? lag : 0; \
					pnp = np;                     \
				}                                 \
			}                                     \
			rp += (np - pnp);                     \
			ANALYTICAL_LAG_CALC(TPE);              \
		} else {                                  \
			rp += cnt;                            \
			ANALYTICAL_LAG_CALC(TPE);              \
		}                                         \
	} while(0)

#define ANALYTICAL_LAG_OTHERS                                                  \
	do {                                                                       \
		for(i=0; i<lag && k<j; i++, k++) {                                     \
			if (BUNappend(r, default_value, false) != GDK_SUCCEED)             \
				goto allocation_error;                                         \
		}                                                                      \
		if(lag > 0 && atomcmp(default_value, nil) == 0)                        \
			has_nils = true;                                                   \
		for(l=k-lag; k<j; k++, l++) {                                          \
			curval = BUNtail(bpi, l);                                          \
			if (BUNappend(r, curval, false) != GDK_SUCCEED)                    \
				goto allocation_error;                                         \
			if (atomcmp(curval, nil) == 0)                                     \
				has_nils = true;                                               \
		}                                                                      \
	} while (0)

gdk_return
GDKanalyticallag(BAT *r, BAT *b, BAT *p, BUN lag, const void* restrict default_value, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void *restrict nil;
	BUN i = 0, j = 0, k = 0, l = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	bool has_nils = false;

	assert(default_value);

	switch (tpe) {
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
		default: {
			BATiter bpi = bat_iterator(b);
			const void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if(lag == BUN_NONE) {
				has_nils = true;
				for (j=0;j < cnt; j++) {
					if (BUNappend(r, nil, false) != GDK_SUCCEED)
						goto allocation_error;
				}
			} else if(p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
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
allocation_error:
	GDKerror("GDKanalyticallag: malloc failure\n");
	return GDK_FAIL;
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
	} while(0)

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
					LEAD_CALC(TPE);		      \
					pnp = np;                     \
				}                                 \
			}                                     \
			ncnt = (np - pnp);                    \
			rp += ncnt;                           \
			LEAD_CALC(TPE);			      \
		} else {                                  \
			ncnt = cnt;                           \
			rp += ncnt;                           \
			LEAD_CALC(TPE);			      \
		}                                         \
	} while(0)

#define ANALYTICAL_LEAD_OTHERS                                                 \
	do {                                                                       \
		j += ncnt;                                                             \
		if(lead < ncnt) {                                                      \
			m = ncnt - lead;                                                   \
			for(i=0,n=k+lead; i<m; i++, n++) {                                 \
				curval = BUNtail(bpi, n);                                      \
				if (BUNappend(r, curval, false) != GDK_SUCCEED)                \
					goto allocation_error;                                     \
				if (atomcmp(curval, nil) == 0)                                 \
					has_nils = true;                                           \
			}                                                                  \
			k += i;                                                            \
		}                                                                      \
		for(; k<j; k++) {                                                      \
			if (BUNappend(r, default_value, false) != GDK_SUCCEED)             \
				goto allocation_error;                                         \
		}                                                                      \
		if(lead > 0 && atomcmp(default_value, nil) == 0)                       \
			has_nils = true;                                                   \
	} while(0)

gdk_return
GDKanalyticallead(BAT *r, BAT *b, BAT *p, BUN lead, const void* restrict default_value, int tpe)
{
	int (*atomcmp)(const void *, const void *);
	const void* restrict nil;
	BUN i = 0, j = 0, k = 0, l = 0, ncnt, cnt = BATcount(b);
	bit *np, *pnp, *end;
	bool has_nils = false;

	assert(default_value);

	switch (tpe) {
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
		default: {
			BUN m = 0, n = 0;
			BATiter bpi = bat_iterator(b);
			const void *restrict curval;
			nil = ATOMnilptr(tpe);
			atomcmp = ATOMcompare(tpe);
			if(lead == BUN_NONE) {
				has_nils = true;
				for (j=0;j < cnt; j++) {
					if (BUNappend(r, nil, false) != GDK_SUCCEED)
						goto allocation_error;
				}
			} else if(p) {
				pnp = np = (bit*)Tloc(p, 0);
				end = np + cnt;
				for(; np<end; np++) {
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
allocation_error:
	GDKerror("GDKanalyticallead: malloc failure\n");
	return GDK_FAIL;
}

#undef ANALYTICAL_LEAD_IMP
#undef LEAD_CALC
#undef ANALYTICAL_LEAD_OTHERS

#define ANALYTICAL_MIN_MAX_CALC(TPE, OP)        \
	do {                                        \
		TPE *bp = (TPE*)Tloc(b, 0), *bs, *be, v, \
			curval = TPE##_nil, *restrict rb = (TPE*)Tloc(r, 0); \
		for(; i<cnt; i++, rb++) {               \
			bs = bp + start[i];                 \
			be = bp + end[i];                   \
			for(; bs<be; bs++) {                \
				v = *bs;                        \
				if(!is_##TPE##_nil(v)) {        \
					if(is_##TPE##_nil(curval))  \
						curval = v;             \
					else                        \
						curval = OP(v, curval); \
				}                               \
			}                                   \
			*rb = curval;                       \
			if(is_##TPE##_nil(curval))          \
				has_nils = true;                \
			else                                \
				curval = TPE##_nil;             \
		}                                       \
	} while(0)

#ifdef HAVE_HUGE
#define ANALYTICAL_MIN_MAX_LIMIT(OP) \
	case TYPE_hge: \
		ANALYTICAL_MIN_MAX_CALC(hge, OP); \
	break;
#else
#define ANALYTICAL_MIN_MAX_LIMIT(OP)
#endif

#define ANALYTICAL_MIN_MAX(OP, IMP, SIGN_OP) \
gdk_return \
GDKanalytical##OP(BAT *r, BAT *b, BAT *s, BAT *e, int tpe) \
{ \
	bool has_nils = false; \
	BUN i = 0, cnt = BATcount(b); \
	lng *restrict start, *restrict end, j = 0, l = 0; \
 \
	assert(s && e); \
	start = (lng*)Tloc(s, 0); \
	end = (lng*)Tloc(e, 0); \
 \
	switch(tpe) { \
		case TYPE_bit: \
			ANALYTICAL_MIN_MAX_CALC(bit, IMP); \
			break; \
		case TYPE_bte: \
			ANALYTICAL_MIN_MAX_CALC(bte, IMP); \
			break; \
		case TYPE_sht: \
			ANALYTICAL_MIN_MAX_CALC(sht, IMP); \
			break; \
		case TYPE_int: \
			ANALYTICAL_MIN_MAX_CALC(int, IMP); \
			break; \
		case TYPE_lng: \
			ANALYTICAL_MIN_MAX_CALC(lng, IMP); \
			break; \
		ANALYTICAL_MIN_MAX_LIMIT(IMP) \
		case TYPE_flt: \
			ANALYTICAL_MIN_MAX_CALC(flt, IMP); \
			break; \
		case TYPE_dbl: \
			ANALYTICAL_MIN_MAX_CALC(dbl, IMP); \
			break; \
		default: { \
			BATiter bpi = bat_iterator(b); \
			const void *nil = ATOMnilptr(tpe); \
			int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe); \
			void *curval; \
			for(; i<cnt; i++) { \
				j = start[i]; \
				l = end[i]; \
				curval = (void*)nil; \
				for (;j < l; j++) { \
					void *next = BUNtail(bpi, (BUN) j); \
					if (atomcmp(next, nil) != 0) { \
						if (atomcmp(curval, nil) == 0) \
							curval = next; \
						else \
							curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next; \
					} \
				} \
				if (BUNappend(r, curval, false) != GDK_SUCCEED) \
					goto allocation_error; \
				if (atomcmp(curval, nil) == 0) \
					has_nils = true; \
			} \
		} \
	} \
	BATsetcount(r, cnt); \
	r->tnonil = !has_nils; \
	r->tnil = has_nils; \
	return GDK_SUCCEED; \
allocation_error: \
	GDKerror("GDKanalytical""OP"": malloc failure\n"); \
	return GDK_FAIL; \
}

ANALYTICAL_MIN_MAX(min, MIN, >)
ANALYTICAL_MIN_MAX(max, MAX, <)

#undef ANALYTICAL_MIN_MAX_CALC
#undef ANALYTICAL_MIN_MAX_LIMIT
#undef ANALYTICAL_MIN_MAX

#define ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(TPE) \
	do {                                            \
		TPE *bp, *bs, *be;                          \
		bp = (TPE*)Tloc(b, 0);                      \
		for(; i<cnt; i++, rb++) {                   \
			bs = bp + start[i];                     \
			be = bp + end[i];                       \
			for(; bs<be; bs++)                      \
				curval += !is_##TPE##_nil(*bs);     \
			*rb = curval;                           \
			curval = 0;                             \
		}                                           \
	} while(0)

#define ANALYTICAL_COUNT_NO_NIL_STR_IMP(TPE_CAST, OFFSET)                 \
	do {                                                                  \
		for(; i<cnt; i++, rb++) {                                         \
			j = start[i];                                                 \
			l = end[i];                                                   \
			for(; j<l; j++)                                               \
				curval += base[(var_t) ((TPE_CAST) bp) OFFSET] != '\200'; \
			*rb = curval;                                                 \
			curval = 0;                                                   \
		}                                                                 \
	} while(0)

gdk_return
GDKanalyticalcount(BAT *r, BAT *b, BAT *s, BAT *e, const bit* restrict ignore_nils, int tpe)
{
	BUN i = 0, cnt = BATcount(b);
	lng *restrict rb = (lng*)Tloc(r, 0), *restrict start, *restrict end, curval = 0, j = 0, l = 0;

	assert(s && e && ignore_nils);
	start = (lng*)Tloc(s, 0);
	end = (lng*)Tloc(e, 0);

	if(!*ignore_nils || b->T.nonil) {
		for(; i<cnt; i++, rb++)
			*rb = (end[i] > start[i]) ? (end[i] - start[i]) : 0;
	} else {
		switch (tpe) {
			case TYPE_bit:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(bit);
				break;
			case TYPE_bte:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(bte);
				break;
			case TYPE_sht:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(sht);
				break;
			case TYPE_int:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(int);
				break;
			case TYPE_lng:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(lng);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(hge);
				break;
#endif
			case TYPE_flt:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(flt);
				break;
			case TYPE_dbl:
				ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP(dbl);
				break;
			case TYPE_str: {
				const char *restrict base = b->tvheap->base;
				const void *restrict bp = Tloc(b, 0);
				switch (b->twidth) {
					case 1:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned char *, [j] + GDK_VAROFFSET);
						break;
					case 2:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned short *, [j] + GDK_VAROFFSET);
						break;
#if SIZEOF_VAR_T != SIZEOF_INT
					case 4:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const unsigned int *, [j]);
						break;
#endif
					default:
						ANALYTICAL_COUNT_NO_NIL_STR_IMP(const var_t *, [j]);
						break;
				}
				break;
			}
			default: {
				const void *restrict nil = ATOMnilptr(tpe);
				int (*cmp)(const void *, const void *) = ATOMcompare(tpe);
				if (b->tvarsized) {
					const char *restrict base = b->tvheap->base;
					const void *restrict bp = Tloc(b, 0);
					for(; i<cnt; i++, rb++) {
						j = start[i];
						l = end[i];
						for(; j<l; j++)
							curval += cmp(nil, base + ((const var_t *) bp)[j]) != 0;
						*rb = curval;
						curval = 0;
					}
				} else {
					for(; i<cnt; i++, rb++) {
						j = start[i];
						l = end[i];
						for(; j<l; j++)
							curval += cmp(Tloc(b, j), nil) != 0;
						*rb = curval;
						curval = 0;
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

#undef ANALYTICAL_COUNT_NO_NIL_FIXED_SIZE_IMP
#undef ANALYTICAL_COUNT_NO_NIL_STR_IMP

#define ANALYTICAL_SUM_IMP_NUM(TPE1, TPE2)      \
	do {                                        \
		TPE1 *bs, *be, v;                       \
		for(; i<cnt; i++, rb++) {               \
			bs = bp + start[i];                 \
			be = bp + end[i];                   \
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
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0)

#define ANALYTICAL_SUM_IMP_FP(TPE1, TPE2)       \
	do {                                        \
		TPE1 *bs;                               \
		BUN parcel;                             \
		for(; i<cnt; i++, rb++) {               \
			if(end[i] > start[i]) {             \
				bs = bp + start[i];             \
				parcel = (end[i] - start[i]);   \
				if(dofsum(bs, 0, 0, parcel, &curval, 1, TYPE_##TPE1, TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true, false, \
					  	  true, "GDKanalyticalsum") == BUN_NONE) { \
					goto bailout;               \
				}                               \
			}                                   \
			*rb = curval;                       \
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0)

#define ANALYTICAL_SUM_CALC(TPE1, TPE2, IMP)    \
	do {                                        \
		TPE1 *bp = (TPE1*)Tloc(b, 0);           \
		TPE2 *restrict rb, curval = TPE2##_nil; \
		rb = (TPE2*)Tloc(r, 0);                 \
		IMP(TPE1, TPE2);			\
	} while(0)

gdk_return
GDKanalyticalsum(BAT *r, BAT *b, BAT *s, BAT *e, int tp1, int tp2)
{
	bool has_nils = false;
	BUN i = 0, cnt = BATcount(b), nils = 0;
	int abort_on_error = 1;
	lng *restrict start, *restrict end;

	assert(s && e);
	start = (lng*)Tloc(s, 0);
	end = (lng*)Tloc(e, 0);

	switch (tp2) {
		case TYPE_bte: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_CALC(bte, bte, ANALYTICAL_SUM_IMP_NUM);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_sht: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_CALC(bte, sht, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_CALC(sht, sht, ANALYTICAL_SUM_IMP_NUM);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_int: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_CALC(bte, int, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_CALC(sht, int, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_int:
					ANALYTICAL_SUM_CALC(int, int, ANALYTICAL_SUM_IMP_NUM);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_lng: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_SUM_CALC(bte, lng, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_CALC(sht, lng, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_int:
					ANALYTICAL_SUM_CALC(int, lng, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_lng:
					ANALYTICAL_SUM_CALC(lng, lng, ANALYTICAL_SUM_IMP_NUM);
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
					ANALYTICAL_SUM_CALC(bte, hge, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_sht:
					ANALYTICAL_SUM_CALC(sht, hge, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_int:
					ANALYTICAL_SUM_CALC(int, hge, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_lng:
					ANALYTICAL_SUM_CALC(lng, hge, ANALYTICAL_SUM_IMP_NUM);
					break;
				case TYPE_hge:
					ANALYTICAL_SUM_CALC(hge, hge, ANALYTICAL_SUM_IMP_NUM);
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
					ANALYTICAL_SUM_CALC(flt, flt, ANALYTICAL_SUM_IMP_FP);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_dbl: {
			switch (tp1) {
				case TYPE_flt:
					ANALYTICAL_SUM_CALC(flt, dbl, ANALYTICAL_SUM_IMP_FP);
					break;
				case TYPE_dbl:
					ANALYTICAL_SUM_CALC(dbl, dbl, ANALYTICAL_SUM_IMP_FP);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		default:
			goto nosupport;
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
bailout:
	GDKerror("GDKanalyticalsum: error while calculating floating-point sum\n");
	return GDK_FAIL;
nosupport:
	GDKerror("GDKanalyticalsum: type combination (sum(%s)->%s) not supported.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}

#undef ANALYTICAL_SUM_IMP_NUM
#undef ANALYTICAL_SUM_IMP_FP
#undef ANALYTICAL_SUM_CALC

#define ANALYTICAL_PROD_CALC_NUM(TPE1, TPE2, TPE3) \
	do {                                          \
		TPE1 *bp = (TPE1*)Tloc(b, 0), *bs, *be, v; \
		TPE2 *restrict rb, curval = TPE2##_nil;   \
		rb = (TPE2*)Tloc(r, 0);                   \
		for(; i<cnt; i++, rb++) {                 \
			bs = bp + start[i];                   \
			be = bp + end[i];                     \
			for(; bs<be; bs++) {                  \
				v = *bs;                          \
				if (!is_##TPE1##_nil(v)) {        \
					if(is_##TPE2##_nil(curval))   \
						curval = (TPE2) v;        \
					else                          \
						MUL4_WITH_CHECK(TPE1, v, TPE2, curval, TPE2, curval, GDK_##TPE2##_max, TPE3, \
										goto calc_overflow); \
				}                                 \
			}                                     \
			*rb = curval;                         \
			if(is_##TPE2##_nil(curval))           \
				has_nils = true;                  \
			else                                  \
				curval = TPE2##_nil;              \
		}                                         \
	} while(0)

#define ANALYTICAL_PROD_CALC_NUM_LIMIT(TPE1, TPE2, REAL_IMP) \
	do {                                        \
		TPE1 *bp = (TPE1*)Tloc(b, 0), *bs, *be, v; \
		TPE2 *restrict rb, curval = TPE2##_nil; \
		rb = (TPE2*)Tloc(r, 0);                 \
		for(; i<cnt; i++, rb++) {               \
			bs = bp + start[i];                 \
			be = bp + end[i];                   \
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
			if(is_##TPE2##_nil(curval))         \
				has_nils = true;                \
			else                                \
				curval = TPE2##_nil;            \
		}                                       \
	} while(0)

#define ANALYTICAL_PROD_CALC_FP(TPE1, TPE2)       \
	do {                                          \
		TPE1 *bp = (TPE1*)Tloc(b, 0), *bs, *be, v; \
		TPE2 *restrict rb, curval = TPE2##_nil;   \
		rb = (TPE2*)Tloc(r, 0);                   \
		for(; i<cnt; i++, rb++) {                 \
			bs = bp + start[i];                   \
			be = bp + end[i];                     \
			for(; bs<be; bs++) {                  \
				v = *bs;                          \
				if (!is_##TPE1##_nil(v)) {        \
					if(is_##TPE2##_nil(curval)) { \
						curval = (TPE2) v;        \
					} else if (ABSOLUTE(curval) > 1 && GDK_##TPE2##_max / ABSOLUTE(v) < ABSOLUTE(curval)) { \
						if (abort_on_error)       \
							goto calc_overflow;   \
						curval = TPE2##_nil;      \
						nils++;                   \
					} else {                      \
						curval *= v;              \
					}                             \
				}                                 \
			}                                     \
			*rb = curval;                         \
			if(is_##TPE2##_nil(curval))           \
				has_nils = true;                  \
			else                                  \
				curval = TPE2##_nil;              \
		}                                         \
	} while(0)

gdk_return
GDKanalyticalprod(BAT *r, BAT *b, BAT *s, BAT *e, int tp1, int tp2)
{
	bool has_nils = false;
	BUN i = 0, cnt = BATcount(b), nils = 0;
	int abort_on_error = 1;
	lng *restrict start, *restrict end;

	assert(s && e);
	start = (lng*)Tloc(s, 0);
	end = (lng*)Tloc(e, 0);

	switch (tp2) {
		case TYPE_bte: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_CALC_NUM(bte, bte, sht);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_sht: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_CALC_NUM(bte, sht, int);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_CALC_NUM(sht, sht, int);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_int: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_CALC_NUM(bte, int, lng);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_CALC_NUM(sht, int, lng);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_CALC_NUM(int, int, lng);
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
					ANALYTICAL_PROD_CALC_NUM(bte, lng, hge);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_CALC_NUM(sht, lng, hge);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_CALC_NUM(int, lng, hge);
					break;
				case TYPE_lng:
					ANALYTICAL_PROD_CALC_NUM(lng, lng, hge);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_hge: {
			switch (tp1) {
				case TYPE_bte:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(bte, hge, HGEMUL_CHECK);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(sht, hge, HGEMUL_CHECK);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(int, hge, HGEMUL_CHECK);
					break;
				case TYPE_lng:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(lng, hge, HGEMUL_CHECK);
					break;
				case TYPE_hge:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(hge, hge, HGEMUL_CHECK);
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
					ANALYTICAL_PROD_CALC_NUM_LIMIT(bte, lng, LNGMUL_CHECK);
					break;
				case TYPE_sht:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(sht, lng, LNGMUL_CHECK);
					break;
				case TYPE_int:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(int, lng, LNGMUL_CHECK);
					break;
				case TYPE_lng:
					ANALYTICAL_PROD_CALC_NUM_LIMIT(lng, lng, LNGMUL_CHECK);
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
					ANALYTICAL_PROD_CALC_FP(flt, flt);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		case TYPE_dbl: {
			switch (tp1) {
				case TYPE_flt:
					ANALYTICAL_PROD_CALC_FP(flt, dbl);
					break;
				case TYPE_dbl:
					ANALYTICAL_PROD_CALC_FP(dbl, dbl);
					break;
				default:
					goto nosupport;
			}
			break;
		}
		default:
			goto nosupport;
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
nosupport:
	GDKerror("GDKanalyticalprod: type combination (prod(%s)->%s) not supported.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
}

#undef ANALYTICAL_PROD_CALC_NUM
#undef ANALYTICAL_PROD_CALC_NUM_LIMIT
#undef ANALYTICAL_PROD_CALC_FP

#define ANALYTICAL_AVERAGE_CALC_NUM(TPE,lng_hge)      \
	do {                                              \
		TPE *bp = (TPE*)Tloc(b, 0), *bs, *be, v, a = 0; \
		for(; i<cnt; i++, rb++) {                     \
			bs = bp + start[i];                       \
			be = bp + end[i];                         \
			for(; bs<be; bs++) {                      \
				v = *bs;                              \
				if (!is_##TPE##_nil(v)) {             \
					ADD_WITH_CHECK(TPE, v, lng_hge, sum, lng_hge, sum, GDK_##lng_hge##_max, goto avg_overflow##TPE); \
					/* count only when no overflow occurs */ \
					n++;                              \
				}                                     \
			}                                         \
			if(0) {                                   \
avg_overflow##TPE:                                    \
				assert(n > 0);                        \
				if (sum >= 0) {                       \
					a = (TPE) (sum / (lng_hge) n);    \
					rr = (BUN) (sum % (SBUN) n);      \
				} else {                              \
					sum = -sum;                       \
					a = - (TPE) (sum / (lng_hge) n);  \
					rr = (BUN) (sum % (SBUN) n);      \
					if (r) {                          \
						a--;                          \
						rr = n - rr;                  \
					}                                 \
				}                                     \
				for(; bs<be; bs++) {                  \
					v = *bs;                          \
					if (is_##TPE##_nil(v))            \
						continue;                     \
					AVERAGE_ITER(TPE, v, a, rr, n);   \
				}                                     \
				curval = a + (dbl) rr / n;            \
				goto calc_done##TPE;                  \
			}                                         \
			curval = n > 0 ? (dbl) sum / n : dbl_nil; \
calc_done##TPE:                                       \
			*rb = curval;                             \
			has_nils = has_nils || (n == 0);          \
			n = 0;                                    \
			sum = 0;                                  \
		}                                             \
	} while(0)

#ifdef HAVE_HGE
#define ANALYTICAL_AVERAGE_LNG_HGE(TPE) ANALYTICAL_AVERAGE_CALC_NUM(TPE,hge)
#else
#define ANALYTICAL_AVERAGE_LNG_HGE(TPE) ANALYTICAL_AVERAGE_CALC_NUM(TPE,lng)
#endif

#define ANALYTICAL_AVERAGE_CALC_FP(TPE)      \
	do {                                     \
		TPE *bp = (TPE*)Tloc(b, 0), *bs, *be, v; \
		dbl a = 0;                           \
		for(; i<cnt; i++, rb++) {            \
			bs = bp + start[i];              \
			be = bp + end[i];                \
			for(; bs<be; bs++) {             \
				v = *bs;                     \
				if (!is_##TPE##_nil(v))      \
					AVERAGE_ITER_FLOAT(TPE, v, a, n); \
			}                                \
			curval = (n > 0) ? a : dbl_nil;  \
			*rb = curval;                    \
			has_nils = has_nils || (n == 0); \
			n = 0;                           \
			a = 0;                           \
		}                                    \
	} while(0)

gdk_return
GDKanalyticalavg(BAT *r, BAT *b, BAT *s, BAT *e, int tpe)
{
	bool has_nils = false;
	BUN i = 0, cnt = BATcount(b), nils = 0, n = 0, rr = 0;
	bool abort_on_error = true;
	lng *restrict start, *restrict end;
	dbl *restrict rb = (dbl*)Tloc(r, 0), curval;
#ifdef HAVE_HGE
	hge sum = 0;
#else
	lng sum = 0;
#endif

	assert(s && e);
	start = (lng*)Tloc(s, 0);
	end = (lng*)Tloc(e, 0);

	switch (tpe) {
		case TYPE_bte:
			ANALYTICAL_AVERAGE_LNG_HGE(bte);
			break;
		case TYPE_sht:
			ANALYTICAL_AVERAGE_LNG_HGE(sht);
			break;
		case TYPE_int:
			ANALYTICAL_AVERAGE_LNG_HGE(int);
			break;
		case TYPE_lng:
			ANALYTICAL_AVERAGE_LNG_HGE(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			ANALYTICAL_AVERAGE_LNG_HGE(hge);
			break;
#endif
		case TYPE_flt:
			ANALYTICAL_AVERAGE_CALC_FP(flt);
			break;
		case TYPE_dbl:
			ANALYTICAL_AVERAGE_CALC_FP(dbl);
			break;
		default:
			GDKerror("GDKanalyticalavg: average of type %s unsupported.\n", ATOMname(tpe));
			return GDK_FAIL;
	}
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_AVERAGE_LNG_HGE
#undef ANALYTICAL_AVERAGE_CALC_NUM
#undef ANALYTICAL_AVERAGE_CALC_FP
