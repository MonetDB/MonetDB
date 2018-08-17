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

#define ANALYTICAL_LIMIT_IMP(TPE, OP)                        \
	do {                                                     \
		TPE *rp, *rb, *restrict bp, *end, curval;            \
		rb = rp = (TPE*)Tloc(r, 0);                          \
		bp = (TPE*)Tloc(b, 0);                               \
		curval = *bp;                                        \
		end = rp + cnt;                                      \
		if (p) {                                             \
			if (o) {                                         \
				np = (bit*)Tloc(p, 0);                       \
				for(; rp<end; np++, rp++, bp++) {            \
					if (*np) {                               \
						if(is_##TPE##_nil(curval))           \
							has_nils = true;                 \
						for (;rb < rp; rb++)                 \
							*rb = curval;                    \
						curval = *bp;                        \
					}                                        \
					if(!is_##TPE##_nil(*bp)) {               \
						if(is_##TPE##_nil(curval))           \
							curval = *bp;                    \
						else                                 \
							curval = OP(*bp, curval);        \
					}                                        \
				}                                            \
				if(is_##TPE##_nil(curval))                   \
					has_nils = true;                         \
				for (;rb < rp; rb++)                         \
					*rb = curval;                            \
			} else { /* single value, ie no ordering */      \
				np = (bit*)Tloc(p, 0);                       \
				for(; rp<end; np++, rp++, bp++) {            \
					if (*np) {                               \
						if(is_##TPE##_nil(curval))           \
							has_nils = true;                 \
						for (;rb < rp; rb++)                 \
							*rb = curval;                    \
						curval = *bp;                        \
					}                                        \
					if(!is_##TPE##_nil(*bp)) {               \
						if(is_##TPE##_nil(curval))           \
							curval = *bp;                    \
						else                                 \
							curval = OP(*bp, curval);        \
					}                                        \
				}                                            \
				if(is_##TPE##_nil(curval))                   \
					has_nils = true;                         \
				for (;rb < rp; rb++)                         \
					*rb = curval;                            \
			}                                                \
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
			if(is_##TPE##_nil(*bp))                          \
				has_nils = true;                             \
			for(; rp<end; rp++, bp++)                        \
				*rp = *bp;                                   \
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

#define ANALYTICAL_LIMIT(OP, IMP, SIGN_OP)                                                  \
gdk_return                                                                                  \
GDKanalytical##OP(BAT *r, BAT *b, BAT *p, BAT *o, int tpe)                                  \
{                                                                                           \
	int (*atomcmp)(const void *, const void *);                                             \
	const void *nil;                                                                        \
	bool has_nils = false;                                                                  \
	BUN i, j, cnt = BATcount(b);                                                            \
	bit *restrict np;                                                                       \
	gdk_return gdk_res = GDK_SUCCEED;                                                       \
                                                                                            \
	switch(ATOMstorage(tpe)) {                                                              \
		case TYPE_bit:                                                                      \
			ANALYTICAL_LIMIT_IMP(bit, IMP)                                                  \
			break;                                                                          \
		case TYPE_bte:                                                                      \
			ANALYTICAL_LIMIT_IMP(bte, IMP)                                                  \
			break;                                                                          \
		case TYPE_sht:                                                                      \
			ANALYTICAL_LIMIT_IMP(sht, IMP)                                                  \
			break;                                                                          \
		case TYPE_int:                                                                      \
			ANALYTICAL_LIMIT_IMP(int, IMP)                                                  \
			break;                                                                          \
		case TYPE_lng:                                                                      \
			ANALYTICAL_LIMIT_IMP(lng, IMP)                                                  \
			break;                                                                          \
		ANALYTICAL_LIMIT_IMP_HUGE(IMP)                                                      \
		case TYPE_flt:                                                                      \
			ANALYTICAL_LIMIT_IMP(flt, IMP)                                                  \
			break;                                                                          \
		case TYPE_dbl:                                                                      \
			ANALYTICAL_LIMIT_IMP(dbl, IMP)                                                  \
			break;                                                                          \
		default: {                                                                          \
			BATiter bpi = bat_iterator(b);                                                  \
			void *restrict curval = BUNtail(bpi, 0);                                        \
			nil = ATOMnilptr(tpe);                                                          \
			atomcmp = ATOMcompare(tpe);                                                     \
			if (p) {                                                                        \
				if (o) {                                                                    \
					np = (bit*)Tloc(p, 0);                                                  \
					for(i=0,j=0; i<cnt; i++, np++) {                                        \
						if (*np) {                                                          \
							if((*atomcmp)(curval, nil) == 0)                                \
								has_nils = true;                                            \
							for (;j < i; j++) {                                             \
								if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED) \
									goto finish;                                            \
							}                                                               \
							curval = BUNtail(bpi, i);                                       \
						}                                                                   \
						void *next = BUNtail(bpi, i);                                       \
						if((*atomcmp)(next, nil) != 0) {                                    \
							if((*atomcmp)(curval, nil) == 0)                                \
								curval = next;                                              \
							else                                                            \
								curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next;   \
						}                                                                   \
					}                                                                       \
					if((*atomcmp)(curval, nil) == 0)                                        \
						has_nils = true;                                                    \
					for (;j < i; j++) {                                                     \
						   if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)      \
							   goto finish;                                                 \
					}                                                                       \
				} else { /* single value, ie no ordering */                                 \
					np = (bit*)Tloc(p, 0);                                                  \
					for(i=0,j=0; i<cnt; i++, np++) {                                        \
						if (*np) {                                                          \
							if((*atomcmp)(curval, nil) == 0)                                \
								has_nils = true;                                            \
							for (;j < i; j++) {                                             \
								if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED) \
									goto finish;                                            \
							}                                                               \
							curval = BUNtail(bpi, i);                                       \
						}                                                                   \
						void *next = BUNtail(bpi, i);                                       \
						if((*atomcmp)(next, nil) != 0) {                                    \
							if((*atomcmp)(curval, nil) == 0)                                \
								curval = next;                                              \
							else                                                            \
								curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next;   \
						}                                                                   \
					}                                                                       \
					if((*atomcmp)(curval, nil) == 0)                                        \
						has_nils = true;                                                    \
					for (;j < i; j++) {                                                     \
						if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)         \
							goto finish;                                                    \
					}                                                                       \
				}                                                                           \
			} else if (o) { /* single value, ie no partitions */                            \
				for(i=0; i<cnt; i++) {                                                      \
					void *next = BUNtail(bpi, i);                                           \
						if((*atomcmp)(next, nil) != 0) {                                    \
							if((*atomcmp)(curval, nil) == 0)                                \
								curval = next;                                              \
							else                                                            \
								curval = atomcmp(next, curval) SIGN_OP 0 ? curval : next;   \
						}                                                                   \
				}                                                                           \
				if((*atomcmp)(curval, nil) == 0)                                            \
					has_nils = true;                                                        \
				for (j=0; j < i; j++) {                                                     \
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)             \
						goto finish;                                                        \
				}                                                                           \
			} else { /* single value, ie no ordering */                                     \
				if((*atomcmp)(curval, nil) == 0)                                            \
					has_nils = true;                                                        \
				for(i=0; i<cnt; i++)                                                        \
					if ((gdk_res = BUNappend(r, curval, false)) != GDK_SUCCEED)             \
						goto finish;                                                        \
			}                                                                               \
		}                                                                                   \
	}                                                                                       \
finish:                                                                                     \
	BATsetcount(r, cnt);                                                                    \
	r->tnonil = !has_nils;                                                                  \
	r->tnil = has_nils;                                                                     \
	return gdk_res;                                                                         \
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
	BUN i, cnt;
	gdk_return gdk_res = GDK_SUCCEED;

	assert(b || p || o);
	cnt = BATcount(b?b:p?p:o);

	if(!*ignore_nils || !b || b->T.nonil) {
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

#define ANALYTICAL_ADD_WITH_CHECK(lft, rgt, TPE2, dst, max, on_overflow) \
	do {								\
		if ((rgt) < 1) {					\
			if (-(max) - (rgt) > (lft)) {			\
				on_overflow;			\
			} else {					\
				(dst) = (TPE2) (lft) + (rgt);		\
			}						\
		} else {						\
			if ((max) - (rgt) < (lft)) {			\
				on_overflow;			\
			} else {					\
				(dst) = (TPE2) (lft) + (rgt);		\
			}						\
		}							\
	} while (0);

#define ANALYTICAL_SUM_IMP(TPE1, TPE2)                              \
	do {                                                            \
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0);                      \
		TPE2 *rp, *rb, *end, curval = TPE2##_nil;                   \
		rb = rp = (TPE2*)Tloc(r, 0);                                \
		end = rp + cnt;                                             \
		if (p) {                                                    \
			if (o) {                                                \
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
							ANALYTICAL_ADD_WITH_CHECK(*bp, curval,  \
										   TPE2, curval,            \
										   GDK_##TPE2##_max,        \
										   goto calc_overflow);     \
					}                                               \
				}                                                   \
				if(is_##TPE2##_nil(curval))                         \
					has_nils = true;                                \
				for (;rb < rp; rb++)                                \
					*rb = curval;                                   \
			} else { /* single value, ie no ordering */             \
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
							ANALYTICAL_ADD_WITH_CHECK(*bp, curval,  \
										   TPE2, curval,            \
										   GDK_##TPE2##_max,        \
										   goto calc_overflow);     \
					}                                               \
				}                                                   \
				if(is_##TPE2##_nil(curval))                         \
					has_nils = true;                                \
				for (;rb < rp; rb++)                                \
					*rb = curval;                                   \
			}                                                       \
		} else if (o) { /* single value, ie no partitions */        \
			for(; rp<end; rp++, bp++) {                             \
				if(!is_##TPE1##_nil(*bp)) {                         \
					if(is_##TPE2##_nil(curval))                     \
						curval = (TPE2) *bp;                        \
					else                                            \
						ANALYTICAL_ADD_WITH_CHECK(*bp, curval,      \
									   TPE2, curval,                \
									   GDK_##TPE2##_max,            \
									   goto calc_overflow);         \
				}                                                   \
			}                                                       \
			for(;rb < rp; rb++)                                     \
				*rb = curval;                                       \
			if(is_##TPE2##_nil(curval))                             \
				has_nils = true;                                    \
		} else { /* single value, ie no ordering */                 \
			for(; rp<end; rp++, bp++)                               \
				*rp = *bp;                                          \
			if(is_##TPE1##_nil(*bp))                                \
				has_nils = true;                                    \
		}                                                           \
		goto finish;                                                \
	} while(0);

#define ANALYTICAL_SUM_FP_IMP(TPE1, TPE2)                              \
	do {                                                               \
		TPE1 *bp, *bprev;                                              \
		TPE2 *rp, *rb, curval = TPE2##_nil;                            \
		bp = bprev = (TPE1*)Tloc(b, 0);                                \
		rb = rp = (TPE2*)Tloc(r, 0);                                   \
		if (p) {                                                       \
			if (o) {                                                   \
				np = (bit*)Tloc(p, 0);                                 \
				for(i=0,j=0; i<cnt; i++, np++, rp++, bp++) {           \
					if (*np) {                                         \
						if(dofsum(bprev, 0, 0, i - j, rb, 1, TYPE_##TPE1, TYPE_##TPE2, \
								  NULL, NULL, NULL, 0, 0, true, false, true,           \
								  "GDKanalyticalsum") == BUN_NONE) {                   \
							goto bailout;                              \
						}                                              \
						curval = *rb;                                  \
						bprev = bp;                                    \
						j = i;                                         \
						for (;rb < rp; rb++)                           \
							*rb = curval;                              \
						if(is_##TPE2##_nil(curval))                    \
							has_nils = true;                           \
					}                                                  \
				}                                                      \
				if(dofsum(bprev, 0, 0, i - j, rb, 1, TYPE_##TPE1,      \
						  TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true,   \
						  false, true, "GDKanalyticalsum") == BUN_NONE) { \
					goto bailout;                                      \
				}                                                      \
				curval = *rb;                                          \
				if(is_##TPE2##_nil(curval))                            \
					has_nils = true;                                   \
				for (;rb < rp; rb++)                                   \
					*rb = curval;                                      \
			} else { /* single value, ie no ordering */                \
				np = (bit*)Tloc(p, 0);                                 \
				for(i=0,j=0; i<cnt; i++, np++, rp++, bp++) {           \
					if (*np) {                                         \
						if(dofsum(bprev, 0, 0, i - j, rb, 1, TYPE_##TPE1, TYPE_##TPE2, \
								  NULL, NULL, NULL, 0, 0, true, false,                 \
								  true, "GDKanalyticalsum") == BUN_NONE) {             \
							goto bailout;                              \
						}                                              \
						curval = *rb;                                  \
						bprev = bp;                                    \
						j = i;                                         \
						for (;rb < rp; rb++)                           \
							*rb = curval;                              \
						if(is_##TPE2##_nil(curval))                    \
							has_nils = true;                           \
					}                                                  \
				}                                                      \
				if(dofsum(bprev, 0, 0, i - j, rb, 1, TYPE_##TPE1,      \
						  TYPE_##TPE2, NULL, NULL, NULL, 0, 0, true,   \
						  false, true, "GDKanalyticalsum") == BUN_NONE) { \
					goto bailout;                                      \
				}                                                      \
				curval = *rb;                                          \
				if(is_##TPE2##_nil(curval))                            \
					has_nils = true;                                   \
				for (;rb < rp; rb++)                                   \
					*rb = curval;                                      \
			}                                                          \
		} else if (o) { /* single value, ie no partitions */           \
			if(dofsum(bp, 0, 0, cnt, rb, 1, TYPE_##TPE1, TYPE_##TPE2,  \
				   NULL, NULL, NULL, 0, 0, true, false, true,          \
				   "GDKanalyticalsum") == BUN_NONE) {                  \
				goto bailout;                                          \
			}                                                          \
			curval = *rb;                                              \
			for(i=0; i<cnt; i++, rb++)                                 \
				*rb = curval;                                          \
			if(is_##TPE2##_nil(curval))                                \
				has_nils = true;                                       \
		} else { /* single value, ie no ordering */                    \
			for(i=0; i<cnt; i++, rp++, bp++)                           \
				*rp = *bp;                                             \
			if(is_##TPE1##_nil(*bp))                                   \
				has_nils = true;                                       \
		}                                                              \
		goto finish;                                                   \
	} while(0);

gdk_return
GDKanalyticalsum(BAT *r, BAT *b, BAT *p, BAT *o, int tp1, int tp2)
{
	bool has_nils = false;
	BUN i, j, cnt = BATcount(b);
	bit *restrict np;

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
nosupport:
	GDKerror("sum: type combination (sum(%s)->%s) not supported.\n", ATOMname(tp1), ATOMname(tp2));
calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
finish:
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
}

#undef ANALYTICAL_SUM_IMP
#undef ANALYTICAL_SUM_FP_IMP
#undef ANALYTICAL_ADD_WITH_CHECK
