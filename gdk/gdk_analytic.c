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

#define ANALYTICAL_LIMIT_IMP(TPE, OP)                        \
	do {                                                     \
		TPE *rp, *rb, *bp, curval;                           \
		rb = rp = (TPE*)Tloc(r, 0);                          \
		bp = (TPE*)Tloc(b, 0);                               \
		curval = *bp;                                        \
		if (p) {                                             \
			if (o) {                                         \
				np = (bit*)Tloc(p, 0);                       \
				for(i=0; i<cnt; i++, np++, rp++, bp++) {     \
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
				for(i=0; i<cnt; i++, np++, rp++, bp++) {     \
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
			for(i=0; i<cnt; i++, rp++, bp++) {               \
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
			for(i=0; i<cnt; i++, rp++, bp++)                 \
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
	bit *np;                                                                                \
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
			void *curval = BUNtail(bpi, 0);                                                 \
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
		TPE1 *bp;                                                   \
		TPE2 *rp, *rb, curval;                                      \
		bp = (TPE1*)Tloc(b, 0);                                     \
		rb = rp = (TPE2*)Tloc(r, 0);                                \
		curval = TPE2##_nil;                                        \
		if (p) {                                                    \
			if (o) {                                                \
				np = (bit*)Tloc(p, 0);                              \
				for(i=0; i<cnt; i++, np++, rp++, bp++) {            \
					if (*np) {                                      \
						if(is_##TPE2##_nil(curval))                 \
							has_nils = true;                        \
						for (;rb < rp; rb++)                        \
							*rb = curval;                           \
						curval = TPE2##_nil;                        \
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
				for(i=0; i<cnt; i++, np++, rp++, bp++) {            \
					if (*np) {                                      \
						if(is_##TPE2##_nil(curval))                 \
							has_nils = true;                        \
						for (;rb < rp; rb++)                        \
							*rb = curval;                           \
						curval = TPE2##_nil;                        \
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
			for(i=0; i<cnt; i++, rp++, bp++) {                      \
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
			if(is_##TPE2##_nil(curval))                             \
				has_nils = true;                                    \
			for(;rb < rp; rb++)                                     \
				*rb = curval;                                       \
		} else { /* single value, ie no ordering */                 \
			if(is_##TPE1##_nil(*bp))                                \
				has_nils = true;                                    \
			for(i=0; i<cnt; i++, rp++, bp++)                        \
				*rp = *bp;                                          \
		}                                                           \
		goto finish;                                                \
	} while(0);

gdk_return
GDKanalyticalsum(BAT *r, BAT *b, BAT *p, BAT *o, int tp1, int tp2)
{
	bool has_nils = false;
	BUN i, cnt = BATcount(b);
	bit *np;

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
		case TYPE_flt:
			if (tp1 != TYPE_flt) {
				goto nosupport;
				break;
			}
			/* fall through */
		case TYPE_dbl:
			if (tp1 != TYPE_flt && tp1 != TYPE_dbl) {
				goto nosupport;
				break;
			}
			goto nosupport;
		default:
			goto nosupport;
	}
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
#undef ANALYTICAL_ADD_WITH_CHECK
