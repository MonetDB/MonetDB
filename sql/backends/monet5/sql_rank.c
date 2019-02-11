/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_rank.h"
#include "gdk_analytic.h"
#include "mtime.h"

#define voidresultBAT(r,tpe,cnt,b,err)					\
	do {								\
		r = COLnew(b->hseqbase, tpe, cnt, TRANSIENT);		\
		if (r == NULL) {					\
			BBPunfix(b->batCacheid);			\
			throw(MAL, err, SQLSTATE(HY001) MAL_MALLOC_FAIL); \
		}							\
		r->tsorted = false;					\
		r->trevsorted = false;					\
		r->tnonil = true;					\
	} while (0)

str 
SQLdiff(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		bat *bid = getArgReference_bat(stk, pci, 1);
		BAT *b = BATdescriptor(*bid), *c, *r;
		gdk_return gdk_code;

		if (!b)
			throw(SQL, "sql.diff", SQLSTATE(HY005) "Cannot access column descriptor");
		voidresultBAT(r, TYPE_bit, BATcount(b), b, "sql.diff");
		if (pci->argc > 2) {
			c = b;
			bid = getArgReference_bat(stk, pci, 2);
			b = BATdescriptor(*bid);
			if (!b) {
				BBPunfix(c->batCacheid);
				throw(SQL, "sql.diff", SQLSTATE(HY005) "Cannot access column descriptor");
			}
			gdk_code = GDKanalyticaldiff(r, b, c, b->ttype);
			BBPunfix(c->batCacheid);
		} else {
			gdk_code = GDKanalyticaldiff(r, b, NULL, b->ttype);
		}
		BBPunfix(b->batCacheid);
		if(gdk_code == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, "sql.diff", GDK_EXCEPTION);
	} else {
		bit *res = getArgReference_bit(stk, pci, 0);

		*res = FALSE;
	}
	return MAL_SUCCEED;
}

#define CHECK_NULLS_AND_NEGATIVES_COLUMN(TPE) \
	for(TPE *lp = (TPE*)Tloc(l, 0), *lend = lp + BATcount(l); lp < lend && !is_null && !is_negative; lp++) { \
		is_null |= is_##TPE##_nil(*lp); \
		is_negative |= (*lp < 0); \
	} \

#define CHECK_NULLS_AND_NEGATIVES_SINGLE(TPE, MEMBER) \
	is_null = is_##TPE##_nil(vlimit->val.MEMBER); \
	is_negative = vlimit->val.MEMBER < 0; \
	limit = &vlimit->val.MEMBER; \

str
SQLwindow_bound(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	bool preceding;
	lng first_half;
	int unit, bound, excl, part_offset = (pci->argc > 6);

	if ((pci->argc != 6 && pci->argc != 7) || getArgType(mb, pci, part_offset + 2) != TYPE_int ||
		getArgType(mb, pci, part_offset + 3) != TYPE_int || getArgType(mb, pci, part_offset + 4) != TYPE_int) {
		throw(SQL, "sql.window_bound", SQLSTATE(42000) "Invalid arguments");
	}

	unit = *getArgReference_int(stk, pci, part_offset + 2);
	bound = *getArgReference_int(stk, pci, part_offset + 3);
	excl = *getArgReference_int(stk, pci, part_offset + 4);

	assert(unit >= 0 && unit <= 3);
	assert(bound >= 0 && bound <= 5);
	assert(excl >= 0 && excl <= 2);
	preceding = (bound % 2 == 0);
	first_half = (bound < 2 || bound == 4);

	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, part_offset + 1)), *p = NULL, *r, *l = NULL;
		int tp1 = getBatType(getArgType(mb, pci, part_offset + 1)), tp2 = getArgType(mb, pci, part_offset + 5);
		void* limit = NULL;
		bool is_negative = false, is_null = false, is_a_bat;
		gdk_return gdk_code;

		if (!b)
			throw(SQL, "sql.window_bound", SQLSTATE(HY005) "Cannot access column descriptor");

		if (excl != 0) {
			BBPunfix(b->batCacheid);
			throw(SQL, "sql.window_bound", SQLSTATE(42000) "Only EXCLUDE NO OTHERS exclusion is currently implemented");
		}

		is_a_bat = isaBatType(tp2);
		if(is_a_bat)
			tp2 = getBatType(tp2);

		voidresultBAT(r, TYPE_lng, BATcount(b), b, "sql.window_bound");
		if(is_a_bat) { //SQL_CURRENT_ROW shall never fall in limit validation
			l = BATdescriptor(*getArgReference_bat(stk, pci, part_offset + 5));
			if (!l) {
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.window_bound", SQLSTATE(HY005) "Cannot access column descriptor");
			}
			switch (tp2) {
				case TYPE_bte:
					CHECK_NULLS_AND_NEGATIVES_COLUMN(bte)
					break;
				case TYPE_sht:
					CHECK_NULLS_AND_NEGATIVES_COLUMN(sht)
					break;
				case TYPE_int:
					CHECK_NULLS_AND_NEGATIVES_COLUMN(int)
					break;
				case TYPE_lng:
					CHECK_NULLS_AND_NEGATIVES_COLUMN(lng)
					break;
				case TYPE_flt:
					CHECK_NULLS_AND_NEGATIVES_COLUMN(flt)
					break;
				case TYPE_dbl:
					CHECK_NULLS_AND_NEGATIVES_COLUMN(dbl)
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					CHECK_NULLS_AND_NEGATIVES_COLUMN(hge)
					break;
#endif
				default: {
					BBPunfix(b->batCacheid);
					BBPunfix(l->batCacheid);
					throw(SQL, "sql.window_bound", SQLSTATE(42000) "%s limit not available for %s", "sql.window_bound", ATOMname(tp2));
				}
			}
			if(is_null || is_negative) {
				BBPunfix(b->batCacheid);
				BBPunfix(l->batCacheid);
				if(is_null)
					throw(SQL, "sql.window_bound", SQLSTATE(HY005) "All values on %s boundary must be non-null", preceding ? "PRECEDING" : "FOLLOWING");
				throw(SQL, "sql.window_bound", SQLSTATE(HY005) "All values on %s boundary must be non-negative", preceding ? "PRECEDING" : "FOLLOWING");
			}
		} else {
			ValRecord *vlimit = &(stk)->stk[(pci)->argv[part_offset + 5]];

			switch (tp2) {
				case TYPE_bte:
					CHECK_NULLS_AND_NEGATIVES_SINGLE(bte, btval)
					break;
				case TYPE_sht:
					CHECK_NULLS_AND_NEGATIVES_SINGLE(sht, shval)
					break;
				case TYPE_int:
					CHECK_NULLS_AND_NEGATIVES_SINGLE(int, ival)
					break;
				case TYPE_lng:
					CHECK_NULLS_AND_NEGATIVES_SINGLE(lng, lval)
					break;
				case TYPE_flt:
					CHECK_NULLS_AND_NEGATIVES_SINGLE(flt, fval)
					break;
				case TYPE_dbl:
					CHECK_NULLS_AND_NEGATIVES_SINGLE(dbl, dval)
					break;
#ifdef HAVE_HGE
				case TYPE_hge:
					CHECK_NULLS_AND_NEGATIVES_SINGLE(hge, hval)
					break;
#endif
				default: {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.window_bound", SQLSTATE(42000) "%s limit is not available for %s", "sql.window_bound", ATOMname(tp2));
				}
			}
			if(is_null)
				throw(SQL, "sql.window_bound", SQLSTATE(42000) "The %s boundary must be non-null", preceding ? "PRECEDING" : "FOLLOWING");
			if(is_negative)
				throw(SQL, "sql.window_bound", SQLSTATE(42000) "The %s boundary must be non-negative", preceding ? "PRECEDING" : "FOLLOWING");
		}
		if (part_offset) {
			p = BATdescriptor(*getArgReference_bat(stk, pci, 1));
			if (!p) {
				if(l) BBPunfix(l->batCacheid);
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.window_bound", SQLSTATE(HY005) "Cannot access column descriptor");
			}
		}

		//On RANGE frame, when "CURRENT ROW" is not specified, the ranges are calculated with SQL intervals in mind
		if((tp1 == TYPE_daytime || tp1 == TYPE_date || tp1 == TYPE_timestamp) && unit == 1 && bound < 4) {
			msg = MTIMEanalyticalrangebounds(r, b, p, l, limit, tp1, tp2, preceding, first_half);
			if(msg == MAL_SUCCEED)
				BBPkeepref(*res = r->batCacheid);
		} else {
			gdk_code = GDKanalyticalwindowbounds(r, b, p, l, limit, tp1, tp2, unit, preceding, first_half);
			if(gdk_code == GDK_SUCCEED)
				BBPkeepref(*res = r->batCacheid);
			else
				msg = createException(SQL, "sql.window_bound", GDK_EXCEPTION);
		}
		if(l) BBPunfix(l->batCacheid);
		if(p) BBPunfix(p->batCacheid);
		BBPunfix(b->batCacheid);
	} else {
		lng *res = getArgReference_lng(stk, pci, 0);

		*res = preceding ? -first_half : first_half;
	}
	return msg;
}

str 
SQLrow_number(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	if (pci->argc != 4 || 
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) || 
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.row_number", SQLSTATE(42000) "row_number(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *r;
		BUN cnt;
		int j, *rp, *end;
		bit *np;

		if (!b)
			throw(SQL, "sql.row_number", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
	 	voidresultBAT(r, TYPE_int, cnt, b, "sql.row_number");
		rp = (int*)Tloc(r, 0);
		if (isaBatType(getArgType(mb, pci, 2))) { 
			/* order info not used */
			p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
			if (!p) {
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.row_number", SQLSTATE(HY005) "Cannot access column descriptor");
			}
			np = (bit*)Tloc(p, 0);
			end = rp + cnt;
			for(j=1; rp<end; j++, np++, rp++) {
				if (*np)
					j=1;
				*rp = j;
			}
			BBPunfix(p->batCacheid);
		} else { /* single value, ie no partitions, order info not used */
			int icnt = (int) cnt;
			for(j=1; j<=icnt; j++, rp++)
				*rp = j;
		}
		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = r->batCacheid);
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}
	return MAL_SUCCEED;
}

str 
SQLrank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	if (pci->argc != 4 || 
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) || 
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.rank", SQLSTATE(42000) "rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *o, *r;
		BUN cnt;
		int j, k, *rp, *end;
		bit *np, *no;

		if (!b)
			throw(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		voidresultBAT(r, TYPE_int, cnt, b, "sql.rank");
		rp = (int*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) { 
			if (isaBatType(getArgType(mb, pci, 3))) { 
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!p || !o) {
					BBPunfix(b->batCacheid);
					if (p) BBPunfix(p->batCacheid);
					if (o) BBPunfix(o->batCacheid);
					throw(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				no = (bit*)Tloc(o, 0);
				for(j=1,k=1; rp<end; k++, np++, no++, rp++) {
					if (*np)
						j=k=1;
					if (*no)
						j=k;
					*rp = j;
				}
				BBPunfix(p->batCacheid);
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering */
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				if (!p) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				for(j=1,k=1; rp<end; k++, np++, rp++) {
					if (*np)
						j=k=1;
					*rp = j;
				}
				BBPunfix(p->batCacheid);
			}
		} else { /* single value, ie no partitions */
			if (isaBatType(getArgType(mb, pci, 3))) { 
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!o) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				no = (bit*)Tloc(o, 0);
				for(j=1,k=1; rp<end; k++, no++, rp++) {
					if (*no)
						j=k;
					*rp = j;
				}
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering */
				int icnt = (int) cnt;
				for(j=1; j<=icnt; j++, rp++)
					*rp = j;
			}
		}
		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = r->batCacheid);
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}
	return MAL_SUCCEED;
}

str 
SQLdense_rank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	if (pci->argc != 4 || 
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) || 
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.dense_rank", SQLSTATE(42000) "dense_rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *o, *r;
		BUN cnt;
		int j, *rp, *end;
		bit *np, *no;

		if (!b)
			throw(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		voidresultBAT(r, TYPE_int, cnt, b, "sql.dense_rank");
		rp = (int*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) { 
			if (isaBatType(getArgType(mb, pci, 3))) { 
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!p || !o) {
					BBPunfix(b->batCacheid);
					if (p) BBPunfix(p->batCacheid);
					if (o) BBPunfix(o->batCacheid);
					throw(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				no = (bit*)Tloc(o, 0);
				for(j=1; rp<end; np++, no++, rp++) {
					if (*np)
						j=1;
					else if (*no)
						j++;
					*rp = j;
				}
				BBPunfix(p->batCacheid);
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering */
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				if (!p) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				for(j=1; rp<end; np++, rp++) {
					if (*np)
						j=1;
					*rp = j;
				}
				BBPunfix(p->batCacheid);
			}
		} else { /* single value, ie no partitions */
			if (isaBatType(getArgType(mb, pci, 3))) { 
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!o) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				no = (bit*)Tloc(o, 0);
				for(j=1; rp<end; no++, rp++) {
					if (*no)
						j++;
					*rp = j;
				}
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering */
				int icnt = (int) cnt;
				for(j=1; j<=icnt; j++, rp++)
					*rp = j;
			}
		}
		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = r->batCacheid);
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}
	return MAL_SUCCEED;
}

str
SQLpercent_rank(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	if (pci->argc != 4 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.percent_rank", SQLSTATE(42000) "percent_rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *o, *r;
		BUN cnt;
		int j, k;
		dbl *rp, *end, cnt_cast;
		bit *np, *no;

		if (!b)
			throw(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		cnt_cast = (dbl) (cnt - 1);
		voidresultBAT(r, TYPE_dbl, cnt, b, "sql.percent_rank");
		rp = (dbl*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) {
			if (isaBatType(getArgType(mb, pci, 3))) {
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!p || !o) {
					BBPunfix(b->batCacheid);
					if (p) BBPunfix(p->batCacheid);
					if (o) BBPunfix(o->batCacheid);
					throw(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				no = (bit*)Tloc(o, 0);
				for(j=0,k=0; rp<end; k++, np++, no++, rp++) {
					if (*np)
						j=k=0;
					if (*no)
						j=k;
					*rp = j / cnt_cast;
				}
				BBPunfix(p->batCacheid);
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering */
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				if (!p) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				for(j=0; rp<end; np++, rp++) {
					if (*np)
						j=0;
					*rp = j / cnt_cast;
				}
				BBPunfix(p->batCacheid);
			}
		} else { /* single value, ie no partitions */
			if (isaBatType(getArgType(mb, pci, 3))) {
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!o) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				no = (bit*)Tloc(o, 0);
				for(j=0,k=0; rp<end; k++, no++, rp++) {
					if (*no)
						j=k;
					*rp = j / cnt_cast;
				}
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering - the outcome will always be 0 */
				for(; rp<end; rp++)
					*rp = 0;
			}
		}
		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = r->batCacheid);
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}
	return MAL_SUCCEED;
}

str
SQLcume_dist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	if (pci->argc != 4 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, "sql.cume_dist", SQLSTATE(42000) "cume_dist(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *r;
		BUN cnt;
		int j;
		dbl *rb, *rp, *end, cnt_cast;
		bit *np;

		if (!b)
			throw(SQL, "sql.cume_dist", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		cnt_cast = (dbl) cnt;
		voidresultBAT(r, TYPE_dbl, cnt, b, "sql.cume_dist");
		rb = rp = (dbl*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) {
			if (isaBatType(getArgType(mb, pci, 3))) {
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				if (!p) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.cume_dist", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				for(j=0; rp<end; j++, np++, rp++) {
					if (*np) {
						for(; rb<rp; rb++)
							*rb = j / cnt_cast;
					}
				}
				for(; rb<rp; rb++)
					*rb = 1;
			} else { /* single value, ie no ordering */
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				if (!p) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.cume_dist", SQLSTATE(HY005) "Cannot access column descriptor");
				}
				np = (bit*)Tloc(p, 0);
				for(j=0; rp<end; j++, np++, rp++) {
					if (*np) {
						for(; rb<rp; rb++)
							*rb = j / cnt_cast;
					}
				}
				for(; rb<rp; rb++)
					*rb = 1;
				BBPunfix(p->batCacheid);
			}
		} else {
			for(; rp<end; rp++)
				*rp = 1;
		}
		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = r->batCacheid);
	} else {
		int *res = getArgReference_int(stk, pci, 0);

		*res = 1;
	}
	return MAL_SUCCEED;
}

#define NTILE_IMP(TPE)                                                                      \
	do {                                                                                    \
		TPE *ntile = getArgReference_##TPE(stk, pci, 2);                                    \
		if(!is_##TPE##_nil(*ntile) && *ntile < 1) {                                         \
			BBPunfix(b->batCacheid);                                                        \
			throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile must be greater than zero");     \
		}                                                                                   \
		voidresultBAT(r, TYPE_##TPE, cnt, b, "sql.ntile");                                  \
		if (isaBatType(getArgType(mb, pci, 3))) {                                           \
			p = BATdescriptor(*getArgReference_bat(stk, pci, 3));                           \
			if (!p) {                                                                       \
				BBPunfix(b->batCacheid);                                                    \
				throw(SQL, "sql.ntile", SQLSTATE(HY005) "Cannot access column descriptor"); \
			}                                                                               \
		}                                                                                   \
		gdk_code = GDKanalyticalntile(r, b, p, TYPE_##TPE, ntile);                          \
	} while(0);

#define NTILE_VALUE_SINGLE_IMP(TPE)                                                     \
	do {                                                                                \
		TPE val = *(TPE*) ntile, *rres = (TPE*) res;                                    \
		if(!is_##TPE##_nil(val) && val < 1)                                             \
			throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile must be greater than zero"); \
		*rres = (is_##TPE##_nil(val) || val > 1) ? TPE##_nil : *(TPE*) in;              \
	} while(0);

str
SQLntile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tp1, tp2;

	(void)cntxt;
	if (pci->argc != 5 || (getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit) ||
		(getArgType(mb, pci, 4) != TYPE_bit && getBatType(getArgType(mb, pci, 4)) != TYPE_bit)) {
		throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile(:any_1,:number,:bit,:bit)");
	}
	tp1 = getArgType(mb, pci, 1), tp2 = getArgType(mb, pci, 2);
	if (isaBatType(tp2))
		throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile first argument must be a single atom");

	if (isaBatType(tp1)) {
		BUN cnt;
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p = NULL, *r;
		if (!b)
			throw(SQL, "sql.ntile", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		gdk_return gdk_code;

		switch (tp2) {
			case TYPE_bte:
				NTILE_IMP(bte)
				break;
			case TYPE_sht:
				NTILE_IMP(sht)
				break;
			case TYPE_int:
				NTILE_IMP(int)
				break;
			case TYPE_lng:
				NTILE_IMP(lng)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				NTILE_IMP(hge)
				break;
#endif
			default: {
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile not available for %s", ATOMname(tp2));
			}
		}

		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		if(p) BBPunfix(p->batCacheid);
		if(gdk_code == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, "sql.ntile", GDK_EXCEPTION);
	} else {
		ptr res = getArgReference_ptr(stk, pci, 0);
		ptr in = getArgReference_ptr(stk, pci, 1);
		ptr ntile = getArgReference_ptr(stk, pci, 2);

		switch (tp2) {
			case TYPE_bte:
				NTILE_VALUE_SINGLE_IMP(bte)
				break;
			case TYPE_sht:
				NTILE_VALUE_SINGLE_IMP(sht)
				break;
			case TYPE_int:
				NTILE_VALUE_SINGLE_IMP(int)
				break;
			case TYPE_lng:
				NTILE_VALUE_SINGLE_IMP(lng)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				NTILE_VALUE_SINGLE_IMP(hge)
				break;
#endif
			default:
				throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile not available for %s", ATOMname(tp2));
		}
	}
	return MAL_SUCCEED;
}

static str
SQLanalytics_args(BAT **r, BAT **b, BAT **s, BAT **e, Client cntxt, MalBlkPtr mb, MalStkPtr stk,
				  InstrPtr pci, int rtype, const char* mod, const char* err)
{
	*r = *b = *s = *e = NULL;

	(void)cntxt;
	if (pci->argc != 4 || ((isaBatType(getArgType(mb, pci, 2)) && getBatType(getArgType(mb, pci, 2)) != TYPE_lng) ||
		 (isaBatType(getArgType(mb, pci, 3)) && getBatType(getArgType(mb, pci, 3)) != TYPE_lng))) {
		throw(SQL, mod, "%s", err);
	}
	if (isaBatType(getArgType(mb, pci, 1))) {
		*b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!*b)
			throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (*b) {
		BUN cnt = BATcount(*b);
		voidresultBAT((*r), rtype ? rtype : (*b)->ttype, cnt, (*b), mod);
		if (!*r && *b)
			BBPunfix((*b)->batCacheid);
	}
	if (isaBatType(getArgType(mb, pci, 2))) {
		*s = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		if (!*s) {
			if (*b) BBPunfix((*b)->batCacheid);
			if (*r) BBPunfix((*r)->batCacheid);
			throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	if (isaBatType(getArgType(mb, pci, 3))) {
		*e = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!*e) {
			if (*b) BBPunfix((*b)->batCacheid);
			if (*r) BBPunfix((*r)->batCacheid);
			if (*s) BBPunfix((*s)->batCacheid);
			throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	return MAL_SUCCEED;
}

static str
do_limit_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char* op, const char* err,
			   gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, int))
{
	BAT *r = NULL, *b = NULL, *s = NULL, *e = NULL;
	int tpe;
	gdk_return gdk_res;

	(void) cntxt;
	if (pci->argc != 4 || (getArgType(mb, pci, 2) != TYPE_lng && getBatType(getArgType(mb, pci, 2)) != TYPE_lng) ||
		(getArgType(mb, pci, 3) != TYPE_lng && getBatType(getArgType(mb, pci, 3)) != TYPE_lng)) {
		throw(SQL, op, "%s", err);
	}
	tpe = getArgType(mb, pci, 1);

	if (isaBatType(tpe))
		tpe = getBatType(tpe);
	if (isaBatType(getArgType(mb, pci, 1))) {
		b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!b)
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (b) {
		BUN cnt = BATcount(b);
		voidresultBAT(r, tpe, cnt, b, op);
	}
	if (isaBatType(getArgType(mb, pci, 2))) {
		s = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		if (!s) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	if (isaBatType(getArgType(mb, pci, 3))) {
		e = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!e) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			if (s) BBPunfix(s->batCacheid);
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = func(r, b, s, e, tpe);
		BBPunfix(b->batCacheid);
		if (s) BBPunfix(s->batCacheid);
		if (e) BBPunfix(e->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, op, GDK_EXCEPTION);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		*res = *in;
	}
	return MAL_SUCCEED;
}

str
SQLfirst_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_limit_value(cntxt, mb, stk, pci, "sql.first_value", SQLSTATE(42000) "first_value(:any_1,:lng,:lng)", GDKanalyticalfirst);
}

str
SQLlast_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_limit_value(cntxt, mb, stk, pci, "sql.last_value", SQLSTATE(42000) "last_value(:any_1,:lng,:lng)", GDKanalyticallast);
}

#define NTH_VALUE_IMP(TPE) \
	do { \
		TPE *nth = NULL; \
		if (is_a_bat) { \
			bool is_non_positive = false; \
			for(TPE *lp = (TPE*)Tloc(l, 0), *lend = lp + BATcount(l); lp < lend && !is_non_positive; lp++) \
				is_non_positive |= (!is_##TPE##_nil(*lp) && *lp < 1); \
			if(is_non_positive) { \
				BBPunfix(b->batCacheid); \
				BBPunfix(s->batCacheid); \
				BBPunfix(e->batCacheid); \
				BBPunfix(l->batCacheid); \
				throw(SQL, "sql.nth_value", SQLSTATE(42000) "All nth_values must be greater than zero"); \
			} \
		} else { \
			nth = getArgReference_##TPE(stk, pci, 2); \
			if(!is_##TPE##_nil(*nth) && *nth < 1) { \
				BBPunfix(b->batCacheid); \
				BBPunfix(s->batCacheid); \
				BBPunfix(e->batCacheid); \
				throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value must be greater than zero"); \
			} \
		} \
		gdk_res = GDKanalyticalnthvalue(r, b, s, e, l, nth, tp1, tp2); \
	} while(0);

#define NTH_VALUE_SINGLE_IMP(TPE) \
	do { \
		TPE val = *(TPE*) VALget(nth), *toset; \
		if(!VALisnil(nth) && val < 1) \
			throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value must be greater than zero"); \
		toset = (VALisnil(nth) || val > 1) ? (TPE*) ATOMnilptr(tp1) : (TPE*) in; \
		VALset(res, tp1, toset); \
	} while(0);

str
SQLnth_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *r = NULL, *s = NULL, *e = NULL, *l = NULL;
	int tp1, tp2;
	gdk_return gdk_res;
	bool is_a_bat;

	(void) cntxt;
	if (pci->argc != 5 || (getArgType(mb, pci, 3) != TYPE_lng && getBatType(getArgType(mb, pci, 3)) != TYPE_lng) ||
		(getArgType(mb, pci, 4) != TYPE_lng && getBatType(getArgType(mb, pci, 4)) != TYPE_lng)) {
		throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value(:any_1,:number,:lng,:lng)");
	}

	tp1 = getArgType(mb, pci, 1);
	tp2 = getArgType(mb, pci, 2);
	is_a_bat = isaBatType(tp2);
	if (isaBatType(tp1)) {
		BUN cnt;
		bat *res = getArgReference_bat(stk, pci, 0);
		b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!b)
			throw(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		tp1 = getBatType(tp1);

		voidresultBAT(r, tp1, cnt, b, "sql.nth_value");
		if (isaBatType(getArgType(mb, pci, 3))) {
			s = BATdescriptor(*getArgReference_bat(stk, pci, 3));
			if (!s) {
				BBPunfix(b->batCacheid);
				BBPunfix(r->batCacheid);
				throw(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
			}
		}
		if (isaBatType(getArgType(mb, pci, 4))) {
			e = BATdescriptor(*getArgReference_bat(stk, pci, 4));
			if (!e) {
				BBPunfix(b->batCacheid);
				BBPunfix(r->batCacheid);
				if (s) BBPunfix(s->batCacheid);
				throw(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
			}
		}
		if (isaBatType(getArgType(mb, pci, 2))) {
			l = BATdescriptor(*getArgReference_bat(stk, pci, 2));
			if (!l) {
				BBPunfix(b->batCacheid);
				BBPunfix(r->batCacheid);
				if (s) BBPunfix(s->batCacheid);
				if (e) BBPunfix(e->batCacheid);
				throw(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
			}
		}

		if(is_a_bat)
			tp2 = getBatType(tp2);
		switch (tp2) {
			case TYPE_bte:
				NTH_VALUE_IMP(bte)
				break;
			case TYPE_sht:
				NTH_VALUE_IMP(sht)
				break;
			case TYPE_int:
				NTH_VALUE_IMP(int)
				break;
			case TYPE_lng:
				NTH_VALUE_IMP(lng)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				NTH_VALUE_IMP(hge)
				break;
#endif
			default: {
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value offset not available for type %s", ATOMname(tp2));
			}
		}

		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		if (s) BBPunfix(s->batCacheid);
		if (e) BBPunfix(e->batCacheid);
		if (l) BBPunfix(l->batCacheid);
		if(gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, "sql.nth_value", GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *in = &(stk)->stk[(pci)->argv[1]];
		ValRecord *nth = &(stk)->stk[(pci)->argv[2]];

		switch (tp2) {
			case TYPE_bte:
				NTH_VALUE_SINGLE_IMP(bte)
				break;
			case TYPE_sht:
				NTH_VALUE_SINGLE_IMP(sht)
				break;
			case TYPE_int:
				NTH_VALUE_SINGLE_IMP(int)
				break;
			case TYPE_lng:
				NTH_VALUE_SINGLE_IMP(lng)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				NTH_VALUE_SINGLE_IMP(hge)
				break;
#endif
			default:
				throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value offset not available for type %s", ATOMname(tp2));
		}
	}
	return MAL_SUCCEED;
}

#define CHECK_L_VALUE(TPE)                                                                     \
	do {                                                                                       \
		TPE rval = *getArgReference_##TPE(stk, pci, 2);                                        \
		l_value = is_##TPE##_nil(rval) ? BUN_NONE : (rval > 0 ? (BUN)rval : (BUN)(-1 * rval)); \
	} while(0);

static str /* the variable m is used to fix the multiplier */
do_lead_lag(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char* op, const char* desc,
			gdk_return (*func)(BAT *, BAT *, BAT *, BUN, const void* restrict, int))
{
	int tp1, tp2, tp3, base = 2;
	BUN l_value = 1;
	const void *restrict default_value;

	(void)cntxt;
	if (pci->argc < 4 || pci->argc > 6)
		throw(SQL, op, SQLSTATE(42000) "%s called with invalid number of arguments", desc);

	tp1 = getArgType(mb, pci, 1);

	if (pci->argc > 4) { //contains (lag or lead) value;
		tp2 = getArgType(mb, pci, 2);
		if (isaBatType(tp2))
			throw(SQL, op, SQLSTATE(42000) "%s second argument must be a single atom", desc);
		switch (tp2) {
			case TYPE_bte:
				CHECK_L_VALUE(bte)
				break;
			case TYPE_sht:
				CHECK_L_VALUE(sht)
				break;
			case TYPE_int:
				CHECK_L_VALUE(int)
				break;
			case TYPE_lng:
				CHECK_L_VALUE(lng)
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				CHECK_L_VALUE(hge)
				break;
#endif
			default:
				throw(SQL, op, SQLSTATE(42000) "%s value not available for %s", desc, ATOMname(tp2));
		}
		base = 3;
	}

	if (pci->argc > 5) { //contains default value;
		ValRecord *vin = &(stk)->stk[(pci)->argv[3]];
		tp3 = getArgType(mb, pci, 3);
		if (isaBatType(tp3))
			throw(SQL, op, SQLSTATE(42000) "%s third argument must be a single atom", desc);
		default_value = VALget(vin);
		base = 4;
	} else {
		int tpe = tp1;
		if (isaBatType(tpe))
			tpe = getBatType(tp1);
		default_value = ATOMnilptr(tpe);
	}

	assert(default_value); //default value must be set

	if (isaBatType(tp1)) {
		BUN cnt;
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p = NULL, *r;
		if (!b)
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		gdk_return gdk_code;

		tp1 = getBatType(tp1);
		voidresultBAT(r, tp1, cnt, b, op);
		if (isaBatType(getArgType(mb, pci, base))) {
			p = BATdescriptor(*getArgReference_bat(stk, pci, base));
			if (!p) {
				BBPunfix(b->batCacheid);
				throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
			}
		}

		gdk_code = func(r, b, p, l_value, default_value, tp1);

		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		if(gdk_code == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, op, GDK_EXCEPTION);
	} else {
		ValRecord *res = &(stk)->stk[(pci)->argv[0]];
		ValRecord *vin = &(stk)->stk[(pci)->argv[1]];
		if(l_value == 0) {
			if(!VALcopy(res, vin))
				throw(SQL, op, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		} else {
			VALset(res, tp1, (ptr) default_value);
		}
	}
	return MAL_SUCCEED;
}

str
SQLlag(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_lead_lag(cntxt, mb, stk, pci, "sql.lag", "lag", GDKanalyticallag);
}

str
SQLlead(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_lead_lag(cntxt, mb, stk, pci, "sql.lead", "lead", GDKanalyticallead);
}

/* we will keep the ordering bat here although is not needed, but maybe later with varied sized windows */
static str
SQLanalytical_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char* op, const char* err,
				   gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, int))
{
	BAT *r, *b, *s, *e;
	str msg = SQLanalytics_args(&r, &b, &s, &e, cntxt, mb, stk, pci, 0, op, err);
	int tpe = getArgType(mb, pci, 1);
	gdk_return gdk_res;

	if (msg)
		return msg;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = func(r, b, s, e, tpe);
		BBPunfix(b->batCacheid);
		if (s) BBPunfix(s->batCacheid);
		if (e) BBPunfix(e->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, op, GDK_EXCEPTION);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		*res = *in;
	}
	return msg;
}

str
SQLmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.min", SQLSTATE(42000) "min(:any_1,:lng,:lng)", GDKanalyticalmin);
}

str
SQLmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.max", SQLSTATE(42000) "max(:any_1,:lng,:lng)", GDKanalyticalmax);
}

str
SQLcount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *s = NULL, *e = NULL;
	int tpe;
	bit *ignore_nils;
	gdk_return gdk_res;

	(void) cntxt;
	if (pci->argc != 5 || getArgType(mb, pci, 2) != TYPE_bit ||
		(getArgType(mb, pci, 3) != TYPE_lng && getBatType(getArgType(mb, pci, 3)) != TYPE_lng) ||
		(getArgType(mb, pci, 4) != TYPE_lng && getBatType(getArgType(mb, pci, 4)) != TYPE_lng)) {
		throw(SQL, "sql.count", "%s", "count(:any_1,:bit,:lng,:lng)");
	}
	tpe = getArgType(mb, pci, 1);
	ignore_nils = getArgReference_bit(stk, pci, 2);

	if (isaBatType(tpe))
		tpe = getBatType(tpe);
	if (isaBatType(getArgType(mb, pci, 1))) {
		b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!b)
			throw(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (b) {
		BUN cnt = BATcount(b);
		voidresultBAT(r, TYPE_lng, cnt, b, "sql.count");
	}
	if (isaBatType(getArgType(mb, pci, 3))) {
		s = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!s) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			throw(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	if (isaBatType(getArgType(mb, pci, 4))) {
		e = BATdescriptor(*getArgReference_bat(stk, pci, 4));
		if (!e) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			if (s) BBPunfix(s->batCacheid);
			throw(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = GDKanalyticalcount(r, b, s, e, ignore_nils, tpe);
		BBPunfix(b->batCacheid);
		if (s) BBPunfix(s->batCacheid);
		if (e) BBPunfix(e->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, "sql.count", GDK_EXCEPTION);
	} else {
		lng *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe);
		const void *nil = ATOMnilptr(tpe);
		if(atomcmp(in, nil) == 0 && *ignore_nils)
			*res = 0;
		else
			*res = 1;
	}
	return MAL_SUCCEED;
}

static str
do_analytical_sumprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char* op, const char* err,
					  gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, int, int))
{
	BAT *r = NULL, *b = NULL, *s = NULL, *e = NULL;
	int tp1, tp2;
	gdk_return gdk_res;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	if (((isaBatType(getArgType(mb, pci, 2)) && getBatType(getArgType(mb, pci, 2)) != TYPE_lng) ||
		(isaBatType(getArgType(mb, pci, 3)) && getBatType(getArgType(mb, pci, 3)) != TYPE_lng))) {
		throw(SQL, op, "%s", err);
	}
	tp1 = getArgType(mb, pci, 1);

	if (isaBatType(tp1)) {
		tp1 = getBatType(tp1);
		b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!b)
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
	}
	switch (tp1) {
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_lng:
#ifdef HAVE_HGE
		case TYPE_hge:
			tp2 = TYPE_hge;
#else
			tp2 = TYPE_lng;
#endif
			break;
		case TYPE_flt:
			tp2 = TYPE_flt;
			break;
		case TYPE_dbl:
			tp2 = TYPE_dbl;
			break;
		default: {
			if(b) BBPunfix(b->batCacheid);
			throw(SQL, op, SQLSTATE(42000) "%s not available for %s", op, ATOMname(tp1));
		}
	}
	if (b) {
		bat *res;

		voidresultBAT(r, tp2, BATcount(b), b, op);
		s = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		if (!s) {
			BBPunfix(b->batCacheid);
			BBPunfix(r->batCacheid);
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
		}
		e = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!e) {
			BBPunfix(b->batCacheid);
			BBPunfix(r->batCacheid);
			BBPunfix(s->batCacheid);
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
		}
		res = getArgReference_bat(stk, pci, 0);
		gdk_res = func(r, b, s, e, tp1, tp2);

		BBPunfix(b->batCacheid);
		BBPunfix(s->batCacheid);
		BBPunfix(e->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, op, GDK_EXCEPTION);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		int scale = 0;

		switch (tp1) {
#ifdef HAVE_HGE
			case TYPE_bte:
				msg = bte_dec2_hge((hge*)res, &scale, (bte*)in);
				break;
			case TYPE_sht:
				msg = sht_dec2_hge((hge*)res, &scale, (sht*)in);
				break;
			case TYPE_int:
				msg = int_dec2_hge((hge*)res, &scale, (int*)in);
				break;
			case TYPE_lng:
				msg = lng_dec2_hge((hge*)res, &scale, (lng*)in);
				break;
			case TYPE_hge:
				*res = *in;
				break;
#else
			case TYPE_bte:
				msg = bte_dec2_lng((lng*)res, &scale, (bte*)in);
				break;
			case TYPE_sht:
				msg = sht_dec2_lng((lng*)res, &scale, (sht*)in);
				break;
			case TYPE_int:
				msg = int_dec2_lng((lng*)res, &scale, (int*)in);
				break;
			case TYPE_lng:
				*res = *in;
				break;
#endif
			case TYPE_flt: {
				flt fp = *((flt*)in);
				dbl *db = (dbl*)res;
				if(is_flt_nil(fp))
					*db = dbl_nil;
				else
					*db = (dbl) fp;
			} break;
			case TYPE_dbl:
				*res = *in;
				break;
			default:
				throw(SQL, op, SQLSTATE(42000) "%s not available for %s", op, ATOMname(tp1));
		}
	}
	return msg;
}

str
SQLsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_analytical_sumprod(cntxt, mb, stk, pci, "sql.sum", SQLSTATE(42000) "sum(:any_1,:lng,:lng)",
								 GDKanalyticalsum);
}

str
SQLprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_analytical_sumprod(cntxt, mb, stk, pci, "sql.prod", SQLSTATE(42000) "prod(:any_1,:lng,:lng)",
								 GDKanalyticalprod);
}

str
SQLavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r, *b, *s, *e;
	str msg = SQLanalytics_args(&r, &b, &s, &e, cntxt, mb, stk, pci, TYPE_dbl, "sql.avg",
								SQLSTATE(42000) "avg(:any_1,:lng,:lng)");
	int tpe = getArgType(mb, pci, 1);
	gdk_return gdk_res;

	if (msg)
		return msg;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = GDKanalyticalavg(r, b, s, e, tpe);
		BBPunfix(b->batCacheid);
		if (s) BBPunfix(s->batCacheid);
		if (e) BBPunfix(e->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, "sql.avg", GDK_EXCEPTION);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		int scale = 0;

		switch (tpe) {
			case TYPE_bte:
				msg = bte_dec2_dbl((dbl*)res, &scale, (bte*)in);
				break;
			case TYPE_sht:
				msg = sht_dec2_dbl((dbl*)res, &scale, (sht*)in);
				break;
			case TYPE_int:
				msg = int_dec2_dbl((dbl*)res, &scale, (int*)in);
				break;
			case TYPE_lng:
				msg = lng_dec2_dbl((dbl*)res, &scale, (lng*)in);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				msg = hge_dec2_dbl((dbl*)res, &scale, (hge*)in);
				break;
#endif
			case TYPE_flt: {
				flt fp = *((flt*)in);
				dbl *db = (dbl*)res;
				if(is_flt_nil(fp))
					*db = dbl_nil;
				else
					*db = (dbl) fp;
			} break;
			case TYPE_dbl:
				*res = *in;
				break;
			default:
				throw(SQL, "sql.avg", SQLSTATE(42000) "average not available for %s", ATOMname(tpe));
		}
	}
	return msg;
}
