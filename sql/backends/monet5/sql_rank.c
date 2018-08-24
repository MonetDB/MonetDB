/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_rank.h"
#include "gdk_analytic.h"

#define voidresultBAT(r,tpe,cnt,b,err)				\
	do {							\
		r = COLnew(b->hseqbase, tpe, cnt, TRANSIENT);	\
		if (r == NULL) {				\
			BBPunfix(b->batCacheid);		\
			throw(MAL, err, SQLSTATE(HY001) MAL_MALLOC_FAIL);	\
		}						\
		r->tsorted = 0;					\
		r->trevsorted = 0;				\
		r->tnonil = 1;					\
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
			throw(SQL, "sql.diff", SQLSTATE(HY001) "Unknown GDK error");
	} else {
		bit *res = getArgReference_bit(stk, pci, 0);

		*res = FALSE;
	}
	return MAL_SUCCEED;
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
		if (isaBatType(getArgType(mb, pci, 4))) {                                           \
			o = BATdescriptor(*getArgReference_bat(stk, pci, 4));                           \
			if (!o) {                                                                       \
				BBPunfix(b->batCacheid);                                                    \
				BBPunfix(p->batCacheid);                                                    \
				throw(SQL, "sql.ntile", SQLSTATE(HY005) "Cannot access column descriptor"); \
			}                                                                               \
		}                                                                                   \
		gdk_code = GDKanalyticalntile(r, b, p, o, TYPE_##TPE, ntile);                       \
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
		throw(SQL, "sql.ntile", SQLSTATE(42000) "ntile first argument must a single atom");

	if (isaBatType(tp1)) {
		BUN cnt;
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p = NULL, *o = NULL, *r;
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
		if(gdk_code == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, "sql.ntile", SQLSTATE(HY001) "Unknown GDK error");
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

#undef NTILE_IMP
#undef NTILE_VALUE_SINGLE_IMP

static str
SQLanalytics_args(BAT **r, BAT **b, BAT **p, BAT **o, Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
				  int rtype, const str mod, const str err)
{
	*r = *b = *p = *o = NULL;

	(void)cntxt;
	if (pci->argc != 8 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, mod, "%s", err);
	}
	if (isaBatType(getArgType(mb, pci, 1))) {
		*b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!*b)
			throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (b && *b) {
		BUN cnt = BATcount(*b);
		voidresultBAT((*r), rtype ? rtype : (*b)->ttype, cnt, (*b), mod);
		if (!*r && *b)
			BBPunfix((*b)->batCacheid);
	}
	if (isaBatType(getArgType(mb, pci, 2))) {
		*p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		if (!*p) {
			if (*b) BBPunfix((*b)->batCacheid);
			if (*r) BBPunfix((*r)->batCacheid);
			throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	if (isaBatType(getArgType(mb, pci, 3))) {
		*o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!*o) {
			if (*b) BBPunfix((*b)->batCacheid);
			if (*r) BBPunfix((*r)->batCacheid);
			if (*p) BBPunfix((*p)->batCacheid);
			throw(SQL, mod, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	return MAL_SUCCEED;
}

/* we will keep the ordering bat here although is not needed, but maybe later with varied sized windows */
static str
SQLanalytical_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const str op, const str err,
					gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, int))
{
	BAT *r, *b, *p, *o;
	str msg = SQLanalytics_args(&r, &b, &p, &o, cntxt, mb, stk, pci, 0, op, err);
	int tpe = getArgType(mb, pci, 1);
	int unit = *getArgReference_int(stk, pci, 4);
	int start = *getArgReference_int(stk, pci, 5);
	int end = *getArgReference_int(stk, pci, 6);
	int excl = *getArgReference_int(stk, pci, 7);
	gdk_return gdk_res;

	if (unit != 0 || excl != 0)
		throw(SQL, op, SQLSTATE(42000) "OVER currently only supports frame extends with unit ROWS (and none of the excludes)");
	(void)start;
	(void)end;

	if (msg)
		return msg;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = func(r, b, p, o, tpe);
		BBPunfix(b->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			return createException(SQL, op, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		*res = *in;
	}
	return msg;
}

str
SQLfirst_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.first_value", SQLSTATE(42000) "first_value(:any_1,:bit,:bit)", GDKanalyticalfirst);
}

str
SQLlast_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.last_value", SQLSTATE(42000) "last_value(:any_1,:bit,:bit)", GDKanalyticallast);
}

#define NTH_VALUE_IMP(TPE)                                                                      \
	do {                                                                                        \
		TPE *nthvalue = getArgReference_##TPE(stk, pci, 2);                                     \
		lng cast_value;                                                                         \
		if(!is_##TPE##_nil(*nthvalue) && *nthvalue < 1) {                                       \
			BBPunfix(b->batCacheid);                                                            \
			throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value must be greater than zero"); \
		}                                                                                       \
		voidresultBAT(r, tp1, cnt, b, "sql.nth_value");                                         \
		if (isaBatType(getArgType(mb, pci, 3))) {                                               \
			p = BATdescriptor(*getArgReference_bat(stk, pci, 3));                               \
			if (!p) {                                                                           \
				BBPunfix(b->batCacheid);                                                        \
				throw(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor"); \
			}                                                                                   \
		}                                                                                       \
		if (isaBatType(getArgType(mb, pci, 4))) {                                               \
			o = BATdescriptor(*getArgReference_bat(stk, pci, 4));                               \
			if (!o) {                                                                           \
				BBPunfix(b->batCacheid);                                                        \
				BBPunfix(p->batCacheid);                                                        \
				throw(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor"); \
			}                                                                                   \
		}                                                                                       \
		cast_value = is_##TPE##_nil(*nthvalue) ? lng_nil : (lng)(((TPE)*nthvalue) - 1);         \
		gdk_code = GDKanalyticalnthvalue(r, b, p, o, cast_value, tp1);                          \
	} while(0);

#define NTH_VALUE_SINGLE_IMP(TPE)                                                               \
	do {                                                                                        \
		TPE val = *(TPE*) nth, *rres = (TPE*) res;                                              \
		if(!is_##TPE##_nil(val) && val < 1)                                                     \
			throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value must be greater than zero"); \
		*rres = (is_##TPE##_nil(val) || val > 1) ? TPE##_nil : *(TPE*) in;                      \
	} while(0);

str
SQLnth_value(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tp1, tp2;

	(void)cntxt;
	if (pci->argc != 9 || (getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit) ||
		(getArgType(mb, pci, 4) != TYPE_bit && getBatType(getArgType(mb, pci, 4)) != TYPE_bit)) {
		throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value(:any_1,:number,:bit,:bit)");
	}
	tp1 = getArgType(mb, pci, 1), tp2 = getArgType(mb, pci, 2);
	if (isaBatType(tp2))
		throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value second argument must a single atom");

	if (isaBatType(tp1)) {
		BUN cnt;
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p = NULL, *o = NULL, *r;
		if (!b)
			throw(SQL, "sql.nth_value", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = BATcount(b);
		gdk_return gdk_code;

		tp1 = getBatType(tp1);
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
				throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value not available for %s", ATOMname(tp2));
			}
		}

		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		if(gdk_code == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			throw(SQL, "sql.nth_value", SQLSTATE(HY001) "Unknown GDK error");
	} else {
		ptr res = getArgReference_ptr(stk, pci, 0);
		ptr in = getArgReference_ptr(stk, pci, 1);
		ptr nth = getArgReference_ptr(stk, pci, 2);

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
				throw(SQL, "sql.nth_value", SQLSTATE(42000) "nth_value not available for %s", ATOMname(tp2));
		}
	}
	return MAL_SUCCEED;
}

#undef NTH_VALUE_IMP
#undef NTH_VALUE_SINGLE_IMP

str
SQLmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.min", SQLSTATE(42000) "min(:any_1,:bit,:bit)", GDKanalyticalmin);
}

str
SQLmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return SQLanalytical_func(cntxt, mb, stk, pci, "sql.max", SQLSTATE(42000) "max(:any_1,:bit,:bit)", GDKanalyticalmax);
}

str
SQLcount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL;
	int tpe, unit, start, end, excl;
	bit *ignore_nils;
	gdk_return gdk_res;

	(void) cntxt;
	if (pci->argc != 9 || getArgType(mb, pci, 2) != TYPE_bit ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit) ||
		(getArgType(mb, pci, 4) != TYPE_bit && getBatType(getArgType(mb, pci, 4)) != TYPE_bit)) {
		throw(SQL, "sql.count", "%s", "count(:any_1,:bit,:bit,:bit)");
	}
	tpe = getArgType(mb, pci, 1);
	ignore_nils = getArgReference_bit(stk, pci, 2);
	unit = *getArgReference_int(stk, pci, 5);
	start = *getArgReference_int(stk, pci, 6);
	end = *getArgReference_int(stk, pci, 7);
	excl = *getArgReference_int(stk, pci, 8);

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
		p = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!p) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			throw(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	if (isaBatType(getArgType(mb, pci, 4))) {
		o = BATdescriptor(*getArgReference_bat(stk, pci, 4));
		if (!o) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			if (p) BBPunfix(p->batCacheid);
			throw(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}

	if (unit != 0 || excl != 0)
		throw(SQL, "sql.count", SQLSTATE(42000) "OVER currently only supports frame extends with unit ROWS (and none of the excludes)");
	(void)start;
	(void)end;

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = GDKanalyticalcount(r, b, p, o, ignore_nils, tpe);
		BBPunfix(b->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			return createException(SQL, "sql.count", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
do_analytical_sumprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
					  gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, int, int), const str op, const str err)
{
	BAT *r = NULL, *b = NULL, *p = NULL, *o = NULL;
	int tp1, tp2, unit, start, end, excl;
	gdk_return gdk_res;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	if (pci->argc != 8 ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) ||
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)) {
		throw(SQL, op, "%s", err);
	}
	tp1 = getArgType(mb, pci, 1);
	unit = *getArgReference_int(stk, pci, 4);
	start = *getArgReference_int(stk, pci, 5);
	end = *getArgReference_int(stk, pci, 6);
	excl = *getArgReference_int(stk, pci, 7);

	if (isaBatType(tp1))
		tp1 = getBatType(tp1);
	if (isaBatType(getArgType(mb, pci, 1))) {
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
		BUN cnt = BATcount(b);
		voidresultBAT(r, tp2, cnt, b, op);
	}
	if (isaBatType(getArgType(mb, pci, 2))) {
		p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		if (!p) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	if (isaBatType(getArgType(mb, pci, 3))) {
		o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!o) {
			if (b) BBPunfix(b->batCacheid);
			if (r) BBPunfix(r->batCacheid);
			if (p) BBPunfix(p->batCacheid);
			throw(SQL, op, SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}

	if (unit != 0 || excl != 0)
		throw(SQL, op, SQLSTATE(42000) "OVER currently only supports frame extends with unit ROWS (and none of the excludes)");
	(void) start;
	(void) end;

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = func(r, b, p, o, tp1, tp2);
		BBPunfix(b->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			return createException(SQL, op, SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	return do_analytical_sumprod(cntxt, mb, stk, pci, GDKanalyticalsum, "sql.sum", SQLSTATE(42000) "sum(:any_1,:bit,:bit)");
}

str
SQLprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_analytical_sumprod(cntxt, mb, stk, pci, GDKanalyticalprod, "sql.prod", SQLSTATE(42000) "prod(:any_1,:bit,:bit)");
}

str
SQLavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r, *b, *p, *o;
	str msg = SQLanalytics_args(&r, &b, &p, &o, cntxt, mb, stk, pci, TYPE_dbl, "sql.avg",
								SQLSTATE(42000) "avg(:any_1,:bit,:bit)");
	int tpe = getArgType(mb, pci, 1);
	int unit = *getArgReference_int(stk, pci, 4);
	int start = *getArgReference_int(stk, pci, 5);
	int end = *getArgReference_int(stk, pci, 6);
	int excl = *getArgReference_int(stk, pci, 7);
	gdk_return gdk_res;

	if (unit != 0 || excl != 0)
		throw(SQL, "sql.avg", SQLSTATE(42000) "OVER currently only supports frame extends with unit ROWS (and none of the excludes)");
	(void)start;
	(void)end;

	if (msg)
		return msg;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = GDKanalyticalavg(r, b, p, o, tpe);
		BBPunfix(b->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			return createException(SQL, "sql.avg", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
