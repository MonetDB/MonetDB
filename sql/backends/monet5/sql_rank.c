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
		voidresultBAT(r, TYPE_bit, (int)BATcount(b), b, "sql.diff");
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
		int j, cnt, *rp, *end;
		bit *np;

		if (!b)
			throw(SQL, "sql.row_number", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = (int)BATcount(b);
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
			for(j=1; j<=cnt; j++, rp++)
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
		int j, k, cnt, *rp, *end;
		bit *np, *no;

		if (!b)
			throw(SQL, "sql.rank", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = (int)BATcount(b);
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
				for(j=1; j<=cnt; j++, rp++)
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
		int j, cnt, *rp, *end;
		bit *np, *no;

		if (!b)
			throw(SQL, "sql.dense_rank", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = (int)BATcount(b);
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
				for(j=1; j<=cnt; j++, rp++)
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
		int j, k, cnt;
		dbl *rp, *end, cnt_cast;
		bit *np, *no;

		if (!b)
			throw(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = (int)BATcount(b);
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
		int j, cnt;
		dbl *rb, *rp, *end, cnt_cast;
		bit *np;

		if (!b)
			throw(SQL, "sql.cume_dist", SQLSTATE(HY005) "Cannot access column descriptor");
		cnt = (int)BATcount(b);
		cnt_cast = (dbl) cnt;
		voidresultBAT(r, TYPE_dbl, cnt, b, "sql.cume_dist");
		rb = rp = (dbl*)Tloc(r, 0);
		end = rp + cnt;
		if (isaBatType(getArgType(mb, pci, 2))) {
			if (isaBatType(getArgType(mb, pci, 3))) {
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				if (!p) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
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
					throw(SQL, "sql.percent_rank", SQLSTATE(HY005) "Cannot access column descriptor");
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
	if (b) {
		size_t cnt = BATcount(*b);
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
	BAT *r = NULL, *p = NULL, *o = NULL, *cr;
	str msg = MAL_SUCCEED;
	int tpe, unit, start, end, excl;
	bit ignore_nils = 0;
	gdk_return gdk_res;

	(void)start;
	(void)end;
	(void)cntxt;
	if (pci->argc != 7 || (getArgType(mb, pci, 1) != TYPE_bit && getBatType(getArgType(mb, pci, 1)) != TYPE_bit) ||
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit)){
		throw(SQL, "sql.count", "%s", "count(:any_1,:bit,:bit)");
	}

	tpe = getArgType(mb, pci, 1);
	unit = *getArgReference_int(stk, pci, 3);
	//start = *getArgReference_int(stk, pci, 4);
	//end = *getArgReference_int(stk, pci, 5);
	excl = *getArgReference_int(stk, pci, 6);
	if (unit != 0 || excl != 0)
		throw(SQL, "sql.count", SQLSTATE(42000) "OVER currently only supports frame extends with unit ROWS (and none of the excludes)");

	if (isaBatType(getArgType(mb, pci, 1))) {
		p = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!p)
			throw(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (isaBatType(getArgType(mb, pci, 2))) {
		o = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		if (!o) {
			BBPunfix(p->batCacheid);
			throw(SQL, "sql.count", SQLSTATE(HY005) "Cannot access column descriptor");
		}
	}
	cr = o?o:p?p:NULL;
	if (cr) {
		voidresultBAT(r, TYPE_lng, BATcount(cr), cr, "sql.count");
	}

	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (cr) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = GDKanalyticalcount(r, NULL, p, o, &ignore_nils, tpe);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			return createException(SQL, "sql.count", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		*res = *in;
	}
	return msg;
}

str
SQLcount_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r, *b, *p, *o;
	str msg = SQLanalytics_args(&r, &b, &p, &o, cntxt, mb, stk, pci, TYPE_lng, "sql.count_no_nil",
								SQLSTATE(42000) "count_no_nil(:any_1,:bit,:bit)");
	int tpe = getArgType(mb, pci, 1);
	int unit = *getArgReference_int(stk, pci, 4);
	int start = *getArgReference_int(stk, pci, 5);
	int end = *getArgReference_int(stk, pci, 6);
	int excl = *getArgReference_int(stk, pci, 7);
	gdk_return gdk_res;
	bit ignore_nils = 1;

	if (unit != 0 || excl != 0)
		throw(SQL, "sql.count_no_nil", SQLSTATE(42000) "OVER currently only supports frame extends with unit ROWS (and none of the excludes)");
	(void)start;
	(void)end;

	if (msg)
		return msg;
	if (isaBatType(tpe))
		tpe = getBatType(tpe);

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);

		gdk_res = GDKanalyticalcount(r, b, p, o, &ignore_nils, tpe);
		BBPunfix(b->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
		else
			return createException(SQL, "sql.count_no_nil", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		*res = *in;
	}
	return msg;
}

static str
do_analytical_sumprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
					  gdk_return (*func)(BAT *, BAT *, BAT *, BAT *, int, int), int tpe, const str op, const str err)
{
	BAT *r, *b, *p, *o;
	str msg = SQLanalytics_args(&r, &b, &p, &o, cntxt, mb, stk, pci, tpe, op, err);
	int tp1 = getArgType(mb, pci, 1), tp2;
	int unit = *getArgReference_int(stk, pci, 4);
	int start = *getArgReference_int(stk, pci, 5);
	int end = *getArgReference_int(stk, pci, 6);
	int excl = *getArgReference_int(stk, pci, 7);
	gdk_return gdk_res;

	if (unit != 0 || excl != 0)
		throw(SQL, op, SQLSTATE(42000) "OVER currently only supports frame extends with unit ROWS (and none of the excludes)");
	(void) start;
	(void) end;

	if (msg)
		return msg;
	if (isaBatType(tp1))
		tp1 = getBatType(tp1);

	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);
		tp2 = getBatType(r->T.type);

		gdk_res = func(r, b, p, o, tp1, tp2);
		BBPunfix(b->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		if (gdk_res == GDK_SUCCEED)
			BBPkeepref(*res = r->batCacheid);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);
		*res = *in;
	}
	return msg;
}

str
SQLscalarsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ValPtr ret = &stk->stk[getArg(pci, 0)];
	return do_analytical_sumprod(cntxt, mb, stk, pci, GDKanalyticalsum, ret->vtype,
								 "sql.sum", SQLSTATE(42000) "sum(:any_1,:bit,:bit)");
}

#define SQLVECTORSUM(TPE)                                                             \
str                                                                                   \
SQLvectorsum_##TPE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)           \
{                                                                                     \
	return do_analytical_sumprod(cntxt, mb, stk, pci, GDKanalyticalsum, TYPE_##TPE,   \
								 "sql.sum", SQLSTATE(42000) "sum(:any_1,:bit,:bit)"); \
}

SQLVECTORSUM(lng)
#ifdef HAVE_HGE
SQLVECTORSUM(hge)
#endif
SQLVECTORSUM(flt)
SQLVECTORSUM(dbl)

#undef SQLVECTORSUM

str
SQLscalarprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ValPtr ret = &stk->stk[getArg(pci, 0)];
	return do_analytical_sumprod(cntxt, mb, stk, pci, GDKanalyticalprod, ret->vtype,
								 "sql.prod", SQLSTATE(42000) "prod(:any_1,:bit,:bit)");
}

#define SQLVECTORPROD(TPE)                                                              \
str                                                                                     \
SQLvectorprod_##TPE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)            \
{                                                                                       \
	return do_analytical_sumprod(cntxt, mb, stk, pci, GDKanalyticalprod, TYPE_##TPE,    \
								 "sql.prod", SQLSTATE(42000) "prod(:any_1,:bit,:bit)"); \
}

SQLVECTORPROD(lng)
SQLVECTORPROD(flt)
SQLVECTORPROD(dbl)
#ifdef HAVE_HGE
SQLVECTORPROD(hge)
#endif

#undef SQLVECTORPROD

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
		*res = *in;
	}
	return msg;
}
