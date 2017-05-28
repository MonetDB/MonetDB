/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_rank.h"

#define voidresultBAT(r,tpe,cnt,b,err)				\
	do {							\
		r = COLnew(b->hseqbase, tpe, cnt, TRANSIENT);	\
		if (r == NULL) {				\
			BBPunfix(b->batCacheid);		\
			throw(MAL, err, MAL_MALLOC_FAIL);	\
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
		BAT *b = BATdescriptor(*bid), *c;
		BAT *r;
		bit *bp, *rp;
		int i, cnt;
		int (*cmp)(const void *, const void *);
		BATiter it;
		ptr v;
			
		if (!b)
			throw(SQL, "sql.rank", "Cannot access descriptor");
		cnt = (int)BATcount(b);
		voidresultBAT(r, TYPE_bit, cnt, b, "Cannot create bat");
		rp = (bit*)Tloc(r, 0);
		if (pci->argc > 2) {
			c = b;
			bid = getArgReference_bat(stk, pci, 2);
			b = BATdescriptor(*bid);

	       		cmp = ATOMcompare(b->ttype);
	       		it = bat_iterator(b);
			v = BUNtail(it, 0);
		       	bp = (bit*)Tloc(c, 0);

			for(i=0; i<cnt; i++, bp++, rp++) {
				*rp = *bp;
				if (cmp(v, BUNtail(it,i)) != 0) { 
					*rp = TRUE;
					v = BUNtail(it, i);
				}
			}
		} else {
	       		cmp = ATOMcompare(b->ttype);
	       		it = bat_iterator(b);
			v = BUNtail(it, 0);

			for(i=0; i<cnt; i++, rp++) {
				*rp = FALSE;
				if (cmp(v, BUNtail(it,i)) != 0) { 
					*rp = TRUE;
					v = BUNtail(it, i);
				}
			}
		}
		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		BBPkeepref(*res = r->batCacheid);
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
		throw(SQL, "sql.row_number", "row_number(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *r;
		int i, j, cnt, *rp;
		bit *np;
			
		if (!b)
			throw(SQL, "sql.row_number", "Cannot access descriptor");
		cnt = (int)BATcount(b);
	 	voidresultBAT(r, TYPE_int, cnt, b, "Cannot create bat");
		rp = (int*)Tloc(r, 0);
		if (isaBatType(getArgType(mb, pci, 2))) { 
			/* order info not used */
			p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
			if (!p) {
				BBPunfix(b->batCacheid);
				throw(SQL, "sql.row_number", "Cannot access descriptor");
			}
			np = (bit*)Tloc(p, 0);
			for(i=1,j=1; i<=cnt; i++, j++, np++, rp++) {
				if (*np)
					j=1;
				*rp = j;
			}
			BBPunfix(p->batCacheid);
		} else { /* single value, ie no partitions, order info not used */
			for(i=1; i<=cnt; i++, rp++) 
				*rp = i;
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
		throw(SQL, "sql.rank", "rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *o, *r;
		int i, j, k, cnt, *rp;
		bit *np, *no;
			
		if (!b)
			throw(SQL, "sql.rank", "Cannot access descriptor");
		cnt = (int)BATcount(b);
		voidresultBAT(r, TYPE_int, cnt, b, "Cannot create bat");
		rp = (int*)Tloc(r, 0);
		if (isaBatType(getArgType(mb, pci, 2))) { 
			if (isaBatType(getArgType(mb, pci, 3))) { 
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!p || !o) {
					BBPunfix(b->batCacheid);
					if (p) BBPunfix(p->batCacheid);
					if (o) BBPunfix(o->batCacheid);
					throw(SQL, "sql.rank", "Cannot access descriptor");
				}
			        np = (bit*)Tloc(p, 0);
			        no = (bit*)Tloc(o, 0);
				for(i=1,j=1,k=1; i<=cnt; i++, k++, np++, no++, rp++) {
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
					throw(SQL, "sql.rank", "Cannot access descriptor");
				}
			        np = (bit*)Tloc(p, 0);
				for(i=1,j=1,k=1; i<=cnt; i++, k++, np++, rp++) {
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
					throw(SQL, "sql.rank", "Cannot access descriptor");
				}
			        no = (bit*)Tloc(o, 0);
				for(i=1,j=1,k=1; i<=cnt; i++, k++, no++, rp++) {
					if (*no)
						j=k;
					*rp = j;
				}
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering */
				for(i=1; i<=cnt; i++, rp++) 
					*rp = i;
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
		throw(SQL, "sql.dense_rank", "dense_rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *o, *r;
		int i, j, cnt, *rp;
		bit *np, *no;
			
		if (!b)
			throw(SQL, "sql.dense_rank", "Cannot access descriptor");
		cnt = (int)BATcount(b);
		voidresultBAT(r, TYPE_int, cnt, b, "Cannot create bat");
		rp = (int*)Tloc(r, 0);
		if (isaBatType(getArgType(mb, pci, 2))) { 
			if (isaBatType(getArgType(mb, pci, 3))) { 
				p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
				o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
				if (!p || !o) {
					BBPunfix(b->batCacheid);
					if (p) BBPunfix(p->batCacheid);
					if (o) BBPunfix(o->batCacheid);
					throw(SQL, "sql.dense_rank", "Cannot access descriptor");
				}
			        np = (bit*)Tloc(p, 0);
			        no = (bit*)Tloc(o, 0);
				for(i=1,j=1; i<=cnt; i++, np++, no++, rp++) {
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
					throw(SQL, "sql.dense_rank", "Cannot access descriptor");
				}
			        np = (bit*)Tloc(p, 0);
				for(i=1,j=1; i<=cnt; i++, np++, rp++) {
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
					throw(SQL, "sql.dense_rank", "Cannot access descriptor");
				}
			        no = (bit*)Tloc(o, 0);
				for(i=1,j=1; i<=cnt; i++, no++, rp++) {
					if (*no)
						j++;
					*rp = j;
				}
				BBPunfix(o->batCacheid);
			} else { /* single value, ie no ordering */
				for(i=1; i<=cnt; i++, rp++) 
					*rp = i;
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

static str
SQLanalytics_args(BAT **r, BAT **b, BAT **p, BAT **o,  Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const str mod, const str err) 
{
	*r = *b = *p = *o = NULL;

	(void)cntxt;
	if (pci->argc != 7 || 
		(getArgType(mb, pci, 2) != TYPE_bit && getBatType(getArgType(mb, pci, 2)) != TYPE_bit) || 
		(getArgType(mb, pci, 3) != TYPE_bit && getBatType(getArgType(mb, pci, 3)) != TYPE_bit)){
		throw(SQL, mod, "%s", err);
	}
	if (isaBatType(getArgType(mb, pci, 1))) {
		*b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (!*b)
			throw(SQL, mod, "Cannot access descriptor");
	}
	if (b) {
		size_t cnt = BATcount(*b);
		voidresultBAT((*r), (*b)->ttype, cnt, (*b), "Cannot create bat");
		if (!*r) 
			if (*b) BBPunfix((*b)->batCacheid);
	}
	if (isaBatType(getArgType(mb, pci, 2))) {
		*p = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		if (!*p) {
			if (*b) BBPunfix((*b)->batCacheid);
			if (*r) BBPunfix((*r)->batCacheid);
			throw(SQL, mod, "Cannot access descriptor");
		}
	}
	if (isaBatType(getArgType(mb, pci, 3))) { 
		*o = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (!*o) {
			if (*b) BBPunfix((*b)->batCacheid);
			if (*r) BBPunfix((*r)->batCacheid);
			if (*p) BBPunfix((*p)->batCacheid);
			throw(SQL, mod, "Cannot access descriptor");
		}
	}
	return NULL;
}

str 
SQLmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *r, *b, *p, *o;
	str res = SQLanalytics_args( &r, &b, &p, &o, cntxt, mb, stk, pci, "sql.min", "min(:any_1,:bit,:bit)");
	int tpe = getArgType(mb, pci, 1); 
	int start = *getArgReference_int(stk, pci, 4);
	int end = *getArgReference_int(stk, pci, 5);
	int excl = *getArgReference_int(stk, pci, 6);

	if (excl != 0)
		throw(SQL, "sql.min", "OVER currently only supports frame extends with unit ROWS");
	(void)start;
	(void)end;

	if (isaBatType(tpe))
		tpe = getBatType(tpe);
	if (res)
		return res;

	/*
	switch(ATOMstorage(tpe)) {
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HUGE
	case TYPE_hge:
#endif
	case TYPE_flt:
	case TYPE_dbl:
	default:
		throw(SQL, "sql.min", "min(:any_1,:bit,:bit)");
	}
	*/

	/* FOR NOW only int input type !! */
	if (b) {
		bat *res = getArgReference_bat(stk, pci, 0);
		int i, j, cnt, *rp, *rb, *bp, curval;
		bit *np, *no;
			
		cnt = (int)BATcount(b);
		rb = rp = (int*)Tloc(r, 0);
		bp = (int*)Tloc(b, 0);
		curval = *bp;
		if (p) {
			if (o) {
			        np = (bit*)Tloc(p, 0);
			        no = (bit*)Tloc(o, 0);
				for(i=1,j=1; i<=cnt; i++, np++, no++, rp++, bp++) {
					if (*np) {
						j=1;
						for (;rb < rp; rb++)
							*rb = curval;
						curval = *bp;
					} else if (*no)
						j++;
					curval = MIN(*bp,curval);
				}
				for (;rb < rp; rb++)
					*rb = curval;
			} else { /* single value, ie no ordering */
			        np = (bit*)Tloc(p, 0);
				for(i=1,j=1; i<=cnt; i++, np++, rp++, bp++) {
					if (*np) {
						j=1;
						for (;rb < rp; rb++)
							*rb = curval;
						curval = *bp;
					}
					curval = MIN(*bp,curval);
				}
				for (;rb < rp; rb++)
					*rb = curval;
			}
		} else if (o) { /* single value, ie no partitions */
			no = (bit*)Tloc(o, 0);
			for(i=1,j=1; i<=cnt; i++, no++, rp++, bp++) {
				if (*no)
					j++;
				*rp = j;
				curval = MIN(*bp,curval);
			}
			for (;rb < rp; rb++)
				*rb = curval;
		} else { /* single value, ie no ordering */
			for(i=1; i<=cnt; i++, rp++, bp++) 
				*rp = *bp;
		}
		BATsetcount(r, cnt);
		BBPunfix(b->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (o) BBPunfix(o->batCacheid);
		BBPkeepref(*res = r->batCacheid);
	} else {
		ptr *res = getArgReference(stk, pci, 0);
		ptr *in = getArgReference(stk, pci, 1);

		*res = *in;
	}
	return MAL_SUCCEED;
}
