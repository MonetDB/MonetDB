/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
			BBPunfix(c->batCacheid);
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
		throw(SQL, "sql.rank", "rank(:any_1,:bit,:bit)");
	}
	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1)), *p, *o, *r;
		int i, j, cnt, *rp;
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
					throw(SQL, "sql.rank", "Cannot access descriptor");
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
					throw(SQL, "sql.rank", "Cannot access descriptor");
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
