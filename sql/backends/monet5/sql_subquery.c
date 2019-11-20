/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_subquery.h"

str
zero_or_one_error(ptr ret, const bat *bid, const bit *err)
{
	BAT *b;
	BUN c;
	size_t _s;
	const void *p;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "zero_or_one", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	c = BATcount(b);
	if (c == 0) {
		p = ATOMnilptr(b->ttype);
	} else if (c == 1 || (c > 1 && *err == false)) {
		BATiter bi = bat_iterator(b);
		p = BUNtail(bi, 0);
	} else {
		p = NULL;
		BBPunfix(b->batCacheid);
		throw(SQL, "zero_or_one", SQLSTATE(21000) "Cardinality violation, scalar value expected");
	}
	_s = ATOMsize(ATOMtype(b->ttype));
	if (ATOMextern(b->ttype)) {
		_s = ATOMlen(ATOMtype(b->ttype), p);
		*(ptr *) ret = GDKmalloc(_s);
		if(*(ptr *) ret == NULL){
			BBPunfix(b->batCacheid);
			throw(SQL, "zero_or_one", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		memcpy(*(ptr *) ret, p, _s);
	} else if (b->ttype == TYPE_bat) {
		bat bid = *(bat *) p;
		if((*(BAT **) ret = BATdescriptor(bid)) == NULL){
			BBPunfix(b->batCacheid);
			throw(SQL, "zero_or_one", SQLSTATE(HY005) "Cannot access column descriptor");
		}
	} else if (_s == 4) {
		*(int *) ret = *(int *) p;
	} else if (_s == 1) {
		*(bte *) ret = *(bte *) p;
	} else if (_s == 2) {
		*(sht *) ret = *(sht *) p;
	} else if (_s == 8) {
		*(lng *) ret = *(lng *) p;
#ifdef HAVE_HGE
	} else if (_s == 16) {
		*(hge *) ret = *(hge *) p;
#endif
	} else {
		memcpy(ret, p, _s);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
zero_or_one_error_bat(ptr ret, const bat *bid, const bat *err)
{
	bit t = FALSE;
	(void)err;
	return zero_or_one_error(ret, bid, &t);
}

str
zero_or_one(ptr ret, const bat *bid)
{
	bit t = TRUE;
	return zero_or_one_error(ret, bid, &t);
}

str
SQLsubzero_or_one(bat *ret, const bat *bid, const bat *gid, const bat *eid, bit *no_nil)
{
	gdk_return r;
	BAT *ng = NULL, *h = NULL, *g, *b;

	(void)no_nil;
	(void)eid;

	g = gid ? BATdescriptor(*gid) : NULL;
	if (g == NULL) {
		if (g)
			BBPunfix(g->batCacheid);
		throw(MAL, "sql.subzero_or_one", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if ((r = BATgroup(&ng, NULL, &h, g, NULL, NULL, NULL, NULL)) == GDK_SUCCEED) {
		lng max = 0;

		if (ng)
			BBPunfix(ng->batCacheid);
		BATmax(h, &max);
		BBPunfix(h->batCacheid);
		if (max != lng_nil && max > 1)
			throw(SQL, "assert", SQLSTATE(M0M29) "zero_or_one: cardinality violation, scalar expression expected");

	}
	BBPunfix(g->batCacheid);
	if (r == GDK_SUCCEED) {
		b = bid ? BATdescriptor(*bid) : NULL;
		BBPkeepref(*ret = b->batCacheid);
	}
	return MAL_SUCCEED;
}

str
SQLall(ptr ret, const bat *bid)
{
	BAT *b;
	BUN c, _s;
	const void *p;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "all", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	c = BATcount(b);
	if (c == 0) {
		p = ATOMnilptr(b->ttype);
	} else if (c == 1 || (b->tsorted && b->trevsorted)) {
		BATiter bi = bat_iterator(b);
		p = BUNtail(bi, 0);
	} else if (b->ttype == TYPE_void && is_oid_nil(b->tseqbase)) {
		p = ATOMnilptr(b->ttype);
	} else {
		BUN q, r;
		int (*ocmp) (const void *, const void *);
		const void *n = ATOMnilptr(b->ttype);
		BATiter bi = bat_iterator(b);
		r = BUNlast(b);
		p = BUNtail(bi, 0);
		ocmp = ATOMcompare(b->ttype);
		for (q = 1; q < r; q++) {
			const void *c = BUNtail(bi, q);
			if (ocmp(p, c) != 0) {
				if (ocmp(n, c) != 0) 
					p = ATOMnilptr(b->ttype);
				break;
			}
		}
	}
	_s = ATOMsize(ATOMtype(b->ttype));
	if (ATOMextern(b->ttype)) {
		_s = ATOMlen(ATOMtype(b->ttype), p);
		*(ptr *) ret = GDKmalloc(_s);
		if(*(ptr *) ret == NULL){
			BBPunfix(b->batCacheid);
			throw(SQL, "SQLall", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		memcpy(*(ptr *) ret, p, _s);
	} else if (b->ttype == TYPE_bat) {
		bat bid = *(bat *) p;
		if ((*(BAT **) ret = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(SQL, "all", SQLSTATE(HY005) "Cannot access column descriptor");
		}
	} else if (_s == 4) {
		*(int *) ret = *(int *) p;
	} else if (_s == 1) {
		*(bte *) ret = *(bte *) p;
	} else if (_s == 2) {
		*(sht *) ret = *(sht *) p;
	} else if (_s == 8) {
		*(lng *) ret = *(lng *) p;
#ifdef HAVE_HGE
	} else if (_s == 16) {
		*(hge *) ret = *(hge *) p;
#endif
	} else {
		memcpy(ret, p, _s);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLall_grp(bat *ret, const bat *bid, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *l, *g, *e, *res;
	BATiter li;
	ssize_t p, *pos = NULL;
	int error = 0, has_nil = 0;
	int (*ocmp) (const void *, const void *);

	(void)no_nil;
	if ((l = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "all =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "all =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "all =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	li = bat_iterator(l);
	ocmp = ATOMcompare(l->ttype);
	if (BATcount(g) > 0) {
		BUN q, o, s, offset = 0;
		BATiter gi = bat_iterator(g);

		pos = GDKmalloc(sizeof(BUN)*BATcount(e)); 
		for (s = 0; s < BATcount(e); s++) 
			pos[s] = -1;

		offset = g->hseqbase - l->hseqbase;
		o = BUNlast(g);
		for (q = offset, s = 0; s < o; q++, s++) {
			oid id = *(oid*)BUNtail(gi, s);
			if (pos[id] == -2)
				continue;
			else if (pos[id] == -1)
				pos[id] = q;
			else {
				const void *lv = BUNtail(li, q);
				const void *rv = BUNtail(li, pos[id]);

				if (ocmp(lv, rv) != 0)
					pos[id] = -2;
			}
		}
	}
	res = COLnew(e->hseqbase, l->ttype, BATcount(e), TRANSIENT);
	const void *nilp = ATOMnilptr(l->ttype);
	for (p = 0; p < (ssize_t)BATcount(e) && !error; p++) {
		const void *v = nilp;
		if (pos[p] >= 0) {
			v = BUNtail(li, pos[p]);
			if (ocmp(v, nilp) == 0)
				has_nil = 1;
		} else
			has_nil = 1;
		if (BUNappend(res, v, false) != GDK_SUCCEED)
			error = 1;
	}
	GDKfree(pos);
	if (error)
		throw(SQL, "all =", SQLSTATE(HY005) "all append failed");

	res->hseqbase = g->hseqbase;
	res->tnil = (has_nil)?1:0;
	res->tnonil = (has_nil)?0:1;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(l->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLnil(bit *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "all", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	*ret = FALSE;
	if (BATcount(b) == 0)
		*ret = bit_nil;
	if (BATcount(b) > 0) {
		BUN q, o;
		int (*ocmp) (const void *, const void *);
		BATiter bi = bat_iterator(b);
		const void *nilp = ATOMnilptr(b->ttype);

		o = BUNlast(b);
		ocmp = ATOMcompare(b->ttype);
		for (q = 0; q < o; q++) {
			const void *c = BUNtail(bi, q);
			if (ocmp(nilp, c) == 0) {
				*ret = TRUE;
				break;
			}
		}
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLnil_grp(bat *ret, const bat *bid, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *l, *g, *e, *res;
	bit F = FALSE;
	BUN offset = 0;
	bit has_nil = 0;

	(void)no_nil;
	if ((l = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	res = BATconstant(0, TYPE_bit, &F, BATcount(e), TRANSIENT);
	BAThseqbase(res, e->hseqbase);
	offset = g->hseqbase - l->hseqbase;
	if (BATcount(g) > 0) {
		BUN q, o, s;
		int (*ocmp) (const void *, const void *);
		BATiter li = bat_iterator(l);
		BATiter gi = bat_iterator(g);
		BATiter rt = bat_iterator(res);

		bit *ret = BUNtail(rt, 0);
		const void *nilp = ATOMnilptr(l->ttype);

		o = BUNlast(g);
		ocmp = ATOMcompare(l->ttype);
		for (q = offset, s = 0; s < o; q++, s++) {
			const void *lv = BUNtail(li, q);
			oid id = *(oid*)BUNtail(gi, s);

			if (ret[id] != TRUE) {
				if (ocmp(lv, nilp) == 0) {
					ret[id] = bit_nil;
					has_nil = 1;
				}
			}
		}
	}
	res->hseqbase = g->hseqbase;
	res->tnil = has_nil != 0;
	res->tnonil = has_nil == 0;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(l->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}

str 
SQLany_cmp(bit *ret, const bit *cmp, const bit *nl, const bit *nr)
{
	*ret = FALSE;
	if (*nr == bit_nil) /* empty -> FALSE */
		*ret = FALSE;
	else if (*cmp == TRUE)
		*ret = TRUE;
	else if (*nl == TRUE || *nr == TRUE)
		*ret = bit_nil;
	return MAL_SUCCEED;
}

str 
SQLall_cmp(bit *ret, const bit *cmp, const bit *nl, const bit *nr)
{
	*ret = TRUE;
	if (*nr == bit_nil) /* empty -> TRUE */
		*ret = TRUE;
	else if (*cmp == FALSE)
		*ret = FALSE;
	else if (*nl == TRUE || *nr == TRUE)
		*ret = bit_nil;
	return MAL_SUCCEED;
}

str
SQLanyequal(bit *ret, const bat *bid1, const bat *bid2)
{
	BAT *l, *r;
	const void *p;

	if ((l = BATdescriptor(*bid1)) == NULL) {
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	*ret = FALSE;
	if (BATcount(r) > 0) {
		BUN q, o;
		int (*ocmp) (const void *, const void *);
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		const void *nilp = ATOMnilptr(l->ttype);

		o = BUNlast(r);
		p = BUNtail(li, 0);
		ocmp = ATOMcompare(l->ttype);
		for (q = 0; q < o; q++) {
			const void *c = BUNtail(ri, q);
			if (ocmp(nilp, c) == 0)
				*ret = bit_nil;
			else if (ocmp(p, c) == 0) {
				*ret = TRUE;
				break;
			}
		}
	}
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	return MAL_SUCCEED;
}

str
SQLanyequal_grp(bat *ret, const bat *bid1, const bat *bid2, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *l, *r, *g, *e, *res;
	bit F = FALSE, hasnil = 0;
	BUN offset = 0;

	(void)no_nil;
	if ((l = BATdescriptor(*bid1)) == NULL) {
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	res = BATconstant(0, TYPE_bit, &F, BATcount(e), TRANSIENT);
	BAThseqbase(res, e->hseqbase);
	assert(BATcount(l) == BATcount(r));
	offset = g->hseqbase - l->hseqbase;
	if (BATcount(g) > 0) {
		BUN q, o, s;
		int (*ocmp) (const void *, const void *);
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BATiter gi = bat_iterator(g);
		BATiter rt = bat_iterator(res);

		bit *ret = BUNtail(rt, 0);
		const void *nilp = ATOMnilptr(l->ttype);

		o = BUNlast(g);
		ocmp = ATOMcompare(l->ttype);
		for (q = offset, s = 0; s < o; q++, s++) {
			const void *lv = BUNtail(li, q);
			const void *rv = BUNtail(ri, q);
			oid id = *(oid*)BUNtail(gi, s);

			if (ret[id] != TRUE) {
				if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
					ret[id] = bit_nil;
					hasnil = 1;
				} else if (ocmp(lv, rv) == 0)
					ret[id] = TRUE;
			}
		}
	}
	res->hseqbase = g->hseqbase;
	res->tnil = hasnil != 0;
	res->tnonil = hasnil == 0;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLanyequal_grp2(bat *ret, const bat *bid1, const bat *bid2, const bat *Rid, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *l, *r, *rid, *g, *e, *res;
	bit F = FALSE, hasnil = 0;
	BUN offset = 0;

	(void)no_nil;
	if ((l = BATdescriptor(*bid1)) == NULL) {
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((rid = BATdescriptor(*Rid)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(rid->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(rid->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	res = BATconstant(0, TYPE_bit, &F, BATcount(e), TRANSIENT);
	BAThseqbase(res, e->hseqbase);
	assert(BATcount(l) == BATcount(r));
	offset = g->hseqbase - l->hseqbase;
	if (BATcount(g) > 0) {
		BUN q, o, s;
		int (*ocmp) (const void *, const void *);
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BATiter ii = bat_iterator(rid);
		BATiter gi = bat_iterator(g);
		BATiter rt = bat_iterator(res);

		bit *ret = BUNtail(rt, 0);
		const void *nilp = ATOMnilptr(l->ttype);

		o = BUNlast(g);
		ocmp = ATOMcompare(l->ttype);
		for (q = offset, s = 0; s < o; q++, s++) {
			const void *lv = BUNtail(li, q);
			const void *rv = BUNtail(ri, q);
			const oid rid = *(oid*)BUNtail(ii, q);
			oid id = *(oid*)BUNtail(gi, s);

			if (ret[id] != TRUE) {
				if (rid == oid_nil) { /* empty */
					ret[id] = FALSE;
				} else if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
					ret[id] = bit_nil;
					hasnil = 1;
				} else if (ocmp(lv, rv) == 0)
					ret[id] = TRUE;
			}
		}
	}
	res->hseqbase = g->hseqbase;
	res->tnil = hasnil != 0;
	res->tnonil = hasnil == 0;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLallnotequal(bit *ret, const bat *bid1, const bat *bid2)
{
	BAT *l, *r;
	const void *p;

	if ((l = BATdescriptor(*bid1)) == NULL) {
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	*ret = TRUE;
	if (BATcount(r) > 0) {
		BUN q, o;
		int (*ocmp) (const void *, const void *);
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		const void *nilp = ATOMnilptr(l->ttype);

		o = BUNlast(r);
		p = BUNtail(li, 0);
		ocmp = ATOMcompare(l->ttype);
		for (q = 0; q < o; q++) {
			const void *c = BUNtail(ri, q);
			if (ocmp(nilp, c) == 0)
				*ret = bit_nil;
			else if (ocmp(p, c) == 0) {
				*ret = FALSE;
				break;
			}
		}
	}
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	return MAL_SUCCEED;
}

str
SQLallnotequal_grp(bat *ret, const bat *bid1, const bat *bid2, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *l, *r, *g, *e, *res;
	bit T = TRUE, hasnil = 0;
	BUN offset = 0;

	(void)no_nil;
	if ((l = BATdescriptor(*bid1)) == NULL) {
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	res = BATconstant(0, TYPE_bit, &T, BATcount(e), TRANSIENT);
	BAThseqbase(res, e->hseqbase);
	assert(BATcount(l) == BATcount(r));
	offset = g->hseqbase - l->hseqbase;
	if (BATcount(g) > 0) {
		BUN q, o, s;
		int (*ocmp) (const void *, const void *);
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BATiter gi = bat_iterator(g);
		BATiter rt = bat_iterator(res);

		bit *ret = BUNtail(rt, 0);
		const void *nilp = ATOMnilptr(l->ttype);

		o = BUNlast(g);
		ocmp = ATOMcompare(l->ttype);
		for (q = offset, s = 0; s < o; q++, s++) {
			const void *lv = BUNtail(li, q);
			const void *rv = BUNtail(ri, q);
			oid id = *(oid*)BUNtail(gi, s);

			if (ret[id] != FALSE) {
				if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
					ret[id] = bit_nil;
					hasnil = 1;
				} else if (ocmp(lv, rv) == 0)
					ret[id] = FALSE;
			}
		}
	}
	res->hseqbase = g->hseqbase;
	res->tnil = hasnil != 0;
	res->tnonil = hasnil == 0;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLallnotequal_grp2(bat *ret, const bat *bid1, const bat *bid2, const bat *Rid, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *l, *r, *rid, *g, *e, *res;
	bit T = TRUE, hasnil = 0;
	BUN offset = 0;

	(void)no_nil;
	if ((l = BATdescriptor(*bid1)) == NULL) {
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(l->batCacheid);
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((rid = BATdescriptor(*Rid)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		throw(SQL, "any =", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(rid->batCacheid);
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(rid->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "all <>", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	res = BATconstant(0, TYPE_bit, &T, BATcount(e), TRANSIENT);
	BAThseqbase(res, e->hseqbase);
	assert(BATcount(l) == BATcount(r));
	offset = g->hseqbase - l->hseqbase;
	if (BATcount(g) > 0) {
		BUN q, o, s;
		int (*ocmp) (const void *, const void *);
		BATiter li = bat_iterator(l);
		BATiter ri = bat_iterator(r);
		BATiter ii = bat_iterator(rid);
		BATiter gi = bat_iterator(g);
		BATiter rt = bat_iterator(res);

		bit *ret = BUNtail(rt, 0);
		const void *nilp = ATOMnilptr(l->ttype);

		o = BUNlast(g);
		ocmp = ATOMcompare(l->ttype);
		for (q = offset, s = 0; s < o; q++, s++) {
			const void *lv = BUNtail(li, q);
			const void *rv = BUNtail(ri, q);
			const oid rid = *(oid*)BUNtail(ii, q);
			oid id = *(oid*)BUNtail(gi, s);

			if (ret[id] != FALSE) {
				if (rid == oid_nil) { /* empty */
					ret[id] = TRUE;
				} else if (ocmp(lv, nilp) == 0 || ocmp(rv, nilp) == 0) {
					ret[id] = bit_nil;
					hasnil = 1;
				} else if (ocmp(lv, rv) == 0)
					ret[id] = FALSE;
			}
		}
	}
	res->hseqbase = g->hseqbase;
	res->tnil = hasnil != 0;
	res->tnonil = hasnil == 0;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLexist_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = getArgReference_bit(stk, pci, 0);
	ptr v = getArgReference(stk, pci, 1);
	int mtype = getArgType(mb, pci, 1);

	(void)cntxt;
	if ((mtype == TYPE_bat || mtype > GDKatomcnt)) {
	       	BAT *b = BATdescriptor(*(bat *)v);

		if (b)
			*res = BATcount(b) != 0;
		else
			throw(SQL, "aggr.exist", SQLSTATE(HY005) "Cannot access column descriptor");
	} else if (ATOMcmp(mtype, v, ATOMnilptr(mtype)) != 0)
		*res = TRUE;
	else
		*res = FALSE;
	return MAL_SUCCEED;
}

str
SQLexist(bit *res, bat *id)
{
	BAT *b;

	if ((b = BATdescriptor(*id)) == NULL)
		throw(SQL, "aggr.exist", SQLSTATE(HY005) "Cannot access column descriptor");
	*res = BATcount(b) != 0;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLsubexist(bat *ret, const bat *bp, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *b, *g, *e, *res;
	bit T = TRUE;
	BUN offset = 0;

	(void)no_nil;
	if ((b = BATdescriptor(*bp)) == NULL) {
		throw(SQL, "exists", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "exists", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "exists", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	res = BATconstant(0, TYPE_bit, &T, BATcount(e), TRANSIENT);
	BAThseqbase(res, e->hseqbase);
	offset = g->hseqbase - b->hseqbase;
	if (BATcount(g) > 0) {
		BUN q, o, s;
		int (*ocmp) (const void *, const void *);
		BATiter bi = bat_iterator(b);
		BATiter gi = bat_iterator(g);
		BATiter rt = bat_iterator(res);

		bit *ret = BUNtail(rt, 0);
		const void *nilp = ATOMnilptr(b->ttype);

		o = BUNlast(g);
		ocmp = ATOMcompare(b->ttype);
		for (q = offset, s = 0; s < o; q++, s++) {
			const void *bv = BUNtail(bi, q);
			oid id = *(oid*)BUNtail(gi, s);

			if (ret[id] == TRUE) {
				if (ocmp(bv, nilp) == 0) {
					ret[id] = FALSE;
				}
			}
		}
	}
	res->hseqbase = g->hseqbase;
	res->tnil = 0;
	res->tnonil = 1;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLnot_exist_val(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = getArgReference_bit(stk, pci, 0);
	ptr v = getArgReference(stk, pci, 1);
	int mtype = getArgType(mb, pci, 1);

	(void)cntxt;
	if ((mtype == TYPE_bat || mtype > GDKatomcnt)) {
	       	BAT *b = BATdescriptor(*(bat *)v);

		if (b)
			*res = BATcount(b) == 0;
		else
			throw(SQL, "aggr.not_exist", SQLSTATE(HY005) "Cannot access column descriptor");
	} else
	//if (ATOMcmp(mtype, v, ATOMnilptr(mtype)) != 0)
		*res = FALSE;
		/*
	else
		*res = FALSE;
		*/
	return MAL_SUCCEED;
}

str
SQLnot_exist(bit *res, bat *id)
{
	BAT *b;

	if ((b = BATdescriptor(*id)) == NULL)
		throw(SQL, "aggr.not_exist", SQLSTATE(HY005) "Cannot access column descriptor");
	*res = BATcount(b) == 0;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLsubnot_exist(bat *ret, const bat *bp, const bat *gp, const bat *gpe, bit *no_nil)
{
	BAT *b, *g, *e, *res;
	bit F = FALSE, hasnil = 0;
	BUN offset = 0;

	(void)no_nil;
	if ((b = BATdescriptor(*bp)) == NULL) {
		throw(SQL, "not_exists", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((g = BATdescriptor(*gp)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "not_exists", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((e = BATdescriptor(*gpe)) == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(SQL, "not_exists", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	res = BATconstant(0, TYPE_bit, &F, BATcount(e), TRANSIENT);
	BAThseqbase(res, e->hseqbase);
	offset = g->hseqbase - b->hseqbase;
	if (BATcount(g) > 0) {
		BUN q, o, s;
		int (*ocmp) (const void *, const void *);
		BATiter bi = bat_iterator(b);
		BATiter gi = bat_iterator(g);
		BATiter rt = bat_iterator(res);

		bit *ret = BUNtail(rt, 0);
		const void *nilp = ATOMnilptr(b->ttype);

		o = BUNlast(g);
		ocmp = ATOMcompare(b->ttype);
		for (q = offset, s = 0; s < o; q++, s++) {
			const void *bv = BUNtail(bi, q);
			oid id = *(oid*)BUNtail(gi, s);

			if (ret[id] != TRUE) {
				if (ocmp(bv, nilp) == 0) {
					ret[id] = bit_nil;
					hasnil = 1;
				}
			}
		}
	}
	res->hseqbase = g->hseqbase;
	res->tnil = hasnil != 0;
	res->tnonil = hasnil == 0;
	res->tsorted = res->trevsorted = 0;
	res->tkey = 0;
	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	BBPkeepref(*ret = res->batCacheid);
	return MAL_SUCCEED;
}
