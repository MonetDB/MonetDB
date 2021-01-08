/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_subquery.h"
#include "gdk_subquery.h"

str
zero_or_one_error(ptr ret, const bat *bid, const bit *err)
{
	BAT *b;
	BUN c;
	size_t _s;
	const void *p = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "sql.zero_or_one", SQLSTATE(HY005) "Cannot access column descriptor");
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
		throw(SQL, "sql.zero_or_one", SQLSTATE(21000) "Cardinality violation, scalar value expected");
	}
	_s = ATOMsize(ATOMtype(b->ttype));
	if (b->ttype == TYPE_void)
		p = &oid_nil;
	if (ATOMextern(b->ttype)) {
		_s = ATOMlen(ATOMtype(b->ttype), p);
		*(ptr *) ret = GDKmalloc(_s);
		if (*(ptr *) ret == NULL) {
			BBPunfix(b->batCacheid);
			throw(SQL, "sql.zero_or_one", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		memcpy(*(ptr *) ret, p, _s);
	} else if (b->ttype == TYPE_bat) {
		bat bid = *(bat *) p;
		if ((*(BAT **) ret = BATdescriptor(bid)) == NULL){
			BBPunfix(b->batCacheid);
			throw(SQL, "sql.zero_or_one", SQLSTATE(HY005) "Cannot access column descriptor");
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
			throw(SQL, "sql.subzero_or_one", SQLSTATE(M0M29) "zero_or_one: cardinality violation, scalar expression expected");
	}
	BBPunfix(g->batCacheid);
	if (r == GDK_SUCCEED) {
		b = bid ? BATdescriptor(*bid) : NULL;
		BBPkeepref(*ret = b->batCacheid);
	}
	return MAL_SUCCEED;
}

#define SQLall_imp(TPE) \
	do {		\
		TPE val = TPE##_nil;	\
		if (c > 0) { \
			TPE *restrict bp = (TPE*)Tloc(b, 0); \
			if (c == 1 || (b->tsorted && b->trevsorted)) { \
				val = bp[0]; \
			} else { \
				for (; q < c; q++) { /* find first non nil */ \
					val = bp[q]; \
					if (!is_##TPE##_nil(val)) \
						break; \
				} \
				for (; q < c; q++) { \
					TPE pp = bp[q]; \
					if (val != pp && !is_##TPE##_nil(pp)) { /* values != and not nil */ \
						val = TPE##_nil; \
						break; \
					} \
				} \
			} \
		} \
		*(TPE *) ret = val; \
	} while (0)

str
SQLall(ptr ret, const bat *bid)
{
	BAT *b;
	BUN c, q = 0;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(SQL, "sql.all", SQLSTATE(HY005) "Cannot access column descriptor");

	c = BATcount(b);
	if (b->ttype == TYPE_void) {
		oid p = oid_nil;
		memcpy(ret, &p, sizeof(oid));
	} else {
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte:
			SQLall_imp(bte);
			break;
		case TYPE_sht:
			SQLall_imp(sht);
			break;
		case TYPE_int:
			SQLall_imp(int);
			break;
		case TYPE_lng:
			SQLall_imp(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLall_imp(hge);
			break;
#endif
		case TYPE_flt:
			SQLall_imp(flt);
			break;
		case TYPE_dbl:
			SQLall_imp(dbl);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(b->ttype);
			const void *n = ATOMnilptr(b->ttype), *p = n;
			size_t s;

			if (c > 0) {
				BATiter bi = bat_iterator(b);
				if (c == 1 || (b->tsorted && b->trevsorted)) {
					p = BUNtail(bi, 0);
				} else {
					for (; q < c; q++) { /* find first non nil */
						p = BUNtail(bi, q);
						if (ocmp(n, p) != 0)
							break;
					}
					for (; q < c; q++) {
						const void *pp = BUNtail(bi, q);
						if (ocmp(p, pp) != 0 && ocmp(n, pp) != 0) { /* values != and not nil */
							p = n;
							break;
						}
					}
				}
			}
			s = ATOMlen(ATOMtype(b->ttype), p);
			if (ATOMextern(b->ttype)) {
				*(ptr *) ret = GDKmalloc(s);
				if (*(ptr *) ret == NULL) {
					BBPunfix(b->batCacheid);
					throw(SQL, "sql.all", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				memcpy(*(ptr *)ret, p, s);
			} else
				memcpy(ret, p, s);
		}
		}
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLall_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *lp = getArgReference_bat(stk, pci, 1);
	bat *gp = getArgReference_bat(stk, pci, 2);
	bat *gpe = getArgReference_bat(stk, pci, 3);
	bat *sp = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 6 ? 5 : 4); no_nil argument is ignored
	BAT *l = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;

	(void)cntxt;
	(void)mb;
	l = BATdescriptor(*lp);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!l || !g || !e || (sp && !s)) {
		if (l)
			BBPunfix(l->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.all =", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	res = BATall_grp(l, g, e, s);

	BBPunfix(l->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.all =", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}

#define SQLnil_imp(TPE) \
	do {		\
		TPE *restrict bp = (TPE*)Tloc(b, 0);	\
		for (BUN q = 0; q < o; q++) {	\
			if (is_##TPE##_nil(bp[q])) { \
				*ret = TRUE; \
				break; \
			} \
		} \
	} while (0)

str
SQLnil(bit *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "sql.nil", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	*ret = FALSE;
	if (BATcount(b) == 0)
		*ret = bit_nil;
	if (BATcount(b) > 0) {
		BUN o = BUNlast(b);

		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte:
			SQLnil_imp(bte);
			break;
		case TYPE_sht:
			SQLnil_imp(sht);
			break;
		case TYPE_int:
			SQLnil_imp(int);
			break;
		case TYPE_lng:
			SQLnil_imp(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLnil_imp(hge);
			break;
#endif
		case TYPE_flt:
			SQLnil_imp(flt);
			break;
		case TYPE_dbl:
			SQLnil_imp(dbl);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(b->ttype);
			const void *restrict nilp = ATOMnilptr(b->ttype);
			BATiter bi = bat_iterator(b);

			for (BUN q = 0; q < o; q++) {
				const void *restrict c = BUNtail(bi, q);
				if (ocmp(nilp, c) == 0) {
					*ret = TRUE;
					break;
				}
			}
		}
		}
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLnil_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *lp = getArgReference_bat(stk, pci, 1);
	bat *gp = getArgReference_bat(stk, pci, 2);
	bat *gpe = getArgReference_bat(stk, pci, 3);
	bat *sp = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 6 ? 5 : 4); no_nil argument is ignored
	BAT *l = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;

	(void)cntxt;
	(void)mb;
	l = BATdescriptor(*lp);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!l || !g || !e || (sp && !s)) {
		if (l)
			BBPunfix(l->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.nil", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	res = BATnil_grp(l, g, e, s);

	BBPunfix(l->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.nil", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}

static inline bit
any_cmp(const bit cmp, const bit nl, const bit nr)
{
	if (nr == bit_nil) /* empty -> FALSE */
		return FALSE;
	else if (cmp == TRUE)
		return TRUE;
	else if (nl == TRUE || nr == TRUE)
		return bit_nil;
	return FALSE;
}

#define ANY_ALL_CMP_BULK(FUNC, CMP, NL, NR) \
	do { \
		for (BUN i = 0 ; i < q ; i++) { \
			res_l[i] = FUNC(CMP, NL, NR); \
			has_nil |= is_bit_nil(res_l[i]); \
		} \
	} while (0);

str
SQLany_cmp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = isaBatType(getArgType(mb, pci, 0)) ? getArgReference_bat(stk, pci, 0) : NULL;
	bat *cid = isaBatType(getArgType(mb, pci, 1)) ? getArgReference_bat(stk, pci, 1) : NULL;
	bat *nlid = isaBatType(getArgType(mb, pci, 2)) ? getArgReference_bat(stk, pci, 2) : NULL;
	bat *nrid = isaBatType(getArgType(mb, pci, 3)) ? getArgReference_bat(stk, pci, 3) : NULL;
	BAT *cmp = NULL, *nl = NULL, *nr = NULL, *res = NULL;
	str msg = MAL_SUCCEED;
	BUN q = 0;
	bit *restrict res_l = NULL, *cmp_l = NULL, *nl_l = NULL, *nr_l = NULL, cmp_at = FALSE, nl_at = FALSE, nr_at = FALSE, has_nil = 0;

	(void) cntxt;
	if (cid && (cmp = BATdescriptor(*cid)) == NULL) {
		msg = createException(SQL, "sql.any_cmp", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (nlid && (nl = BATdescriptor(*nlid)) == NULL) {
		msg = createException(SQL, "sql.any_cmp", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (nrid && (nr = BATdescriptor(*nrid)) == NULL) {
		msg = createException(SQL, "sql.any_cmp", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (cmp)
		cmp_l = (bit *) Tloc(cmp, 0);
	else
		cmp_at = *getArgReference_bit(stk, pci, 1);
	if (nl)
		nl_l = (bit *) Tloc(nl, 0);
	else
		nl_at = *getArgReference_bit(stk, pci, 2);
	if (nr)
		nr_l = (bit *) Tloc(nr, 0);
	else
		nr_at = *getArgReference_bit(stk, pci, 3);

	if (cmp || nl || nr) {
		q = cmp ? BATcount(cmp) : nl ? BATcount(nl) : BATcount(nr);
		if (!(res = COLnew(cmp ? cmp->hseqbase : nl ? nl->hseqbase : nr->hseqbase, TYPE_bit, q, TRANSIENT))) {
			msg = createException(SQL, "sql.any_cmp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		res_l = (bit *) Tloc(res, 0);
	}

	if (!cmp && !nl && !nr) {
		bit *b = getArgReference_bit(stk, pci, 0);
		*b = any_cmp(cmp_at, nl_at, nr_at);
	} else if (cmp && !nl && !nr) {
		ANY_ALL_CMP_BULK(any_cmp, cmp_l[i], nl_at, nr_at);
	} else if (!cmp && nl && !nr) {
		ANY_ALL_CMP_BULK(any_cmp, cmp_at, nl_l[i], nr_at);
	} else if (!cmp && !nl && nr) {
		ANY_ALL_CMP_BULK(any_cmp, cmp_at, nl_at, nr_l[i]);
	} else if (!cmp && nl && nr) {
		ANY_ALL_CMP_BULK(any_cmp, cmp_at, nl_l[i], nr_l[i]);
	} else if (cmp && !nl && nr) {
		ANY_ALL_CMP_BULK(any_cmp, cmp_l[i], nl_at, nr_l[i]);
	} else if (cmp && nl && !nr) {
		ANY_ALL_CMP_BULK(any_cmp, cmp_l[i], nl_l[i], nr_at);
	} else {
		ANY_ALL_CMP_BULK(any_cmp, cmp_l[i], nl_l[i], nr_l[i]);
	}

	if (res) {
		BATsetcount(res, q);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = has_nil;
		res->tnonil = !has_nil;
	}

bailout:
	if (res && !msg)
		BBPkeepref(*ret = res->batCacheid);
	else if (res)
		BBPreclaim(res);
	if (cmp)
		BBPunfix(cmp->batCacheid);
	if (nl)
		BBPunfix(nl->batCacheid);
	if (nr)
		BBPunfix(nr->batCacheid);
	return msg;
}

static inline bit
all_cmp(const bit cmp, const bit nl, const bit nr)
{
	if (nr == bit_nil) /* empty -> TRUE */
		return TRUE;
	else if (cmp == FALSE || (cmp == bit_nil && !nl && !nr))
		return FALSE;
	else if (nl == TRUE || nr == TRUE)
		return bit_nil;
	else
		return cmp;
	return TRUE;
}

str
SQLall_cmp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = isaBatType(getArgType(mb, pci, 0)) ? getArgReference_bat(stk, pci, 0) : NULL;
	bat *cid = isaBatType(getArgType(mb, pci, 1)) ? getArgReference_bat(stk, pci, 1) : NULL;
	bat *nlid = isaBatType(getArgType(mb, pci, 2)) ? getArgReference_bat(stk, pci, 2) : NULL;
	bat *nrid = isaBatType(getArgType(mb, pci, 3)) ? getArgReference_bat(stk, pci, 3) : NULL;
	BAT *cmp = NULL, *nl = NULL, *nr = NULL, *res = NULL;
	str msg = MAL_SUCCEED;
	BUN q = 0;
	bit *restrict res_l = NULL, *cmp_l = NULL, *nl_l = NULL, *nr_l = NULL, cmp_at = FALSE, nl_at = FALSE, nr_at = FALSE, has_nil = 0;

	(void) cntxt;
	if (cid && (cmp = BATdescriptor(*cid)) == NULL) {
		msg = createException(SQL, "sql.all_cmp", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (nlid && (nl = BATdescriptor(*nlid)) == NULL) {
		msg = createException(SQL, "sql.all_cmp", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (nrid && (nr = BATdescriptor(*nrid)) == NULL) {
		msg = createException(SQL, "sql.all_cmp", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (cmp)
		cmp_l = (bit *) Tloc(cmp, 0);
	else
		cmp_at = *getArgReference_bit(stk, pci, 1);
	if (nl)
		nl_l = (bit *) Tloc(nl, 0);
	else
		nl_at = *getArgReference_bit(stk, pci, 2);
	if (nr)
		nr_l = (bit *) Tloc(nr, 0);
	else
		nr_at = *getArgReference_bit(stk, pci, 3);

	if (cmp || nl || nr) {
		q = cmp ? BATcount(cmp) : nl ? BATcount(nl) : BATcount(nr);
		if (!(res = COLnew(cmp ? cmp->hseqbase : nl ? nl->hseqbase : nr->hseqbase, TYPE_bit, q, TRANSIENT))) {
			msg = createException(SQL, "sql.all_cmp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		res_l = (bit *) Tloc(res, 0);
	}

	if (!cmp && !nl && !nr) {
		bit *b = getArgReference_bit(stk, pci, 0);
		*b = all_cmp(cmp_at, nl_at, nr_at);
	} else if (cmp && !nl && !nr) {
		ANY_ALL_CMP_BULK(all_cmp, cmp_l[i], nl_at, nr_at);
	} else if (!cmp && nl && !nr) {
		ANY_ALL_CMP_BULK(all_cmp, cmp_at, nl_l[i], nr_at);
	} else if (!cmp && !nl && nr) {
		ANY_ALL_CMP_BULK(all_cmp, cmp_at, nl_at, nr_l[i]);
	} else if (!cmp && nl && nr) {
		ANY_ALL_CMP_BULK(all_cmp, cmp_at, nl_l[i], nr_l[i]);
	} else if (cmp && !nl && nr) {
		ANY_ALL_CMP_BULK(all_cmp, cmp_l[i], nl_at, nr_l[i]);
	} else if (cmp && nl && !nr) {
		ANY_ALL_CMP_BULK(all_cmp, cmp_l[i], nl_l[i], nr_at);
	} else {
		ANY_ALL_CMP_BULK(all_cmp, cmp_l[i], nl_l[i], nr_l[i]);
	}

	if (res) {
		BATsetcount(res, q);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = has_nil;
		res->tnonil = !has_nil;
	}

bailout:
	if (res && !msg)
		BBPkeepref(*ret = res->batCacheid);
	else if (res)
		BBPreclaim(res);
	if (cmp)
		BBPunfix(cmp->batCacheid);
	if (nl)
		BBPunfix(nl->batCacheid);
	if (nr)
		BBPunfix(nr->batCacheid);
	return msg;
}

#define SQLanyequal_or_not_imp_single(TPE, OUTPUT) \
	do {							\
		TPE *rp = (TPE*)Tloc(r, 0), *lp = (TPE*)Tloc(l, 0), p = lp[0];	\
		for (BUN q = 0; q < o; q++) {	\
			TPE c = rp[q]; \
			if (is_##TPE##_nil(c)) { \
				*ret = bit_nil; \
			} else if (p == c) { \
				*ret = OUTPUT; \
				break; \
			} \
		} \
	} while (0)

#define SQLanyequal_or_not_imp_multi(TPE, CMP) \
	do {							\
		TPE *rp = (TPE*)Tloc(r, 0), *lp = (TPE*)Tloc(l, 0);	\
		for (BUN q = 0; q < o; q++) {	\
			TPE c = rp[q], d = lp[q]; \
			res_l[q] = (is_##TPE##_nil(c) || is_##TPE##_nil(d)) ? bit_nil : c CMP d; \
		} \
	} while (0)

str
SQLanyequal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *bret = isaBatType(getArgType(mb, pci, 0)) ? getArgReference_bat(stk, pci, 0) : NULL;
	bat *bid1 = getArgReference_bat(stk, pci, 1);
	bat *bid2 = getArgReference_bat(stk, pci, 2);
	BAT *res = NULL, *l = NULL, *r = NULL;
	str msg = MAL_SUCCEED;
	BUN o = 0;

	(void) cntxt;
	if ((l = BATdescriptor(*bid1)) == NULL) {
		msg = createException(SQL, "sql.any =", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		msg = createException(SQL, "sql.any =", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (l->ttype != r->ttype) {
		msg = createException(SQL, "sql.any =", SQLSTATE(42000) "sql.any = requires both arguments of the same type");
		goto bailout;
	}

	o = BATcount(r);
	if (bret) {
		if (!(res = COLnew(r->hseqbase, TYPE_bit, o, TRANSIENT))) {
			msg = createException(SQL, "sql.any =", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		bit *restrict res_l = (bit*) Tloc(res, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLanyequal_or_not_imp_multi(bte, ==);
			break;
		case TYPE_sht:
			SQLanyequal_or_not_imp_multi(sht, ==);
			break;
		case TYPE_int:
			SQLanyequal_or_not_imp_multi(int, ==);
			break;
		case TYPE_lng:
			SQLanyequal_or_not_imp_multi(lng, ==);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLanyequal_or_not_imp_multi(hge, ==);
			break;
#endif
		case TYPE_flt:
			SQLanyequal_or_not_imp_multi(flt, ==);
			break;
		case TYPE_dbl:
			SQLanyequal_or_not_imp_multi(dbl, ==);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l), ri = bat_iterator(r);

			for (BUN q = 0; q < o; q++) {
				const void *c = BUNtail(ri, q), *d = BUNtail(li, q);
				res_l[q] = ocmp(nilp, c) == 0 || ocmp(nilp, d) == 0 ? bit_nil : ocmp(c, d) == 0;
			}
		}
		}

		BATsetcount(res, o);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = l->tnil || r->tnil;
		res->tnonil = l->tnonil && r->tnonil;
	} else {
		bit *ret = getArgReference_bit(stk, pci, 0);

		*ret = FALSE;
		if (o > 0) {
			switch (ATOMbasetype(l->ttype)) {
			case TYPE_bte:
				SQLanyequal_or_not_imp_single(bte, TRUE);
				break;
			case TYPE_sht:
				SQLanyequal_or_not_imp_single(sht, TRUE);
				break;
			case TYPE_int:
				SQLanyequal_or_not_imp_single(int, TRUE);
				break;
			case TYPE_lng:
				SQLanyequal_or_not_imp_single(lng, TRUE);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				SQLanyequal_or_not_imp_single(hge, TRUE);
				break;
#endif
			case TYPE_flt:
				SQLanyequal_or_not_imp_single(flt, TRUE);
				break;
			case TYPE_dbl:
				SQLanyequal_or_not_imp_single(dbl, TRUE);
				break;
			default: {
				int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
				const void *nilp = ATOMnilptr(l->ttype);
				BATiter li = bat_iterator(l), ri = bat_iterator(r);
				const void *p = BUNtail(li, 0);

				for (BUN q = 0; q < o; q++) {
					const void *c = BUNtail(ri, q);
					if (ocmp(nilp, c) == 0)
						*ret = bit_nil;
					else if (ocmp(p, c) == 0) {
						*ret = TRUE;
						break;
					}
				}
			}
			}
		}
	}

bailout:
	if (res && !msg)
		BBPkeepref(*bret = res->batCacheid);
	else if (res)
		BBPreclaim(res);
	if (l)
		BBPunfix(l->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	return msg;
}

str
SQLanyequal_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *lp = getArgReference_bat(stk, pci, 1);
	bat *rp = getArgReference_bat(stk, pci, 2);
	bat *gp = getArgReference_bat(stk, pci, 3);
	bat *gpe = getArgReference_bat(stk, pci, 4);
	bat *sp = pci->argc == 7 ? getArgReference_bat(stk, pci, 5) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 7 ? 6 : 5); no_nil argument is ignored
	BAT *l = NULL, *r = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;

	(void)cntxt;
	(void)mb;
	l = BATdescriptor(*lp);
	r = BATdescriptor(*rp);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!l || !r || !g || !e || (sp && !s)) {
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.any =", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (l->ttype != r->ttype) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(g->batCacheid);
		BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.any =", SQLSTATE(42000) "sql.any = requires both arguments of the same type");
	}

	res = BATanyequal_grp(l, r, g, e, s);

	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.any =", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLanyequal_grp2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *lp = getArgReference_bat(stk, pci, 1);
	bat *rp = getArgReference_bat(stk, pci, 2);
	bat *ip = getArgReference_bat(stk, pci, 3);
	bat *gp = getArgReference_bat(stk, pci, 4);
	bat *gpe = getArgReference_bat(stk, pci, 5);
	bat *sp = pci->argc == 8 ? getArgReference_bat(stk, pci, 6) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 8 ? 7 : 6); no_nil argument is ignored
	BAT *l = NULL, *r = NULL, *rid = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;
	(void)cntxt;
	(void)mb;

	l = BATdescriptor(*lp);
	r = BATdescriptor(*rp);
	rid = BATdescriptor(*ip);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!l || !r || !rid || !g || !e || (sp && !s)) {
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (rid)
			BBPunfix(rid->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.any =", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (l->ttype != r->ttype) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(rid->batCacheid);
		BBPunfix(g->batCacheid);
		BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.any =", SQLSTATE(42000) "sql.any = requires both arguments of the same type");
	}

	res = BATanyequal_grp2(l, r, rid, g, e, s);

	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(rid->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.any =", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLallnotequal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *bret = isaBatType(getArgType(mb, pci, 0)) ? getArgReference_bat(stk, pci, 0) : NULL;
	bat *bid1 = getArgReference_bat(stk, pci, 1);
	bat *bid2 = getArgReference_bat(stk, pci, 2);
	BAT *res = NULL, *l = NULL, *r = NULL;
	str msg = MAL_SUCCEED;
	BUN o = 0;

	(void) cntxt;
	if ((l = BATdescriptor(*bid1)) == NULL) {
		msg = createException(SQL, "sql.all <>", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if ((r = BATdescriptor(*bid2)) == NULL) {
		msg = createException(SQL, "sql.all <>", SQLSTATE(HY005) "Cannot access column descriptor");
		goto bailout;
	}
	if (l->ttype != r->ttype) {
		msg = createException(SQL, "sql.all <>", SQLSTATE(42000) "sql.all <> requires both arguments of the same type");
		goto bailout;
	}

	o = BATcount(r);
	if (bret) {
		if (!(res = COLnew(r->hseqbase, TYPE_bit, o, TRANSIENT))) {
			msg = createException(SQL, "sql.all <>", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		bit *restrict res_l = (bit*) Tloc(res, 0);

		switch (ATOMbasetype(l->ttype)) {
		case TYPE_bte:
			SQLanyequal_or_not_imp_multi(bte, !=);
			break;
		case TYPE_sht:
			SQLanyequal_or_not_imp_multi(sht, !=);
			break;
		case TYPE_int:
			SQLanyequal_or_not_imp_multi(int, !=);
			break;
		case TYPE_lng:
			SQLanyequal_or_not_imp_multi(lng, !=);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SQLanyequal_or_not_imp_multi(hge, !=);
			break;
#endif
		case TYPE_flt:
			SQLanyequal_or_not_imp_multi(flt, !=);
			break;
		case TYPE_dbl:
			SQLanyequal_or_not_imp_multi(dbl, !=);
			break;
		default: {
			int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
			const void *nilp = ATOMnilptr(l->ttype);
			BATiter li = bat_iterator(l), ri = bat_iterator(r);

			for (BUN q = 0; q < o; q++) {
				const void *c = BUNtail(ri, q), *d = BUNtail(li, q);
				res_l[q] = ocmp(nilp, c) == 0 || ocmp(nilp, d) == 0 ? bit_nil : ocmp(c, d) != 0;
			}
		}
		}

		BATsetcount(res, o);
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		res->tnil = l->tnil || r->tnil;
		res->tnonil = l->tnonil && r->tnonil;
	} else {
		bit *ret = getArgReference_bit(stk, pci, 0);

		*ret = TRUE;
		if (o > 0) {
			switch (ATOMbasetype(l->ttype)) {
			case TYPE_bte:
				SQLanyequal_or_not_imp_single(bte, FALSE);
				break;
			case TYPE_sht:
				SQLanyequal_or_not_imp_single(sht, FALSE);
				break;
			case TYPE_int:
				SQLanyequal_or_not_imp_single(int, FALSE);
				break;
			case TYPE_lng:
				SQLanyequal_or_not_imp_single(lng, FALSE);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				SQLanyequal_or_not_imp_single(hge, FALSE);
				break;
#endif
			case TYPE_flt:
				SQLanyequal_or_not_imp_single(flt, FALSE);
				break;
			case TYPE_dbl:
				SQLanyequal_or_not_imp_single(dbl, FALSE);
				break;
			default: {
				int (*ocmp) (const void *, const void *) = ATOMcompare(l->ttype);
				const void *nilp = ATOMnilptr(l->ttype);
				BATiter li = bat_iterator(l), ri = bat_iterator(r);
				const void *p = BUNtail(li, 0);

				for (BUN q = 0; q < o; q++) {
					const void *c = BUNtail(ri, q);
					if (ocmp(nilp, c) == 0)
						*ret = bit_nil;
					else if (ocmp(p, c) == 0) {
						*ret = FALSE;
						break;
					}
				}
			}
			}
		}
	}

bailout:
	if (res && !msg)
		BBPkeepref(*bret = res->batCacheid);
	else if (res)
		BBPreclaim(res);
	if (l)
		BBPunfix(l->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	return msg;
}

str
SQLallnotequal_grp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *lp = getArgReference_bat(stk, pci, 1);
	bat *rp = getArgReference_bat(stk, pci, 2);
	bat *gp = getArgReference_bat(stk, pci, 3);
	bat *gpe = getArgReference_bat(stk, pci, 4);
	bat *sp = pci->argc == 7 ? getArgReference_bat(stk, pci, 5) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 7 ? 6 : 5); no_nil argument is ignored
	BAT *l = NULL, *r = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;

	(void)cntxt;
	(void)mb;
	l = BATdescriptor(*lp);
	r = BATdescriptor(*rp);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!l || !r || !g || !e || (sp && !s)) {
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.all <>", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (l->ttype != r->ttype) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(g->batCacheid);
		BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.all <>", SQLSTATE(42000) "sql.all <> requires both arguments of the same type");
	}

	res = BATallnotequal_grp(l, r, g, e, s);

	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.all <>", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLallnotequal_grp2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *lp = getArgReference_bat(stk, pci, 1);
	bat *rp = getArgReference_bat(stk, pci, 2);
	bat *ip = getArgReference_bat(stk, pci, 3);
	bat *gp = getArgReference_bat(stk, pci, 4);
	bat *gpe = getArgReference_bat(stk, pci, 5);
	bat *sp = pci->argc == 8 ? getArgReference_bat(stk, pci, 6) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 8 ? 7 : 6); no_nil argument is ignored
	BAT *l = NULL, *r = NULL, *rid = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;

	(void)cntxt;
	(void)mb;

	l = BATdescriptor(*lp);
	r = BATdescriptor(*rp);
	rid = BATdescriptor(*ip);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!l || !r || !rid || !g || !e || (sp && !s)) {
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (rid)
			BBPunfix(rid->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.all <>", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (l->ttype != r->ttype) {
		BBPunfix(l->batCacheid);
		BBPunfix(r->batCacheid);
		BBPunfix(rid->batCacheid);
		BBPunfix(g->batCacheid);
		BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.all <>", SQLSTATE(42000) "sql.all <> requires both arguments of the same type");
	}

	res = BATallnotequal_grp2(l, r, rid, g, e, s);

	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(rid->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.all <>", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLexist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *r = NULL;
	bit count = TRUE;

	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *bid = getArgReference_bat(stk, pci, 1);
		if (!(b = BATdescriptor(*bid)))
			throw(SQL, "aggr.exist", SQLSTATE(HY005) "Cannot access column descriptor");
		count = BATcount(b) != 0;
	}
	if (isaBatType(getArgType(mb, pci, 0))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		if ((r = BATconstant(b ? b->hseqbase : 0, TYPE_bit, &count, b ? BATcount(b) : 1, TRANSIENT)) == NULL) {
			if (b)
				BBPunfix(b->batCacheid);
			throw(SQL, "aggr.exist", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		BBPkeepref(*res = r->batCacheid);
	} else {
		bit *res = getArgReference_bit(stk, pci, 0);
		*res = count;
	}

	if (b)
		BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLsubexist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *bp = getArgReference_bat(stk, pci, 1);
	bat *gp = getArgReference_bat(stk, pci, 2);
	bat *gpe = getArgReference_bat(stk, pci, 3);
	bat *sp = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 6 ? 5 : 4); no_nil argument is ignored
	BAT *b = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;

	(void)cntxt;
	(void)mb;
	b = BATdescriptor(*bp);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!b || !g || !e || (sp && !s)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.subexist", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	res = BATsubexist(b, g, e, s);

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.subexist", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}

str
SQLnot_exist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *r = NULL;
	bit count = FALSE;

	(void)cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *bid = getArgReference_bat(stk, pci, 1);
		if (!(b = BATdescriptor(*bid)))
			throw(SQL, "aggr.not_exist", SQLSTATE(HY005) "Cannot access column descriptor");
		count = BATcount(b) == 0;
	}
	if (isaBatType(getArgType(mb, pci, 0))) {
		bat *res = getArgReference_bat(stk, pci, 0);
		if ((r = BATconstant(b ? b->hseqbase : 0, TYPE_bit, &count, b ? BATcount(b) : 1, TRANSIENT)) == NULL) {
			if (b)
				BBPunfix(b->batCacheid);
			throw(SQL, "aggr.not_exist", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		BBPkeepref(*res = r->batCacheid);
	} else {
		bit *res = getArgReference_bit(stk, pci, 0);
		*res = count;
	}

	if (b)
		BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
SQLsubnot_exist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *bp = getArgReference_bat(stk, pci, 1);
	bat *gp = getArgReference_bat(stk, pci, 2);
	bat *gpe = getArgReference_bat(stk, pci, 3);
	bat *sp = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL;
	//bit *no_nil = getArgReference_bit(stk, pci, pci->argc == 6 ? 5 : 4); no_nil argument is ignored
	BAT *b = NULL, *g = NULL, *e = NULL, *s = NULL, *res = NULL;

	(void)cntxt;
	(void)mb;
	b = BATdescriptor(*bp);
	g = BATdescriptor(*gp);
	e = BATdescriptor(*gpe);
	if (sp)
		s = BATdescriptor(*sp);

	if (!b || !g || !e || (sp && !s)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "sql.subnot_exist", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	res = BATsubnot_exist(b, g, e, s);

	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res == NULL)
		throw(MAL, "sql.subnot_exist", GDK_EXCEPTION);
	*ret = res->batCacheid;
	BBPkeepref(res->batCacheid);
	return MAL_SUCCEED;
}
