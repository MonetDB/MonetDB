/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "sql_cast.h"
#include "sql_result.h"
#include "mal_instruction.h"

str
str_2_blob(blob **res, const str *val)
{
	ptr p = NULL;
	size_t len = 0;
	ssize_t e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_blob, &p, &len, *val, false);
	if (e < 0 || !p || (ATOMcmp(TYPE_blob, p, ATOMnilptr(TYPE_blob)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "Conversion of string '%s' failed", *val? *val:"");
		throw(SQL, "blob", SQLSTATE(42000) "%s", buf);
	}
	*res = (blob *) p;
	return MAL_SUCCEED;
}

str
SQLblob_2_str(str *res, const blob *val)
{
	char *p = NULL;
	size_t len = 0;
	if (BLOBtostr(&p, &len, val, false) < 0) {
		GDKfree(p);
		throw(SQL, "blob", GDK_EXCEPTION);
	}
	*res = p;
	return MAL_SUCCEED;
}

str
batstr_2_blob_cand(bat *res, const bat *bid, const bat *sid)
{
	BAT *b, *s = NULL, *dst;
	BATiter bi;
	char *msg = NULL;
	struct canditer ci;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_blob", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc.str_2_blob", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	canditer_init(&ci, b, s);
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_blob, ci.ncand, TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(SQL, "sql.2_blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	for (BUN i = 0; i < ci.ncand; i++) {
		BUN p = (BUN) (canditer_next(&ci) - b->hseqbase);
		str v = (str) BUNtvar(bi, p);
		blob *r;
		msg = str_2_blob(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			return msg;
		}
		if (BUNappend(dst, r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(r);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	return msg;
}

str
batstr_2_blob(bat *res, const bat *bid)
{
	return batstr_2_blob_cand(res, bid, NULL);
}

static str
SQLstr_cast_(str *res, mvc *m, sql_class eclass, int d, int s, int has_tz, ptr p, int tpe, int len)
{
	char *r = NULL;
	int sz = MAX(2, len + 1);	/* nil should fit */

	if (tpe != TYPE_str) {
		/* TODO get max size for all from type */
		if (len == 0 && tpe == TYPE_bit) /* should hold false */
			sz = 6;
		r = GDKmalloc(sz);
		if (r == NULL)
			throw(SQL, "str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		sz = convert2str(m, eclass, d, s, has_tz, p, tpe, &r, sz);
	} else {
		str v = (str) p;
		STRLength(&sz, &v);
		if (len == 0 || (sz >= 0 && sz <= len)) {
			r = GDKstrdup(v);
			if (r == NULL)
				throw(SQL, "str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	if ((len > 0 && sz > len) || sz < 0) {
		if (r)
			GDKfree(r);
		if (ATOMcmp(tpe, ATOMnilptr(tpe), p) != 0) {
			throw(SQL, "str_cast", SQLSTATE(22001) "value too long for type (var)char(%d)", len);
		} else {
			r = GDKstrdup(str_nil);
			if (r == NULL)
				throw(SQL, "str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	*res = r;
	return MAL_SUCCEED;
}

str
SQLstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *res = getArgReference_str(stk, pci, 0);
	sql_class eclass = (sql_class)*getArgReference_int(stk, pci, 1);
	int d = *getArgReference_int(stk, pci, 2);
	int s = *getArgReference_int(stk, pci, 3);
	int has_tz = *getArgReference_int(stk, pci, 4);
	ptr p = getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5);
	int len = *getArgReference_int(stk, pci, 6);
	mvc *m = NULL;
	str msg;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (ATOMextern(tpe))
		p = *(ptr *) p;
	return SQLstr_cast_(res, m, eclass, d, s, has_tz, p, tpe, len);
}

/* str SQLbatstr_cast(int *res, int *eclass, int *d1, int *s1, int *has_tz, int *bid, int *digits); */
str
SQLbatstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b, *s = NULL, *dst;
	BATiter bi;
	mvc *m = NULL;
	str msg;
	char *r = NULL;
	bat *res = getArgReference_bat(stk, pci, 0);
	sql_class eclass = (sql_class) *getArgReference_int(stk, pci, 1);
	int *d1 = getArgReference_int(stk, pci, 2);
	int *s1 = getArgReference_int(stk, pci, 3);
	int *has_tz = getArgReference_int(stk, pci, 4);
	bat *bid = getArgReference_bat(stk, pci, 5);
	bat *sid = pci->argc == 7 ? NULL : getArgReference_bat(stk, pci, 6);
	int *digits = pci->argc == 7 ? getArgReference_int(stk, pci, 6) : getArgReference_int(stk, pci, 7);
	struct canditer ci;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc.str", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_str, ci.ncand, TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(b->batCacheid);
		throw(SQL, "sql.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	canditer_init(&ci, b, s);
	for (BUN i = 0; i < ci.ncand; i++) {
		BUN p = (BUN) (canditer_next(&ci) - b->hseqbase);
		ptr v = BUNtail(bi, p);
		msg = SQLstr_cast_(&r, m, eclass, *d1, *s1, *has_tz, v, b->ttype, *digits);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (r != str_nil)
			GDKfree(r);
		r = NULL;
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(b->batCacheid);
	return msg;
}

#define flt_is_numeric 0
#define dbl_is_numeric 0
#define bte_is_numeric 1
#define sht_is_numeric 1
#define int_is_numeric 1
#define lng_is_numeric 1
#define hge_is_numeric 1

/* up casting */

#define TP1 bte
#define TP2 bte
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 bte
#define TP2 sht
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 sht
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 bte
#define TP2 int
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 int
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 int
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 bte
#define TP2 lng
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 lng
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 lng
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 lng
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 bte
#define TP2 hge
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 hge
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 hge
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 hge
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 hge
#define TP2 hge
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

/* sql_cast_impl_down_from_flt */

#define round_float(x)	roundf(x)

#define TP1 flt
#define TP2 bte
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 flt
#define TP2 sht
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 flt
#define TP2 int
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 flt
#define TP2 lng
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 flt
#define TP2 hge
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

#undef round_float
#define round_float(x)	round(x)

#define TP1 dbl
#define TP2 bte
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 dbl
#define TP2 sht
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 dbl
#define TP2 int
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 dbl
#define TP2 lng
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 dbl
#define TP2 hge
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

/* sql_cast_impl_up_to_flt */

#define TP1 bte
#define TP2 flt
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 flt
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 flt
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 flt
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 flt
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

#define TP1 bte
#define TP2 dbl
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 dbl
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 dbl
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 dbl
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 dbl
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

#define TP1 sht
#define TP2 bte
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 bte
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 bte
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 bte
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

#define TP1 int
#define TP2 sht
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 sht
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 sht
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

#define TP1 lng
#define TP2 int
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 int
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 lng
#include "sql_cast_impl_int.h"
#undef TP2
#undef TP1
#endif
