/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "sql_cast.h"
#include "sql_result.h"
#include "mal_instruction.h"

static inline str
str_2_blob_imp(blob **r, size_t *rlen, const str val)
{
	ssize_t e = ATOMfromstr(TYPE_blob, (void**)r, rlen, val, false);
	if (e < 0 || (ATOMcmp(TYPE_blob, *r, ATOMnilptr(TYPE_blob)) == 0 && !strNil(val))) {
		if (strNil(val))
			throw(SQL, "calc.str_2_blob", SQLSTATE(42000) "Conversion of NULL string to blob failed");
		throw(SQL, "calc.str_2_blob", SQLSTATE(42000) "Conversion of string '%s' to blob failed", val);
	}
	return MAL_SUCCEED;
}

str
str_2_blob(blob **res, const str *val)
{
	size_t rlen = 0;
	str msg;

	*res = NULL;
	if ((msg = str_2_blob_imp(res, &rlen, *val))) {
		GDKfree(*res);
		*res = NULL;
	}
	return msg;
}

str
batstr_2_blob_cand(bat *res, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	char *msg = NULL;
	struct canditer ci;
	BUN q;
	oid off;
	blob *r = NULL;
	size_t rlen = 0;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.str_2_blob", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.str_2_blob", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	q = canditer_init(&ci, b, s);
	bi = bat_iterator(b);
	if (!(dst = COLnew(ci.hseq, TYPE_blob, q, TRANSIENT))) {
		msg = createException(SQL, "batcalc.str_2_blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	for (BUN i = 0; i < q; i++) {
		BUN p = (BUN) (canditer_next(&ci) - off);
		str v = (str) BUNtvar(bi, p);

		if ((msg = str_2_blob_imp(&r, &rlen, v)))
			goto bailout;
		if (BUNappend(dst, r, false) != GDK_SUCCEED) {
			msg = createException(SQL, "batcalc.str_2_blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}

bailout:
	GDKfree(r);
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst && !msg)
		BBPkeepref(*res = dst->batCacheid);
	else if (dst)
		BBPreclaim(dst);
	return msg;
}

str
batstr_2_blob(bat *res, const bat *bid)
{
	return batstr_2_blob_cand(res, bid, NULL);
}

/* TODO get max size for all from type  */
static int
str_buf_initial_capacity(sql_class eclass, int digits)
{
	switch (eclass)
	{
		case EC_BIT:
			/* should hold false for clob type and (var)char > 4 */
			return (digits == 0 || digits > 4) ? 8 : 2;
		case EC_SEC:
		case EC_MONTH:
		case EC_NUM:
		case EC_DEC:
		case EC_POS:
		case EC_TIME:
		case EC_TIME_TZ:
		case EC_DATE:
		case EC_TIMESTAMP:
		case EC_TIMESTAMP_TZ:
			return 64;
		case EC_FLT:
			return 128;
		default:
			return 128;
	}
}

static inline str
SQLstr_cast_any_type(str *r, int rlen, mvc *m, sql_class eclass, int d, int s, int has_tz, ptr p, int tpe, int len)
{
	int sz = convert2str(m, eclass, d, s, has_tz, p, tpe, r, rlen);
	if ((len > 0 && sz > len) || sz < 0)
		throw(SQL, "str_cast", SQLSTATE(22001) "value too long for type (var)char(%d)", len);
	return MAL_SUCCEED;
}

static inline str
SQLstr_cast_str(str *r, int *rlen, str v, int len)
{
	int intput_strlen;

	if (!strNil(v) && len > 0) {
		int logical_size = 0;
		STRLength(&logical_size, &v);

		if (logical_size > len)
			throw(SQL, "str_cast", SQLSTATE(22001) "value too long for type (var)char(%d)", len);
	}

	intput_strlen = (int) strlen(v) + 1;
	if (intput_strlen > *rlen) {
		str newr = GDKmalloc(intput_strlen);

		if (!newr)
			throw(SQL, "str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		GDKfree(*r);
		*r = newr;
		*rlen = intput_strlen;
	}
	strcpy(*r, v);
	return MAL_SUCCEED;
}

str
SQLstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *res = getArgReference_str(stk, pci, 0), msg;
	sql_class eclass = (sql_class)*getArgReference_int(stk, pci, 1);
	int d = *getArgReference_int(stk, pci, 2), s = *getArgReference_int(stk, pci, 3);
	int has_tz = *getArgReference_int(stk, pci, 4);
	ptr p = getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5), digits = *getArgReference_int(stk, pci, 6), rlen = 0;
	mvc *m = NULL;
	int initial_capacity = MAX(str_buf_initial_capacity(eclass, digits), (int) strlen(str_nil) + 1); /* don't reallocate on str_nil */

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (ATOMextern(tpe))
		p = *(ptr *) p;

	assert(initial_capacity > 0);
	if (!(EC_VARCHAR(eclass) || tpe == TYPE_str)) { /* for decimals and other fixed size types allocate once */
		if (!(*res = GDKmalloc(initial_capacity)))
			return createException(SQL, "calc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		rlen = initial_capacity;
	} else {
		*res = NULL;
	}

	if (EC_VARCHAR(eclass) || tpe == TYPE_str)
		msg = SQLstr_cast_str(res, &rlen, (str) p, digits);
	else
		msg = SQLstr_cast_any_type(res, rlen, m, eclass, d, s, has_tz, p, tpe, digits);

	if (msg) {
		GDKfree(*res);
		*res = NULL;
	} else if (!(EC_VARCHAR(eclass) || tpe == TYPE_str)) { /* if a too long string was allocated, return what is needed */
		str newr = GDKstrdup(*res);
		GDKfree(*res);
		if (!newr)
			return createException(SQL, "calc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*res = newr;
	}
	return msg;
}

/* str SQLbatstr_cast(int *res, int *eclass, int *d1, int *s1, int *has_tz, int *bid, int *digits); */
str
SQLbatstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	mvc *m = NULL;
	str msg, r = NULL;
	bat *res = getArgReference_bat(stk, pci, 0);
	sql_class eclass = (sql_class) *getArgReference_int(stk, pci, 1);
	int d1 = *getArgReference_int(stk, pci, 2), s1 = *getArgReference_int(stk, pci, 3);
	int has_tz = *getArgReference_int(stk, pci, 4);
	bat *bid = getArgReference_bat(stk, pci, 5), *sid = pci->argc == 7 ? NULL : getArgReference_bat(stk, pci, 6);
	int digits = pci->argc == 7 ? *getArgReference_int(stk, pci, 6) : *getArgReference_int(stk, pci, 7);
	int rlen = 0, tpe = getBatType(getArgType(mb, pci, 5));
	struct canditer ci;
	BUN q;
	int initial_capacity = MAX(str_buf_initial_capacity(eclass, digits), (int) strlen(str_nil) + 1); /* don't reallocate on str_nil */
	oid off;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.str", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.str", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	q = canditer_init(&ci, b, s);
	bi = bat_iterator(b);
	if (!(dst = COLnew(ci.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	assert(initial_capacity > 0);
	if (!(EC_VARCHAR(eclass) || tpe == TYPE_str)) { /* for decimals and other fixed size types allocate once */
		if (!(r = GDKmalloc(initial_capacity))) {
			msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		rlen = initial_capacity;
	}

	for (BUN i = 0; i < q; i++) {
		BUN p = (BUN) (canditer_next(&ci) - off);
		ptr v = BUNtail(bi, p);

		if (EC_VARCHAR(eclass) || tpe == TYPE_str)
			msg = SQLstr_cast_str(&r, &rlen, (str) v, digits);
		else
			msg = SQLstr_cast_any_type(&r, rlen, m, eclass, d1, s1, has_tz, v, tpe, digits);

		if (msg)
			goto bailout;
		if (BUNappend(dst, r, false) != GDK_SUCCEED) {
			msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}

bailout:
	GDKfree(r);
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst && !msg)
		BBPkeepref(*res = dst->batCacheid);
	else if (dst)
		BBPreclaim(dst);
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
