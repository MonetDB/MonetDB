/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "sql_cast.h"
#include "sql_result.h"
#include "mal_instruction.h"

/* TODO get max size for all from type */
static size_t
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
		case EC_CHAR:
		case EC_STRING:
		case EC_BLOB:
		case EC_GEOM:
			return 1024;
		default:
			return 128;
	}
}

static inline str
SQLstr_cast_any_type(str *r, size_t *rlen, mvc *m, sql_class eclass, int d, int s, int has_tz, const void *p, int tpe, int len)
{
	ssize_t sz = convert2str(m, eclass, d, s, has_tz, p, tpe, r, rlen);
	if ((len > 0 && sz > (ssize_t) len) || sz < 0)
		throw(SQL, "str_cast", SQLSTATE(22001) "value too long for type (var)char(%d)", len);
	return MAL_SUCCEED;
}

str
SQLstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *res = getArgReference_str(stk, pci, 0), r = NULL, msg;
	sql_class eclass = (sql_class)*getArgReference_int(stk, pci, 1);
	int d = *getArgReference_int(stk, pci, 2), s = *getArgReference_int(stk, pci, 3);
	int has_tz = *getArgReference_int(stk, pci, 4), tpe = getArgType(mb, pci, 5), digits = *getArgReference_int(stk, pci, 6);
	const void *p = getArgReference(stk, pci, 5);
	mvc *m = NULL;
	bool from_str = EC_VARCHAR(eclass) || tpe == TYPE_str;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (ATOMextern(tpe))
		p = *(ptr *) p;

	if (from_str) {
		r = (str) p;
		if (digits > 0 && !strNil(r) && UTF8_strlen(r) > digits)
			throw(SQL, "calc.str_cast", SQLSTATE(22001) "value too long for type (var)char(%d)", digits);
	} else {
		size_t rlen = MAX(str_buf_initial_capacity(eclass, digits), strlen(str_nil) + 1); /* don't reallocate on str_nil */
		if (!(r = GDKmalloc(rlen)))
			throw(SQL, "calc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = SQLstr_cast_any_type(&r, &rlen, m, eclass, d, s, has_tz, p, tpe, digits)) != MAL_SUCCEED) {
			GDKfree(r);
			return msg;
		}
	}

	*res = GDKstrdup(r);
	if (!from_str)
		GDKfree(r);
	if (!*res)
		throw(SQL, "calc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

#define SQLstr_cast_str(v, digits) \
	if (UTF8_strlen(v) > digits) { \
		msg = createException(SQL, "batcalc.str_cast", SQLSTATE(22001) "value too long for type (var)char(%d)", digits); \
		goto bailout1; \
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
	int d1 = *getArgReference_int(stk, pci, 2), s1 = *getArgReference_int(stk, pci, 3), has_tz = *getArgReference_int(stk, pci, 4);
	bat *bid = getArgReference_bat(stk, pci, 5), *sid = pci->argc == 7 ? NULL : getArgReference_bat(stk, pci, 6);
	int tpe = getBatType(getArgType(mb, pci, 5)), digits = pci->argc == 7 ? *getArgReference_int(stk, pci, 6) : *getArgReference_int(stk, pci, 7);
	struct canditer ci;
	oid off;
	bool nils = false, from_str = EC_VARCHAR(eclass) || tpe == TYPE_str, btkey = false, btsorted = false, btrevsorted = false;
	size_t rlen = 0;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.str", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.str", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	assert(digits >= 0);
	if (from_str && digits == 0) {
		if (s) {
			if (!(dst = BATproject(s, b))) {
				msg = createException(SQL, "batcalc.str", GDK_EXCEPTION);
				goto bailout;
			}
			BBPunfix(s->batCacheid);
			BBPunfix(b->batCacheid);
			*res = dst->batCacheid;
			BBPkeepref(dst);
		} else {
			*res = b->batCacheid;
			BBPkeepref(b);
		}
		return MAL_SUCCEED;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	bi = bat_iterator(b);

	if (from_str && ci.tpe == cand_dense && ci.ncand == BATcount(b)) { /* from string case, just do validation, if right, return */
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			const char *v = BUNtvar(bi, p);

			if (!strNil(v))
				SQLstr_cast_str(v, digits);
		}
		if (s)
			BBPunfix(s->batCacheid);
		bat_iterator_end(&bi);
		*res = b->batCacheid;
		BBPkeepref(b);
		return MAL_SUCCEED;
	}

	if (!(dst = COLnew(ci.hseq, TYPE_str, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout1;
	}

	rlen = MAX(str_buf_initial_capacity(eclass, digits), strlen(str_nil) + 1); /* don't reallocate on str_nil */
	assert(rlen > 0);
	if (!from_str && !(r = GDKmalloc(rlen))) {
		msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout1;
	}

	if (ci.tpe == cand_dense) {
		if (from_str) { /* string to string */
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				const char *v = BUNtvar(bi, p);

				if (strNil(v)) {
					if (tfastins_nocheckVAR(dst, i, str_nil) != GDK_SUCCEED) {
						msg = createException(MAL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout1;
					}
					nils = true;
				} else {
					SQLstr_cast_str(v, digits);
					if (tfastins_nocheckVAR(dst, i, v) != GDK_SUCCEED) {
						msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout1;
					}
				}
			}
		} else { /* any other type to string */
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				const void *v = BUNtail(bi, p);

				if ((msg = SQLstr_cast_any_type(&r, &rlen, m, eclass, d1, s1, has_tz, v, tpe, digits)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(dst, i, r) != GDK_SUCCEED) {
					msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils |= strNil(r);
			}
		}
	} else {
		if (from_str) { /* string to string */
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next(&ci) - off);
				const char *v = BUNtvar(bi, p);

				if (strNil(v)) {
					if (tfastins_nocheckVAR(dst, i, str_nil) != GDK_SUCCEED) {
						msg = createException(MAL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout1;
					}
					nils = true;
				} else {
					SQLstr_cast_str(v, digits);
					if (tfastins_nocheckVAR(dst, i, v) != GDK_SUCCEED) {
						msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout1;
					}
				}
			}
		} else { /* any other type to string */
			for (BUN i = 0; i < ci.ncand; i++) {
				oid p = (canditer_next(&ci) - off);
				const void *v = BUNtail(bi, p);

				if ((msg = SQLstr_cast_any_type(&r, &rlen, m, eclass, d1, s1, has_tz, v, tpe, digits)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(dst, i, r) != GDK_SUCCEED) {
					msg = createException(SQL, "batcalc.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils |= strNil(r);
			}
		}
	}
	btkey = from_str ? bi.key : ci.ncand <= 1;
	btsorted = from_str ? bi.sorted : ci.ncand <= 1;
	btrevsorted = from_str ? bi.revsorted : ci.ncand <= 1;
bailout1:
	bat_iterator_end(&bi);

bailout:
	GDKfree(r);
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = btsorted;
		dst->trevsorted = btrevsorted;
		*res = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
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

/* stringify token */
#define _STRNG_(s) #s
#define STRNG(t) _STRNG_(t)

/* concatenate two, three or four tokens */
#define CONCAT_2(a,b)		a##b
#define CONCAT_3(a,b,c)		a##b##c
#define CONCAT_4(a,b,c,d)	a##b##c##d

#define NIL(t)				CONCAT_2(t,_nil)
#define ISNIL(t)			CONCAT_3(is_,t,_nil)
#define TPE(t)				CONCAT_2(TYPE_,t)
#define GDKmin(t)			CONCAT_3(GDK_,t,_min)
#define GDKmax(t)			CONCAT_3(GDK_,t,_max)
#define FUN(a,b,c,d)		CONCAT_4(a,b,c,d)
#define IS_NUMERIC(t)		CONCAT_2(t,_is_numeric)

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
