/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "sql_result.h"
#include "sql_cast.h"
#include "sql_gencode.h"
#include <sql_storage.h>
#include <sql_scenario.h>
#include <store_sequence.h>
#include <sql_optimizer.h>
#include <sql_datetime.h>
#include <rel_optimizer.h>
#include <rel_distribute.h>
#include <rel_select.h>
#include <rel_exp.h>
#include <rel_dump.h>
#include <rel_bin.h>
#include <opt_pipes.h>
#include "clients.h"
#include "mal_instruction.h"

str
nil_2_timestamp(timestamp *res, const void *val)
{
	(void) val;
	*res = *timestamp_nil;
	return MAL_SUCCEED;
}

str
str_2_timestamp(timestamp *res, const str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_timestamp, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_timestamp, p, ATOMnilptr(TYPE_timestamp)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val? *val:"");
		throw(SQL, "timestamp", "%s", buf);
	}
	*res = *(timestamp *) p;
	if (!ATOMextern(TYPE_timestamp)) {
		if (p)
			GDKfree(p);
	}
	return MAL_SUCCEED;
}

str
batnil_2_timestamp(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_timestamp", "Cannot access descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_timestamp", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		timestamp r = *timestamp_nil;
		BUNappend(dst, &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2_timestamp(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_timestamp", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_timestamp", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		timestamp r;
		msg = str_2_timestamp(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		BUNappend(dst, &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
nil_2_daytime(daytime *res, const void *val)
{
	(void) val;
	*res = daytime_nil;
	return MAL_SUCCEED;
}

str
str_2_daytime(daytime *res, const str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_daytime, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_daytime, p, ATOMnilptr(TYPE_daytime)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val? *val:"");
		throw(SQL, "daytime", "%s", buf);
	}
	*res = *(daytime *) p;
	if (!ATOMextern(TYPE_daytime)) {
		if (p)
			GDKfree(p);
	}
	return MAL_SUCCEED;
}

str
batnil_2_daytime(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_daytime", "Cannot access descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_daytime", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		daytime r = daytime_nil;
		BUNappend(dst, &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2_daytime(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_daytime", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_daytime", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		daytime r;
		msg = str_2_daytime(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		BUNappend(dst, &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
nil_2_date(date *res, const void *val)
{
	(void) val;
	*res = date_nil;
	return MAL_SUCCEED;
}

str
str_2_date(date *res, const str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_date, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_date, p, ATOMnilptr(TYPE_date)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val? *val:"");
		throw(SQL, "date", "%s", buf);
	}
	*res = *(date *) p;
	if (!ATOMextern(TYPE_date)) {
		if (p)
			GDKfree(p);
	}
	return MAL_SUCCEED;
}

str
SQLdate_2_str(str *res, const date *val)
{
	char *p = NULL;
	int len = 0;
	date_tostr(&p, &len, val);
	*res = p;
	return MAL_SUCCEED;
}

str
batnil_2_date(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_date", "Cannot access descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_date, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_date", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		date r = date_nil;
		BUNappend(dst, &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2_date(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_date", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_date, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_date", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		date r;
		msg = str_2_date(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		BUNappend(dst, &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
str_2_sqlblob(sqlblob **res, const str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_sqlblob, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_sqlblob, p, ATOMnilptr(TYPE_sqlblob)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val? *val:"");
		throw(SQL, "sqlblob", "%s", buf);
	}
	*res = (sqlblob *) p;
	return MAL_SUCCEED;
}

str
SQLsqlblob_2_str(str *res, const sqlblob * val)
{
	char *p = NULL;
	int len = 0;
	sqlblob_tostr(&p, &len, val);
	*res = p;
	return MAL_SUCCEED;
}

str
batstr_2_sqlblob(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_sqlblob", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_sqlblob, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_sqlblob", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		sqlblob *r;
		msg = str_2_sqlblob(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		BUNappend(dst, r, FALSE);
		GDKfree(r);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

static str
SQLstr_cast_(str *res, mvc *m, int eclass, int d, int s, int has_tz, ptr p, int tpe, int len)
{
	char *r = NULL;
	int sz = MAX(2, len + 1);	/* nil should fit */

	if (tpe != TYPE_str) {
		r = GDKmalloc(sz);
		if (r == NULL)
			throw(SQL, "str_cast", MAL_MALLOC_FAIL);
		sz = convert2str(m, eclass, d, s, has_tz, p, tpe, &r, sz);
	} else {
		str v = (str) p;
		STRLength(&sz, &v);
		if (len == 0 || (sz >= 0 && sz <= len)) {
			r = GDKstrdup(v);
			if (r == NULL)
				throw(SQL, "str_cast", MAL_MALLOC_FAIL);
		}
	}
	if ((len > 0 && sz > len) || sz < 0) {
		if (r)
			GDKfree(r);
		if (ATOMcmp(tpe, ATOMnilptr(tpe), p) != 0) {
			throw(SQL, "str_cast", "22001!value too long for type (var)char(%d)", len);
		} else {
			r = GDKstrdup(str_nil);
			if (r == NULL)
				throw(SQL, "str_cast", MAL_MALLOC_FAIL);
		}
	}
	*res = r;
	return MAL_SUCCEED;
}

str
SQLstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *res = getArgReference_str(stk, pci, 0);
	int eclass = *getArgReference_int(stk, pci, 1);
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

/* str SQLbatstr_cast(int *res, int *eclass, int *d1, int *s1, int *has_tz, int *bid, int *digits ); */
str
SQLbatstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	mvc *m = NULL;
	str msg;
	char *r = NULL;
	bat *res = getArgReference_bat(stk, pci, 0);
	int *eclass = getArgReference_int(stk, pci, 1);
	int *d1 = getArgReference_int(stk, pci, 2);
	int *s1 = getArgReference_int(stk, pci, 3);
	int *has_tz = getArgReference_int(stk, pci, 4);
	bat *bid = getArgReference_bat(stk, pci, 5);
	int *digits = getArgReference_int(stk, pci, 6);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.str_cast", MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		ptr v = (ptr) BUNtail(bi, p);
		msg = SQLstr_cast_(&r, m, *eclass, *d1, *s1, *has_tz, v, b->ttype, *digits);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		BUNappend(dst, r, FALSE);
		GDKfree(r);
		r = NULL;
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

/* sql_cast_impl_up_to_int */

#define TP1 bte
#define TP2 bte
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 bte
#define TP2 sht
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 sht
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 bte
#define TP2 int
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 int
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 int
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 bte
#define TP2 lng
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 lng
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 lng
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 lng
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 bte
#define TP2 hge
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 hge
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 hge
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 hge
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 hge
#define TP2 hge
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1
#endif

/* sql_cast_impl_down_from_flt */

#define TP1 flt
#define TP2 bte
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 flt
#define TP2 sht
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 flt
#define TP2 int
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 flt
#define TP2 lng
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 flt
#define TP2 hge
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1
#endif

#define TP1 dbl
#define TP2 bte
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 dbl
#define TP2 sht
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 dbl
#define TP2 int
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 dbl
#define TP2 lng
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 dbl
#define TP2 hge
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1
#endif

/* sql_cast_impl_up_to_flt */

#define TP1 bte
#define TP2 flt
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 flt
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 flt
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 flt
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 flt
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1
#endif

#define TP1 bte
#define TP2 dbl
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 dbl
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 dbl
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 dbl
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 dbl
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1
#endif

/* sql_cast_impl_down_from_int */

#define TP1 sht
#define TP2 bte
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 bte
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 bte
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 bte
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1
#endif

#define TP1 int
#define TP2 sht
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 sht
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 sht
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1
#endif

#define TP1 lng
#define TP2 int
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 int
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1
#endif

#ifdef HAVE_HGE
#define TP1 hge
#define TP2 lng
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1
#endif

