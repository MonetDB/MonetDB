/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
#include <opt_dictionary.h>
#include <opt_pipes.h>
#include "clients.h"
#include "mal_instruction.h"

static lng scales[20] = {
	LL_CONSTANT(1),
	LL_CONSTANT(10),
	LL_CONSTANT(100),
	LL_CONSTANT(1000),
	LL_CONSTANT(10000),
	LL_CONSTANT(100000),
	LL_CONSTANT(1000000),
	LL_CONSTANT(10000000),
	LL_CONSTANT(100000000),
	LL_CONSTANT(1000000000),
	LL_CONSTANT(10000000000),
	LL_CONSTANT(100000000000),
	LL_CONSTANT(1000000000000),
	LL_CONSTANT(10000000000000),
	LL_CONSTANT(100000000000000),
	LL_CONSTANT(1000000000000000),
	LL_CONSTANT(10000000000000000),
	LL_CONSTANT(100000000000000000),
	LL_CONSTANT(1000000000000000000)
};

str
nil_2_timestamp(timestamp *res, void *val)
{
	(void) val;
	*res = *timestamp_nil;
	return MAL_SUCCEED;
}

str
str_2_timestamp(timestamp *res, str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_timestamp, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_timestamp, p, ATOMnilptr(TYPE_timestamp)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val);
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
SQLtimestamp_2_str(str *res, timestamp *val)
{
	char *p = NULL;
	int len = 0;
	timestamp_tostr(&p, &len, val);
	*res = p;
	return MAL_SUCCEED;
}

str
batnil_2_timestamp(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_timestamp", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_timestamp, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_timestamp", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		timestamp r = *timestamp_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2_timestamp(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_timestamp", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_timestamp, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_timestamp", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		timestamp r;
		msg = str_2_timestamp(&r, &v);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
nil_2_daytime(daytime *res, void *val)
{
	(void) val;
	*res = daytime_nil;
	return MAL_SUCCEED;
}

str
str_2_daytime(daytime *res, str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_daytime, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_daytime, p, ATOMnilptr(TYPE_daytime)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val);
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
SQLdaytime_2_str(str *res, daytime *val)
{
	char *p = NULL;
	int len = 0;
	daytime_tostr(&p, &len, val);
	*res = p;
	return MAL_SUCCEED;
}

str
batnil_2_daytime(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_daytime", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_daytime, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_daytime", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		daytime r = daytime_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2_daytime(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_daytime", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_daytime, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_daytime", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		daytime r;
		msg = str_2_daytime(&r, &v);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
nil_2_date(date *res, void *val)
{
	(void) val;
	*res = date_nil;
	return MAL_SUCCEED;
}

str
str_2_date(date *res, str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_date, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_date, p, ATOMnilptr(TYPE_date)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val);
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
SQLdate_2_str(str *res, date *val)
{
	char *p = NULL;
	int len = 0;
	date_tostr(&p, &len, val);
	*res = p;
	return MAL_SUCCEED;
}

str
batnil_2_date(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_date", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_date, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_date", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		date r = date_nil;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2_date(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_date", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_date, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_date", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		date r;
		msg = str_2_date(&r, &v);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
nil_2_sqlblob(sqlblob * *res, void *val)
{
	(void) val;
	*res = ATOMnilptr(TYPE_blob);
	return MAL_SUCCEED;
}

str
str_2_sqlblob(sqlblob * *res, str *val)
{
	ptr p = NULL;
	int len = 0;
	int e;
	char buf[BUFSIZ];

	e = ATOMfromstr(TYPE_sqlblob, &p, &len, *val);
	if (e < 0 || !p || (ATOMcmp(TYPE_sqlblob, p, ATOMnilptr(TYPE_sqlblob)) == 0 && ATOMcmp(TYPE_str, *val, ATOMnilptr(TYPE_str)) != 0)) {
		if (p)
			GDKfree(p);
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val);
		throw(SQL, "sqlblob", "%s", buf);
	}
	*res = (sqlblob *) p;
	if (!ATOMextern(TYPE_sqlblob)) {
		if (p)
			GDKfree(p);
	}
	return MAL_SUCCEED;
}

str
SQLsqlblob_2_str(str *res, sqlblob * val)
{
	char *p = NULL;
	int len = 0;
	sqlblob_tostr(&p, &len, val);
	*res = p;
	return MAL_SUCCEED;
}

str
batnil_2_sqlblob(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_sqlblob", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sqlblob, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_sqlblob", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sqlblob *r = ATOMnilptr(TYPE_blob);
		BUNins(dst, BUNhead(bi, p), r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batstr_2_sqlblob(int *res, int *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_sqlblob", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sqlblob, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.2_sqlblob", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		str v = (str) BUNtail(bi, p);
		sqlblob *r;
		msg = str_2_sqlblob(&r, &v);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), r, FALSE);
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
		strLength(&sz, v);
		if (len == 0 || (sz >= 0 && sz <= len)) {
			r = GDKstrdup(v);
			if (r == NULL)
				throw(SQL, "str_cast", MAL_MALLOC_FAIL);
		}
	}
	if ((len > 0 && sz > len) || sz < 0) {
		if (r)
			GDKfree(r);
		if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), p) != 0) {
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
	str *res = (str *) getArgReference(stk, pci, 0);
	int eclass = *(int *) getArgReference(stk, pci, 1);
	int d = *(int *) getArgReference(stk, pci, 2);
	int s = *(int *) getArgReference(stk, pci, 3);
	int has_tz = *(int *) getArgReference(stk, pci, 4);
	ptr p = (ptr) getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5);
	int len = *(int *) getArgReference(stk, pci, 6);
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
	int *res = (int *) getArgReference(stk, pci, 0);
	int *eclass = (int *) getArgReference(stk, pci, 1);
	int *d1 = (int *) getArgReference(stk, pci, 2);
	int *s1 = (int *) getArgReference(stk, pci, 3);
	int *has_tz = (int *) getArgReference(stk, pci, 4);
	int *bid = (int *) getArgReference(stk, pci, 5);
	int *digits = (int *) getArgReference(stk, pci, 6);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_str, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.str_cast", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		ptr v = (ptr) BUNtail(bi, p);
		msg = SQLstr_cast_(&r, m, *eclass, *d1, *s1, *has_tz, v, b->ttype, *digits);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), r, FALSE);
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
#define TP2 wrd
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 sht
#define TP2 wrd
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 wrd
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 wrd
#define TP2 wrd
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

#define TP1 wrd
#define TP2 lng
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 lng
#include "sql_cast_impl_up_to_int.h"
#undef TP2
#undef TP1

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
#define TP2 wrd
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 flt
#define TP2 lng
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

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
#define TP2 wrd
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

#define TP1 dbl
#define TP2 lng
#include "sql_cast_impl_down_from_flt.h"
#undef TP2
#undef TP1

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

#define TP1 wrd
#define TP2 flt
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 flt
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

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

#define TP1 wrd
#define TP2 dbl
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 dbl
#include "sql_cast_impl_up_to_flt.h"
#undef TP2
#undef TP1

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

#define TP1 wrd
#define TP2 bte
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 bte
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 int
#define TP2 sht
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 wrd
#define TP2 sht
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 sht
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 wrd
#define TP2 int
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 int
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

#define TP1 lng
#define TP2 wrd
#include "sql_cast_impl_down_from_int.h"
#undef TP2
#undef TP1

