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
		snprintf(buf, BUFSIZ, "conversion of string '%s' failed", *val? *val:"");
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
		sz = convert2str(m, eclass, d, s, has_tz, p, tpe, &r, sz);
	} else {
		str v = (str) p;
		strLength(&sz, v);
		if (len == 0 || (sz >= 0 && sz <= len))
			r = GDKstrdup(v);
	}
	if ((len > 0 && sz > len) || sz < 0) {
		if (r)
			GDKfree(r);
		if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), p) != 0) {
			throw(SQL, "str_cast", "22001!value too long for type (var)char(%d)", len);
		} else {
			r = GDKstrdup(str_nil);
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
	str msg = getSQLContext(cntxt, mb, &m, NULL);

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
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	char *r = NULL;
	int *res = (int *) getArgReference(stk, pci, 0);
	int *eclass = (int *) getArgReference(stk, pci, 1);
	int *d1 = (int *) getArgReference(stk, pci, 2);
	int *s1 = (int *) getArgReference(stk, pci, 3);
	int *has_tz = (int *) getArgReference(stk, pci, 4);
	int *bid = (int *) getArgReference(stk, pci, 5);
	int *digits = (int *) getArgReference(stk, pci, 6);

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
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_2_bte(bte *res, bte *v)
{
	/* shortcut nil */
	if (*v == bte_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* since the bte type is bigger than or equal to the bte type, it will
	   always fit */
	*res = (bte) *v;
	return (MAL_SUCCEED);
}

str
batbte_2_bte(int *res, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	bte *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.bte_2_bte", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (bte) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == bte_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (bte) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
bte_dec2_bte(bte *res, int *s1, bte *v)
{
	int scale = *s1;
	bte r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* since the bte type is bigger than or equal to the bte type, it will
	   always fit */
	r = (bte) *v;
	if (scale)
		r = (bte) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_dec2dec_bte(bte *res, int *S1, bte *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	bte cpyval = *v;
	int s1 = *S1, s2 = *S2;
	bte r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the bte type is bigger than or equal to the bte type, it will
	   always fit */
	r = (bte) *v;
	if (s2 > s1)
		r *= (bte) scales[s2 - s1];
	else if (s2 != s1)
		r = (bte) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_num2dec_bte(bte *res, bte *v, int *d2, int *s2)
{
	int zero = 0;
	return bte_dec2dec_bte(res, &zero, v, d2, s2);
}

str
batbte_dec2_bte(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	char *msg = NULL;
	int scale = *s1;
	bte *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_bte", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (bte) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (bte) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == bte_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (bte) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (bte) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batbte_dec2dec_bte(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		bte r;
		msg = bte_dec2dec_bte(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batbte_num2dec_bte(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_num2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		bte r;
		msg = bte_num2dec_bte(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_2_sht(sht *res, bte *v)
{
	/* shortcut nil */
	if (*v == bte_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* since the sht type is bigger than or equal to the bte type, it will
	   always fit */
	*res = (sht) *v;
	return (MAL_SUCCEED);
}

str
batbte_2_sht(int *res, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	sht *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.bte_2_sht", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (sht) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == bte_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (sht) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
bte_dec2_sht(sht *res, int *s1, bte *v)
{
	int scale = *s1;
	sht r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* since the sht type is bigger than or equal to the bte type, it will
	   always fit */
	r = (sht) *v;
	if (scale)
		r = (sht) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_dec2dec_sht(sht *res, int *S1, bte *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	bte cpyval = *v;
	int s1 = *S1, s2 = *S2;
	sht r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the sht type is bigger than or equal to the bte type, it will
	   always fit */
	r = (sht) *v;
	if (s2 > s1)
		r *= (sht) scales[s2 - s1];
	else if (s2 != s1)
		r = (sht) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_num2dec_sht(sht *res, bte *v, int *d2, int *s2)
{
	int zero = 0;
	return bte_dec2dec_sht(res, &zero, v, d2, s2);
}

str
batbte_dec2_sht(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	char *msg = NULL;
	int scale = *s1;
	sht *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_sht", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (sht) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (sht) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == bte_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (sht) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (sht) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batbte_dec2dec_sht(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		sht r;
		msg = bte_dec2dec_sht(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batbte_num2dec_sht(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_num2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		sht r;
		msg = bte_num2dec_sht(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_2_sht(sht *res, sht *v)
{
	/* shortcut nil */
	if (*v == sht_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* since the sht type is bigger than or equal to the sht type, it will
	   always fit */
	*res = (sht) *v;
	return (MAL_SUCCEED);
}

str
batsht_2_sht(int *res, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	sht *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.sht_2_sht", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (sht) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == sht_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (sht) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
sht_dec2_sht(sht *res, int *s1, sht *v)
{
	int scale = *s1;
	sht r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* since the sht type is bigger than or equal to the sht type, it will
	   always fit */
	r = (sht) *v;
	if (scale)
		r = (sht) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_dec2dec_sht(sht *res, int *S1, sht *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	sht cpyval = *v;
	int s1 = *S1, s2 = *S2;
	sht r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the sht type is bigger than or equal to the sht type, it will
	   always fit */
	r = (sht) *v;
	if (s2 > s1)
		r *= (sht) scales[s2 - s1];
	else if (s2 != s1)
		r = (sht) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_num2dec_sht(sht *res, sht *v, int *d2, int *s2)
{
	int zero = 0;
	return sht_dec2dec_sht(res, &zero, v, d2, s2);
}

str
batsht_dec2_sht(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	int scale = *s1;
	sht *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_sht", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (sht) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (sht) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (sht) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (sht) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batsht_dec2dec_sht(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		sht r;
		msg = sht_dec2dec_sht(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batsht_num2dec_sht(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_num2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		sht r;
		msg = sht_num2dec_sht(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_2_int(int *res, bte *v)
{
	/* shortcut nil */
	if (*v == bte_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the bte type, it will
	   always fit */
	*res = (int) *v;
	return (MAL_SUCCEED);
}

str
batbte_2_int(int *res, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	int *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.bte_2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (int) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == bte_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (int) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
bte_dec2_int(int *res, int *s1, bte *v)
{
	int scale = *s1;
	int r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the bte type, it will
	   always fit */
	r = (int) *v;
	if (scale)
		r = (int) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_dec2dec_int(int *res, int *S1, bte *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	bte cpyval = *v;
	int s1 = *S1, s2 = *S2;
	int r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the int type is bigger than or equal to the bte type, it will
	   always fit */
	r = (int) *v;
	if (s2 > s1)
		r *= (int) scales[s2 - s1];
	else if (s2 != s1)
		r = (int) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_num2dec_int(int *res, bte *v, int *d2, int *s2)
{
	int zero = 0;
	return bte_dec2dec_int(res, &zero, v, d2, s2);
}

str
batbte_dec2_int(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	char *msg = NULL;
	int scale = *s1;
	int *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (int) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == bte_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (int) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batbte_dec2dec_int(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		int r;
		msg = bte_dec2dec_int(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batbte_num2dec_int(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_num2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		int r;
		msg = bte_num2dec_int(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_2_int(int *res, sht *v)
{
	/* shortcut nil */
	if (*v == sht_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the sht type, it will
	   always fit */
	*res = (int) *v;
	return (MAL_SUCCEED);
}

str
batsht_2_int(int *res, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	int *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.sht_2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (int) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == sht_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (int) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
sht_dec2_int(int *res, int *s1, sht *v)
{
	int scale = *s1;
	int r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the sht type, it will
	   always fit */
	r = (int) *v;
	if (scale)
		r = (int) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_dec2dec_int(int *res, int *S1, sht *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	sht cpyval = *v;
	int s1 = *S1, s2 = *S2;
	int r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the int type is bigger than or equal to the sht type, it will
	   always fit */
	r = (int) *v;
	if (s2 > s1)
		r *= (int) scales[s2 - s1];
	else if (s2 != s1)
		r = (int) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_num2dec_int(int *res, sht *v, int *d2, int *s2)
{
	int zero = 0;
	return sht_dec2dec_int(res, &zero, v, d2, s2);
}

str
batsht_dec2_int(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	int scale = *s1;
	int *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (int) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (int) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batsht_dec2dec_int(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		int r;
		msg = sht_dec2dec_int(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batsht_num2dec_int(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_num2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		int r;
		msg = sht_num2dec_int(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_2_int(int *res, int *v)
{
	/* shortcut nil */
	if (*v == int_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the int type, it will
	   always fit */
	*res = (int) *v;
	return (MAL_SUCCEED);
}

str
batint_2_int(int *res, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	int *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.int_2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (int) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == int_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (int) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
int_dec2_int(int *res, int *s1, int *v)
{
	int scale = *s1;
	int r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the int type, it will
	   always fit */
	r = (int) *v;
	if (scale)
		r = (int) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
int_dec2dec_int(int *res, int *S1, int *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	int cpyval = *v;
	int s1 = *S1, s2 = *S2;
	int r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the int type is bigger than or equal to the int type, it will
	   always fit */
	r = (int) *v;
	if (s2 > s1)
		r *= (int) scales[s2 - s1];
	else if (s2 != s1)
		r = (int) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
int_num2dec_int(int *res, int *v, int *d2, int *s2)
{
	int zero = 0;
	return int_dec2dec_int(res, &zero, v, d2, s2);
}

str
batint_dec2_int(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	int scale = *s1;
	int *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (int) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (int) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batint_dec2dec_int(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		int r;
		msg = int_dec2dec_int(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batint_num2dec_int(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_num2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		int r;
		msg = int_num2dec_int(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_2_wrd(wrd *res, bte *v)
{
	/* shortcut nil */
	if (*v == bte_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the bte type, it will
	   always fit */
	*res = (wrd) *v;
	return (MAL_SUCCEED);
}

str
batbte_2_wrd(int *res, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.bte_2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (wrd) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == bte_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (wrd) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
bte_dec2_wrd(wrd *res, int *s1, bte *v)
{
	int scale = *s1;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the bte type, it will
	   always fit */
	r = (wrd) *v;
	if (scale)
		r = (wrd) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_dec2dec_wrd(wrd *res, int *S1, bte *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	bte cpyval = *v;
	int s1 = *S1, s2 = *S2;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the wrd type is bigger than or equal to the bte type, it will
	   always fit */
	r = (wrd) *v;
	if (s2 > s1)
		r *= (wrd) scales[s2 - s1];
	else if (s2 != s1)
		r = (wrd) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_num2dec_wrd(wrd *res, bte *v, int *d2, int *s2)
{
	int zero = 0;
	return bte_dec2dec_wrd(res, &zero, v, d2, s2);
}

str
batbte_dec2_wrd(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	char *msg = NULL;
	int scale = *s1;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (wrd) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == bte_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (wrd) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batbte_dec2dec_wrd(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		wrd r;
		msg = bte_dec2dec_wrd(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batbte_num2dec_wrd(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_num2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		wrd r;
		msg = bte_num2dec_wrd(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_2_wrd(wrd *res, sht *v)
{
	/* shortcut nil */
	if (*v == sht_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the sht type, it will
	   always fit */
	*res = (wrd) *v;
	return (MAL_SUCCEED);
}

str
batsht_2_wrd(int *res, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.sht_2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (wrd) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == sht_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (wrd) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
sht_dec2_wrd(wrd *res, int *s1, sht *v)
{
	int scale = *s1;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the sht type, it will
	   always fit */
	r = (wrd) *v;
	if (scale)
		r = (wrd) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_dec2dec_wrd(wrd *res, int *S1, sht *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	sht cpyval = *v;
	int s1 = *S1, s2 = *S2;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the wrd type is bigger than or equal to the sht type, it will
	   always fit */
	r = (wrd) *v;
	if (s2 > s1)
		r *= (wrd) scales[s2 - s1];
	else if (s2 != s1)
		r = (wrd) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_num2dec_wrd(wrd *res, sht *v, int *d2, int *s2)
{
	int zero = 0;
	return sht_dec2dec_wrd(res, &zero, v, d2, s2);
}

str
batsht_dec2_wrd(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	int scale = *s1;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (wrd) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (wrd) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batsht_dec2dec_wrd(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		wrd r;
		msg = sht_dec2dec_wrd(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batsht_num2dec_wrd(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_num2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		wrd r;
		msg = sht_num2dec_wrd(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_2_wrd(wrd *res, int *v)
{
	/* shortcut nil */
	if (*v == int_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the int type, it will
	   always fit */
	*res = (wrd) *v;
	return (MAL_SUCCEED);
}

str
batint_2_wrd(int *res, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.int_2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (wrd) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == int_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (wrd) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
int_dec2_wrd(wrd *res, int *s1, int *v)
{
	int scale = *s1;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the int type, it will
	   always fit */
	r = (wrd) *v;
	if (scale)
		r = (wrd) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
int_dec2dec_wrd(wrd *res, int *S1, int *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	int cpyval = *v;
	int s1 = *S1, s2 = *S2;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the wrd type is bigger than or equal to the int type, it will
	   always fit */
	r = (wrd) *v;
	if (s2 > s1)
		r *= (wrd) scales[s2 - s1];
	else if (s2 != s1)
		r = (wrd) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
int_num2dec_wrd(wrd *res, int *v, int *d2, int *s2)
{
	int zero = 0;
	return int_dec2dec_wrd(res, &zero, v, d2, s2);
}

str
batint_dec2_wrd(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	int scale = *s1;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (wrd) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (wrd) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batint_dec2dec_wrd(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		wrd r;
		msg = int_dec2dec_wrd(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batint_num2dec_wrd(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_num2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		wrd r;
		msg = int_num2dec_wrd(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_2_wrd(wrd *res, wrd *v)
{
	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the wrd type, it will
	   always fit */
	*res = (wrd) *v;
	return (MAL_SUCCEED);
}

str
batwrd_2_wrd(int *res, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.wrd_2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (wrd) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == wrd_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (wrd) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
wrd_dec2_wrd(wrd *res, int *s1, wrd *v)
{
	int scale = *s1;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (wrd) *v;
	if (scale)
		r = (wrd) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
wrd_dec2dec_wrd(wrd *res, int *S1, wrd *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	wrd cpyval = *v;
	int s1 = *S1, s2 = *S2;
	wrd r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the wrd type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (wrd) *v;
	if (s2 > s1)
		r *= (wrd) scales[s2 - s1];
	else if (s2 != s1)
		r = (wrd) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
wrd_num2dec_wrd(wrd *res, wrd *v, int *d2, int *s2)
{
	int zero = 0;
	return wrd_dec2dec_wrd(res, &zero, v, d2, s2);
}

str
batwrd_dec2_wrd(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int scale = *s1;
	wrd *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (wrd) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (wrd) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batwrd_dec2dec_wrd(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		wrd r;
		msg = wrd_dec2dec_wrd(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batwrd_num2dec_wrd(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_num2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		wrd r;
		msg = wrd_num2dec_wrd(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_2_lng(lng *res, bte *v)
{
	/* shortcut nil */
	if (*v == bte_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the bte type, it will
	   always fit */
	*res = (lng) *v;
	return (MAL_SUCCEED);
}

str
batbte_2_lng(int *res, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.bte_2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (lng) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == bte_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (lng) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
bte_dec2_lng(lng *res, int *s1, bte *v)
{
	int scale = *s1;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the bte type, it will
	   always fit */
	r = (lng) *v;
	if (scale)
		r = (lng) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_dec2dec_lng(lng *res, int *S1, bte *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	bte cpyval = *v;
	int s1 = *S1, s2 = *S2;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the lng type is bigger than or equal to the bte type, it will
	   always fit */
	r = (lng) *v;
	if (s2 > s1)
		r *= (lng) scales[s2 - s1];
	else if (s2 != s1)
		r = (lng) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
bte_num2dec_lng(lng *res, bte *v, int *d2, int *s2)
{
	int zero = 0;
	return bte_dec2dec_lng(res, &zero, v, d2, s2);
}

str
batbte_dec2_lng(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	char *msg = NULL;
	int scale = *s1;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (lng) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == bte_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (lng) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batbte_dec2dec_lng(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		lng r;
		msg = bte_dec2dec_lng(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batbte_num2dec_lng(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_num2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		lng r;
		msg = bte_num2dec_lng(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_2_lng(lng *res, sht *v)
{
	/* shortcut nil */
	if (*v == sht_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the sht type, it will
	   always fit */
	*res = (lng) *v;
	return (MAL_SUCCEED);
}

str
batsht_2_lng(int *res, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.sht_2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (lng) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == sht_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (lng) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
sht_dec2_lng(lng *res, int *s1, sht *v)
{
	int scale = *s1;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the sht type, it will
	   always fit */
	r = (lng) *v;
	if (scale)
		r = (lng) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_dec2dec_lng(lng *res, int *S1, sht *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	sht cpyval = *v;
	int s1 = *S1, s2 = *S2;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the lng type is bigger than or equal to the sht type, it will
	   always fit */
	r = (lng) *v;
	if (s2 > s1)
		r *= (lng) scales[s2 - s1];
	else if (s2 != s1)
		r = (lng) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
sht_num2dec_lng(lng *res, sht *v, int *d2, int *s2)
{
	int zero = 0;
	return sht_dec2dec_lng(res, &zero, v, d2, s2);
}

str
batsht_dec2_lng(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	int scale = *s1;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (lng) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (lng) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batsht_dec2dec_lng(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		lng r;
		msg = sht_dec2dec_lng(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batsht_num2dec_lng(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_num2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		lng r;
		msg = sht_num2dec_lng(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_2_lng(lng *res, int *v)
{
	/* shortcut nil */
	if (*v == int_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the int type, it will
	   always fit */
	*res = (lng) *v;
	return (MAL_SUCCEED);
}

str
batint_2_lng(int *res, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.int_2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (lng) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == int_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (lng) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
int_dec2_lng(lng *res, int *s1, int *v)
{
	int scale = *s1;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the int type, it will
	   always fit */
	r = (lng) *v;
	if (scale)
		r = (lng) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
int_dec2dec_lng(lng *res, int *S1, int *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	int cpyval = *v;
	int s1 = *S1, s2 = *S2;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the lng type is bigger than or equal to the int type, it will
	   always fit */
	r = (lng) *v;
	if (s2 > s1)
		r *= (lng) scales[s2 - s1];
	else if (s2 != s1)
		r = (lng) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
int_num2dec_lng(lng *res, int *v, int *d2, int *s2)
{
	int zero = 0;
	return int_dec2dec_lng(res, &zero, v, d2, s2);
}

str
batint_dec2_lng(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	int scale = *s1;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (lng) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (lng) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batint_dec2dec_lng(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		lng r;
		msg = int_dec2dec_lng(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batint_num2dec_lng(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_num2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		lng r;
		msg = int_num2dec_lng(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_2_lng(lng *res, wrd *v)
{
	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the wrd type, it will
	   always fit */
	*res = (lng) *v;
	return (MAL_SUCCEED);
}

str
batwrd_2_lng(int *res, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.wrd_2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (lng) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == wrd_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (lng) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
wrd_dec2_lng(lng *res, int *s1, wrd *v)
{
	int scale = *s1;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (lng) *v;
	if (scale)
		r = (lng) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
wrd_dec2dec_lng(lng *res, int *S1, wrd *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	wrd cpyval = *v;
	int s1 = *S1, s2 = *S2;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the lng type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (lng) *v;
	if (s2 > s1)
		r *= (lng) scales[s2 - s1];
	else if (s2 != s1)
		r = (lng) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
wrd_num2dec_lng(lng *res, wrd *v, int *d2, int *s2)
{
	int zero = 0;
	return wrd_dec2dec_lng(res, &zero, v, d2, s2);
}

str
batwrd_dec2_lng(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int scale = *s1;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (lng) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (lng) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batwrd_dec2dec_lng(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		lng r;
		msg = wrd_dec2dec_lng(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batwrd_num2dec_lng(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_num2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		lng r;
		msg = wrd_num2dec_lng(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_2_lng(lng *res, lng *v)
{
	/* shortcut nil */
	if (*v == lng_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the lng type, it will
	   always fit */
	*res = (lng) *v;
	return (MAL_SUCCEED);
}

str
batlng_2_lng(int *res, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.lng_2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (lng) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == lng_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (lng) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
lng_dec2_lng(lng *res, int *s1, lng *v)
{
	int scale = *s1;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the lng type, it will
	   always fit */
	r = (lng) *v;
	if (scale)
		r = (lng) ((r + h * scales[scale - 1]) / scales[scale]);
	*res = r;
	return (MAL_SUCCEED);
}

str
lng_dec2dec_lng(lng *res, int *S1, lng *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng cpyval = *v;
	int s1 = *S1, s2 = *S2;
	lng r, h = (*v < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;

	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the lng type is bigger than or equal to the lng type, it will
	   always fit */
	r = (lng) *v;
	if (s2 > s1)
		r *= (lng) scales[s2 - s1];
	else if (s2 != s1)
		r = (lng) ((r + h * scales[s1 - s2 - 1]) / scales[s1 - s2]);
	*res = r;
	return (MAL_SUCCEED);
}

str
lng_num2dec_lng(lng *res, lng *v, int *d2, int *s2)
{
	int zero = 0;
	return lng_dec2dec_lng(res, &zero, v, d2, s2);
}

str
batlng_dec2_lng(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int scale = *s1;
	lng *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_lng", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		if (scale)
			for (; p < q; p++, o++)
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
		else
			for (; p < q; p++, o++)
				*o = (lng) (*p);
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else if (scale) {
				*o = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			} else {
				*o = (lng) (*p);
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batlng_dec2dec_lng(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		lng r;
		msg = lng_dec2dec_lng(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batlng_num2dec_lng(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_num2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		lng r;
		msg = lng_num2dec_lng(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
flt_2_bte(bte *res, flt *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (bte) val > (dbl) GDK_bte_min && val > (dbl) GDK_bte_min && val <= (dbl) GDK_bte_max) {
		*res = (bte) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type bte", val);
	}
}

str
batflt_2_bte(int *res, int *bid)
{
	BAT *b, *bn;
	flt *p, *q;
	char *msg = NULL;
	bte *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.flt_2_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (flt *) Tloc(b, BUNfirst(b));
	q = (flt *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (bte) val > (dbl) GDK_bte_min && val > (dbl) GDK_bte_min && val <= (dbl) GDK_bte_max) {
				*o = (bte) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type bte", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == flt_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (bte) val > (dbl) GDK_bte_min && val > (dbl) GDK_bte_min && val <= (dbl) GDK_bte_max) {
					*o = (bte) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type bte", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
flt_num2dec_bte(bte *res, flt *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	flt r;
	lng cpyval;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* since the bte type is bigger than or equal to the flt type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (bte) r;
	return MAL_SUCCEED;
}

str
batflt_num2dec_bte(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_num2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		flt *v = (flt *) BUNtail(bi, p);
		bte r;
		msg = flt_num2dec_bte(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
flt_2_sht(sht *res, flt *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (sht) val > (dbl) GDK_sht_min && val > (dbl) GDK_sht_min && val <= (dbl) GDK_sht_max) {
		*res = (sht) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type sht", val);
	}
}

str
batflt_2_sht(int *res, int *bid)
{
	BAT *b, *bn;
	flt *p, *q;
	char *msg = NULL;
	sht *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.flt_2_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (flt *) Tloc(b, BUNfirst(b));
	q = (flt *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (sht) val > (dbl) GDK_sht_min && val > (dbl) GDK_sht_min && val <= (dbl) GDK_sht_max) {
				*o = (sht) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type sht", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == flt_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (sht) val > (dbl) GDK_sht_min && val > (dbl) GDK_sht_min && val <= (dbl) GDK_sht_max) {
					*o = (sht) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type sht", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
flt_num2dec_sht(sht *res, flt *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	flt r;
	lng cpyval;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* since the sht type is bigger than or equal to the flt type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (sht) r;
	return MAL_SUCCEED;
}

str
batflt_num2dec_sht(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_num2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		flt *v = (flt *) BUNtail(bi, p);
		sht r;
		msg = flt_num2dec_sht(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
flt_2_int(int *res, flt *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (int) val > (dbl) GDK_int_min && val > (dbl) GDK_int_min && val <= (dbl) GDK_int_max) {
		*res = (int) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type int", val);
	}
}

str
batflt_2_int(int *res, int *bid)
{
	BAT *b, *bn;
	flt *p, *q;
	char *msg = NULL;
	int *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.flt_2_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (flt *) Tloc(b, BUNfirst(b));
	q = (flt *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (int) val > (dbl) GDK_int_min && val > (dbl) GDK_int_min && val <= (dbl) GDK_int_max) {
				*o = (int) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type int", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == flt_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (int) val > (dbl) GDK_int_min && val > (dbl) GDK_int_min && val <= (dbl) GDK_int_max) {
					*o = (int) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type int", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
flt_num2dec_int(int *res, flt *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	flt r;
	lng cpyval;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the flt type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (int) r;
	return MAL_SUCCEED;
}

str
batflt_num2dec_int(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_num2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		flt *v = (flt *) BUNtail(bi, p);
		int r;
		msg = flt_num2dec_int(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
flt_2_wrd(wrd *res, flt *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (wrd) val > (dbl) GDK_wrd_min && val > (dbl) GDK_wrd_min && val <= (dbl) GDK_wrd_max) {
		*res = (wrd) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type wrd", val);
	}
}

str
batflt_2_wrd(int *res, int *bid)
{
	BAT *b, *bn;
	flt *p, *q;
	char *msg = NULL;
	wrd *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.flt_2_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (flt *) Tloc(b, BUNfirst(b));
	q = (flt *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (wrd) val > (dbl) GDK_wrd_min && val > (dbl) GDK_wrd_min && val <= (dbl) GDK_wrd_max) {
				*o = (wrd) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type wrd", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == flt_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (wrd) val > (dbl) GDK_wrd_min && val > (dbl) GDK_wrd_min && val <= (dbl) GDK_wrd_max) {
					*o = (wrd) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type wrd", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
flt_num2dec_wrd(wrd *res, flt *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	flt r;
	lng cpyval;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the flt type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (wrd) r;
	return MAL_SUCCEED;
}

str
batflt_num2dec_wrd(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_num2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		flt *v = (flt *) BUNtail(bi, p);
		wrd r;
		msg = flt_num2dec_wrd(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
flt_2_lng(lng *res, flt *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (lng) val > (dbl) GDK_lng_min && val > (dbl) GDK_lng_min && val <= (dbl) GDK_lng_max) {
		*res = (lng) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type lng", val);
	}
}

str
batflt_2_lng(int *res, int *bid)
{
	BAT *b, *bn;
	flt *p, *q;
	char *msg = NULL;
	lng *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.flt_2_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (flt *) Tloc(b, BUNfirst(b));
	q = (flt *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (lng) val > (dbl) GDK_lng_min && val > (dbl) GDK_lng_min && val <= (dbl) GDK_lng_max) {
				*o = (lng) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type lng", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == flt_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (lng) val > (dbl) GDK_lng_min && val > (dbl) GDK_lng_min && val <= (dbl) GDK_lng_max) {
					*o = (lng) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type lng", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
flt_num2dec_lng(lng *res, flt *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	flt r;
	lng cpyval;

	/* shortcut nil */
	if (*v == flt_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the flt type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (lng) r;
	return MAL_SUCCEED;
}

str
batflt_num2dec_lng(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.flt_num2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		flt *v = (flt *) BUNtail(bi, p);
		lng r;
		msg = flt_num2dec_lng(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
dbl_2_bte(bte *res, dbl *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (bte) val > (dbl) GDK_bte_min && val > (dbl) GDK_bte_min && val <= (dbl) GDK_bte_max) {
		*res = (bte) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type bte", val);
	}
}

str
batdbl_2_bte(int *res, int *bid)
{
	BAT *b, *bn;
	dbl *p, *q;
	char *msg = NULL;
	bte *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dbl_2_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (dbl *) Tloc(b, BUNfirst(b));
	q = (dbl *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (bte) val > (dbl) GDK_bte_min && val > (dbl) GDK_bte_min && val <= (dbl) GDK_bte_max) {
				*o = (bte) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type bte", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == dbl_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (bte) val > (dbl) GDK_bte_min && val > (dbl) GDK_bte_min && val <= (dbl) GDK_bte_max) {
					*o = (bte) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type bte", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
dbl_num2dec_bte(bte *res, dbl *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	dbl r;
	lng cpyval;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* since the bte type is bigger than or equal to the dbl type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (bte) r;
	return MAL_SUCCEED;
}

str
batdbl_num2dec_bte(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_num2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		dbl *v = (dbl *) BUNtail(bi, p);
		bte r;
		msg = dbl_num2dec_bte(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
dbl_2_sht(sht *res, dbl *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (sht) val > (dbl) GDK_sht_min && val > (dbl) GDK_sht_min && val <= (dbl) GDK_sht_max) {
		*res = (sht) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type sht", val);
	}
}

str
batdbl_2_sht(int *res, int *bid)
{
	BAT *b, *bn;
	dbl *p, *q;
	char *msg = NULL;
	sht *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dbl_2_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (dbl *) Tloc(b, BUNfirst(b));
	q = (dbl *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (sht) val > (dbl) GDK_sht_min && val > (dbl) GDK_sht_min && val <= (dbl) GDK_sht_max) {
				*o = (sht) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type sht", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == dbl_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (sht) val > (dbl) GDK_sht_min && val > (dbl) GDK_sht_min && val <= (dbl) GDK_sht_max) {
					*o = (sht) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type sht", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
dbl_num2dec_sht(sht *res, dbl *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	dbl r;
	lng cpyval;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* since the sht type is bigger than or equal to the dbl type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (sht) r;
	return MAL_SUCCEED;
}

str
batdbl_num2dec_sht(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_num2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		dbl *v = (dbl *) BUNtail(bi, p);
		sht r;
		msg = dbl_num2dec_sht(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
dbl_2_int(int *res, dbl *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (int) val > (dbl) GDK_int_min && val > (dbl) GDK_int_min && val <= (dbl) GDK_int_max) {
		*res = (int) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type int", val);
	}
}

str
batdbl_2_int(int *res, int *bid)
{
	BAT *b, *bn;
	dbl *p, *q;
	char *msg = NULL;
	int *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dbl_2_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (dbl *) Tloc(b, BUNfirst(b));
	q = (dbl *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (int) val > (dbl) GDK_int_min && val > (dbl) GDK_int_min && val <= (dbl) GDK_int_max) {
				*o = (int) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type int", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == dbl_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (int) val > (dbl) GDK_int_min && val > (dbl) GDK_int_min && val <= (dbl) GDK_int_max) {
					*o = (int) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type int", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
dbl_num2dec_int(int *res, dbl *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	dbl r;
	lng cpyval;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* since the int type is bigger than or equal to the dbl type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (int) r;
	return MAL_SUCCEED;
}

str
batdbl_num2dec_int(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_num2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		dbl *v = (dbl *) BUNtail(bi, p);
		int r;
		msg = dbl_num2dec_int(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
dbl_2_wrd(wrd *res, dbl *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (wrd) val > (dbl) GDK_wrd_min && val > (dbl) GDK_wrd_min && val <= (dbl) GDK_wrd_max) {
		*res = (wrd) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type wrd", val);
	}
}

str
batdbl_2_wrd(int *res, int *bid)
{
	BAT *b, *bn;
	dbl *p, *q;
	char *msg = NULL;
	wrd *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dbl_2_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (dbl *) Tloc(b, BUNfirst(b));
	q = (dbl *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (wrd) val > (dbl) GDK_wrd_min && val > (dbl) GDK_wrd_min && val <= (dbl) GDK_wrd_max) {
				*o = (wrd) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type wrd", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == dbl_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (wrd) val > (dbl) GDK_wrd_min && val > (dbl) GDK_wrd_min && val <= (dbl) GDK_wrd_max) {
					*o = (wrd) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type wrd", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
dbl_num2dec_wrd(wrd *res, dbl *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	dbl r;
	lng cpyval;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* since the wrd type is bigger than or equal to the dbl type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (wrd) r;
	return MAL_SUCCEED;
}

str
batdbl_num2dec_wrd(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_num2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		dbl *v = (dbl *) BUNtail(bi, p);
		wrd r;
		msg = dbl_num2dec_wrd(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
dbl_2_lng(lng *res, dbl *v)
{
	dbl val = *v;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((dbl) (lng) val > (dbl) GDK_lng_min && val > (dbl) GDK_lng_min && val <= (dbl) GDK_lng_max) {
		*res = (lng) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type lng", val);
	}
}

str
batdbl_2_lng(int *res, int *bid)
{
	BAT *b, *bn;
	dbl *p, *q;
	char *msg = NULL;
	lng *o;
	dbl val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_2_lng", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_lng, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dbl_2_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (lng *) Tloc(bn, BUNfirst(bn));
	p = (dbl *) Tloc(b, BUNfirst(b));
	q = (dbl *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((dbl) (lng) val > (dbl) GDK_lng_min && val > (dbl) GDK_lng_min && val <= (dbl) GDK_lng_max) {
				*o = (lng) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type lng", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == dbl_nil) {
				*o = lng_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((dbl) (lng) val > (dbl) GDK_lng_min && val > (dbl) GDK_lng_min && val <= (dbl) GDK_lng_max) {
					*o = (lng) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" "%f" ") exceeds limits of type lng", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

/* when casting a floating point to an decimal we like to preserve the 
 * precision.  This means we first scale the float before converting.
*/
str
dbl_num2dec_lng(lng *res, dbl *v, int *d2, int *s2)
{
	int p = *d2, inlen = 1, scale = *s2;
	dbl r;
	lng cpyval;

	/* shortcut nil */
	if (*v == dbl_nil) {
		*res = lng_nil;
		return (MAL_SUCCEED);
	}

	/* since the lng type is bigger than or equal to the dbl type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r *= scales[scale];
	cpyval = (lng) r;

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}
	*res = (lng) r;
	return MAL_SUCCEED;
}

str
batdbl_num2dec_lng(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.dbl_num2dec_lng", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_lng, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_lng", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		dbl *v = (dbl *) BUNtail(bi, p);
		lng r;
		msg = dbl_num2dec_lng(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_2_flt(flt *res, bte *v)
{
	/* shortcut nil */
	if (*v == bte_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the bte type, it will
	   always fit */
	*res = (flt) *v;
	return (MAL_SUCCEED);
}

str
batbte_2_flt(int *res, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.bte_2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (flt) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == bte_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (flt) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
bte_dec2_flt(flt *res, int *s1, bte *v)
{
	int scale = *s1;
	flt r;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the bte type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
bte_dec2dec_flt(flt *res, int *S1, bte *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	bte cpyval = *v;
	int s1 = *S1, s2 = *S2;
	flt r;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the flt type is bigger than or equal to the bte type, it will
	   always fit */
	r = (flt) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
bte_num2dec_flt(flt *res, bte *v, int *d2, int *s2)
{
	int zero = 0;
	return bte_dec2dec_flt(res, &zero, v, d2, s2);
}

str
batbte_dec2_flt(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	char *msg = NULL;
	int scale = *s1;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((flt) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == bte_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((flt) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batbte_dec2dec_flt(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		flt r;
		msg = bte_dec2dec_flt(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batbte_num2dec_flt(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_num2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		flt r;
		msg = bte_num2dec_flt(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_2_flt(flt *res, sht *v)
{
	/* shortcut nil */
	if (*v == sht_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the sht type, it will
	   always fit */
	*res = (flt) *v;
	return (MAL_SUCCEED);
}

str
batsht_2_flt(int *res, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.sht_2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (flt) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == sht_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (flt) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
sht_dec2_flt(flt *res, int *s1, sht *v)
{
	int scale = *s1;
	flt r;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the sht type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
sht_dec2dec_flt(flt *res, int *S1, sht *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	sht cpyval = *v;
	int s1 = *S1, s2 = *S2;
	flt r;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the flt type is bigger than or equal to the sht type, it will
	   always fit */
	r = (flt) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
sht_num2dec_flt(flt *res, sht *v, int *d2, int *s2)
{
	int zero = 0;
	return sht_dec2dec_flt(res, &zero, v, d2, s2);
}

str
batsht_dec2_flt(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	int scale = *s1;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((flt) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((flt) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batsht_dec2dec_flt(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		flt r;
		msg = sht_dec2dec_flt(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batsht_num2dec_flt(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_num2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		flt r;
		msg = sht_num2dec_flt(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_2_flt(flt *res, int *v)
{
	/* shortcut nil */
	if (*v == int_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the int type, it will
	   always fit */
	*res = (flt) *v;
	return (MAL_SUCCEED);
}

str
batint_2_flt(int *res, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.int_2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (flt) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == int_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (flt) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
int_dec2_flt(flt *res, int *s1, int *v)
{
	int scale = *s1;
	flt r;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the int type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
int_dec2dec_flt(flt *res, int *S1, int *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	int cpyval = *v;
	int s1 = *S1, s2 = *S2;
	flt r;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the flt type is bigger than or equal to the int type, it will
	   always fit */
	r = (flt) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
int_num2dec_flt(flt *res, int *v, int *d2, int *s2)
{
	int zero = 0;
	return int_dec2dec_flt(res, &zero, v, d2, s2);
}

str
batint_dec2_flt(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	int scale = *s1;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((flt) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((flt) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batint_dec2dec_flt(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		flt r;
		msg = int_dec2dec_flt(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batint_num2dec_flt(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_num2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		flt r;
		msg = int_num2dec_flt(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_2_flt(flt *res, wrd *v)
{
	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the wrd type, it will
	   always fit */
	*res = (flt) *v;
	return (MAL_SUCCEED);
}

str
batwrd_2_flt(int *res, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.wrd_2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (flt) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == wrd_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (flt) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
wrd_dec2_flt(flt *res, int *s1, wrd *v)
{
	int scale = *s1;
	flt r;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
wrd_dec2dec_flt(flt *res, int *S1, wrd *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	wrd cpyval = *v;
	int s1 = *S1, s2 = *S2;
	flt r;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the flt type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (flt) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
wrd_num2dec_flt(flt *res, wrd *v, int *d2, int *s2)
{
	int zero = 0;
	return wrd_dec2dec_flt(res, &zero, v, d2, s2);
}

str
batwrd_dec2_flt(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int scale = *s1;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((flt) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((flt) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batwrd_dec2dec_flt(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		flt r;
		msg = wrd_dec2dec_flt(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batwrd_num2dec_flt(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_num2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		flt r;
		msg = wrd_num2dec_flt(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_2_flt(flt *res, lng *v)
{
	/* shortcut nil */
	if (*v == lng_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the lng type, it will
	   always fit */
	*res = (flt) *v;
	return (MAL_SUCCEED);
}

str
batlng_2_flt(int *res, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.lng_2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (flt) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == lng_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (flt) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
lng_dec2_flt(flt *res, int *s1, lng *v)
{
	int scale = *s1;
	flt r;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* since the flt type is bigger than or equal to the lng type, it will
	   always fit */
	r = (flt) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
lng_dec2dec_flt(flt *res, int *S1, lng *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng cpyval = *v;
	int s1 = *S1, s2 = *S2;
	flt r;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = flt_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the flt type is bigger than or equal to the lng type, it will
	   always fit */
	r = (flt) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
lng_num2dec_flt(flt *res, lng *v, int *d2, int *s2)
{
	int zero = 0;
	return lng_dec2dec_flt(res, &zero, v, d2, s2);
}

str
batlng_dec2_flt(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int scale = *s1;
	flt *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2_flt", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_flt, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_flt", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (flt *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((flt) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = flt_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((flt) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batlng_dec2dec_flt(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		flt r;
		msg = lng_dec2dec_flt(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batlng_num2dec_flt(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_num2dec_flt", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_flt, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_flt", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		flt r;
		msg = lng_num2dec_flt(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
bte_2_dbl(dbl *res, bte *v)
{
	/* shortcut nil */
	if (*v == bte_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the bte type, it will
	   always fit */
	*res = (dbl) *v;
	return (MAL_SUCCEED);
}

str
batbte_2_dbl(int *res, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.bte_2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (dbl) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == bte_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (dbl) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
bte_dec2_dbl(dbl *res, int *s1, bte *v)
{
	int scale = *s1;
	dbl r;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the bte type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
bte_dec2dec_dbl(dbl *res, int *S1, bte *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	bte cpyval = *v;
	int s1 = *S1, s2 = *S2;
	dbl r;

	/* shortcut nil */
	if (*v == bte_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the dbl type is bigger than or equal to the bte type, it will
	   always fit */
	r = (dbl) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
bte_num2dec_dbl(dbl *res, bte *v, int *d2, int *s2)
{
	int zero = 0;
	return bte_dec2dec_dbl(res, &zero, v, d2, s2);
}

str
batbte_dec2_dbl(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	bte *p, *q;
	char *msg = NULL;
	int scale = *s1;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (bte *) Tloc(b, BUNfirst(b));
	q = (bte *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((dbl) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == bte_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((dbl) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batbte_dec2dec_dbl(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_dec2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		dbl r;
		msg = bte_dec2dec_dbl(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batbte_num2dec_dbl(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.bte_num2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		bte *v = (bte *) BUNtail(bi, p);
		dbl r;
		msg = bte_num2dec_dbl(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_2_dbl(dbl *res, sht *v)
{
	/* shortcut nil */
	if (*v == sht_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the sht type, it will
	   always fit */
	*res = (dbl) *v;
	return (MAL_SUCCEED);
}

str
batsht_2_dbl(int *res, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.sht_2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (dbl) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == sht_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (dbl) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
sht_dec2_dbl(dbl *res, int *s1, sht *v)
{
	int scale = *s1;
	dbl r;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the sht type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
sht_dec2dec_dbl(dbl *res, int *S1, sht *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	sht cpyval = *v;
	int s1 = *S1, s2 = *S2;
	dbl r;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the dbl type is bigger than or equal to the sht type, it will
	   always fit */
	r = (dbl) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
sht_num2dec_dbl(dbl *res, sht *v, int *d2, int *s2)
{
	int zero = 0;
	return sht_dec2dec_dbl(res, &zero, v, d2, s2);
}

str
batsht_dec2_dbl(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	int scale = *s1;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((dbl) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((dbl) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batsht_dec2dec_dbl(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		dbl r;
		msg = sht_dec2dec_dbl(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batsht_num2dec_dbl(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_num2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		dbl r;
		msg = sht_num2dec_dbl(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_2_dbl(dbl *res, int *v)
{
	/* shortcut nil */
	if (*v == int_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the int type, it will
	   always fit */
	*res = (dbl) *v;
	return (MAL_SUCCEED);
}

str
batint_2_dbl(int *res, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.int_2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (dbl) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == int_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (dbl) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
int_dec2_dbl(dbl *res, int *s1, int *v)
{
	int scale = *s1;
	dbl r;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the int type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
int_dec2dec_dbl(dbl *res, int *S1, int *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	int cpyval = *v;
	int s1 = *S1, s2 = *S2;
	dbl r;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the dbl type is bigger than or equal to the int type, it will
	   always fit */
	r = (dbl) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
int_num2dec_dbl(dbl *res, int *v, int *d2, int *s2)
{
	int zero = 0;
	return int_dec2dec_dbl(res, &zero, v, d2, s2);
}

str
batint_dec2_dbl(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	int scale = *s1;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((dbl) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((dbl) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batint_dec2dec_dbl(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		dbl r;
		msg = int_dec2dec_dbl(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batint_num2dec_dbl(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_num2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		dbl r;
		msg = int_num2dec_dbl(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_2_dbl(dbl *res, wrd *v)
{
	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the wrd type, it will
	   always fit */
	*res = (dbl) *v;
	return (MAL_SUCCEED);
}

str
batwrd_2_dbl(int *res, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.wrd_2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (dbl) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == wrd_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (dbl) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
wrd_dec2_dbl(dbl *res, int *s1, wrd *v)
{
	int scale = *s1;
	dbl r;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
wrd_dec2dec_dbl(dbl *res, int *S1, wrd *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	wrd cpyval = *v;
	int s1 = *S1, s2 = *S2;
	dbl r;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the dbl type is bigger than or equal to the wrd type, it will
	   always fit */
	r = (dbl) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
wrd_num2dec_dbl(dbl *res, wrd *v, int *d2, int *s2)
{
	int zero = 0;
	return wrd_dec2dec_dbl(res, &zero, v, d2, s2);
}

str
batwrd_dec2_dbl(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int scale = *s1;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((dbl) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((dbl) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batwrd_dec2dec_dbl(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		dbl r;
		msg = wrd_dec2dec_dbl(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batwrd_num2dec_dbl(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_num2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		dbl r;
		msg = wrd_num2dec_dbl(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_2_dbl(dbl *res, lng *v)
{
	/* shortcut nil */
	if (*v == lng_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the lng type, it will
	   always fit */
	*res = (dbl) *v;
	return (MAL_SUCCEED);
}

str
batlng_2_dbl(int *res, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.lng_2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (dbl) *p;
	} else {
		for (; p < q; p++, o++)
			if (*p == lng_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (dbl) *p;
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
lng_dec2_dbl(dbl *res, int *s1, lng *v)
{
	int scale = *s1;
	dbl r;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* since the dbl type is bigger than or equal to the lng type, it will
	   always fit */
	r = (dbl) *v;
	if (scale)
		r /= scales[scale];
	*res = r;
	return MAL_SUCCEED;
}

str
lng_dec2dec_dbl(dbl *res, int *S1, lng *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng cpyval = *v;
	int s1 = *S1, s2 = *S2;
	dbl r;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = dbl_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "convert", "22003!too many digits (%d > %d)", inlen, p);
	}

	/* since the dbl type is bigger than or equal to the lng type, it will
	   always fit */
	r = (dbl) *v;
	if (s2 > s1)
		r *= scales[s2 - s1];
	else if (s2 != s1)
		r /= scales[s1 - s2];
	*res = r;
	return MAL_SUCCEED;
}

str
lng_num2dec_dbl(dbl *res, lng *v, int *d2, int *s2)
{
	int zero = 0;
	return lng_dec2dec_dbl(res, &zero, v, d2, s2);
}

str
batlng_dec2_dbl(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int scale = *s1;
	dbl *o;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2_dbl", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_dbl, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2_dbl", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (dbl *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++)
			*o = (((dbl) *p) / scales[scale]);
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = dbl_nil;
				bn->T->nonil = FALSE;
			} else
				*o = (((dbl) *p) / scales[scale]);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batlng_dec2dec_dbl(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		dbl r;
		msg = lng_dec2dec_dbl(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batlng_num2dec_dbl(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_num2dec_dbl", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_dbl, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_dbl", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		dbl r;
		msg = lng_num2dec_dbl(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
sht_2_bte(bte *res, sht *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
		*res = (bte) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
batsht_2_bte(int *res, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	bte *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.sht_2_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
				*o = (bte) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
					*o = (bte) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
sht_dec2_bte(bte *res, int *s1, sht *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
sht_dec2dec_bte(bte *res, int *S1, sht *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == sht_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "sht_2_bte", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
sht_num2dec_bte(bte *res, sht *v, int *d2, int *s2)
{
	int zero = 0;
	return sht_dec2dec_bte(res, &zero, v, d2, s2);
}

str
batsht_dec2_bte(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	sht *p, *q;
	char *msg = NULL;
	int scale = *s1;
	bte *o;
	sht val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.decsht_2_bte", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (sht *) Tloc(b, BUNfirst(b));
	q = (sht *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (sht) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (sht) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_bte_min && val <= GDK_bte_max)
				*o = (bte) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == sht_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (sht) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (sht) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_bte_min && val <= GDK_bte_max)
					*o = (bte) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batsht_dec2dec_bte(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_dec2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		bte r;
		msg = sht_dec2dec_bte(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batsht_num2dec_bte(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.sht_num2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		sht *v = (sht *) BUNtail(bi, p);
		bte r;
		msg = sht_num2dec_bte(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_2_bte(bte *res, int *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
		*res = (bte) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
batint_2_bte(int *res, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	bte *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.int_2_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
				*o = (bte) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
					*o = (bte) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
int_dec2_bte(bte *res, int *s1, int *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
int_dec2dec_bte(bte *res, int *S1, int *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "int_2_bte", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
int_num2dec_bte(bte *res, int *v, int *d2, int *s2)
{
	int zero = 0;
	return int_dec2dec_bte(res, &zero, v, d2, s2);
}

str
batint_dec2_bte(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	int scale = *s1;
	bte *o;
	int val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.decint_2_bte", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (int) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_bte_min && val <= GDK_bte_max)
				*o = (bte) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (int) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_bte_min && val <= GDK_bte_max)
					*o = (bte) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batint_dec2dec_bte(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		bte r;
		msg = int_dec2dec_bte(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batint_num2dec_bte(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_num2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		bte r;
		msg = int_num2dec_bte(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_2_bte(bte *res, wrd *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
		*res = (bte) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
batwrd_2_bte(int *res, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	bte *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.wrd_2_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
				*o = (bte) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
					*o = (bte) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
wrd_dec2_bte(bte *res, int *s1, wrd *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
wrd_dec2dec_bte(bte *res, int *S1, wrd *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "wrd_2_bte", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
wrd_num2dec_bte(bte *res, wrd *v, int *d2, int *s2)
{
	int zero = 0;
	return wrd_dec2dec_bte(res, &zero, v, d2, s2);
}

str
batwrd_dec2_bte(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int scale = *s1;
	bte *o;
	wrd val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.decwrd_2_bte", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (wrd) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_bte_min && val <= GDK_bte_max)
				*o = (bte) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (wrd) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_bte_min && val <= GDK_bte_max)
					*o = (bte) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batwrd_dec2dec_bte(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		bte r;
		msg = wrd_dec2dec_bte(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batwrd_num2dec_bte(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_num2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		bte r;
		msg = wrd_num2dec_bte(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_2_bte(bte *res, lng *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
		*res = (bte) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
batlng_2_bte(int *res, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	bte *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.lng_2_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
				*o = (bte) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (bte) val > (lng) GDK_bte_min && val > (lng) GDK_bte_min && val <= (lng) GDK_bte_max) {
					*o = (bte) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
lng_dec2_bte(bte *res, int *s1, lng *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
lng_dec2dec_bte(bte *res, int *S1, lng *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = bte_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "lng_2_bte", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_bte_min && val <= GDK_bte_max) {
		*res = (bte) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", val);
	}
}

str
lng_num2dec_bte(bte *res, lng *v, int *d2, int *s2)
{
	int zero = 0;
	return lng_dec2dec_bte(res, &zero, v, d2, s2);
}

str
batlng_dec2_bte(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int scale = *s1;
	bte *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2_bte", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_bte, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.declng_2_bte", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (bte *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (lng) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_bte_min && val <= GDK_bte_max)
				*o = (bte) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = bte_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (lng) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_bte_min && val <= GDK_bte_max)
					*o = (bte) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type bte", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batlng_dec2dec_bte(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		bte r;
		msg = lng_dec2dec_bte(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batlng_num2dec_bte(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_num2dec_bte", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_bte, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_bte", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		bte r;
		msg = lng_num2dec_bte(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
int_2_sht(sht *res, int *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
		*res = (sht) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
batint_2_sht(int *res, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	sht *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.int_2_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
				*o = (sht) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
					*o = (sht) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
int_dec2_sht(sht *res, int *s1, int *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_sht_min && val <= GDK_sht_max) {
		*res = (sht) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
int_dec2dec_sht(sht *res, int *S1, int *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == int_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "int_2_sht", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_sht_min && val <= GDK_sht_max) {
		*res = (sht) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
int_num2dec_sht(sht *res, int *v, int *d2, int *s2)
{
	int zero = 0;
	return int_dec2dec_sht(res, &zero, v, d2, s2);
}

str
batint_dec2_sht(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	int *p, *q;
	char *msg = NULL;
	int scale = *s1;
	sht *o;
	int val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.decint_2_sht", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (int *) Tloc(b, BUNfirst(b));
	q = (int *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (int) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_sht_min && val <= GDK_sht_max)
				*o = (sht) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == int_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (int) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (int) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_sht_min && val <= GDK_sht_max)
					*o = (sht) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batint_dec2dec_sht(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_dec2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		sht r;
		msg = int_dec2dec_sht(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batint_num2dec_sht(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.int_num2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		int *v = (int *) BUNtail(bi, p);
		sht r;
		msg = int_num2dec_sht(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_2_sht(sht *res, wrd *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
		*res = (sht) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
batwrd_2_sht(int *res, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	sht *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.wrd_2_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
				*o = (sht) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
					*o = (sht) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
wrd_dec2_sht(sht *res, int *s1, wrd *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_sht_min && val <= GDK_sht_max) {
		*res = (sht) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
wrd_dec2dec_sht(sht *res, int *S1, wrd *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "wrd_2_sht", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_sht_min && val <= GDK_sht_max) {
		*res = (sht) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
wrd_num2dec_sht(sht *res, wrd *v, int *d2, int *s2)
{
	int zero = 0;
	return wrd_dec2dec_sht(res, &zero, v, d2, s2);
}

str
batwrd_dec2_sht(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int scale = *s1;
	sht *o;
	wrd val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.decwrd_2_sht", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (wrd) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_sht_min && val <= GDK_sht_max)
				*o = (sht) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (wrd) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_sht_min && val <= GDK_sht_max)
					*o = (sht) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batwrd_dec2dec_sht(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		sht r;
		msg = wrd_dec2dec_sht(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batwrd_num2dec_sht(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_num2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		sht r;
		msg = wrd_num2dec_sht(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_2_sht(sht *res, lng *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
		*res = (sht) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
batlng_2_sht(int *res, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	sht *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.lng_2_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
				*o = (sht) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (sht) val > (lng) GDK_sht_min && val > (lng) GDK_sht_min && val <= (lng) GDK_sht_max) {
					*o = (sht) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
lng_dec2_sht(sht *res, int *s1, lng *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_sht_min && val <= GDK_sht_max) {
		*res = (sht) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
lng_dec2dec_sht(sht *res, int *S1, lng *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = sht_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "lng_2_sht", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_sht_min && val <= GDK_sht_max) {
		*res = (sht) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", val);
	}
}

str
lng_num2dec_sht(sht *res, lng *v, int *d2, int *s2)
{
	int zero = 0;
	return lng_dec2dec_sht(res, &zero, v, d2, s2);
}

str
batlng_dec2_sht(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int scale = *s1;
	sht *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2_sht", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_sht, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.declng_2_sht", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (sht *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (lng) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_sht_min && val <= GDK_sht_max)
				*o = (sht) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = sht_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (lng) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_sht_min && val <= GDK_sht_max)
					*o = (sht) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type sht", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batlng_dec2dec_sht(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		sht r;
		msg = lng_dec2dec_sht(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batlng_num2dec_sht(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_num2dec_sht", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_sht, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_sht", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		sht r;
		msg = lng_num2dec_sht(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
wrd_2_int(int *res, wrd *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (int) val > (lng) GDK_int_min && val > (lng) GDK_int_min && val <= (lng) GDK_int_max) {
		*res = (int) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
	}
}

str
batwrd_2_int(int *res, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.wrd_2_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (int) val > (lng) GDK_int_min && val > (lng) GDK_int_min && val <= (lng) GDK_int_max) {
				*o = (int) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (int) val > (lng) GDK_int_min && val > (lng) GDK_int_min && val <= (lng) GDK_int_max) {
					*o = (int) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
wrd_dec2_int(int *res, int *s1, wrd *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_int_min && val <= GDK_int_max) {
		*res = (int) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
	}
}

str
wrd_dec2dec_int(int *res, int *S1, wrd *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == wrd_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "wrd_2_int", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_int_min && val <= GDK_int_max) {
		*res = (int) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
	}
}

str
wrd_num2dec_int(int *res, wrd *v, int *d2, int *s2)
{
	int zero = 0;
	return wrd_dec2dec_int(res, &zero, v, d2, s2);
}

str
batwrd_dec2_int(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	wrd *p, *q;
	char *msg = NULL;
	int scale = *s1;
	int *o;
	wrd val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.decwrd_2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (wrd *) Tloc(b, BUNfirst(b));
	q = (wrd *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (wrd) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_int_min && val <= GDK_int_max)
				*o = (int) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == wrd_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (wrd) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (wrd) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_int_min && val <= GDK_int_max)
					*o = (int) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batwrd_dec2dec_int(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_dec2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		int r;
		msg = wrd_dec2dec_int(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batwrd_num2dec_int(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.wrd_num2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		wrd *v = (wrd *) BUNtail(bi, p);
		int r;
		msg = wrd_num2dec_int(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_2_int(int *res, lng *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (int) val > (lng) GDK_int_min && val > (lng) GDK_int_min && val <= (lng) GDK_int_max) {
		*res = (int) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
	}
}

str
batlng_2_int(int *res, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.lng_2_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (int) val > (lng) GDK_int_min && val > (lng) GDK_int_min && val <= (lng) GDK_int_max) {
				*o = (int) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (int) val > (lng) GDK_int_min && val > (lng) GDK_int_min && val <= (lng) GDK_int_max) {
					*o = (int) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
lng_dec2_int(int *res, int *s1, lng *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_int_min && val <= GDK_int_max) {
		*res = (int) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
	}
}

str
lng_dec2dec_int(int *res, int *S1, lng *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = int_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "lng_2_int", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_int_min && val <= GDK_int_max) {
		*res = (int) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", val);
	}
}

str
lng_num2dec_int(int *res, lng *v, int *d2, int *s2)
{
	int zero = 0;
	return lng_dec2dec_int(res, &zero, v, d2, s2);
}

str
batlng_dec2_int(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int scale = *s1;
	int *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2_int", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_int, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.declng_2_int", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (int *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (lng) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_int_min && val <= GDK_int_max)
				*o = (int) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = int_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (lng) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_int_min && val <= GDK_int_max)
					*o = (int) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type int", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batlng_dec2dec_int(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		int r;
		msg = lng_dec2dec_int(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batlng_num2dec_int(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_num2dec_int", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_int, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_int", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		int r;
		msg = lng_num2dec_int(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
lng_2_wrd(wrd *res, lng *v)
{
	lng val = *v;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* see if the number fits in the data type */
	if ((lng) (wrd) val > (lng) GDK_wrd_min && val > (lng) GDK_wrd_min && val <= (lng) GDK_wrd_max) {
		*res = (wrd) val;
		return (MAL_SUCCEED);
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type wrd", val);
	}
}

str
batlng_2_wrd(int *res, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	wrd *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.lng_2_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(bn, b->hseqbase);
	bn->H->nonil = 1;
	bn->T->nonil = 1;
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			val = *p;
			/* see if the number fits in the data type */
			if ((lng) (wrd) val > (lng) GDK_wrd_min && val > (lng) GDK_wrd_min && val <= (lng) GDK_wrd_max) {
				*o = (wrd) val;
			} else {
				msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type wrd", val);
				break;
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else {
				val = *p;
				/* see if the number fits in the data type */
				if ((lng) (wrd) val > (lng) GDK_wrd_min && val > (lng) GDK_wrd_min && val <= (lng) GDK_wrd_max) {
					*o = (wrd) val;
				} else {
					msg = createException(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type wrd", val);
					break;
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
lng_dec2_wrd(wrd *res, int *s1, lng *v)
{
	int scale = *s1;
	lng val = *v, h = (val < 0) ? -5 : 5;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	if (scale)
		val = (val + h * scales[scale - 1]) / scales[scale];
	/* see if the number fits in the data type */
	if (val > GDK_wrd_min && val <= GDK_wrd_max) {
		*res = (wrd) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type wrd", val);
	}
}

str
lng_dec2dec_wrd(wrd *res, int *S1, lng *v, int *d2, int *S2)
{
	int p = *d2, inlen = 1;
	lng val = *v, cpyval = val, h = (val < 0) ? -5 : 5;
	int s1 = *S1, s2 = *S2;

	/* shortcut nil */
	if (*v == lng_nil) {
		*res = wrd_nil;
		return (MAL_SUCCEED);
	}

	/* count the number of digits in the input */
	while (cpyval /= 10)
		inlen++;
	/* rounding is allowed */
	inlen += (s2 - s1);
	if (p && inlen > p) {
		throw(SQL, "lng_2_wrd", "22003!too many digits (%d > %d)", inlen, p);
	}

	if (s2 > s1)
		val *= scales[s2 - s1];
	else if (s2 != s1)
		val = (val + h * scales[s1 - s2 - 1]) / scales[s1 - s2];

	/* see if the number fits in the data type */
	if (val > GDK_wrd_min && val <= GDK_wrd_max) {
		*res = (wrd) val;
		return MAL_SUCCEED;
	} else {
		throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type wrd", val);
	}
}

str
lng_num2dec_wrd(wrd *res, lng *v, int *d2, int *s2)
{
	int zero = 0;
	return lng_dec2dec_wrd(res, &zero, v, d2, s2);
}

str
batlng_dec2_wrd(int *res, int *s1, int *bid)
{
	BAT *b, *bn;
	lng *p, *q;
	char *msg = NULL;
	int scale = *s1;
	wrd *o;
	lng val;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2_wrd", "Cannot access descriptor");
	}
	bn = BATnew(TYPE_void, TYPE_wrd, BATcount(b));
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.declng_2_wrd", MAL_MALLOC_FAIL);
	}
	bn->hsorted = b->hsorted;
	bn->hrevsorted = b->hrevsorted;
	BATseqbase(bn, b->hseqbase);
	o = (wrd *) Tloc(bn, BUNfirst(bn));
	p = (lng *) Tloc(b, BUNfirst(b));
	q = (lng *) Tloc(b, BUNlast(b));
	bn->T->nonil = 1;
	if (b->T->nonil) {
		for (; p < q; p++, o++) {
			if (scale)
				val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
			else
				val = (lng) (*p);
			/* see if the number fits in the data type */
			if (val > GDK_wrd_min && val <= GDK_wrd_max)
				*o = (wrd) val;
			else {
				BBPreleaseref(b->batCacheid);
				BBPreleaseref(bn->batCacheid);
				throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type wrd", (lng) val);
			}
		}
	} else {
		for (; p < q; p++, o++) {
			if (*p == lng_nil) {
				*o = wrd_nil;
				bn->T->nonil = FALSE;
			} else {
				if (scale)
					val = (lng) ((*p + (*p < 0 ? -5 : 5) * scales[scale - 1]) / scales[scale]);
				else
					val = (lng) (*p);
				/* see if the number fits in the data type */
				if (val > GDK_wrd_min && val <= GDK_wrd_max)
					*o = (wrd) val;
				else {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(bn->batCacheid);
					throw(SQL, "convert", "22003!value (" LLFMT ") exceeds limits of type wrd", (lng) val);
				}
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	BATkey(BATmirror(bn), FALSE);

	if (!(bn->batDirty & 2))
		bn = BATsetaccess(bn, BAT_READ);

	if (b->htype != bn->htype) {
		BAT *r = VIEWcreate(b, bn);

		BBPkeepref(*res = r->batCacheid);
		BBPreleaseref(bn->batCacheid);
		BBPreleaseref(b->batCacheid);
		return msg;
	}
	BBPkeepref(*res = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
	return msg;
}

str
batlng_dec2dec_wrd(int *res, int *S1, int *bid, int *d2, int *S2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_dec2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.dec2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		wrd r;
		msg = lng_dec2dec_wrd(&r, S1, v, d2, S2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batlng_num2dec_wrd(int *res, int *bid, int *d2, int *s2)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.lng_num2dec_wrd", "Cannot access descriptor");
	}
	bi = bat_iterator(b);
	dst = BATnew(b->htype, TYPE_wrd, BATcount(b));
	if (dst == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(SQL, "sql.num2dec_wrd", MAL_MALLOC_FAIL);
	}
	BATseqbase(dst, b->hseqbase);
	BATloop(b, p, q) {
		lng *v = (lng *) BUNtail(bi, p);
		wrd r;
		msg = lng_num2dec_wrd(&r, v, d2, s2);
		if (msg)
			break;
		BUNins(dst, BUNhead(bi, p), &r, FALSE);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}
