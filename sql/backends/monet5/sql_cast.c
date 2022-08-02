/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "sql_result.h"
#include "sql_cast.h"
#include "sql_gencode.h"
#include "sql_storage.h"
#include "sql_scenario.h"
#include "store_sequence.h"
#include "sql_optimizer.h"
#include "sql_datetime.h"
#include "rel_optimizer.h"
#include "rel_distribute.h"
#include "rel_select.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "opt_pipes.h"
#include "clients.h"
#include "mal_instruction.h"

str
nil_2_timestamp(timestamp *res, const void *val)
{
	(void) val;
	*res = timestamp_nil;
	return MAL_SUCCEED;
}

str
str_2_timestamp(timestamp *res, const str *val)
{
	ssize_t pos = 0;
	timestamp tp = 0, *conv = &tp;
	str buf = *val;

	if (strNil(buf)) {
		*res = timestamp_nil;
	} else {
		pos = timestamp_fromstr(buf, &(size_t){sizeof(timestamp)}, &conv, false);
		if (pos < (ssize_t) strlen(buf) || /* includes pos < 0 */ ATOMcmp(TYPE_timestamp, conv, ATOMnilptr(TYPE_timestamp)) == 0)
			return createException(SQL, "calc.str_2_date", SQLSTATE(22007) "Timestamp '%s' has incorrect format", buf);
		else
			*res = *conv;
	}
	return MAL_SUCCEED;
}

str
batnil_2_timestamp(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		if (BUNappend(dst, &timestamp_nil, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
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
		throw(SQL, "batcalc.str_2_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtvar(bi, p);
		timestamp r;
		msg = str_2_timestamp(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
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
	ssize_t pos = 0;
	daytime dt = 0, *conv = &dt;
	str buf = *val;

	if (strNil(buf)) {
		*res = daytime_nil;
	} else {
		pos = daytime_fromstr(buf, &(size_t){sizeof(daytime)}, &conv, false);
		if (pos < (ssize_t) strlen(buf) || /* includes pos < 0 */ ATOMcmp(TYPE_daytime, conv, ATOMnilptr(TYPE_daytime)) == 0)
			return createException(SQL, "calc.str_2_date", SQLSTATE(22007) "Time '%s' has incorrect format", buf);
		else
			*res = *conv;
	}
	return MAL_SUCCEED;
}

str
batnil_2_daytime(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	daytime r = daytime_nil;
	BATloop(b, p, q) {
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
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
		throw(SQL, "batcalc.str_2_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtvar(bi, p);
		daytime r;
		msg = str_2_daytime(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
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
	ssize_t pos = 0;
	date dt = 0, *conv = &dt;
	str buf = *val;

	if (strNil(buf)) {
		*res = date_nil;
	} else {
		pos = date_fromstr(buf, &(size_t){sizeof(date)}, &conv, false);
		if (pos < (ssize_t) strlen(buf) || /* includes pos < 0 */ ATOMcmp(TYPE_date, conv, ATOMnilptr(TYPE_date)) == 0)
			return createException(SQL, "calc.str_2_date", SQLSTATE(22007) "Date '%s' has incorrect format", buf);
		else
			*res = *conv;
	}
	return MAL_SUCCEED;
}

str
SQLdate_2_str(str *res, const date *val)
{
	char *p = NULL;
	size_t len = 0;
	if (date_tostr(&p, &len, val, false) < 0) {
		GDKfree(p);
		throw(SQL, "date", GDK_EXCEPTION);
	}
	*res = p;
	return MAL_SUCCEED;
}

str
batnil_2_date(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BUN p, q;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2_date", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_date, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	date r = date_nil;
	BATloop(b, p, q) {
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
batnil_ce_2_date(bat *res, const bat *bid, const bat *r)
{
	(void)r;
	return batnil_2_date(res, bid);
}

str
batstr_2_date(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_date", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_date, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtvar(bi, p);
		date r;
		msg = str_2_date(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batstr_ce_2_date(bat *res, const bat *bid, const bat *r)
{
	BAT *b, *c, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;
	bit *ce;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_date", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((c = BATdescriptor(*r)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc.str_2_date", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	ce = Tloc(c, 0);
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_date, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(c->batCacheid);
		throw(SQL, "sql.2_date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtvar(bi, p);
		date r;
		if(ce[p])
			msg = str_2_date(&r, &v);
		else
			r = date_nil;
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			BBPunfix(c->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(c->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(c->batCacheid);
	return msg;
}

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
batstr_2_blob(bat *res, const bat *bid)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_blob", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_blob, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.2_blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtvar(bi, p);
		blob *r;
		msg = str_2_blob(&r, &v);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(r);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batstr_ce_2_blob(bat *res, const bat *bid, const bat *r)
{
	BAT *b, *c, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;
	bit *ce;
	const blob *n;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2_blob", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if ((c = BATdescriptor(*r)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "batcalc.str_2_blob", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	ce = Tloc(c, 0);
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_blob, BATcount(b), TRANSIENT);
	n = ATOMnilptr(TYPE_blob);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(c->batCacheid);
		throw(SQL, "sql.2_blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		str v = (str) BUNtvar(bi, p);
		blob *r;
		if (ce[p])
			msg = str_2_blob(&r, &v);
		else
			r = (blob*)n;
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			BBPunfix(c->batCacheid);
			return msg;
		}
		if (BUNappend(dst, r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(c->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(r);
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(c->batCacheid);
	return msg;
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

/* str SQLbatstr_cast(int *res, int *eclass, int *d1, int *s1, int *has_tz, int *bid, int *digits, [ bat[:int] *condexec] ); */
str
SQLbatstr_cast(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b, *dst, *ce = NULL;
	BATiter bi;
	BUN p, q;
	mvc *m = NULL;
	str msg;
	char *r = NULL;
	bat *res = getArgReference_bat(stk, pci, 0);
	sql_class eclass = (sql_class) *getArgReference_int(stk, pci, 1);
	int *d1 = getArgReference_int(stk, pci, 2);
	int *s1 = getArgReference_int(stk, pci, 3);
	int *has_tz = getArgReference_int(stk, pci, 4);
	bat *bid = getArgReference_bat(stk, pci, 5);
	int *digits = getArgReference_int(stk, pci, 6);
	bit *e = NULL;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	if (pci->argc == 8) {
		bid = getArgReference_bat(stk, pci, 7);
		if (bid && (ce = BATdescriptor(*bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(SQL, "batcalc.str", SQLSTATE(HY005) "Cannot access column descriptor");
		}
		assert(BATcount(b) == BATcount(ce));
	}

	bi = bat_iterator(b);
	if (ce)
		e = (bit*)Tloc(ce, 0);
	dst = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		if (ce)
			BBPunfix(ce->batCacheid);
		throw(SQL, "sql.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		ptr v = (ptr) BUNtail(bi, p);
		if (!ce || e[p])
			msg = SQLstr_cast_(&r, m, eclass, *d1, *s1, *has_tz, v, b->ttype, *digits);
		else
			r = (str)str_nil;
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			if (ce)
				BBPunfix(ce->batCacheid);
			return msg;
		}
		if (BUNappend(dst, r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			if (ce)
				BBPunfix(ce->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.str_cast", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (r != str_nil)
			GDKfree(r);
		r = NULL;
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	if (ce)
		BBPunfix(ce->batCacheid);
	return msg;
}

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

#undef round_float
#define round_float(x)	round(x)

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

