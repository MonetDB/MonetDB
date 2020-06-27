/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "sql_scenario.h"
#include "sql_datetime.h"
#include "mal_instruction.h"

static inline daytime
daytime_2time_daytime_imp(daytime input,
#ifdef HAVE_HGE
hge shift, hge divider, hge multiplier
#else
lng shift, lng divider, lng multiplier
#endif
)
{
	return ((input + shift) / divider) * multiplier;
}

str
daytime_2time_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *res = NULL;
	BUN q = 0;
	daytime *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), *digits = getArgReference_int(stk, pci, 2), d = (*digits) ? *digits - 1 : 0;
	bit hasnil = 0;
	bool is_a_bat = false;
	bat *r = NULL;
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.daytime_2time_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_daytime, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.daytime_2time_daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (daytime*) Tloc(res, 0);
	} else {
		ret = (daytime*) getArgReference(stk, pci, 0);
	}

	/* correct fraction */
	if (d < 6) {
		divider *= scales[6 - d];
#ifndef TRUNCATE_NUMBERS
		shift += (scales[6 - d] >> 1);
#endif
		multiplier *= scales[6 - d];
	}

	if (is_a_bat) {
		daytime *restrict vals = (daytime*) Tloc(b, 0);
		for (BUN i = 0 ; i < q ; i++) {
			daytime next = vals[i];
			if (is_daytime_nil(next)) {
				hasnil = 1;
				ret[i] = daytime_nil;
			} else {
				ret[i] = daytime_2time_daytime_imp(next, shift, divider, multiplier);
			}
		}
	} else {
		daytime next = *(daytime*)getArgReference(stk, pci, 1);
		*ret = is_daytime_nil(next) ? daytime_nil : daytime_2time_daytime_imp(next, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
second_interval_2_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	daytime *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, 2);
	bool is_a_bat = false;
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;
	BUN q = 0;
	bit hasnil = 0;
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.second_interval_2_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_daytime, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.second_interval_2_daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (daytime*) Tloc(res, 0);
	} else {
		ret = (daytime*) getArgReference(stk, pci, 0);
	}

	if (digits < 6) {
		divider *= scales[6 - digits];
#ifndef TRUNCATE_NUMBERS
		shift += (scales[6 - digits] >> 1);
#endif
		multiplier *= scales[6 - digits];
	}

	if (is_a_bat) {
		lng *restrict vals = (lng*) Tloc(b, 0);
		for (BUN i = 0 ; i < q && !msg ; i++) {
			lng next = vals[i];
			if (is_lng_nil(next)) {
				hasnil = 1;
				ret[i] = daytime_nil;
			} else {
				daytime d = daytime_add_usec(daytime_create(0, 0, 0, 0), next * 1000);
				ret[i] = daytime_2time_daytime_imp(d, shift, divider, multiplier);
			}
		}
	} else {
		lng next = *(lng*)getArgReference(stk, pci, 1);
		if (is_lng_nil(next)) {
			*ret = daytime_nil;
		} else {
			daytime d = daytime_add_usec(daytime_create(0, 0, 0, 0), next * 1000);
			*ret = daytime_2time_daytime_imp(d, shift, divider, multiplier);
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
nil_2time_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;

	(void) cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		daytime d = daytime_nil;
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1))))
			throw(SQL, "batcalc.nil_2time_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
		res = BATconstant(b->hseqbase, TYPE_daytime, &d, BATcount(b), TRANSIENT);
		BBPunfix(b->batCacheid);
		if (!res)
			throw(SQL, "batcalc.nil_2time_daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		r = getArgReference_bat(stk, pci, 0);
		BBPkeepref(*r = res->batCacheid);
	} else {
		daytime *ret = (daytime*) getArgReference(stk, pci, 0);
		*ret = daytime_nil;
	}
	return MAL_SUCCEED;
}

static str
str_2time_daytimetz_internal(ptr out, ptr in, int tpe, int digits, int tz)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *res = NULL;
	BUN q = 0;
	daytime *restrict ret = NULL;
	int d = (digits) ? digits - 1 : 0;
	bit hasnil = 0;
	bool is_a_bat = false;
	bat *r = NULL;
	size_t len = sizeof(daytime);
	ssize_t pos = 0;
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*(bat*) in))) {
			msg = createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_daytime, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = (bat*) out;
		ret = (daytime*) Tloc(res, 0);
	} else {
		ret = (daytime*) out;
	}

	/* correct fraction */
	if (d < 6) {
		divider *= scales[6 - d];
#ifndef TRUNCATE_NUMBERS
		shift += (scales[6 - d] >> 1);
#endif
		multiplier *= scales[6 - d];
	}

	if (is_a_bat) {
		BATiter it = bat_iterator(b);
		for (BUN i = 0 ; i < q && !msg; i++) {
			str next = BUNtail(it, i);
			if (strNil(next)) {
				hasnil = 1;
				ret[i] = daytime_nil;
			} else {
				daytime conv = 0, *cconv = &conv;
				if (tz)
					pos = daytime_tz_fromstr(next, &len, &cconv, false);
				else
					pos = daytime_fromstr(next, &len, &cconv, false);
				if (pos < (ssize_t) strlen(next) || /* includes pos < 0 */ ATOMcmp(TYPE_daytime, cconv, ATOMnilptr(TYPE_daytime)) == 0) {
					msg = createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(22007) "Daytime (%s) has incorrect format", next);
				} else {
					ret[i] = daytime_2time_daytime_imp(*cconv, shift, divider, multiplier);
				}
			}
		}
	} else {
		str next = *(str*)in;
		if (strNil(next)) {
			*ret = daytime_nil;
		} else {
			daytime conv = 0, *cconv = &conv;
			if (tz)
				pos = daytime_tz_fromstr(next, &len, &cconv, false);
			else
				pos = daytime_fromstr(next, &len, &cconv, false);
			if (pos < (ssize_t) strlen(next) || /* includes pos < 0 */ ATOMcmp(TYPE_daytime, cconv, ATOMnilptr(TYPE_daytime)) == 0) {
				msg = createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(22007) "Daytime (%s) has incorrect format", next);
			} else {
				*ret = daytime_2time_daytime_imp(*cconv, shift, divider, multiplier);
			}
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
str_2time_daytimetz(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, 2), tz = *getArgReference_int(stk, pci, 3);
	(void) cntxt;
	return str_2time_daytimetz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), tpe, digits, tz); 
}

str
batstr_2time_daytime(bat *res, const bat *bid, const int *digits)
{
	return str_2time_daytimetz_internal((ptr) res, (ptr) bid, newBatType(TYPE_str), *digits, 0);
}

str
str_2time_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, 2);
	(void) cntxt;
	return str_2time_daytimetz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), tpe, digits, 0);
}

static inline daytime
timestamp_2_daytime_imp(timestamp input,
#ifdef HAVE_HGE
hge shift, hge divider, hge multiplier
#else
lng shift, lng divider, lng multiplier
#endif
)
{
	daytime dt = timestamp_daytime(input);
	return ((dt + shift) / divider) * multiplier;
}

str
timestamp_2_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *res = NULL;
	BUN q = 0;
	daytime *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), *digits = getArgReference_int(stk, pci, 2), d = (*digits) ? *digits - 1 : 0;
	bit hasnil = 0;
	bool is_a_bat = false;
	bat *r = NULL;
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.timestamp_2_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_daytime, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.timestamp_2_daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (daytime*) Tloc(res, 0);
	} else {
		ret = (daytime*) getArgReference(stk, pci, 0);
	}

	/* correct fraction */
	if (d < 6) {
		divider *= scales[6 - d];
#ifndef TRUNCATE_NUMBERS
		shift += (scales[6 - d] >> 1);
#endif
		multiplier *= scales[6 - d];
	}

	if (is_a_bat) {
		timestamp *restrict vals = (timestamp*) Tloc(b, 0);
		for (BUN i = 0 ; i < q ; i++) {
			timestamp next = vals[i];
			if (is_timestamp_nil(next)) {
				hasnil = 1;
				ret[i] = daytime_nil;
			} else {
				ret[i] = timestamp_2_daytime_imp(next, shift, divider, multiplier);
			}
		}
	} else {
		timestamp next = *(timestamp*)getArgReference(stk, pci, 1);
		*ret = is_timestamp_nil(next) ? daytime_nil : timestamp_2_daytime_imp(next, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
date_2_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *res = NULL;
	BUN q = 0;
	timestamp *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1);
	bit hasnil = 0;
	bool is_a_bat = false;
	bat *r = NULL;

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.date_2_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_timestamp, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.date_2_timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (timestamp*) Tloc(res, 0);
	} else {
		ret = (timestamp*) getArgReference(stk, pci, 0);
	}

	if (is_a_bat) {
		date *restrict vals = (date*) Tloc(b, 0);
		for (BUN i = 0 ; i < q ; i++) {
			ret[i] = timestamp_fromdate(vals[i]);
			hasnil |= is_timestamp_nil(ret[i]);
		}
	} else {
		*ret = timestamp_fromdate(*(date*)getArgReference(stk, pci, 1));
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

static inline timestamp
timestamp_2time_timestamp_imp(timestamp input,
#ifdef HAVE_HGE
hge shift, hge divider, hge multiplier
#else
lng shift, lng divider, lng multiplier
#endif
)
{
	date dt = timestamp_date(input);
	daytime tm = timestamp_daytime(input);
	tm = ((tm + shift) / divider) * multiplier;
	return timestamp_create(dt, tm);
}

str
timestamp_2time_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *res = NULL;
	BUN q = 0;
	timestamp *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), *digits = getArgReference_int(stk, pci, 2), d = (*digits) ? *digits - 1 : 0;
	bit hasnil = 0;
	bool is_a_bat = false;
	bat *r = NULL;
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.timestamp_2time_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_timestamp, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.timestamp_2time_timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (timestamp*) Tloc(res, 0);
	} else {
		ret = (timestamp*) getArgReference(stk, pci, 0);
	}

	/* correct fraction */
	if (d < 6) {
		divider *= scales[6 - d];
#ifndef TRUNCATE_NUMBERS
		shift += (scales[6 - d] >> 1);
#endif
		multiplier *= scales[6 - d];
	}

	if (is_a_bat) {
		timestamp *restrict vals = (timestamp*) Tloc(b, 0);
		for (BUN i = 0 ; i < q ; i++) {
			timestamp next = vals[i];
			if (is_timestamp_nil(next)) {
				hasnil = 1;
				ret[i] = timestamp_nil;
			} else {
				ret[i] = timestamp_2time_timestamp_imp(next, shift, divider, multiplier);
			}
		}
	} else {
		timestamp next = *(timestamp*)getArgReference(stk, pci, 1);
		*ret = is_timestamp_nil(next) ? timestamp_nil : timestamp_2time_timestamp_imp(next, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
nil_2time_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;

	(void) cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		timestamp d = timestamp_nil;
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1))))
			throw(SQL, "batcalc.nil_2time_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
		res = BATconstant(b->hseqbase, TYPE_timestamp, &d, BATcount(b), TRANSIENT);
		BBPunfix(b->batCacheid);
		if (!res)
			throw(SQL, "batcalc.nil_2time_timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		r = getArgReference_bat(stk, pci, 0);
		BBPkeepref(*r = res->batCacheid);
	} else {
		timestamp *ret = (timestamp*) getArgReference(stk, pci, 0);
		*ret = timestamp_nil;
	}
	return MAL_SUCCEED;
}

static str
str_2time_timestamptz_internal(ptr out, ptr in, int tpe, int digits, int tz)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *res = NULL;
	BUN q = 0;
	timestamp *restrict ret = NULL;
	int d = (digits) ? digits - 1 : 0;
	bit hasnil = 0;
	bool is_a_bat = false;
	bat *r = NULL;
	size_t len = sizeof(timestamp);
	ssize_t pos = 0;
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*(bat*) in))) {
			msg = createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_timestamp, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = (bat*) out;
		ret = (timestamp*) Tloc(res, 0);
	} else {
		ret = (timestamp*) out;
	}

	/* correct fraction */
	if (d < 6) {
		divider *= scales[6 - d];
#ifndef TRUNCATE_NUMBERS
		shift += (scales[6 - d] >> 1);
#endif
		multiplier *= scales[6 - d];
	}

	if (is_a_bat) {
		BATiter it = bat_iterator(b);
		for (BUN i = 0 ; i < q && !msg; i++) {
			str next = BUNtail(it, i);
			if (strNil(next)) {
				hasnil = 1;
				ret[i] = timestamp_nil;
			} else {
				timestamp conv = 0, *cconv = &conv;
				if (tz)
					pos = timestamp_tz_fromstr(next, &len, &cconv, false);
				else
					pos = timestamp_fromstr(next, &len, &cconv, false);
				if (!pos || pos < (ssize_t) strlen(next) || ATOMcmp(TYPE_timestamp, cconv, ATOMnilptr(TYPE_timestamp)) == 0) {
					msg = createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(22007) "Timestamp (%s) has incorrect format", next);
				} else {
					ret[i] = timestamp_2time_timestamp_imp(*cconv, shift, divider, multiplier);
				}
			}
		}
	} else {
		str next = *(str*)in;
		if (strNil(next)) {
			*ret = timestamp_nil;
		} else {
			timestamp conv = 0, *cconv = &conv;
			if (tz)
				pos = timestamp_tz_fromstr(next, &len, &cconv, false);
			else
				pos = timestamp_fromstr(next, &len, &cconv, false);
			if (!pos || pos < (ssize_t) strlen(next) || ATOMcmp(TYPE_timestamp, cconv, ATOMnilptr(TYPE_timestamp)) == 0) {
				msg = createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(22007) "Timestamp (%s) has incorrect format", next);
			} else {
				*ret = timestamp_2time_timestamp_imp(*cconv, shift, divider, multiplier);
			}
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
str_2time_timestamptz(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, 2), tz = *getArgReference_int(stk, pci, 3);
	(void) cntxt;
	return str_2time_timestamptz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), tpe, digits, tz); 
}

str
batstr_2time_timestamptz(bat *res, const bat *bid, const int *digits, int *tz)
{
	return str_2time_timestamptz_internal((ptr) res, (ptr) bid, newBatType(TYPE_str), *digits, *tz);
}

str
str_2time_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, 2);
	(void) cntxt;
	return str_2time_timestamptz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), tpe, digits, 0);
}

str
batstr_2time_timestamp(bat *res, const bat *bid, const int *digits)
{
	return str_2time_timestamptz_internal((ptr) res, (ptr) bid, newBatType(TYPE_str), *digits, 0);
}

str
month_interval_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int *restrict ret = NULL;
	int d = *getArgReference_int(stk, pci, 2), sk = *getArgReference_int(stk, pci, 3), tpe = getArgType(mb, pci, 1);
	bool is_a_bat = false;
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;
	BUN q = 0;
	bit hasnil = 0;

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.month_interval_str", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.month_interval_str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (int*) Tloc(res, 0);
	} else {
		ret = getArgReference_int(stk, pci, 0);
	}

	if (is_a_bat) {
		BATiter bi = bat_iterator(b);
		for (BUN i = 0 ; i < q ; i++) {
			const str next = BUNtail(bi, i);

			if (strNil(next)) {
				ret[i] = int_nil;
				hasnil = 1;
			} else {
				lng upcast;
				if (interval_from_str(next, d, sk, &upcast) < 0) {
					msg = createException(SQL, "batcalc.month_interval_str", SQLSTATE(42000) "Wrong format (%s)", next);
					goto bailout;
				}
				assert((lng) GDK_int_min <= upcast && upcast <= (lng) GDK_int_max);
				ret[i] = (int) upcast;
			}
		}
	} else {
		const str next = *getArgReference_str(stk, pci, 1);

		if (strNil(next)) {
			*ret = int_nil;
		} else {
			lng upcast;
			if (interval_from_str(next, d, sk, &upcast) < 0) {
				msg = createException(SQL, "batcalc.month_interval_str", SQLSTATE(42000) "Wrong format (%s)", next);
				goto bailout;
			}
			assert((lng) GDK_int_min <= upcast && upcast <= (lng) GDK_int_max);
			*ret = (int) upcast;
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
second_interval_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng *restrict ret = NULL;
	int d = *getArgReference_int(stk, pci, 2), sk = *getArgReference_int(stk, pci, 3), tpe = getArgType(mb, pci, 1);
	bool is_a_bat = false;
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;
	BUN q = 0;
	bit hasnil = 0;

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.second_interval_str", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_lng, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.second_interval_str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (lng*) Tloc(res, 0);
	} else {
		ret = getArgReference_lng(stk, pci, 0);
	}

	if (is_a_bat) {
		BATiter bi = bat_iterator(b);
		for (BUN i = 0 ; i < q ; i++) {
			const str next = BUNtail(bi, i);

			if (strNil(next)) {
				ret[i] = lng_nil;
				hasnil = 1;
			} else if (interval_from_str(next, d, sk, &(ret[i])) < 0) {
				msg = createException(SQL, "batcalc.second_interval_str", SQLSTATE(42000) "Wrong format (%s)", next);
				goto bailout;
			}
		}
	} else {
		const str next = *getArgReference_str(stk, pci, 1);
		if (strNil(next)) {
			*ret = lng_nil;
		} else if (interval_from_str(next, d, sk, ret) < 0) {
			msg = createException(SQL, "batcalc.second_interval_str", SQLSTATE(42000) "Wrong format (%s)", next);
			goto bailout;
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

#define interval_loop(FUNC, TPE) \
	do { \
		if (is_a_bat) { \
			TPE *restrict vals = Tloc(b, 0); \
			for (BUN i = 0 ; i < q ; i++) \
				FUNC(ret[i], TPE, vals[i]); \
		} else { \
			TPE val = *(TPE*)getArgReference(stk, pci, 1); \
			FUNC(*ret, TPE, val); \
		} \
	} while(0)

#define month_interval_convert(OUT, TPE, IN) \
	do { \
		if (is_##TPE##_nil(IN)) { \
			OUT = int_nil; \
			hasnil = 1; \
		} else { \
			int r = (int) IN; \
			r *= multiplier; \
			OUT = r; \
		} \
	} while (0)

str
month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int *restrict ret = NULL, multiplier = 1;
	int k = digits2ek(*getArgReference_int(stk, pci, 2)), tpe = getArgType(mb, pci, 1);
	bool is_a_bat = false;
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;
	BUN q = 0;
	bit hasnil = 0;

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.month_interval", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_int, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.month_interval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (int*) Tloc(res, 0);
	} else {
		ret = getArgReference_int(stk, pci, 0);
	}

	switch (k) {
	case iyear:
		multiplier *= 12;
		break;
	case imonth:
		break;
	default: {
		msg = createException(ILLARG, "batcalc.month_interval", SQLSTATE(42000) "Illegal argument");
		goto bailout;
	}
	}

	switch (tpe) {
	case TYPE_bte:
		interval_loop(month_interval_convert, bte);
		break;
	case TYPE_sht:
		interval_loop(month_interval_convert, sht);
		break;
	case TYPE_int:
		interval_loop(month_interval_convert, int);
		break;
	case TYPE_lng:
		interval_loop(month_interval_convert, lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		interval_loop(month_interval_convert, hge);
		break;
#endif
	default: {
		msg = createException(ILLARG, "batcalc.month_interval", SQLSTATE(42000) "Illegal argument in month interval");
	}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

#define second_interval_convert(OUT, TPE, IN) \
	do { \
		if (is_##TPE##_nil(IN)) { \
			OUT = lng_nil; \
			hasnil = 1; \
		} else { \
			lng r = (lng) IN; \
			r *= multiplier; \
			if (scale) { \
				r += shift; \
				r /= divider; \
			} \
			OUT = r; \
		} \
	} while (0)

str
second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng *restrict ret = NULL, multiplier = 1;
	int tpe = getArgType(mb, pci, 1), k = digits2ek(*getArgReference_int(stk, pci, 2)), scale = 0;
	bool is_a_bat = false;
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;
	BUN q = 0;
	bit hasnil = 0;
#ifdef HAVE_HGE
	hge shift = 0, divider = 1;
#else
	lng shift = 0, divider = 1;
#endif

	(void) cntxt;
	if (pci->argc > 3)
		scale = *getArgReference_int(stk, pci, 3);
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.sec_interval", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_lng, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.sec_interval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (lng*) Tloc(res, 0);
	} else {
		ret = getArgReference_lng(stk, pci, 0);
	}

	switch (k) {
	case iday:
		multiplier *= 24;
		/* fall through */
	case ihour:
		multiplier *= 60;
		/* fall through */
	case imin:
		multiplier *= 60;
		/* fall through */
	case isec:
		multiplier *= 1000;
		break;
	default: {
		msg = createException(ILLARG, "batcalc.sec_interval", SQLSTATE(42000) "Illegal argument in second interval");
		goto bailout;
	}
	}
	if (scale) {
#ifndef TRUNCATE_NUMBERS
		shift = scales[scale] >> 1;
#else
		(void) shift;
#endif
		divider = scales[scale];
	}

	switch (tpe) {
	case TYPE_bte:
		interval_loop(second_interval_convert, bte);
		break;
	case TYPE_sht:
		interval_loop(second_interval_convert, sht);
		break;
	case TYPE_int:
		interval_loop(second_interval_convert, int);
		break;
	case TYPE_lng:
		interval_loop(second_interval_convert, lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		interval_loop(second_interval_convert, hge);
		break;
#endif
	default: {
		msg = createException(ILLARG, "batcalc.sec_interval", SQLSTATE(42000) "Illegal argument in second interval");
	}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
second_interval_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng *restrict ret = NULL, multiplier = 1, divider = 1;
	int tpe = getArgType(mb, pci, 1), k = digits2ek(*getArgReference_int(stk, pci, 2));
	bool is_a_bat = false;
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;
	BUN q = 0;
	bit hasnil = 0;

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.second_interval_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
			goto bailout;
		}
		q = BATcount(b);
		if (!(res = COLnew(b->hseqbase, TYPE_lng, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.second_interval_daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (lng*) Tloc(res, 0);
	} else {
		ret = getArgReference_lng(stk, pci, 0);
	}

	switch (k) {
	case isec:
		break;
	case imin:
		divider *= 60000;
		multiplier *= 60000;
		break;
	case ihour:
		divider *= 3600000;
		multiplier *= 3600000;
		break;
	case iday:
		divider *= (24 * 3600000);
		multiplier *= (24 * 3600000);
		break;
	default: {
		msg = createException(ILLARG, "batcalc.second_interval_daytime", SQLSTATE(42000) "Illegal argument in daytime interval");
		goto bailout;
	}
	}

	if (is_a_bat) {
		daytime *restrict vals = (daytime*) Tloc(b, 0);
		for (BUN i = 0 ; i < q ; i++) {
			daytime next = vals[i];
			if (is_daytime_nil(next)) {
				ret[i] = lng_nil;
				hasnil = 1;
			} else {
				ret[i] = (next / divider) * multiplier;
			}
		}
	} else {
		daytime next = *(daytime*)getArgReference(stk, pci, 1);
		*ret = is_daytime_nil(next) ? lng_nil : (next / divider) * multiplier;
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = hasnil;
		res->tnonil = !hasnil;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
SQLcurrent_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	daytime *res = getArgReference_TYPE(stk, pci, 0, daytime);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	*res = timestamp_daytime(timestamp_add_usec(timestamp_current(),
						    m->timezone * LL_CONSTANT(1000)));
	return msg;
}

str
SQLcurrent_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg;
	timestamp *res = getArgReference_TYPE(stk, pci, 0, timestamp);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;

	*res = timestamp_add_usec(timestamp_current(), m->timezone * LL_CONSTANT(1000));
	return msg;
}
