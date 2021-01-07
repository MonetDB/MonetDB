/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
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
	BAT *b = NULL, *s = NULL, *res = NULL;
	BUN q = 0;
	daytime *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), *digits = getArgReference_int(stk, pci, pci->argc == 4 ? 3 : 2), d = (*digits) ? *digits - 1 : 0;
	bool is_a_bat = false, nils = false;
	bat *r = NULL, *sid = pci->argc == 4 ? getArgReference_bat(stk, pci, 2): NULL;
	struct canditer ci = {0};
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	if (d < 0 || (size_t) d >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.daytime_2time_daytime", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.daytime_2time_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.timestamp_2_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_daytime, q, TRANSIENT))) {
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
		oid off = b->hseqbase;
		daytime *restrict vals = (daytime*) Tloc(b, 0);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0; i < q; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				daytime next = vals[p];

				if (is_daytime_nil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					ret[i] = daytime_2time_daytime_imp(next, shift, divider, multiplier);
				}
			}
		} else {
			for (BUN i = 0 ; i < q ; i++) {
				oid p = (canditer_next(&ci) - off);
				daytime next = vals[p];

				if (is_daytime_nil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					ret[i] = daytime_2time_daytime_imp(next, shift, divider, multiplier);
				}
			}
		}
	} else {
		daytime next = *(daytime*)getArgReference(stk, pci, 1);
		*ret = is_daytime_nil(next) ? daytime_nil : daytime_2time_daytime_imp(next, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = b->tkey;
		res->tsorted = b->tsorted;
		res->trevsorted  = b->trevsorted;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

static inline daytime
second_interval_2_daytime_imp(lng next,
#ifdef HAVE_HGE
hge shift, hge divider, hge multiplier
#else
lng shift, lng divider, lng multiplier
#endif
) {
	lng usec = (next % (24*60*60*1000)) * 1000;
	if (usec < 0) /* for negative intervals add the complement */
		usec = DAY_USEC - (-usec);
	daytime d = daytime_add_usec(daytime_create(0, 0, 0, 0), usec);
	assert(!is_daytime_nil(d));
	return daytime_2time_daytime_imp(d, shift, divider, multiplier);
}

str
second_interval_2_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	daytime *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, pci->argc == 4 ? 3 : 2);
	bool is_a_bat = false, nils = false;
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = NULL, *sid = pci->argc == 4 ? getArgReference_bat(stk, pci, 2) : NULL;
	BUN q = 0;
	struct canditer ci = {0};
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	if (digits < 0 || (size_t) digits >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.second_interval_2_daytime", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.second_interval_2_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.second_interval_2_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_daytime, q, TRANSIENT))) {
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
		oid off = b->hseqbase;
		lng *restrict vals = (lng*) Tloc(b, 0);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q ; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				lng next = vals[p];

				if (is_lng_nil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					ret[i] = second_interval_2_daytime_imp(next, shift, divider, multiplier);
				}
			}
		} else {
			for (BUN i = 0 ; i < q ; i++) {
				oid p = (canditer_next(&ci) - off);
				lng next = vals[p];

				if (is_lng_nil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					ret[i] = second_interval_2_daytime_imp(next, shift, divider, multiplier);
				}
			}
		}
	} else {
		lng next = *(lng*)getArgReference(stk, pci, 1);
		*ret = is_lng_nil(next) ? daytime_nil : second_interval_2_daytime_imp(next, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
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
			throw(SQL, "batcalc.nil_2time_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
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

static inline str
str_2time_daytimetz_internal_imp(daytime *ret, str next, ssize_t (*fromstr_func)(const char *, size_t *, daytime **, bool),
#ifdef HAVE_HGE
hge shift, hge divider, hge multiplier
#else
lng shift, lng divider, lng multiplier
#endif
)
{
	ssize_t pos = 0;
	daytime dt = 0, *conv = &dt;

	pos = fromstr_func(next, &(size_t){sizeof(daytime)}, &conv, false);
	if (pos < (ssize_t) strlen(next) || /* includes pos < 0 */ is_daytime_nil(*conv))
		return createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(22007) "Daytime '%s' has incorrect format", next);
	*ret = daytime_2time_daytime_imp(*conv, shift, divider, multiplier);
	return MAL_SUCCEED;
}

static str
str_2time_daytimetz_internal(ptr out, ptr in, const bat *sid, int tpe, int digits, int tz)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *s = NULL, *res = NULL;
	BUN q = 0;
	daytime *restrict ret = NULL;
	int d = (digits) ? digits - 1 : 0;
	bool is_a_bat = false, nils = false;
	bat *r = NULL;
	ssize_t (*fromstr_func)(const char *, size_t *, daytime **, bool) = tz ? daytime_tz_fromstr : daytime_fromstr;
	struct canditer ci = {0};
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	if (d < 0 || (size_t) d >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*(bat*) in))) {
			msg = createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.str_2time_daytimetz", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_daytime, q, TRANSIENT))) {
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
		oid off = b->hseqbase;
		BATiter it = bat_iterator(b);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				str next = BUNtvar(it, p);

				if (strNil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					msg = str_2time_daytimetz_internal_imp(&(ret[i]), next, fromstr_func, shift, divider, multiplier);
				}
			}
		} else {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next(&ci) - off);
				str next = BUNtvar(it, p);

				if (strNil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					msg = str_2time_daytimetz_internal_imp(&(ret[i]), next, fromstr_func, shift, divider, multiplier);
				}
			}
		}
	} else {
		str next = *(str*)in;
		if (strNil(next))
			*ret = daytime_nil;
		else
			msg = str_2time_daytimetz_internal_imp(ret, next, fromstr_func, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
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
	int tpe = getArgType(mb, pci, 1),
		digits = *getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2),
		tz = *getArgReference_int(stk, pci, pci->argc == 5 ? 4 : 3);
	bat *sid = pci->argc == 5 ? getArgReference_bat(stk, pci, 2) : NULL;
	(void) cntxt;
	return str_2time_daytimetz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), sid, tpe, digits, tz);
}

str
batstr_2time_daytime(bat *res, const bat *bid, const bat *s, const int *digits)
{
	return str_2time_daytimetz_internal((ptr) res, (ptr) bid, s, newBatType(TYPE_str), *digits, 0);
}

str
str_2time_daytime(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, 2);
	(void) cntxt;
	return str_2time_daytimetz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), NULL, tpe, digits, 0);
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
	BAT *b = NULL, *s = NULL, *res = NULL;
	BUN q = 0;
	daytime *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), *digits = getArgReference_int(stk, pci, pci->argc == 4 ? 3 : 2), d = (*digits) ? *digits - 1 : 0;
	bool is_a_bat = false, nils = false;
	bat *r = NULL, *sid = pci->argc == 4 ? getArgReference_bat(stk, pci, 2): NULL;
	struct canditer ci = {0};
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	if (d < 0 || (size_t) d >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.timestamp_2_daytime", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.timestamp_2_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.timestamp_2_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_daytime, q, TRANSIENT))) {
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
		oid off = b->hseqbase;
		timestamp *restrict vals = (timestamp*) Tloc(b, 0);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				timestamp next = vals[p];

				if (is_timestamp_nil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					ret[i] = timestamp_2_daytime_imp(next, shift, divider, multiplier);
				}
			}
		} else {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next(&ci) - off);
				timestamp next = vals[p];

				if (is_timestamp_nil(next)) {
					ret[i] = daytime_nil;
					nils = true;
				} else {
					ret[i] = timestamp_2_daytime_imp(next, shift, divider, multiplier);
				}
			}
		}
	} else {
		timestamp next = *(timestamp*)getArgReference(stk, pci, 1);
		*ret = is_timestamp_nil(next) ? daytime_nil : timestamp_2_daytime_imp(next, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
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
	BAT *b = NULL, *s = NULL, *res = NULL;
	BUN q = 0;
	timestamp *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1);
	bool is_a_bat = false, nils = false;
	bat *r = NULL, *sid = pci->argc == 4 ? getArgReference_bat(stk, pci, 2) : NULL;
	struct canditer ci = {0};

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.date_2_timestamp", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.date_2_timestamp", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_timestamp, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.date_2_timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (timestamp*) Tloc(res, 0);
	} else {
		ret = (timestamp*) getArgReference(stk, pci, 0);
	}

	if (is_a_bat) {
		oid off = b->hseqbase;
		date *restrict vals = (date*) Tloc(b, 0);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				ret[i] = timestamp_fromdate(vals[p]);
				nils |= is_timestamp_nil(ret[i]);
			}
		} else {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next(&ci) - off);
				ret[i] = timestamp_fromdate(vals[p]);
				nils |= is_timestamp_nil(ret[i]);
			}
		}
	} else {
		*ret = timestamp_fromdate(*(date*)getArgReference(stk, pci, 1));
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = b->tkey;
		res->tsorted = b->tsorted;
		res->trevsorted  = b->trevsorted;
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
	BAT *b = NULL, *s = NULL, *res = NULL;
	BUN q = 0;
	timestamp *restrict ret = NULL;
	int tpe = getArgType(mb, pci, 1), *digits = getArgReference_int(stk, pci, pci->argc == 4 ? 3 : 2), d = (*digits) ? *digits - 1 : 0;
	bool is_a_bat = false, nils = false;
	bat *r = NULL, *sid = pci->argc == 4 ? getArgReference_bat(stk, pci, 2): NULL;
	struct canditer ci = {0};
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	(void) cntxt;
	if (d < 0 || (size_t) d >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.timestamp_2time_timestamp", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.timestamp_2time_timestamp", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.timestamp_2time_timestamp", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_timestamp, q, TRANSIENT))) {
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
		oid off = b->hseqbase;
		timestamp *restrict vals = (timestamp*) Tloc(b, 0);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				timestamp next = vals[p];

				if (is_timestamp_nil(next)) {
					ret[i] = timestamp_nil;
					nils = true;
				} else {
					ret[i] = timestamp_2time_timestamp_imp(next, shift, divider, multiplier);
				}
			}
		} else {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next(&ci) - off);
				timestamp next = vals[p];

				if (is_timestamp_nil(next)) {
					ret[i] = timestamp_nil;
					nils = true;
				} else {
					ret[i] = timestamp_2time_timestamp_imp(next, shift, divider, multiplier);
				}
			}
		}
	} else {
		timestamp next = *(timestamp*)getArgReference(stk, pci, 1);
		*ret = is_timestamp_nil(next) ? timestamp_nil : timestamp_2time_timestamp_imp(next, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = b->tkey;
		res->tsorted = b->tsorted;
		res->trevsorted  = b->trevsorted;
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
			throw(SQL, "batcalc.nil_2time_timestamp", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
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

static inline str
str_2time_timestamptz_internal_imp(timestamp *ret, str next, ssize_t (*fromstr_func)(const char *, size_t *, timestamp **, bool),
#ifdef HAVE_HGE
hge shift, hge divider, hge multiplier
#else
lng shift, lng divider, lng multiplier
#endif
)
{
	ssize_t pos = 0;
	timestamp tp = 0, *conv = &tp;

	pos = fromstr_func(next, &(size_t){sizeof(timestamp)}, &conv, false);
	if (pos < (ssize_t) strlen(next) || /* includes pos < 0 */ is_timestamp_nil(*conv))
		return createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(22007) "Timestamp '%s' has incorrect format", next);
	*ret = timestamp_2time_timestamp_imp(*conv, shift, divider, multiplier);
	return MAL_SUCCEED;
}

static str
str_2time_timestamptz_internal(ptr out, ptr in, const bat *sid, int tpe, int digits, int tz)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *s = NULL, *res = NULL;
	BUN q = 0;
	timestamp *restrict ret = NULL;
	int d = (digits) ? digits - 1 : 0;
	bool is_a_bat = false, nils = false;
	bat *r = NULL;
	ssize_t (*fromstr_func)(const char *, size_t *, timestamp **, bool) = tz ? timestamp_tz_fromstr : timestamp_fromstr;
	struct canditer ci = {0};
#ifdef HAVE_HGE
	hge shift = 0, divider = 1, multiplier = 1;
#else
	lng shift = 0, divider = 1, multiplier = 1;
#endif

	if (d < 0 || (size_t) d >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*(bat*) in))) {
			msg = createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.str_2time_timestamptz_internal", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_timestamp, q, TRANSIENT))) {
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
		oid off = b->hseqbase;
		BATiter bi = bat_iterator(b);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				str next = BUNtvar(bi, p);

				if (strNil(next)) {
					ret[i] = timestamp_nil;
					nils = true;
				} else {
					msg = str_2time_timestamptz_internal_imp(&(ret[i]), next, fromstr_func, shift, divider, multiplier);
				}
			}
		} else {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next(&ci) - off);
				str next = BUNtvar(bi, p);

				if (strNil(next)) {
					ret[i] = timestamp_nil;
					nils = true;
				} else {
					msg = str_2time_timestamptz_internal_imp(&(ret[i]), next, fromstr_func, shift, divider, multiplier);
				}
			}
		}
	} else {
		str next = *(str*)in;
		if (strNil(next))
			*ret = timestamp_nil;
		else
			msg = str_2time_timestamptz_internal_imp(ret, next, fromstr_func, shift, divider, multiplier);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
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
	int tpe = getArgType(mb, pci, 1),
		digits = *getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2),
		tz = *getArgReference_int(stk, pci, pci->argc == 5 ? 4 : 3);
	(void) cntxt;
	return str_2time_timestamptz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), NULL, tpe, digits, tz);
}

str
batstr_2time_timestamptz(bat *res, const bat *bid, const bat *sid, const int *digits, int *tz)
{
	return str_2time_timestamptz_internal((ptr) res, (ptr) bid, sid, newBatType(TYPE_str), *digits, *tz);
}

str
str_2time_timestamp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1), digits = *getArgReference_int(stk, pci, 2);
	(void) cntxt;
	return str_2time_timestamptz_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), NULL, tpe, digits, 0);
}

str
batstr_2time_timestamp(bat *res, const bat *bid, const bat *sid, const int *digits)
{
	return str_2time_timestamptz_internal((ptr) res, (ptr) bid, sid, newBatType(TYPE_str), *digits, 0);
}

static inline str
month_interval_str_imp(int *ret, str next, int d, int sk)
{
	lng upcast;
	if (interval_from_str(next, d, sk, &upcast) < 0)
		return createException(SQL, "batcalc.month_interval_str", SQLSTATE(42000) "Wrong format (%s)", next);
	assert((lng) GDK_int_min <= upcast && upcast <= (lng) GDK_int_max);
	*ret = (int) upcast;
	return MAL_SUCCEED;
}

str
month_interval_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int *restrict ret = NULL;
	int d = *getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2), sk = *getArgReference_int(stk, pci, pci->argc == 5 ? 4 : 3),
		tpe = getArgType(mb, pci, 1);
	bool is_a_bat = false, nils = false;
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = NULL, *sid = pci->argc == 5 ? getArgReference_bat(stk, pci, 2): NULL;
	BUN q = 0;
	struct canditer ci = {0};

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.month_interval_str", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.month_interval_str", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_int, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.month_interval_str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (int*) Tloc(res, 0);
	} else {
		ret = getArgReference_int(stk, pci, 0);
	}

	if (is_a_bat) {
		oid off = b->hseqbase;
		BATiter bi = bat_iterator(b);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				str next = BUNtvar(bi, p);

				if (strNil(next)) {
					ret[i] = int_nil;
					nils = true;
				} else {
					msg = month_interval_str_imp(&(ret[i]), next, d, sk);
				}
			}
		} else {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next(&ci) - off);
				str next = BUNtvar(bi, p);

				if (strNil(next)) {
					ret[i] = int_nil;
					nils = true;
				} else {
					msg = month_interval_str_imp(&(ret[i]), next, d, sk);
				}
			}
		}
	} else {
		const str next = *getArgReference_str(stk, pci, 1);

		if (strNil(next))
			*ret = int_nil;
		else
			msg = month_interval_str_imp(ret, next, d, sk);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

static inline str
second_interval_str_imp(lng *ret, str next, int d, int sk)
{
	if (interval_from_str(next, d, sk, ret) < 0)
		return createException(SQL, "batcalc.second_interval_str", SQLSTATE(42000) "Wrong format (%s)", next);
	return MAL_SUCCEED;
}

str
second_interval_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng *restrict ret = NULL;
	int d = *getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2), sk = *getArgReference_int(stk, pci, pci->argc == 5 ? 4 : 3),
		tpe = getArgType(mb, pci, 1);
	bool is_a_bat = false, nils = false;
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = NULL, *sid = pci->argc == 5 ? getArgReference_bat(stk, pci, 2): NULL;
	BUN q = 0;
	struct canditer ci = {0};

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.second_interval_str", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.second_interval_str", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_lng, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.second_interval_str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = getArgReference_bat(stk, pci, 0);
		ret = (lng*) Tloc(res, 0);
	} else {
		ret = getArgReference_lng(stk, pci, 0);
	}

	if (is_a_bat) {
		oid off = b->hseqbase;
		BATiter bi = bat_iterator(b);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				str next = BUNtvar(bi, p);

				if (strNil(next)) {
					ret[i] = lng_nil;
					nils = true;
				} else {
					msg = second_interval_str_imp(&(ret[i]), next, d, sk);
				}
			}
		} else {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next(&ci) - off);
				str next = BUNtvar(bi, p);

				if (strNil(next)) {
					ret[i] = lng_nil;
					nils = true;
				} else {
					msg = second_interval_str_imp(&(ret[i]), next, d, sk);
				}
			}
		}
	} else {
		const str next = *getArgReference_str(stk, pci, 1);

		if (strNil(next))
			*ret = lng_nil;
		else
			msg = second_interval_str_imp(ret, next, d, sk);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

#define interval_loop(FUNC, TPE_IN, TPE_OUT, FUNC_NAME, MAX_VALUE, CAST_VALIDATION, MUL_VALIDATION) \
	do { \
		if (is_a_bat) { \
			oid off = b->hseqbase; \
			TPE_IN *restrict vals = Tloc(b, 0); \
			if (ci.tpe == cand_dense) { \
				for (BUN i = 0; i < q; i++) { \
					oid p = (canditer_next_dense(&ci) - off); \
					TPE_IN next = vals[p]; \
					if (is_##TPE_IN##_nil(next)) { \
						ret[i] = TPE_OUT##_nil; \
						nils = true; \
					} else { \
						FUNC(ret[i], TPE_IN, FUNC_NAME, MAX_VALUE, CAST_VALIDATION, MUL_VALIDATION); \
					} \
				} \
			} else { \
				for (BUN i = 0; i < q; i++) { \
					oid p = (canditer_next(&ci) - off); \
					TPE_IN next = vals[p]; \
					if (is_##TPE_IN##_nil(next)) { \
						ret[i] = TPE_OUT##_nil; \
						nils = true; \
					} else { \
						FUNC(ret[i], TPE_IN, FUNC_NAME, MAX_VALUE, CAST_VALIDATION, MUL_VALIDATION); \
					} \
				} \
			} \
		} else { \
			TPE_IN next = *(TPE_IN*)getArgReference(stk, pci, 1); \
			FUNC(*ret, TPE_IN, FUNC_NAME, MAX_VALUE, CAST_VALIDATION, MUL_VALIDATION); \
		} \
	} while(0)

#define month_interval_convert(OUT, TPE_IN, FUNC_NAME, MAX_VALUE, CAST_VALIDATION, MUL_VALIDATION) \
	do { \
		int cast, r; \
		CAST_VALIDATION(TPE_IN, FUNC_NAME, MAX_VALUE); \
		cast = (int) next; \
		r = cast * multiplier; \
		MUL_VALIDATION(TPE_IN, FUNC_NAME, MAX_VALUE); \
		OUT = r; \
	} while (0)

#define DO_NOTHING(TPE_IN, FUNC_NAME, MAX_VALUE) ;

#define CAST_VALIDATION(TPE_IN, FUNC_NAME, MAX_VALUE) \
	do { \
		if (next > (TPE_IN) MAX_VALUE) { \
			size_t len = 0; \
			char *str_val = NULL; \
			if (BATatoms[tpe].atomToStr(&str_val, &len, &next, false) < 0) { \
				msg = createException(SQL, "batcalc." FUNC_NAME, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				goto bailout; \
			} \
			msg = createException(SQL, "batcalc." FUNC_NAME, SQLSTATE(22003) "Value %s too large to fit at a " FUNC_NAME, str_val); \
			GDKfree(str_val); \
			goto bailout; \
		} \
	} while (0)

#define MUL_OVERFLOW(TPE_IN, FUNC_NAME, MAX_VALUE) /* TPE_IN and MAX_VALUE are ignored on this macro */ \
	do { \
		if (r < cast) { \
			size_t len = 0; \
			char *str_val = NULL; \
			if (BATatoms[tpe].atomToStr(&str_val, &len, &cast, false) < 0) { \
				msg = createException(SQL, "batcalc." FUNC_NAME, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				goto bailout; \
			} \
			msg = createException(SQL, "batcalc." FUNC_NAME, SQLSTATE(22003) "Overflow in conversion of %s to " FUNC_NAME, str_val); \
			GDKfree(str_val); \
			goto bailout; \
		} \
	} while (0)

str
month_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int *restrict ret = NULL, multiplier = 1, k = digits2ek(*getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2)),
		tpe = getArgType(mb, pci, 1);
	bool is_a_bat = false, nils = false;
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = NULL, *sid = pci->argc == 5 ? getArgReference_bat(stk, pci, 2): NULL;
	BUN q = 0;
	struct canditer ci = {0};

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.month_interval", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.month_interval", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_int, q, TRANSIENT))) {
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
		interval_loop(month_interval_convert, bte, int, "month_interval", GDK_int_max, DO_NOTHING, DO_NOTHING);
		break;
	case TYPE_sht:
		interval_loop(month_interval_convert, sht, int, "month_interval", GDK_int_max, DO_NOTHING, DO_NOTHING);
		break;
	case TYPE_int:
		interval_loop(month_interval_convert, int, int, "month_interval", GDK_int_max, DO_NOTHING, MUL_OVERFLOW);
		break;
	case TYPE_lng:
		interval_loop(month_interval_convert, lng, int, "month_interval", GDK_int_max, CAST_VALIDATION, MUL_OVERFLOW);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		interval_loop(month_interval_convert, hge, int, "month_interval", GDK_int_max, CAST_VALIDATION, MUL_OVERFLOW);
		break;
#endif
	default: {
		msg = createException(ILLARG, "batcalc.month_interval", SQLSTATE(42000) "Illegal argument in month interval");
	}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = b->tkey;
		res->tsorted = b->tsorted;
		res->trevsorted  = b->trevsorted;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

#define second_interval_convert(OUT, TPE_IN, FUNC_NAME, MAX_VALUE, CAST_VALIDATION, MUL_VALIDATION) \
	do { \
		lng cast, r; \
		CAST_VALIDATION(TPE_IN, FUNC_NAME, MAX_VALUE); \
		cast = (lng) next; \
		r = cast * multiplier; \
		if (scale) { \
			r += shift; \
			r /= divider; \
		} \
		MUL_VALIDATION(TPE_IN, FUNC_NAME, MAX_VALUE); \
		OUT = r; \
	} while (0)

str
second_interval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng *restrict ret = NULL, multiplier = 1;
	int tpe = getArgType(mb, pci, 1), k = digits2ek(*getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2)), 
		scale = *getArgReference_int(stk, pci, pci->argc == 5 ? 4 : 3);
	bool is_a_bat = false, nils = false;
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = NULL, *sid = pci->argc == 5 ? getArgReference_bat(stk, pci, 2) : NULL;
	BUN q = 0;
	struct canditer ci = {0};
#ifdef HAVE_HGE
	hge shift = 0, divider = 1;
#else
	lng shift = 0, divider = 1;
#endif

	(void) cntxt;
	if (scale < 0 || (size_t) scale >= sizeof(scales) / sizeof(scales[0])) {
		msg = createException(SQL, "batcalc.sec_interval", SQLSTATE(42000) "Digits out of bounds");
		goto bailout;
	}
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.sec_interval", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.sec_interval", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_lng, q, TRANSIENT))) {
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
		shift += 5*scales[scale-1];
#endif
		divider = scales[scale];
	}

	switch (tpe) {
	case TYPE_bte:
		interval_loop(second_interval_convert, bte, lng, "sec_interval", GDK_lng_max, DO_NOTHING, MUL_OVERFLOW);
		break;
	case TYPE_sht:
		interval_loop(second_interval_convert, sht, lng, "sec_interval", GDK_lng_max, DO_NOTHING, MUL_OVERFLOW);
		break;
	case TYPE_int:
		interval_loop(second_interval_convert, int, lng, "sec_interval", GDK_lng_max, DO_NOTHING, MUL_OVERFLOW);
		break;
	case TYPE_lng:
		interval_loop(second_interval_convert, lng, lng, "sec_interval", GDK_lng_max, DO_NOTHING, MUL_OVERFLOW);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		interval_loop(second_interval_convert, hge, lng, "sec_interval", GDK_lng_max, CAST_VALIDATION, MUL_OVERFLOW);
		break;
#endif
	default: {
		msg = createException(ILLARG, "batcalc.sec_interval", SQLSTATE(42000) "Illegal argument in second interval");
	}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = b->tkey;
		res->tsorted = b->tsorted;
		res->trevsorted  = b->trevsorted;
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
	int tpe = getArgType(mb, pci, 1), k = digits2ek(*getArgReference_int(stk, pci, pci->argc == 5 ? 3 : 2));
	bool is_a_bat = false, nils = false;
	BAT *b = NULL, *s = NULL, *res = NULL;
	bat *r = NULL, *sid = pci->argc == 5 ? getArgReference_bat(stk, pci, 2) : NULL;
	BUN q = 0;
	struct canditer ci = {0};

	(void) cntxt;
	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1)))) {
			msg = createException(SQL, "batcalc.second_interval_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.second_interval_daytime", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_lng, q, TRANSIENT))) {
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
		oid off = b->hseqbase;
		daytime *restrict vals = (daytime*) Tloc(b, 0);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				daytime next = vals[p];

				if (is_daytime_nil(next)) {
					ret[i] = lng_nil;
					nils = true;
				} else {
					ret[i] = (next / divider) * multiplier;
				}
			}
		} else {
			for (BUN i = 0 ; i < q; i++) {
				oid p = (canditer_next(&ci) - off);
				daytime next = vals[p];

				if (is_daytime_nil(next)) {
					ret[i] = lng_nil;
					nils = true;
				} else {
					ret[i] = (next / divider) * multiplier;
				}
			}
		}
	} else {
		daytime next = *(daytime*)getArgReference(stk, pci, 1);
		*ret = is_daytime_nil(next) ? lng_nil : (next / divider) * multiplier;
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = b->tkey;
		res->tsorted = b->tsorted;
		res->trevsorted  = b->trevsorted;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
nil_2_date(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *res = NULL;
	bat *r = NULL;

	(void) cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		date d = date_nil;
		if (!(b = BATdescriptor(*getArgReference_bat(stk, pci, 1))))
			throw(SQL, "batcalc.nil_2_date", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		res = BATconstant(b->hseqbase, TYPE_date, &d, BATcount(b), TRANSIENT);
		BBPunfix(b->batCacheid);
		if (!res)
			throw(SQL, "batcalc.nil_2_date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		r = getArgReference_bat(stk, pci, 0);
		BBPkeepref(*r = res->batCacheid);
	} else {
		date *ret = (date*) getArgReference(stk, pci, 0);
		*ret = date_nil;
	}
	return MAL_SUCCEED;
}

static inline str
str_2_date_internal_imp(date *ret, str next)
{
	ssize_t pos = 0;
	date dt = 0, *conv = &dt;

	pos = date_fromstr(next, &(size_t){sizeof(date)}, &conv, false);
	if (pos < (ssize_t) strlen(next) || /* includes pos < 0 */ is_date_nil(*conv))
		return createException(SQL, "batcalc.str_2_date", SQLSTATE(22007) "Date '%s' has incorrect format", next);
	*ret = *conv;
	return MAL_SUCCEED;
}

static str
str_2_date_internal(ptr out, ptr in, const bat *sid, int tpe)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *s = NULL, *res = NULL;
	BUN q = 0;
	date *restrict ret = NULL;
	bool is_a_bat = false, nils = false;
	bat *r = NULL;
	struct canditer ci = {0};

	is_a_bat = isaBatType(tpe);
	if (is_a_bat) {
		tpe = getBatType(tpe);
		if (!(b = BATdescriptor(*(bat*) in))) {
			msg = createException(SQL, "batcalc.batstr_2_date", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.batstr_2_date", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
		q = canditer_init(&ci, b, s);
		if (!(res = COLnew(ci.hseq, TYPE_date, q, TRANSIENT))) {
			msg = createException(SQL, "batcalc.batstr_2_date", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		r = (bat*) out;
		ret = (date*) Tloc(res, 0);
	} else {
		ret = (date*) out;
	}

	if (is_a_bat) {
		oid off = b->hseqbase;
		BATiter it = bat_iterator(b);
		if (ci.tpe == cand_dense) {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next_dense(&ci) - off);
				str next = BUNtvar(it, p);

				if (strNil(next)) {
					ret[i] = date_nil;
					nils = true;
				} else {
					msg = str_2_date_internal_imp(&(ret[i]), next);
				}
			}
		} else {
			for (BUN i = 0 ; i < q && !msg; i++) {
				oid p = (canditer_next(&ci) - off);
				str next = BUNtvar(it, p);

				if (strNil(next)) {
					ret[i] = date_nil;
					nils = true;
				} else {
					msg = str_2_date_internal_imp(&(ret[i]), next);
				}
			}
		}
	} else {
		str next = *(str*)in;
		if (strNil(next))
			*ret = date_nil;
		else
			msg = str_2_date_internal_imp(ret, next);
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (res && !msg) {
		BATsetcount(res, q);
		res->tnil = nils;
		res->tnonil = !nils;
		res->tkey = BATcount(res) <= 1;
		res->tsorted = BATcount(res) <= 1;
		res->trevsorted = BATcount(res) <= 1;
		BBPkeepref(*r = res->batCacheid);
	} else if (res)
		BBPreclaim(res);
	return msg;
}

str
batstr_2_date(bat *res, const bat *bid, const bat *s)
{
	return str_2_date_internal((ptr) res, (ptr) bid, s, newBatType(TYPE_str));
}

str
str_2_date(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tpe = getArgType(mb, pci, 1);
	(void) cntxt;
	return str_2_date_internal(getArgReference(stk, pci, 0), getArgReference(stk, pci, 1), NULL, tpe);
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
