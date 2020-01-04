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
#include "sql_gencode.h"
#include "sql_storage.h"
#include "sql_scenario.h"
#include "store_sequence.h"
#include "sql_optimizer.h"
#include "sql_datetime.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_distribute.h"
#include "rel_select.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "clients.h"
#include "mal_instruction.h"

str
batstr_2time_timestamptz(bat *res, const bat *bid, const int *digits, int *tz)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2time_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		char *v = (char *) BUNtvar(bi, p);
		timestamp r;
		msg = str_2time_timestamptz(&r, &v, digits, tz);
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
batstr_2time_timestamp(bat *res, const bat *bid, const int *digits)
{
	int zero = 0;
	return batstr_2time_timestamptz( res, bid, digits, &zero);
}

str
battimestamp_2time_timestamp(bat *res, const bat *bid, const int *digits)
{
	BAT *b, *dst;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.timestamp_2time_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	const timestamp *v = (const timestamp *) Tloc(b, 0);
	BATloop(b, p, q) {
		timestamp r;
		msg = timestamp_2time_timestamp(&r, v, digits);
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
		v++;
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2time_timestamp(bat *res, const bat *bid, const int *digits)
{
	BAT *b, *dst;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2time_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.timestamp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		timestamp r;
		msg = nil_2time_timestamp(&r, NULL, digits);
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
batstr_2time_daytimetz(bat *res, const bat *bid, const int *digits, int *tz)
{
	BAT *b, *dst;
	BATiter bi;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.str_2time_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bi = bat_iterator(b);
	dst = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		char *v = (char *) BUNtvar(bi, p);
		daytime r;
		msg = str_2time_daytimetz(&r, &v, digits, tz);
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
batstr_2time_daytime(bat *res, const bat *bid, const int *digits)
{
	int zero = 0;
	return batstr_2time_daytimetz( res, bid, digits, &zero);
}

str
batdaytime_2time_daytime(bat *res, const bat *bid, const int *digits)
{
	BAT *b, *dst;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.daytime_2time_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	const daytime *v = (const daytime *) Tloc(b, 0);
	BATloop(b, p, q) {
		daytime r;
		msg = daytime_2time_daytime(&r, v, digits);
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
		v++;
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
batnil_2time_daytime(bat *res, const bat *bid, const int *digits)
{
	BAT *b, *dst;
	BUN p, q;
	char *msg = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.nil_2time_daytime", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	dst = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (dst == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		daytime r;
		msg = nil_2time_daytime(&r, NULL, digits);
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

