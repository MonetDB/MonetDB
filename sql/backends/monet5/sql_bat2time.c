/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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
		throw(SQL, "sql.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		char *v = (char *) BUNtvar(bi, p);
		union {
			lng l;
			timestamp r;
		} u;
		msg = str_2time_timestamptz(&u.r, &v, digits, tz);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &u.r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, "sql.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	const timestamp *v = (const timestamp *) Tloc(b, 0);
	BATloop(b, p, q) {
		union {
			lng l;
			timestamp r;
		} u;
		msg = timestamp_2time_timestamp(&u.r, v, digits);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &u.r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, "sql.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		union {
			lng l;
			timestamp r;
		} u;
		msg = nil_2time_timestamp(&u.r, NULL, digits);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &u.r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, "sql.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		char *v = (char *) BUNtvar(bi, p);
		union {
			lng l;
			daytime r;
		} u;
		msg = str_2time_daytimetz(&u.r, &v, digits, tz);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &u.r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, "sql.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	const daytime *v = (const daytime *) Tloc(b, 0);
	BATloop(b, p, q) {
		union {
			lng l;
			daytime r;
		} u;
		msg = daytime_2time_daytime(&u.r, v, digits);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &u.r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(SQL, "sql.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	BATloop(b, p, q) {
		union {
			lng l;
			daytime r;
		} u;
		msg = nil_2time_daytime(&u.r, NULL, digits);
		if (msg) {
			BBPunfix(dst->batCacheid);
			BBPunfix(b->batCacheid);
			return msg;
		}
		if (BUNappend(dst, &u.r, false) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPreclaim(dst);
			throw(SQL, "sql.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref(*res = dst->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

static int truncate_check(const str *scale){
	(void) scale;
	return 
		strcmp(*scale, "millenium") == 0 ||
		strcmp(*scale, "century") == 0  ||
		strcmp(*scale, "decade") == 0 ||
		strcmp(*scale, "year") == 0 ||
		strcmp(*scale, "quarter" ) == 0 ||
		strcmp(*scale, "month") == 0 ||
		strcmp(*scale, "week") == 0 ||
		strcmp(*scale, "day") == 0  ||
		strcmp(*scale, "hour") == 0 ||
		strcmp(*scale, "minute") == 0 ||
		strcmp(*scale, "second") == 0 ||
		strcmp(*scale, "milliseconds") == 0 ||
		strcmp(*scale, "microseconds") == 0;
}

#define date_trunc_time_loop(NAME, TYPE, DIVISOR) 	\
	if  ( strcmp(*scale, NAME) == 0){ \
		for( ; lo < hi; lo++)		\
			if (timestamp_isnil(bt[lo])) {     		\
					dt[lo] = *timestamp_nil;     		\
					nils++;		\
			} else {                 		\
				ts = bt[0];					\
				ts.msecs = (ts.msecs / DIVISOR) * DIVISOR; \
				dt[lo] = ts;					\
	}		}

str
bat_date_trunc(bat *res, const str *scale, const bat *bid)
{
	BAT *b, *bn;
	oid lo, hi;
	timestamp *bt;
	timestamp *dt;
	char *msg = NULL;
	lng nils = 0;
	timestamp ts;

	if ( truncate_check(scale) == 0)
		throw(SQL, "batcalc.truncate_timestamp", SQLSTATE(HY005) "Improper directive ");

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.truncate_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bn = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.truncate", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	bt = (timestamp *) Tloc(b, 0);
	dt = (timestamp *) Tloc(bn, 0);

	lo = 0;
	hi = lo + BATcount(b);

	date_trunc_time_loop("microseconds", TIMESTAMP, 1)
	date_trunc_time_loop("milliseconds", TIMESTAMP, 1000)
	date_trunc_time_loop("seconds", TIMESTAMP, (1000 * 60))
	date_trunc_time_loop("minute", TIMESTAMP, (1000 * 60 * 60))
	date_trunc_time_loop("hour", TIMESTAMP, (1000 * 60 * 60 * 24))

	// week
	// quarter
	// decade
	// century
	// millenium
	if( nils){
		bn->tnonil = false;  
		bn->tnil = true;     
		bn->tsorted = false;     
		bn->trevsorted = false;  
		bn->tkey = false;    
	}
	BATsetcount(bn, (BUN) lo);
	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

#define date_trunc_single_time(NAME, TYPE, DIVISOR) 	\
	if  ( strcmp(*scale, NAME) == 0){ \
		if (timestamp_isnil(*bt)) {     		\
			*dt = *timestamp_nil;     		\
		} else {                 		\
			ts = *bt;					\
			ts.msecs = (ts.msecs / DIVISOR) * DIVISOR; \
			*dt = ts;					\
	}	}

str
date_trunc(timestamp *dt, const str *scale, const timestamp *bt)
{
	str msg = MAL_SUCCEED;
	timestamp ts;

	if (truncate_check(scale) == 0)
		throw(SQL, "sql.truncate", SQLSTATE(HY001) "Improper directive ");	

	date_trunc_single_time("microseconds", TIMESTAMP, 1)
	date_trunc_single_time("milliseconds", TIMESTAMP, 1000)
	date_trunc_single_time("seconds", TIMESTAMP, (1000 * 60))
	date_trunc_single_time("minute", TIMESTAMP, (1000 * 60 * 60))
	date_trunc_single_time("hour", TIMESTAMP, (1000 * 60 * 60 * 24))
	// week
	// quarter
	// decade
	// century
	// millenium
	return msg;
}
