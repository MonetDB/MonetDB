/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "mal_instruction.h"

#define date_trunc_time_loop(NAME, TYPE, DIVISOR) 	\
	if  ( strcmp(*scale, NAME) == 0){ \
		for( ; lo < hi; lo++)		\
			if (is_timestamp_nil(bt[lo])) {     		\
					dt[lo] = *timestamp_nil;     		\
					nils++;		\
			} else {                 		\
				ts = bt[0];					\
				ts.msecs = (int) ((lng)ts.msecs / (lng)DIVISOR) * (lng)DIVISOR; \
				dt[lo] = ts;					\
	}		}

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
	int dow, y, m, d, one = 1;

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
	date_trunc_time_loop("milliseconds", TIMESTAMP, 1)
	date_trunc_time_loop("second", TIMESTAMP, (1000 ))
	date_trunc_time_loop("minute", TIMESTAMP, (1000 * 60))
	date_trunc_time_loop("hour", TIMESTAMP, (1000 * 60 * 24))

	if  ( strcmp(*scale, "day") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				dt[lo] = ts;					
	}		}

	if  ( strcmp(*scale, "week") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				MTIMEdate_extract_dayofweek(&dow, &ts.days);
				d =  d - dow - 1;
				MTIMEdate_create(&ts.days, &y, &m, &d);
				dt[lo] = ts;					
	}		}

	if  ( strcmp(*scale, "month") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				MTIMEdate_create(&ts.days, &y, &m, &one);
				dt[lo] = ts;					
	}		}

	if  ( strcmp(*scale, "quarter") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				m = m/4 + 1;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;					
	}		}

	if  ( strcmp(*scale, "year") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;					
	}		}

	if  ( strcmp(*scale, "decade") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				y = (y /10 ) *10;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;					
	}		}

	if  ( strcmp(*scale, "century") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				y = (y /100 ) *100;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;					
	}		}

	if  ( strcmp(*scale, "millenium") == 0){ 
		for( ; lo < hi; lo++)		
			if (is_timestamp_nil(bt[lo])) {     		
				dt[lo] = *timestamp_nil;     		
			} else {                 		
				ts = bt[lo];					
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				y = (y /1000 ) *1000;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;					
	}		}

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
		if (is_timestamp_nil(*bt)) {     		\
			*dt = *timestamp_nil;     		\
		} else {                 		\
			ts = *bt;					\
			ts.msecs = (int) ((lng)ts.msecs / (lng)DIVISOR) * (lng)DIVISOR; \
			*dt = ts;					\
	}	}

str
date_trunc(timestamp *dt, const str *scale, const timestamp *bt)
{
	str msg = MAL_SUCCEED;
	timestamp ts;
	int dow, y, m, d, one = 1;

	if (truncate_check(scale) == 0)
		throw(SQL, "sql.truncate", SQLSTATE(HY001) "Improper directive ");	

	date_trunc_single_time("microseconds", TIMESTAMP, 1)
	date_trunc_single_time("milliseconds", TIMESTAMP, 1)
	date_trunc_single_time("second", TIMESTAMP, (1000))
	date_trunc_single_time("minute", TIMESTAMP, (1000 * 60))
	date_trunc_single_time("hour", TIMESTAMP, (1000 * 60 * 24))

	if  ( strcmp(*scale, "day") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			*dt = ts;					
	}	}
	
	if  ( strcmp(*scale, "week") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
			MTIMEdate_extract_dayofweek(&dow, &ts.days);
			d =  d - dow - 1;
			MTIMEdate_create(&ts.days, &y, &m, &d);
			*dt = ts;					
	}	}
	
	if  ( strcmp(*scale, "month") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
			MTIMEdate_create(&ts.days, &y, &m, &one);
			*dt = ts;					
	}	}
	
	if  ( strcmp(*scale, "quarter") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
			m = m/4 + 1;
			MTIMEdate_create(&ts.days, &y, &one, &one);
			*dt = ts;					
	}	}

	if  ( strcmp(*scale, "year") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
			MTIMEdate_create(&ts.days, &y, &one, &one);
			*dt = ts;					
	}	}

	if  ( strcmp(*scale, "decade") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
			y = (y /10 ) *10;
			MTIMEdate_create(&ts.days, &y, &one, &one);
			*dt = ts;					
	}	}

	if  ( strcmp(*scale, "century") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
			y = (y /100 ) *100 +1;
			MTIMEdate_create(&ts.days, &y, &one, &one);
			*dt = ts;					
	}	}

	if  ( strcmp(*scale, "millenium") == 0){ 
		if (is_timestamp_nil(*bt)) {     		
			*dt = *timestamp_nil;     		
		} else {                 		
			ts = *bt;					
			ts.msecs = 0;
			MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
			y = (y /1000 ) *1000 +1;
			MTIMEdate_create(&ts.days, &y, &one, &one);
			*dt = ts;					
	}	}
	return msg;
}
