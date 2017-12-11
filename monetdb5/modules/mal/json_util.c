/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal_exception.h"
#include "json_util.h"

str
JSONresultSet(json *res, bat *uuid, bat *rev, bat *js)
{
	BAT *bu, *br, *bj;
	char *result;
	size_t sz, len=0;

	if ((bu = BATdescriptor(*uuid)) == NULL) 
		throw(MAL, "json.resultset", INTERNAL_BAT_ACCESS);
	if ((br = BATdescriptor(*rev)) == NULL) {
		BBPunfix(bu->batCacheid);
		throw(MAL, "json.resultset", INTERNAL_BAT_ACCESS);
	}
	if ((bj = BATdescriptor(*js)) == NULL) {
		BBPunfix(bu->batCacheid);
		BBPunfix(br->batCacheid);
		throw(MAL, "json.resultset", INTERNAL_BAT_ACCESS);
	}
	if ( !(BATcount(bu) == BATcount(br) && BATcount(br) == BATcount(bj)) ){
		BBPunfix(bu->batCacheid);
		BBPunfix(br->batCacheid);
		BBPunfix(bj->batCacheid);
		throw(MAL, "json.resultset", "Input not aligned");
	}
	sz= (22 + 12 + 20) * BATcount(bu);
	result = (char*) GDKmalloc(sz);
	if (result == NULL){
		BBPunfix(bu->batCacheid);
		BBPunfix(br->batCacheid);
		BBPunfix(bj->batCacheid);
		throw(MAL, "json.resultset", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	len += snprintf(result,sz,"[");
	/* here the dirty work follows */
	/* loop over the triple store */
	snprintf(result+len,sz-len,"]");
	BBPunfix(bu->batCacheid);
	BBPunfix(br->batCacheid);
	BBPunfix(bj->batCacheid);
	*res = result;
	return MAL_SUCCEED;

}
