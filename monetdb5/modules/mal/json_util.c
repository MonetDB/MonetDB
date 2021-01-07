/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal_exception.h"

typedef str json;

static str
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
		throw(MAL, "json.resultset", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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

#include "mel.h"
mel_func json_util_init_funcs[] = {
 command("json", "resultSet", JSONresultSet, false, "Converts the json store into a single json string:", args(1,4, arg("",json),batarg("u",uuid),batarg("rev",lng),batarg("js",json))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_json_util_mal)
{ mal_module("json_util", NULL, json_util_init_funcs); }
