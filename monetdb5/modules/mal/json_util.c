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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) Martin Kersten
 */

#include "monetdb_config.h"
#include <gdk.h>
#include <mal_exception.h>
#include "json_util.h"

str
JSONresultSet(str *res, bat *uuid, bat *rev, bat *js)
{
	BAT *bu, *br, *bj;
	char *result;
	size_t sz, len=0;

	if ((bu = BATdescriptor(*uuid)) == NULL) 
		throw(MAL, "json.resultset", INTERNAL_BAT_ACCESS);
	if ((br = BATdescriptor(*rev)) == NULL) {
		BBPreleaseref(bu->batCacheid);
		throw(MAL, "json.resultset", INTERNAL_BAT_ACCESS);
	}
	if ((bj = BATdescriptor(*js)) == NULL) {
		BBPreleaseref(bu->batCacheid);
		BBPreleaseref(br->batCacheid);
		throw(MAL, "json.resultset", INTERNAL_BAT_ACCESS);
	}
	if ( !(BATcount(bu) == BATcount(br) && BATcount(br) == BATcount(bj)) ){
		BBPreleaseref(bu->batCacheid);
		BBPreleaseref(br->batCacheid);
		BBPreleaseref(bj->batCacheid);
		throw(MAL, "json.resultset", "Input not aligned");
	}
	sz= (22 + 12 + 20) * BATcount(bu);
	result = (char*) GDKmalloc(sz);
	if (result == NULL){
		BBPreleaseref(bu->batCacheid);
		BBPreleaseref(br->batCacheid);
		BBPreleaseref(bj->batCacheid);
		throw(MAL, "json.resultset", MAL_MALLOC_FAIL);
	}
	len += snprintf(result,sz,"[");
	/* here the dirty work follows */
	/* loop over the triple store */
	snprintf(result+len,sz-len,"]");
	BBPreleaseref(bu->batCacheid);
	BBPreleaseref(br->batCacheid);
	BBPreleaseref(bj->batCacheid);
	*res = result;
	return MAL_SUCCEED;

}
