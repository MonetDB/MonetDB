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

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "group.h"
#include "algebra.h"

str
GRPsubgroup4(bat *ngid, bat *next, bat *nhis, bat *bid, bat *gid, bat *eid, bat *hid)
{
	BAT *b, *g, *e, *h, *gn, *en, *hn;
	gdk_return r;

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	h = hid ? BATdescriptor(*hid) : NULL;
	if (b == NULL ||
		(gid != NULL && g == NULL) ||
		(eid != NULL && e == NULL) ||
		(hid != NULL && h == NULL)) {
		if (g)
			BBPreleaseref(g->batCacheid);
		if (e)
			BBPreleaseref(e->batCacheid);
		if (h)
			BBPreleaseref(h->batCacheid);
		throw(MAL, "group.subgroup", RUNTIME_OBJECT_MISSING);
	}
	if ((r = BATgroup(&gn, &en, &hn, b, g, e, h)) == GDK_SUCCEED) {
		*ngid = gn->batCacheid;
		*next = en->batCacheid;
		*nhis = hn->batCacheid;
		BBPkeepref(*ngid);
		BBPkeepref(*next);
		BBPkeepref(*nhis);
	}
	BBPreleaseref(b->batCacheid);
	if (g)
		BBPreleaseref(g->batCacheid);
	if (e)
		BBPreleaseref(e->batCacheid);
	if (h)
		BBPreleaseref(h->batCacheid);
	return r == GDK_SUCCEED ? MAL_SUCCEED : createException(MAL, "group.subgroup", GDK_EXCEPTION);
}

str
GRPsubgroup2(bat *ngid, bat *next, bat *nhis, bat *bid, bat *gid)
{
	return GRPsubgroup4(ngid, next, nhis, bid, gid, NULL, NULL);
}

str
GRPsubgroup1(bat *ngid, bat *next, bat *nhis, bat *bid)
{
	return GRPsubgroup4(ngid, next, nhis, bid, NULL, NULL, NULL);
}
