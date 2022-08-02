/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "group.h"
#include "algebra.h"

str
GRPsubgroup5(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *sid, const bat *gid, const bat *eid, const bat *hid)
{
	BAT *b, *s, *g, *e, *h, *gn, *en, *hn;
	gdk_return r;

	b = BATdescriptor(*bid);
	s = sid ? BATdescriptor(*sid) : NULL;
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	h = hid ? BATdescriptor(*hid) : NULL;
	if (b == NULL ||
		(sid != NULL && s == NULL) ||
		(gid != NULL && g == NULL) ||
		(eid != NULL && e == NULL) ||
		(hid != NULL && h == NULL)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (h)
			BBPunfix(h->batCacheid);
		throw(MAL, gid ? "group.subgroup" : "group.group", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((r = BATgroup(&gn, next ? &en : NULL, nhis ? &hn : NULL, b, s, g, e, h)) == GDK_SUCCEED) {
		*ngid = gn->batCacheid;
		BBPkeepref(*ngid);
		if (next) {
			*next = en->batCacheid;
			BBPkeepref(*next);
		}
		if (nhis){
			*nhis = hn->batCacheid;
			BBPkeepref(*nhis);
		}
	}
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (h)
		BBPunfix(h->batCacheid);
	return r == GDK_SUCCEED ? MAL_SUCCEED : createException(MAL, gid ? "group.subgroup" : "group.group", GDK_EXCEPTION);
}

str
GRPsubgroup9(bat *ngid, bat *next, const bat *bid, const bat *sid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, sid, gid, eid, hid);
}

str
GRPsubgroup8(bat *ngid, bat *next, const bat *bid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, NULL, gid, eid, hid);
}

str
GRPsubgroup7(bat *ngid, bat *next, const bat *bid, const bat *sid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, sid, gid, NULL, NULL);
}

str
GRPsubgroup6(bat *ngid, bat *next, const bat *bid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, NULL, gid, NULL, NULL);
}

str
GRPsubgroup4(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, NULL, gid, eid, hid);
}

str
GRPsubgroup3(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *sid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, sid, gid, NULL, NULL);
}

str
GRPsubgroup2(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, NULL, gid, NULL, NULL);
}

str
GRPgroup4(bat *ngid, bat *next, const bat *bid, const bat *sid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, sid, NULL, NULL, NULL);
}

str
GRPgroup3(bat *ngid, bat *next, const bat *bid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, NULL, NULL, NULL, NULL);
}

str
GRPgroup2(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *sid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, sid, NULL, NULL, NULL);
}

str
GRPgroup1(bat *ngid, bat *next, bat *nhis, const bat *bid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, NULL, NULL, NULL, NULL);
}

str
GRPgroup11(bat *ngid, const bat *bid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, NULL, NULL, NULL, NULL);
}

str
GRPgroup21(bat *ngid, const bat *bid, const bat *sid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, sid, NULL, NULL, NULL);
}

str
GRPsubgroup51(bat *ngid, const bat *bid, const bat *sid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, sid, gid, eid, hid);
}

str
GRPsubgroup41(bat *ngid, const bat *bid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, NULL, gid, eid, hid);
}

str
GRPsubgroup31(bat *ngid, const bat *bid, const bat *sid, const bat *gid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, sid, gid, NULL, NULL);
}

str
GRPsubgroup21(bat *ngid, const bat *bid, const bat *gid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, NULL, gid, NULL, NULL);
}
