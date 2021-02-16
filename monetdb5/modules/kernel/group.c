/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "group.h"

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

static str
GRPsubgroup9(bat *ngid, bat *next, const bat *bid, const bat *sid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, sid, gid, eid, hid);
}

static str
GRPsubgroup8(bat *ngid, bat *next, const bat *bid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, NULL, gid, eid, hid);
}

static str
GRPsubgroup7(bat *ngid, bat *next, const bat *bid, const bat *sid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, sid, gid, NULL, NULL);
}

static str
GRPsubgroup6(bat *ngid, bat *next, const bat *bid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, NULL, gid, NULL, NULL);
}

static str
GRPsubgroup4(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, NULL, gid, eid, hid);
}

static str
GRPsubgroup3(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *sid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, sid, gid, NULL, NULL);
}

static str
GRPsubgroup2(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *gid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, NULL, gid, NULL, NULL);
}

static str
GRPgroup4(bat *ngid, bat *next, const bat *bid, const bat *sid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, sid, NULL, NULL, NULL);
}

static str
GRPgroup3(bat *ngid, bat *next, const bat *bid)
{
	return GRPsubgroup5(ngid, next, NULL, bid, NULL, NULL, NULL, NULL);
}

static str
GRPgroup2(bat *ngid, bat *next, bat *nhis, const bat *bid, const bat *sid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, sid, NULL, NULL, NULL);
}

str
GRPgroup1(bat *ngid, bat *next, bat *nhis, const bat *bid)
{
	return GRPsubgroup5(ngid, next, nhis, bid, NULL, NULL, NULL, NULL);
}

static str
GRPgroup11(bat *ngid, const bat *bid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, NULL, NULL, NULL, NULL);
}

static str
GRPgroup21(bat *ngid, const bat *bid, const bat *sid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, sid, NULL, NULL, NULL);
}

static str
GRPsubgroup51(bat *ngid, const bat *bid, const bat *sid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, sid, gid, eid, hid);
}

static str
GRPsubgroup41(bat *ngid, const bat *bid, const bat *gid, const bat *eid, const bat *hid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, NULL, gid, eid, hid);
}

static str
GRPsubgroup31(bat *ngid, const bat *bid, const bat *sid, const bat *gid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, sid, gid, NULL, NULL);
}

static str
GRPsubgroup21(bat *ngid, const bat *bid, const bat *gid)
{
	return GRPsubgroup5(ngid, NULL, NULL, bid, NULL, gid, NULL, NULL);
}

#include "mel.h"
mel_func group_init_funcs[] = {
 command("group", "group", GRPgroup1, false, "", args(3,4, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1))),
 command("group", "group", GRPgroup2, false, "", args(3,5, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("s",oid))),
 command("group", "group", GRPgroup3, false, "", args(2,3, batarg("groups",oid),batarg("extents",oid),batargany("b",1))),
 command("group", "group", GRPgroup4, false, "", args(2,4, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("s",oid))),
 command("group", "group", GRPgroup11, false, "", args(1,2, batarg("",oid),batargany("b",1))),
 command("group", "group", GRPgroup21, false, "", args(1,3, batarg("",oid),batargany("b",1),batarg("s",oid))),
 command("group", "subgroup", GRPsubgroup2, false, "", args(3,5, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("g",oid))),
 command("group", "subgroup", GRPsubgroup3, false, "", args(3,6, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("s",oid),batarg("g",oid))),
 command("group", "subgroup", GRPsubgroup4, false, "", args(3,7, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroup", GRPsubgroup5, false, "", args(3,8, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("s",oid),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroup", GRPsubgroup6, false, "", args(2,4, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("g",oid))),
 command("group", "subgroup", GRPsubgroup7, false, "", args(2,5, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("s",oid),batarg("g",oid))),
 command("group", "subgroup", GRPsubgroup8, false, "", args(2,6, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroup", GRPsubgroup9, false, "", args(2,7, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("s",oid),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroup", GRPsubgroup21, false, "", args(1,3, batarg("",oid),batargany("b",1),batarg("g",oid))),
 command("group", "subgroup", GRPsubgroup31, false, "", args(1,4, batarg("",oid),batargany("b",1),batarg("s",oid),batarg("g",oid))),
 command("group", "subgroup", GRPsubgroup41, false, "", args(1,5, batarg("",oid),batargany("b",1),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroup", GRPsubgroup51, false, "", args(1,6, batarg("",oid),batargany("b",1),batarg("s",oid),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "groupdone", GRPgroup1, false, "", args(3,4, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1))),
 command("group", "groupdone", GRPgroup2, false, "", args(3,5, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("s",oid))),
 command("group", "groupdone", GRPgroup3, false, "", args(2,3, batarg("groups",oid),batarg("extents",oid),batargany("b",1))),
 command("group", "groupdone", GRPgroup4, false, "", args(2,4, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("s",oid))),
 command("group", "groupdone", GRPgroup11, false, "", args(1,2, batarg("",oid),batargany("b",1))),
 command("group", "groupdone", GRPgroup21, false, "", args(1,3, batarg("",oid),batargany("b",1),batarg("s",oid))),
 command("group", "subgroupdone", GRPsubgroup2, false, "", args(3,5, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("g",oid))),
 command("group", "subgroupdone", GRPsubgroup3, false, "", args(3,6, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("s",oid),batarg("g",oid))),
 command("group", "subgroupdone", GRPsubgroup4, false, "", args(3,7, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroupdone", GRPsubgroup5, false, "", args(3,8, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),batargany("b",1),batarg("s",oid),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroupdone", GRPsubgroup6, false, "", args(2,4, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("g",oid))),
 command("group", "subgroupdone", GRPsubgroup7, false, "", args(2,5, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("s",oid),batarg("g",oid))),
 command("group", "subgroupdone", GRPsubgroup8, false, "", args(2,6, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroupdone", GRPsubgroup9, false, "", args(2,7, batarg("groups",oid),batarg("extents",oid),batargany("b",1),batarg("s",oid),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroupdone", GRPsubgroup21, false, "", args(1,3, batarg("",oid),batargany("b",1),batarg("g",oid))),
 command("group", "subgroupdone", GRPsubgroup31, false, "", args(1,4, batarg("",oid),batargany("b",1),batarg("s",oid),batarg("g",oid))),
 command("group", "subgroupdone", GRPsubgroup41, false, "", args(1,5, batarg("",oid),batargany("b",1),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 command("group", "subgroupdone", GRPsubgroup51, false, "", args(1,6, batarg("",oid),batargany("b",1),batarg("s",oid),batarg("g",oid),batarg("e",oid),batarg("h",lng))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_group_mal)
{ mal_module("group", NULL, group_init_funcs); }
