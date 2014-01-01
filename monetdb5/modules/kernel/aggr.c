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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define aggr_export extern __declspec(dllimport)
#else
#define aggr_export extern __declspec(dllexport)
#endif
#else
#define aggr_export extern
#endif

/*
 * grouped aggregates
 */
static str
AGGRgrouped(bat *retval, BAT *b, BAT *g, BAT *e, int tp,
			BAT *(*grpfunc)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			int skip_nils,
			const char *malfunc)
{
	BAT *bn, *t, *map;

	if (b == NULL || g == NULL || e == NULL) {
		if (b)
			BBPreleaseref(b->batCacheid);
		if (g)
			BBPreleaseref(g->batCacheid);
		if (e)
			BBPreleaseref(e->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	if (tp == TYPE_any && grpfunc == BATgroupmedian)
		tp = b->ttype;
	if (!BAThdense(b) || !BAThdense(g)) {
		/* if b or g don't have a dense head, replace the head with a
		 * dense sequence */
		t = BATjoin(BATmirror(b), g, MIN(BATcount(b), BATcount(g)));
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(g->batCacheid);
		b = BATmirror(BATmark(t, 0));
		g = BATmirror(BATmark(BATmirror(t), 0));
		BBPreleaseref(t->batCacheid);
	}
	if (b->hseqbase != g->hseqbase || BATcount(b) != BATcount(g)) {
		/* b and g are not aligned: align them by creating a view on
		 * one or the other */
		oid min;				/* lowest common oid */
		oid max;				/* highest common oid */
		min = b->hseqbase;
		if (min < g->hseqbase)
			min = g->hseqbase;
		max = b->hseqbase + BATcount(b);
		if (g->hseqbase + BATcount(g) < max)
			max = g->hseqbase + BATcount(g);
		if (b->hseqbase != min || b->hseqbase + BATcount(b) != max) {
			if (min >= max)
				min = max = b->hseqbase;
			t = BATslice(b, BUNfirst(b) + (BUN) (min - b->hseqbase),
						 BUNfirst(b) + (BUN) (max - b->hseqbase));
			BBPreleaseref(b->batCacheid);
			b = t;
		}
		if (g->hseqbase != min || g->hseqbase + BATcount(g) != max) {
			if (min >= max)
				min = max = g->hseqbase;
			t = BATslice(g, BUNfirst(g) + (BUN) (min - g->hseqbase),
						 BUNfirst(g) + (BUN) (max - g->hseqbase));
			BBPreleaseref(g->batCacheid);
			g = t;
		}
	}
	if (!BAThdense(e)) {
		/* if e doesn't have a dense head, renumber the group ids with
		 * a dense sequence at the cost of some left joins */
		map = BATmark(e, 0);	/* [gid,newgid(dense)] */
		BBPreleaseref(e->batCacheid);
		e = BATmirror(map);		/* [newgid(dense),gid] */
		t = BATleftjoin(g, map, BATcount(g)); /* [oid,newgid] */
		BBPreleaseref(g->batCacheid);
		g = t;
	} else {
		map = NULL;
	}
	bn = (*grpfunc)(b, g, e, NULL, tp, skip_nils, 1);
	if (bn != NULL && (grpfunc == BATgroupmin || grpfunc == BATgroupmax)) {
		BAT *bnn = BATouterjoin(bn, b, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = bnn;
	}
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(g->batCacheid);
	if (map == NULL)			/* if map!=NULL, e is mirror of map */
		BBPreleaseref(e->batCacheid);
	if (bn == NULL) {
		char *errbuf = GDKerrbuf;
		char *s;

		if (map)
			BBPreleaseref(map->batCacheid);

		if (errbuf && *errbuf) {
			if (strncmp(errbuf, "!ERROR: ", 8) == 0)
				errbuf += 8;
			if (strchr(errbuf, '!') == errbuf + 5) {
				s = createException(MAL, malfunc, "%s", errbuf);
			} else if ((s = strchr(errbuf, ':')) != NULL && s[1] == ' ') {
				s = createException(MAL, malfunc, "%s", s + 2);
			} else {
				s = createException(MAL, malfunc, "%s", errbuf);
			}
			*GDKerrbuf = 0;
			return s;
		}
		throw(MAL, malfunc, OPERATION_FAILED);
	}
	if (map) {
		t = BATleftjoin(map, bn, BATcount(bn));
		BBPreleaseref(map->batCacheid);
		BBPreleaseref(bn->batCacheid);
		bn = t;
	}
	*retval = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
AGGRgrouped3(bat *retval, bat *bid, bat *gid, bat *eid, int tp,
			 BAT *(*grpfunc)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			 int skip_nils,
			 const char *malfunc)
{
	BAT *b, *g, *e;

	b = BATdescriptor(*bid);	/* [head,value] */
	g = BATdescriptor(*gid);	/* [head,gid] */
	e = BATdescriptor(*eid);	/* [gid,any] */
	return AGGRgrouped(retval, b, g, e, tp, grpfunc, skip_nils, malfunc);
}

static str
AGGRgrouped2(bat *retval, bat *bid, bat *eid, int tp,
			 BAT *(*grpfunc)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			 int skip_nils,
			 const char *malfunc)
{
	BAT *b, *g, *e;

	b = BATdescriptor(*bid);	/* [gid,value] */
	if (b == NULL)
		throw(MAL, "aggr.sum", RUNTIME_OBJECT_MISSING);
	g = BATmirror(BATmark(b, 0)); /* [dense,gid] */
	e = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,value] */
	BBPreleaseref(b->batCacheid);
	b = e;
	e = BATdescriptor(*eid);	/* [gid,any] */
	return AGGRgrouped(retval, b, g, e, tp, grpfunc, skip_nils, malfunc);
}

aggr_export str AGGRsum3_bte(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRsum3_bte(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_bte,
						BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum3_sht(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRsum3_sht(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_sht,
						BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum3_int(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRsum3_int(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_int,
						BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum3_wrd(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRsum3_wrd(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_wrd,
						BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum3_lng(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRsum3_lng(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_lng,
						BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum3_flt(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRsum3_flt(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_flt,
						BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum3_dbl(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRsum3_dbl(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_dbl,
						BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum2_bte(bat *retval, bat *bid, bat *eid);
str
AGGRsum2_bte(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_bte, BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum2_sht(bat *retval, bat *bid, bat *eid);
str
AGGRsum2_sht(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_sht, BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum2_int(bat *retval, bat *bid, bat *eid);
str
AGGRsum2_int(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_int, BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum2_wrd(bat *retval, bat *bid, bat *eid);
str
AGGRsum2_wrd(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_wrd, BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum2_lng(bat *retval, bat *bid, bat *eid);
str
AGGRsum2_lng(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_lng, BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum2_flt(bat *retval, bat *bid, bat *eid);
str
AGGRsum2_flt(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_flt, BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRsum2_dbl(bat *retval, bat *bid, bat *eid);
str
AGGRsum2_dbl(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_dbl, BATgroupsum, 1, "aggr.sum");
}

aggr_export str AGGRprod3_bte(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRprod3_bte(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_bte,
						BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod3_sht(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRprod3_sht(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_sht,
						BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod3_int(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRprod3_int(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_int,
						BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod3_wrd(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRprod3_wrd(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_wrd,
						BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod3_lng(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRprod3_lng(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_lng,
						BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod3_flt(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRprod3_flt(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_flt,
						BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod3_dbl(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRprod3_dbl(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_dbl,
						BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod2_bte(bat *retval, bat *bid, bat *eid);
str
AGGRprod2_bte(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_bte, BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod2_sht(bat *retval, bat *bid, bat *eid);
str
AGGRprod2_sht(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_sht, BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod2_int(bat *retval, bat *bid, bat *eid);
str
AGGRprod2_int(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_int, BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod2_wrd(bat *retval, bat *bid, bat *eid);
str
AGGRprod2_wrd(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_wrd, BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod2_lng(bat *retval, bat *bid, bat *eid);
str
AGGRprod2_lng(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_lng, BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod2_flt(bat *retval, bat *bid, bat *eid);
str
AGGRprod2_flt(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_flt, BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRprod2_dbl(bat *retval, bat *bid, bat *eid);
str
AGGRprod2_dbl(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_dbl, BATgroupprod, 1, "aggr.prod");
}

aggr_export str AGGRavg3_dbl(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRavg3_dbl(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_dbl,
						BATgroupavg, 1, "aggr.avg");
}

aggr_export str AGGRavg2_dbl(bat *retval, bat *bid, bat *eid);
str
AGGRavg2_dbl(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_dbl, BATgroupavg, 1, "aggr.avg");
}

aggr_export str AGGRstdev3_dbl(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRstdev3_dbl(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_dbl,
						BATgroupstdev_sample, 1, "aggr.stdev");
}

aggr_export str AGGRstdev2_dbl(bat *retval, bat *bid, bat *eid);
str
AGGRstdev2_dbl(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_dbl, BATgroupstdev_sample, 1, "aggr.stdev");
}

aggr_export str AGGRstdevp3_dbl(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRstdevp3_dbl(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_dbl,
						BATgroupstdev_population, 1, "aggr.stdevp");
}

aggr_export str AGGRstdevp2_dbl(bat *retval, bat *bid, bat *eid);
str
AGGRstdevp2_dbl(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_dbl, BATgroupstdev_population, 1, "aggr.stdevp");
}

aggr_export str AGGRvariance3_dbl(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRvariance3_dbl(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_dbl,
						BATgroupvariance_sample, 1, "aggr.variance");
}

aggr_export str AGGRvariance2_dbl(bat *retval, bat *bid, bat *eid);
str
AGGRvariance2_dbl(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_dbl, BATgroupvariance_sample, 1, "aggr.variance");
}

aggr_export str AGGRvariancep3_dbl(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRvariancep3_dbl(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_dbl,
						BATgroupvariance_population, 1, "aggr.variancep");
}

aggr_export str AGGRvariancep2_dbl(bat *retval, bat *bid, bat *eid);
str
AGGRvariancep2_dbl(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_dbl, BATgroupvariance_population, 1, "aggr.variancep");
}

aggr_export str AGGRcount3(bat *retval, bat *bid, bat *gid, bat *eid, bit *ignorenils);
str
AGGRcount3(bat *retval, bat *bid, bat *gid, bat *eid, bit *ignorenils)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_wrd,
						BATgroupcount, *ignorenils, "aggr.count");
}

aggr_export str AGGRcount3nonils(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRcount3nonils(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_wrd,
						BATgroupcount, 1, "aggr.count");
}

aggr_export str AGGRcount3nils(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRcount3nils(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_wrd,
						BATgroupcount, 0, "aggr.count");
}

aggr_export str AGGRcount2(bat *retval, bat *bid, bat *eid, bit *ignorenils);
str
AGGRcount2(bat *retval, bat *bid, bat *eid, bit *ignorenils)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_wrd,
						BATgroupcount, *ignorenils, "aggr.count");
}

aggr_export str AGGRcount2nonils(bat *retval, bat *bid, bat *eid);
str
AGGRcount2nonils(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_wrd,
						BATgroupcount, 1, "aggr.count");
}

aggr_export str AGGRcount2nils(bat *retval, bat *bid, bat *eid);
str
AGGRcount2nils(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_wrd,
						BATgroupcount, 0, "aggr.count");
}

aggr_export str AGGRsize2(bat *retval, bat *bid, bat *eid);
str
AGGRsize2(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_wrd,
						BATgroupsize, 1, "aggr.size");
}

aggr_export str AGGRmin3(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRmin3(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_oid,
						BATgroupmin, 0, "aggr.min");
}

aggr_export str AGGRmin2(bat *retval, bat *bid, bat *eid);
str
AGGRmin2(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_oid, BATgroupmin, 0, "aggr.min");
}

aggr_export str AGGRmax3(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRmax3(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_oid,
						BATgroupmax, 0, "aggr.max");
}

aggr_export str AGGRmax2(bat *retval, bat *bid, bat *eid);
str
AGGRmax2(bat *retval, bat *bid, bat *eid)
{
	return AGGRgrouped2(retval, bid, eid, TYPE_oid, BATgroupmax, 0, "aggr.max");
}

aggr_export str AGGRmedian3(bat *retval, bat *bid, bat *gid, bat *eid);
str
AGGRmedian3(bat *retval, bat *bid, bat *gid, bat *eid)
{
	return AGGRgrouped3(retval, bid, gid, eid, TYPE_any,
						BATgroupmedian, 0, "aggr.median");
}

static str
AGGRsubgrouped(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid,
			   int skip_nils, int abort_on_error, int tp,
			   BAT *(*grpfunc)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			   const char *malfunc)
{
	BAT *b, *g, *e, *s, *bn;

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	if (b == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL)) {
		if (b)
			BBPreleaseref(b->batCacheid);
		if (g)
			BBPreleaseref(g->batCacheid);
		if (e)
			BBPreleaseref(e->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	if (tp == TYPE_any && grpfunc == BATgroupmedian)
		tp = b->ttype;

	if (sid) {
		s = BATdescriptor(*sid);
		if (s == NULL) {
			BBPreleaseref(b->batCacheid);
			if (g)
				BBPreleaseref(g->batCacheid);
			if (e)
				BBPreleaseref(e->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
	} else {
		if (!BAThdense(b)) {
			/* XXX backward compatibility code: ignore non-dense head, but
			 * only if no candidate list */
			s = BATmirror(BATmark(BATmirror(b), 0));
			BBPreleaseref(b->batCacheid);
			b = s;
		}
		s = NULL;
	}
	bn = (*grpfunc)(b, g, e, s, tp, skip_nils, abort_on_error);
	BBPreleaseref(b->batCacheid);
	if (g)
		BBPreleaseref(g->batCacheid);
	if (e)
		BBPreleaseref(e->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL) {
		char *errbuf = GDKerrbuf;
		char *s;

		if (errbuf && *errbuf) {
			if (strncmp(errbuf, "!ERROR: ", 8) == 0)
				errbuf += 8;
			if (strchr(errbuf, '!') == errbuf + 5) {
				s = createException(MAL, malfunc, "%s", errbuf);
			} else if ((s = strchr(errbuf, ':')) != NULL && s[1] == ' ') {
				s = createException(MAL, malfunc, "%s", s + 2);
			} else {
				s = createException(MAL, malfunc, "%s", errbuf);
			}
			*GDKerrbuf = 0;
			return s;
		}
		throw(MAL, malfunc, OPERATION_FAILED);
	}
	*retval = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

aggr_export str AGGRsubsum_bte(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_bte(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsum_sht(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_sht(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsum_int(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_int(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsum_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_wrd, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsum_lng(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_lng(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsum_flt(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_flt(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsum_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsumcand_bte(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsumcand_bte(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsumcand_sht(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsumcand_sht(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsumcand_int(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsumcand_int(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsumcand_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsumcand_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_wrd, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsumcand_lng(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsumcand_lng(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsumcand_flt(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsumcand_flt(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubsumcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsumcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupsum, "aggr.subsum");
}

aggr_export str AGGRsubprod_bte(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprod_bte(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprod_sht(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprod_sht(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprod_int(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprod_int(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprod_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprod_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_wrd, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprod_lng(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprod_lng(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprod_flt(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprod_flt(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprod_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprod_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprodcand_bte(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprodcand_bte(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprodcand_sht(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprodcand_sht(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprodcand_int(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprodcand_int(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprodcand_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprodcand_wrd(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_wrd, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprodcand_lng(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprodcand_lng(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprodcand_flt(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprodcand_flt(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubprodcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubprodcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupprod, "aggr.subprod");
}

aggr_export str AGGRsubavg_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubavg_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupavg, "aggr.subavg");
}

aggr_export str AGGRsubavgcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubavgcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupavg, "aggr.subavg");
}

aggr_export str AGGRsubstdev_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubstdev_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_sample, "aggr.substdev");
}

aggr_export str AGGRsubstdevcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubstdevcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_sample, "aggr.substdev");
}

aggr_export str AGGRsubstdevp_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubstdevp_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_population, "aggr.substdevp");
}

aggr_export str AGGRsubstdevpcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubstdevpcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_population, "aggr.substdevp");
}

aggr_export str AGGRsubvariance_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubvariance_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupvariance_sample, "aggr.subvariance");
}

aggr_export str AGGRsubvariancecand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubvariancecand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupvariance_sample, "aggr.subvariance");
}

aggr_export str AGGRsubvariancep_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubvariancep_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupvariance_population, "aggr.subvariancep");
}

aggr_export str AGGRsubvariancepcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubvariancepcand_dbl(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupvariance_population, "aggr.subvariancep");
}

aggr_export str AGGRsubcount(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils);
str
AGGRsubcount(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_wrd, BATgroupcount, "aggr.subcount");
}

aggr_export str AGGRsubcountcand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils);
str
AGGRsubcountcand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_wrd, BATgroupcount, "aggr.subcount");
}

aggr_export str AGGRsubmin(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils);
str
AGGRsubmin(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_oid, BATgroupmin, "aggr.submin");
}

aggr_export str AGGRsubmincand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils);
str
AGGRsubmincand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_oid, BATgroupmin, "aggr.submin");
}

aggr_export str AGGRsubmax(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils);
str
AGGRsubmax(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_oid, BATgroupmax, "aggr.submax");
}

aggr_export str AGGRsubmaxcand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils);
str
AGGRsubmaxcand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_oid, BATgroupmax, "aggr.submax");
}

aggr_export str AGGRsubmin_val(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils);
str
AGGRsubmin_val(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_oid, BATgroupmin, "aggr.submin")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	a = BATdescriptor(ret);
	r = BATouterjoin(a, b, BATcount(a));
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(a->batCacheid);
	BBPdecref(ret, TRUE);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}

aggr_export str AGGRsubmincand_val(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils);
str
AGGRsubmincand_val(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_oid, BATgroupmin, "aggr.submin")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	a = BATdescriptor(ret);
	r = BATouterjoin(a, b, BATcount(a));
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(a->batCacheid);
	BBPdecref(ret, TRUE);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}

aggr_export str AGGRsubmax_val(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils);
str
AGGRsubmax_val(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_oid, BATgroupmax, "aggr.submax")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	a = BATdescriptor(ret);
	r = BATouterjoin(a, b, BATcount(a));
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(a->batCacheid);
	BBPdecref(ret, TRUE);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}

aggr_export str AGGRsubmaxcand_val(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils);
str
AGGRsubmaxcand_val(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_oid, BATgroupmax, "aggr.submax")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	a = BATdescriptor(ret);
	r = BATouterjoin(a, b, BATcount(a));
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(a->batCacheid);
	BBPdecref(ret, TRUE);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}


aggr_export str AGGRmedian(bat *retval, bat *bid, bit *skip_nils);
str
AGGRmedian(bat *retval, bat *bid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, NULL, NULL, NULL, *skip_nils,
						  0, TYPE_any, BATgroupmedian, "aggr.submedian");
}

aggr_export str AGGRsubmedian(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils);
str
AGGRsubmedian(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_any, BATgroupmedian, "aggr.submedian");
}

aggr_export str AGGRsubmediancand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils);
str
AGGRsubmediancand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	return AGGRsubgrouped(retval, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_any, BATgroupmedian, "aggr.submedian");
}
