/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"

/*
 * grouped aggregates
 */
static str
AGGRgrouped(bat *retval1, bat *retval2, BAT *b, BAT *g, BAT *e, int tp,
			BAT *(*grpfunc1)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			gdk_return (*grpfunc2)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *, int, int, int),
			BAT *(*quantilefunc)(BAT *, BAT *, BAT *, BAT *, int, double, int, int),
			BAT *quantile,
			int skip_nils,
			const char *malfunc)
{
	BAT *bn, *cnts = NULL;
	double qvalue;

   /* one of grpfunc1, grpfunc2 and quantilefunc is non-NULL and the others are */
	assert((grpfunc1 != NULL && grpfunc2 == NULL && quantilefunc == NULL) ||
			(grpfunc1 == NULL && grpfunc2 != NULL && quantilefunc == NULL) ||
			(grpfunc1 == NULL && grpfunc2 == NULL && quantilefunc != NULL) );
	/* if retval2 is non-NULL, we must have grpfunc2 */
	assert(retval2 == NULL || grpfunc2 != NULL);
	assert(quantile == NULL || quantilefunc != NULL);

	if (b == NULL || g == NULL || e == NULL) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	assert(b->hseqbase == g->hseqbase);
	assert(BATcount(b) == BATcount(g));
	if (tp == TYPE_any && (grpfunc1 == BATgroupmedian || quantilefunc == BATgroupquantile))
		tp = b->ttype;
	if (grpfunc1)
		bn = (*grpfunc1)(b, g, e, NULL, tp, skip_nils, 1);
	if (quantilefunc) {
		assert(BATcount(quantile)>0);
		assert(quantile->ttype == TYPE_dbl);
		qvalue = ((const double *)Tloc(quantile, 0))[0];
		if (qvalue <  0|| qvalue > 1) {
			char *s;
			s = createException(MAL, malfunc, "quantile value of %f is not in range [0,1]", qvalue);
			return s;
		}
		bn = (*quantilefunc)(b, g, e, NULL, tp, qvalue, skip_nils, 1);
		BBPunfix(quantile->batCacheid);
	}
	if (grpfunc2 && (*grpfunc2)(&bn, retval2 ? &cnts : NULL, b, g, e, NULL, tp, skip_nils, 1) != GDK_SUCCEED)
		bn = NULL;
	if (bn != NULL && (grpfunc1 == BATgroupmin || grpfunc1 == BATgroupmax)) {
		BAT *t = BATproject(bn, b);
		BBPunfix(bn->batCacheid);
		bn = t;
	}
	BBPunfix(b->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(e->batCacheid);
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
	*retval1 = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	if (retval2) {
		*retval2 = cnts->batCacheid;
		BBPkeepref(cnts->batCacheid);
	}
	return MAL_SUCCEED;
}

static str
AGGRgrouped3(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, int tp,
			 BAT *(*grpfunc1)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			 gdk_return (*grpfunc2)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *, int, int, int),
			 int skip_nils,
			 const char *malfunc)
{
	BAT *b, *g, *e;

	b = BATdescriptor(*bid);	/* [head,value] */
	g = BATdescriptor(*gid);	/* [head,gid] */
	e = BATdescriptor(*eid);	/* [gid,any] */
	return AGGRgrouped(retval1, retval2, b, g, e, tp, grpfunc1, grpfunc2, NULL, 0, skip_nils, malfunc);
}

mal_export str AGGRsum3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_bte,
						BATgroupsum, NULL, 1, "aggr.sum");
}

mal_export str AGGRsum3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_sht,
						BATgroupsum, NULL, 1, "aggr.sum");
}

mal_export str AGGRsum3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_int,
						BATgroupsum, NULL, 1, "aggr.sum");
}

mal_export str AGGRsum3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_lng,
						BATgroupsum, NULL, 1, "aggr.sum");
}

#ifdef HAVE_HGE
mal_export str AGGRsum3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_hge,
						BATgroupsum, NULL, 1, "aggr.sum");
}
#endif

mal_export str AGGRsum3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_flt,
						BATgroupsum, NULL, 1, "aggr.sum");
}

mal_export str AGGRsum3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_dbl,
						BATgroupsum, NULL, 1, "aggr.sum");
}

mal_export str AGGRprod3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_bte,
						BATgroupprod, NULL, 1, "aggr.prod");
}

mal_export str AGGRprod3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_sht,
						BATgroupprod, NULL, 1, "aggr.prod");
}

mal_export str AGGRprod3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_int,
						BATgroupprod, NULL, 1, "aggr.prod");
}

mal_export str AGGRprod3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_lng,
						BATgroupprod, NULL, 1, "aggr.prod");
}

#ifdef HAVE_HGE
mal_export str AGGRprod3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_hge,
						BATgroupprod, NULL, 1, "aggr.prod");
}
#endif

mal_export str AGGRprod3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_flt,
						BATgroupprod, NULL, 1, "aggr.prod");
}

mal_export str AGGRprod3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_dbl,
						BATgroupprod, NULL, 1, "aggr.prod");
}

mal_export str AGGRavg13_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRavg13_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_dbl,
						NULL, BATgroupavg, 1, "aggr.avg");
}

mal_export str AGGRavg23_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid);
str
AGGRavg23_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval1, retval2, bid, gid, eid, TYPE_dbl,
						NULL, BATgroupavg, 1, "aggr.avg");
}

mal_export str AGGRstdev3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRstdev3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_dbl,
						BATgroupstdev_sample, NULL, 1, "aggr.stdev");
}

mal_export str AGGRstdevp3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRstdevp3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_dbl,
						BATgroupstdev_population, NULL, 1, "aggr.stdevp");
}

mal_export str AGGRvariance3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRvariance3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_dbl,
						BATgroupvariance_sample, NULL, 1, "aggr.variance");
}

mal_export str AGGRvariancep3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRvariancep3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_dbl,
						BATgroupvariance_population, NULL, 1, "aggr.variancep");
}

mal_export str AGGRcount3(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *ignorenils);
str
AGGRcount3(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *ignorenils)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_lng,
						BATgroupcount, NULL, *ignorenils, "aggr.count");
}

mal_export str AGGRcount3nonils(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRcount3nonils(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_lng,
						BATgroupcount, NULL, 1, "aggr.count");
}

mal_export str AGGRcount3nils(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRcount3nils(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_lng,
						BATgroupcount, NULL, 0, "aggr.count");
}

mal_export str AGGRmin3(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRmin3(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_oid,
						BATgroupmin, NULL, 0, "aggr.min");
}

mal_export str AGGRmax3(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRmax3(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_oid,
						BATgroupmax, NULL, 0, "aggr.max");
}

mal_export str AGGRmedian3(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRmedian3(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped3(retval, NULL, bid, gid, eid, TYPE_any,
						BATgroupmedian, NULL, 0, "aggr.median");
}


// XXX: when are these functions called?
mal_export str AGGRquantile3(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *quantile);
str
AGGRquantile3(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *quantile)
{
	// this is inlined from AGGRgrouped3 to avoid changing all the other functions for now
	BAT *b, *g, *e, *q;
	b = BATdescriptor(*bid);	/* [head,value] */
	g = BATdescriptor(*gid);	/* [head,gid] */
	e = BATdescriptor(*eid);	/* [gid,any] */
	q = BATdescriptor(*quantile);
	return AGGRgrouped(retval, NULL, b, g, e, TYPE_any, NULL, NULL,
					   BATgroupquantile, q, 0, "aggr.quantile");
}

static str
AGGRsubgroupedExt(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid,
			   int skip_nils, int abort_on_error, int tp,
			   BAT *(*grpfunc1)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			   gdk_return (*grpfunc2)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *, int, int, int),
			   BAT *(*quantilefunc)(BAT *, BAT *, BAT *, BAT *, int, double, int, int),
			   const bat *quantile,
			   const char *malfunc)
{
	BAT *b, *g, *e, *s, *bn = NULL, *cnts, *q = NULL;
	double qvalue;

   /* one of grpfunc1, grpfunc2 and quantilefunc is non-NULL and the others are */
	assert((grpfunc1 && grpfunc2 == NULL && quantilefunc == NULL) ||
			(grpfunc1 == NULL && grpfunc2 && quantilefunc == NULL) ||
			(grpfunc1 == NULL && grpfunc2 == NULL && quantilefunc) );

	/* if retval2 is non-NULL, we must have grpfunc2 */
	assert(retval2 == NULL || grpfunc2 != NULL);

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	q = quantile ? BATdescriptor(*quantile) : NULL;

	if (b == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	if (tp == TYPE_any && (grpfunc1 == BATgroupmedian || quantilefunc == BATgroupquantile))
		tp = b->ttype;

	if (sid) {
		s = BATdescriptor(*sid);
		if (s == NULL) {
			BBPunfix(b->batCacheid);
			if (g)
				BBPunfix(g->batCacheid);
			if (e)
				BBPunfix(e->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
	} else 
		s = NULL;
	if (grpfunc1)
		bn = (*grpfunc1)(b, g, e, s, tp, skip_nils, abort_on_error);
	if (quantilefunc) {
		assert(BATcount(q) > 0 || BATcount(b) == 0);
		assert(q->ttype == TYPE_dbl);
		if (BATcount(q) == 0) {
			qvalue = 0.5;
		} else {
			qvalue = ((const dbl *)Tloc(q, 0))[0];
			if (qvalue <  0|| qvalue > 1) {
				char *s;
				s = createException(MAL, malfunc, "quantile value of %f is not in range [0,1]", qvalue);
				return s;
			}
		}
		bn = (*quantilefunc)(b, g, e, s, tp, qvalue, skip_nils, abort_on_error);
		BBPunfix(q->batCacheid);
	}
	if (grpfunc2 && (*grpfunc2)(&bn, retval2 ? &cnts : NULL, b, g, e, s, tp, skip_nils, abort_on_error) != GDK_SUCCEED)
		bn = NULL;

	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
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
	*retval1 = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	if (retval2) {
		*retval2 = cnts->batCacheid;
		BBPkeepref(cnts->batCacheid);
	}
	return MAL_SUCCEED;
}

static str
AGGRsubgrouped(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid,
			   int skip_nils, int abort_on_error, int tp,
			   BAT *(*grpfunc1)(BAT *, BAT *, BAT *, BAT *, int, int, int),
			   gdk_return (*grpfunc2)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *, int, int, int),
			   const char *malfunc) {
	return AGGRsubgroupedExt(retval1, retval2, bid, gid, eid, sid, skip_nils, abort_on_error, tp, grpfunc1, grpfunc2, NULL, 0, malfunc);
}

mal_export str AGGRsubsum_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupsum, NULL, "aggr.subsum");
}

#ifdef HAVE_HGE
mal_export str AGGRsubsum_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_hge, BATgroupsum, NULL, "aggr.subsum");
}
#endif

mal_export str AGGRsubsum_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupsum, NULL, "aggr.subsum");
}

#ifdef HAVE_HGE
mal_export str AGGRsubsumcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_hge, BATgroupsum, NULL, "aggr.subsum");
}
#endif

mal_export str AGGRsubsumcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupsum, NULL, "aggr.subsum");
}

mal_export str AGGRsubprod_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupprod, NULL, "aggr.subprod");
}

#ifdef HAVE_HGE
mal_export str AGGRsubprod_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_hge, BATgroupprod, NULL, "aggr.subprod");
}
#endif

mal_export str AGGRsubprod_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_bte, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_sht, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_int, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_lng, BATgroupprod, NULL, "aggr.subprod");
}

#ifdef HAVE_HGE
mal_export str AGGRsubprodcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_hge, BATgroupprod, NULL, "aggr.subprod");
}
#endif

mal_export str AGGRsubprodcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_flt, BATgroupprod, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupprod, NULL,
						  "aggr.subprod");
}

mal_export str AGGRsubavg1_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg1_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, NULL, BATgroupavg,
						  "aggr.subavg");
}

mal_export str AGGRsubavg1cand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg1cand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, NULL, BATgroupavg,
						  "aggr.subavg");
}

mal_export str AGGRsubavg2_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg2_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval1, retval2, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, NULL, BATgroupavg,
						  "aggr.subavg");
}

mal_export str AGGRsubavg2cand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg2cand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval1, retval2, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, NULL, BATgroupavg,
						  "aggr.subavg");
}

mal_export str AGGRsubstdev_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdev_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_sample, NULL, "aggr.substdev");
}

mal_export str AGGRsubstdevcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdevcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_sample,
						  NULL, "aggr.substdev");
}

mal_export str AGGRsubstdevp_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdevp_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_population, NULL, "aggr.substdevp");
}

mal_export str AGGRsubstdevpcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdevpcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupstdev_population,
						  NULL, "aggr.substdevp");
}

mal_export str AGGRsubvariance_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariance_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupvariance_sample,
						  NULL, "aggr.subvariance");
}

mal_export str AGGRsubvariancecand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariancecand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl, BATgroupvariance_sample,
						  NULL, "aggr.subvariance");
}

mal_export str AGGRsubvariancep_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariancep_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  *abort_on_error, TYPE_dbl,
						  BATgroupvariance_population, NULL,
						  "aggr.subvariancep");
}

mal_export str AGGRsubvariancepcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariancepcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  *abort_on_error, TYPE_dbl,
						  BATgroupvariance_population, NULL,
						  "aggr.subvariancep");
}

mal_export str AGGRsubcount(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubcount(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_lng, BATgroupcount, NULL, "aggr.subcount");
}

mal_export str AGGRsubcountcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubcountcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_lng, BATgroupcount, NULL, "aggr.subcount");
}

mal_export str AGGRsubmin(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmin(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_oid, BATgroupmin, NULL, "aggr.submin");
}

mal_export str AGGRsubmincand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmincand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_oid, BATgroupmin, NULL, "aggr.submin");
}

mal_export str AGGRsubmax(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmax(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_oid, BATgroupmax, NULL, "aggr.submax");
}

mal_export str AGGRsubmaxcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmaxcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_oid, BATgroupmax, NULL, "aggr.submax");
}

mal_export str AGGRsubmin_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmin_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, NULL, bid, gid, eid, NULL, *skip_nils,
							  0, TYPE_oid, BATgroupmin, NULL,
							  "aggr.submin")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "aggr.submax", INTERNAL_BAT_ACCESS);
	a = BATdescriptor(ret);
	if( a == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "aggr.submax", INTERNAL_BAT_ACCESS);
	}
	r = BATproject(a, b);
	BBPunfix(b->batCacheid);
	BBPunfix(a->batCacheid);
	BBPrelease(ret);
	if (r == NULL)
		throw(MAL, "aggr.submin", MAL_MALLOC_FAIL);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}

mal_export str AGGRsubmincand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmincand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, NULL, bid, gid, eid, sid, *skip_nils,
							  0, TYPE_oid, BATgroupmin, NULL,
							  "aggr.submin")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "aggr.submax", INTERNAL_BAT_ACCESS);
	a = BATdescriptor(ret);
	if( a == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "aggr.submax", INTERNAL_BAT_ACCESS);
	}
	r = BATproject(a, b);
	BBPunfix(b->batCacheid);
	BBPunfix(a->batCacheid);
	BBPrelease(ret);
	if (r == NULL)
		throw(MAL, "aggr.submin", MAL_MALLOC_FAIL);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}

mal_export str AGGRsubmax_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmax_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, NULL, bid, gid, eid, NULL, *skip_nils,
							  0, TYPE_oid, BATgroupmax, NULL,
							  "aggr.submax")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "aggr.submax", INTERNAL_BAT_ACCESS);
	a = BATdescriptor(ret);
	if( a == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "aggr.submax", INTERNAL_BAT_ACCESS);
	}
	r = BATproject(a, b);
	BBPunfix(b->batCacheid);
	BBPunfix(a->batCacheid);
	BBPrelease(ret);
	if (r == NULL)
		throw(MAL, "aggr.submax", MAL_MALLOC_FAIL);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}

mal_export str AGGRsubmaxcand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmaxcand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	BAT *a, *b, *r;
	str res;
	bat ret;

	if ((res = AGGRsubgrouped(&ret, NULL, bid, gid, eid, sid, *skip_nils,
							  0, TYPE_oid, BATgroupmax, NULL,
							  "aggr.submax")) != MAL_SUCCEED)
		return res;
	b = BATdescriptor(*bid);
	if ( b == NULL)
		throw(MAL, "aggr.max", RUNTIME_OBJECT_MISSING);
	a = BATdescriptor(ret);
	if ( a == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "aggr.max", RUNTIME_OBJECT_MISSING);
	}
	r = BATproject(a, b);
	BBPunfix(b->batCacheid);
	BBPunfix(a->batCacheid);
	BBPrelease(ret);
	if (r == NULL)
		throw(MAL, "aggr.submax", MAL_MALLOC_FAIL);
	BBPkeepref(*retval = r->batCacheid);
	return MAL_SUCCEED;
}


mal_export str AGGRmedian(bat *retval, const bat *bid, const bit *skip_nils);
str
AGGRmedian(bat *retval, const bat *bid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, NULL, NULL, NULL, *skip_nils,
						  0, TYPE_any, BATgroupmedian, NULL, "aggr.submedian");
}

mal_export str AGGRsubmedian(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmedian(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
						  0, TYPE_any, BATgroupmedian, NULL, "aggr.submedian");
}

mal_export str AGGRsubmediancand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmediancand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRsubgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
						  0, TYPE_any, BATgroupmedian, NULL, "aggr.submedian");
}

/* quantile functions, could make median functions obsolete completely */
mal_export str AGGRquantile(bat *retval, const bat *bid, const bat *quantile, const bit *skip_nils);
str
AGGRquantile(bat *retval, const bat *bid, const bat *quantile, const bit *skip_nils)
{
	return AGGRsubgroupedExt(retval, NULL, bid, NULL, NULL, NULL, *skip_nils,
							 0, TYPE_any, NULL, NULL, BATgroupquantile,
							 quantile, "aggr.subquantile");
}

mal_export str AGGRsubquantile(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubquantile(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubgroupedExt(retval, NULL, bid, gid, eid, NULL, *skip_nils,
							 0, TYPE_any, NULL, NULL, BATgroupquantile,
							 quantile, "aggr.subquantile");
}

mal_export str AGGRsubquantilecand(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubquantilecand(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRsubgroupedExt(retval, NULL, bid, gid, eid, sid, *skip_nils,
							 0, TYPE_any, NULL, NULL, BATgroupquantile,
							 quantile, "aggr.subquantile");
}
