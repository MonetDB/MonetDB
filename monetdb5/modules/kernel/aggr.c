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

/*
 * grouped aggregates
 */
static str
AGGRgrouped(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid,
			bool skip_nils, bool abort_on_error, int scale, int tp,
			BAT *(*grpfunc1)(BAT *, BAT *, BAT *, BAT *, int, bool, bool),
			gdk_return (*grpfunc2)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *, int, bool, bool, int),
			BAT *(*quantilefunc)(BAT *, BAT *, BAT *, BAT *, int, double, bool, bool),
			const bat *quantile,
			const char *malfunc)
{
	BAT *b, *g, *e, *s, *bn = NULL, *cnts, *q = NULL;
	double qvalue;

	/* exactly one of grpfunc1, grpfunc2 and quantilefunc is non-NULL */
	assert((grpfunc1 != NULL) + (grpfunc2 != NULL) + (quantilefunc != NULL) == 1);

	/* if retval2 is non-NULL, we must have grpfunc2 */
	assert(retval2 == NULL || grpfunc2 != NULL);
	/* only quantiles need a quantile BAT */
	assert((quantilefunc == NULL) == (quantile == NULL));

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	s = sid ? BATdescriptor(*sid) : NULL;
	q = quantile ? BATdescriptor(*quantile) : NULL;

	if (b == NULL ||
		(gid != NULL && g == NULL) ||
		(eid != NULL && e == NULL) ||
		(sid != NULL && s == NULL) ||
		(quantile != NULL && q == NULL)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		if (q)
			BBPunfix(q->batCacheid);
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (tp == TYPE_any &&
		(grpfunc1 == BATgroupmedian ||
		 grpfunc1 == BATgroupmedian_avg ||
		 quantilefunc == BATgroupquantile ||
		 quantilefunc == BATgroupquantile_avg))
		tp = b->ttype;

	if (grpfunc1) {
		bn = (*grpfunc1)(b, g, e, s, tp, skip_nils, abort_on_error);
	} else if (quantilefunc) {
		assert(BATcount(q) > 0 || BATcount(b) == 0);
		assert(q->ttype == TYPE_dbl);
		if (BATcount(q) == 0) {
			qvalue = 0.5;
		} else {
			qvalue = ((const dbl *)Tloc(q, 0))[0];
			if (qvalue < 0 || qvalue > 1) {
				BBPunfix(b->batCacheid);
				if (g)
					BBPunfix(g->batCacheid);
				if (e)
					BBPunfix(e->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				BBPunfix(q->batCacheid);
				throw(MAL, malfunc,
					  "quantile value of %f is not in range [0,1]", qvalue);
			}
		}
		BBPunfix(q->batCacheid);
		bn = (*quantilefunc)(b, g, e, s, tp, qvalue, skip_nils, abort_on_error);
	} else if ((*grpfunc2)(&bn, retval2 ? &cnts : NULL, b, g, e, s, tp,
						   skip_nils, abort_on_error, scale) != GDK_SUCCEED) {
		bn = NULL;
	}

	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, malfunc, GDK_EXCEPTION);
	*retval1 = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	if (retval2) {
		*retval2 = cnts->batCacheid;
		BBPkeepref(cnts->batCacheid);
	}
	return MAL_SUCCEED;
}

mal_export str AGGRsum3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_bte,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

mal_export str AGGRsum3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_sht,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

mal_export str AGGRsum3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_int,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

mal_export str AGGRsum3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_lng,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

#ifdef HAVE_HGE
mal_export str AGGRsum3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_hge,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}
#endif

mal_export str AGGRsum3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_flt,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

mal_export str AGGRsum3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRsum3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

mal_export str AGGRprod3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_bte,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

mal_export str AGGRprod3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_sht,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

mal_export str AGGRprod3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_int,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

mal_export str AGGRprod3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_lng,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

#ifdef HAVE_HGE
mal_export str AGGRprod3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_hge,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}
#endif

mal_export str AGGRprod3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_flt,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

mal_export str AGGRprod3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRprod3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

mal_export str AGGRavg13_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRavg13_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

mal_export str AGGRavg23_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid);
str
AGGRavg23_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

mal_export str AGGRavg14_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, int *scale);
str
AGGRavg14_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, int *scale)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, *scale, TYPE_dbl,
					   NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

mal_export str AGGRavg24_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, int *scale);
str
AGGRavg24_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, int *scale)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, 1, 1, *scale, TYPE_dbl,
					   NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

mal_export str AGGRstdev3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRstdev3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   BATgroupstdev_sample, NULL, NULL, NULL, "aggr.stdev");
}

mal_export str AGGRstdevp3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRstdevp3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   BATgroupstdev_population, NULL, NULL, NULL, "aggr.stdevp");
}

mal_export str AGGRvariance3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRvariance3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   BATgroupvariance_sample, NULL, NULL, NULL, "aggr.variance");
}

mal_export str AGGRvariancep3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRvariancep3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_dbl,
					   BATgroupvariance_population, NULL, NULL, NULL, "aggr.variancep");
}

mal_export str AGGRcount3(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *ignorenils);
str
AGGRcount3(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *ignorenils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *ignorenils, 1, 0, TYPE_lng,
					   BATgroupcount, NULL, NULL, NULL, "aggr.count");
}

mal_export str AGGRcount3nonils(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRcount3nonils(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 1, 1, 0, TYPE_lng,
					   BATgroupcount, NULL, NULL, NULL, "aggr.count");
}

mal_export str AGGRcount3nils(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRcount3nils(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, 0, 1, 0, TYPE_lng,
					   BATgroupcount, NULL, NULL, NULL, "aggr.count");
}

#include "algebra.h"			/* for ALGprojection */
mal_export str AGGRmin3(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRmin3(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, NULL, 0, 1, 0, TYPE_oid,
					  BATgroupmin, NULL, NULL, NULL, "aggr.min");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

mal_export str AGGRmax3(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRmax3(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, NULL, 0, 1, 0, TYPE_oid,
					  BATgroupmax, NULL, NULL, NULL, "aggr.max");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

mal_export str AGGRsubsum_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_bte, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_sht, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_int, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_lng, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

#ifdef HAVE_HGE
mal_export str AGGRsubsum_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, bit *skip_nils, bit *abort_on_error);
str
AGGRsubsum_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, bit *skip_nils, bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_hge, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}
#endif

mal_export str AGGRsubsum_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_flt, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsum_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsum_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_bte, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_sht, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_int, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_lng, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

#ifdef HAVE_HGE
mal_export str AGGRsubsumcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_hge, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}
#endif

mal_export str AGGRsubsumcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_flt, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubsumcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubsumcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

mal_export str AGGRsubprod_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_bte, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_sht, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_int, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_lng, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

#ifdef HAVE_HGE
mal_export str AGGRsubprod_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_hge, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}
#endif

mal_export str AGGRsubprod_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_flt, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprod_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprod_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_bte, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_sht, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_int, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_lng, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

#ifdef HAVE_HGE
mal_export str AGGRsubprodcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_hge, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}
#endif

mal_export str AGGRsubprodcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_flt, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubprodcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubprodcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

mal_export str AGGRsubavg1_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg1_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubavg1cand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg1cand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubavg2_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg2_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubavg2cand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubavg2cand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubavg1s_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error, int *scale);
str
AGGRsubavg1s_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error, int *scale)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubavg1scand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error, int *scale);
str
AGGRsubavg1scand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error, int *scale)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubavg2s_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error, int *scale);
str
AGGRsubavg2s_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error, int *scale)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubavg2scand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error, int *scale);
str
AGGRsubavg2scand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error, int *scale)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

mal_export str AGGRsubstdev_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdev_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupstdev_sample,
					   NULL, NULL, NULL, "aggr.substdev");
}

mal_export str AGGRsubstdevcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdevcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupstdev_sample,
					   NULL, NULL, NULL, "aggr.substdev");
}

mal_export str AGGRsubstdevp_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdevp_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl,
					   BATgroupstdev_population, NULL, NULL, NULL,
					   "aggr.substdevp");
}

mal_export str AGGRsubstdevpcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstdevpcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl,
					   BATgroupstdev_population,
					   NULL, NULL, NULL, "aggr.substdevp");
}

mal_export str AGGRsubvariance_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariance_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupvariance_sample,
					   NULL, NULL, NULL, "aggr.subvariance");
}

mal_export str AGGRsubvariancecand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariancecand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl, BATgroupvariance_sample,
					   NULL, NULL, NULL, "aggr.subvariance");
}

mal_export str AGGRsubvariancep_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariancep_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl,
					   BATgroupvariance_population, NULL,
					   NULL, NULL, "aggr.subvariancep");
}

mal_export str AGGRsubvariancepcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubvariancepcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *abort_on_error, 0, TYPE_dbl,
					   BATgroupvariance_population, NULL,
					   NULL, NULL, "aggr.subvariancep");
}

mal_export str AGGRsubcount(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubcount(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, 0, TYPE_lng, BATgroupcount, NULL, NULL,
					   NULL, "aggr.subcount");
}

mal_export str AGGRsubcountcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubcountcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, 0, TYPE_lng, BATgroupcount, NULL,
					   NULL, NULL, "aggr.subcount");
}

mal_export str AGGRsubmin(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmin(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, 0, TYPE_oid, BATgroupmin, NULL,
					   NULL, NULL, "aggr.submin");
}

mal_export str AGGRsubmincand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmincand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, 0, TYPE_oid, BATgroupmin, NULL,
					   NULL, NULL, "aggr.submin");
}

mal_export str AGGRsubmax(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmax(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, 0, TYPE_oid, BATgroupmax, NULL,
					   NULL, NULL, "aggr.submax");
}

mal_export str AGGRsubmaxcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmaxcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, 0, TYPE_oid, BATgroupmax, NULL,
					   NULL, NULL, "aggr.submax");
}

mal_export str AGGRsubmincand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmincand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, sid, *skip_nils, 0,
					  0, TYPE_oid, BATgroupmin, NULL, NULL, NULL, "aggr.submin");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

mal_export str AGGRsubmin_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmin_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubmincand_val(retval, bid, gid, eid, NULL, skip_nils);
}

mal_export str AGGRsubmaxcand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmaxcand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, sid, *skip_nils, 0,
					  0, TYPE_oid, BATgroupmax, NULL, NULL, NULL, "aggr.submax");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

mal_export str AGGRsubmax_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmax_val(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubmaxcand_val(retval, bid, gid, eid, NULL, skip_nils);
}

mal_export str AGGRmedian(void *retval, const bat *bid);
str
AGGRmedian(void *retval, const bat *bid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, 1,
						   0, 0, TYPE_any, BATgroupmedian, NULL,
						   NULL, NULL, "aggr.submedian")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

mal_export str AGGRsubmedian(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmedian(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, 0, TYPE_any, BATgroupmedian, NULL,
					   NULL, NULL, "aggr.submedian");
}

mal_export str AGGRsubmediancand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmediancand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, 0, TYPE_any, BATgroupmedian, NULL,
					   NULL, NULL, "aggr.submedian");
}

/* quantile functions, could make median functions obsolete completely */
mal_export str AGGRquantile(void *retval, const bat *bid, const bat *qid);
str
AGGRquantile(void *retval, const bat *bid, const bat *qid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, 1,
						   0, 0, TYPE_any, NULL, NULL, BATgroupquantile,
						   qid, "aggr.subquantile")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

mal_export str AGGRsubquantile(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubquantile(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, 0, TYPE_any, NULL, NULL, BATgroupquantile,
					   quantile, "aggr.subquantile");
}

mal_export str AGGRsubquantilecand(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubquantilecand(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, 0, TYPE_any, NULL, NULL, BATgroupquantile,
					   quantile, "aggr.subquantile");
}

mal_export str AGGRmedian_avg(dbl *retval, const bat *bid);
str
AGGRmedian_avg(dbl *retval, const bat *bid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, 1,
						   0, 0, TYPE_any, BATgroupmedian_avg, NULL,
						   NULL, NULL, "aggr.submedian_avg")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

mal_export str AGGRsubmedian_avg(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubmedian_avg(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, 0, TYPE_any, BATgroupmedian_avg, NULL,
					   NULL, NULL, "aggr.submedian_avg");
}

mal_export str AGGRsubmediancand_avg(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubmediancand_avg(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, 0, TYPE_any, BATgroupmedian_avg, NULL,
					   NULL, NULL, "aggr.submedian_avg");
}

/* quantile functions, could make median functions obsolete completely */
mal_export str AGGRquantile_avg(dbl *retval, const bat *bid, const bat *qid);
str
AGGRquantile_avg(dbl *retval, const bat *bid, const bat *qid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, 1,
						   0, 0, TYPE_any, NULL, NULL, BATgroupquantile_avg,
						   qid, "aggr.subquantile_avg")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

mal_export str AGGRsubquantile_avg(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bit *skip_nils);
str
AGGRsubquantile_avg(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, 0, TYPE_any, NULL, NULL, BATgroupquantile_avg,
					   quantile, "aggr.subquantile_avg");
}

mal_export str AGGRsubquantilecand_avg(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
str
AGGRsubquantilecand_avg(bat *retval, const bat *bid, const bat *quantile, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, 0, TYPE_any, NULL, NULL, BATgroupquantile_avg,
					   quantile, "aggr.subquantile_avg");
}

static str
AGGRgroup_str_concat(bat *retval1, const bat *bid, const bat *gid, const bat *eid, const bat *sid, bool skip_nils,
					 bool abort_on_error, const bat *sepid, const char *separator, const char *malfunc)
{
	BAT *b, *g, *e, *s, *sep, *bn = NULL;

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	s = sid ? BATdescriptor(*sid) : NULL;
	sep = sepid ? BATdescriptor(*sepid) : NULL;

	if (b == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL) ||
		(sid != NULL && s == NULL) || (sepid != NULL && sep == NULL)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		if (sep)
			BBPunfix(sep->batCacheid);
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	bn = BATgroupstr_group_concat(b, g, e, s, sep, skip_nils, abort_on_error, separator);

	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (sep)
		BBPunfix(sep->batCacheid);
	if (bn == NULL)
		throw(MAL, malfunc, GDK_EXCEPTION);
	*retval1 = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

#define DEFAULT_SEPARATOR ","

mal_export str AGGRstr_group_concat(bat *retval, const bat *bid, const bat *gid, const bat *eid);
str
AGGRstr_group_concat(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, 1, 1, NULL, DEFAULT_SEPARATOR, "aggr.str_group_concat");
}

mal_export str AGGRsubstr_group_concat(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstr_group_concat(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, *skip_nils, *abort_on_error, NULL, DEFAULT_SEPARATOR, "aggr.substr_group_concat");
}

mal_export str AGGRsubstr_group_concatcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstr_group_concatcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, sid, *skip_nils, *abort_on_error, NULL, DEFAULT_SEPARATOR, "aggr.substr_group_concat");
}

mal_export str AGGRstr_group_concat_sep(bat *retval, const bat *bid, const bat *sep, const bat *gid, const bat *eid);
str
AGGRstr_group_concat_sep(bat *retval, const bat *bid, const bat *sep, const bat *gid, const bat *eid)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, true, true, sep, NULL, "aggr.str_group_concat_sep");;
}

mal_export str AGGRsubstr_group_concat_sep(bat *retval, const bat *bid, const bat *sep, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstr_group_concat_sep(bat *retval, const bat *bid, const bat *sep, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, *skip_nils, *abort_on_error, sep, NULL, "aggr.substr_group_concat_sep");
}

mal_export str AGGRsubstr_group_concatcand_sep(bat *retval, const bat *bid, const bat *sep, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubstr_group_concatcand_sep(bat *retval, const bat *bid, const bat *sep, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, sid, *skip_nils, *abort_on_error, sep, NULL, "aggr.substr_group_concat_sep");
}

static str
AGGRgrouped2(bat *retval, const bat *bid1, const bat *bid2, const bat *gid, const bat *eid, const bat *sid, bool skip_nils, bool abort_on_error,
			 int tp, BAT *(*func)(BAT *, BAT *, BAT *, BAT *, BAT *, int tp, bool skip_nils, bool abort_on_error),
			 const char *malfunc)
{
	BAT *b1, *b2, *g, *e, *s, *bn = NULL;

	assert(func != NULL);

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	s = sid ? BATdescriptor(*sid) : NULL;

	if (b1 == NULL || b2 == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL) ||
		(sid != NULL && s == NULL)) {
		if (b1)
			BBPunfix(b1->batCacheid);
		if (b2)
			BBPunfix(b2->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (b1->ttype != b2->ttype) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, malfunc, SQLSTATE(42000) "%s requires both arguments of the same type", malfunc);
	}

	bn = (*func)(b1, b2, g, e, s, tp, skip_nils, abort_on_error);

	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, malfunc, GDK_EXCEPTION);
	*retval = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

mal_export str AGGRcovariance(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid);
str
AGGRcovariance(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, 1, 0, TYPE_dbl, BATgroupcovariance_sample, "aggr.covariance");
}

mal_export str AGGRsubcovariance(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubcovariance(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, *skip_nils, *abort_on_error, TYPE_dbl, BATgroupcovariance_sample, "aggr.subcovariance");
}

mal_export str AGGRsubcovariancecand(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubcovariancecand(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, sid, *skip_nils, *abort_on_error, TYPE_dbl, BATgroupcovariance_sample, "aggr.subcovariance");
}

mal_export str AGGRcovariancep(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid);
str
AGGRcovariancep(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, 1, 0, TYPE_dbl, BATgroupcovariance_population, "aggr.covariancep");
}

mal_export str AGGRsubcovariancep(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubcovariancep(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, *skip_nils, *abort_on_error, TYPE_dbl, BATgroupcovariance_population, "aggr.subcovariancep");
}

mal_export str AGGRsubcovariancepcand(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubcovariancepcand(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, sid, *skip_nils, *abort_on_error, TYPE_dbl, BATgroupcovariance_population, "aggr.subcovariancep");
}

mal_export str AGGRcorr(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid);
str
AGGRcorr(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, 1, 0, TYPE_dbl, BATgroupcorrelation, "aggr.corr");
}

mal_export str AGGRsubcorr(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubcorr(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, *skip_nils, *abort_on_error, TYPE_dbl, BATgroupcorrelation, "aggr.subcorr");
}

mal_export str AGGRsubcorrcand(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error);
str
AGGRsubcorrcand(bat *retval, const bat *b1, const bat *b2, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils, const bit *abort_on_error)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, sid, *skip_nils, *abort_on_error, TYPE_dbl, BATgroupcorrelation, "aggr.subcorr");
}
