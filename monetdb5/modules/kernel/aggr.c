/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "aggr.h"

/*
 * grouped aggregates
 */
static str
AGGRgrouped_bat_or_val(bat *retval1, bat *retval2, const bat *bid,
					   const bat *gid, const bat *eid, const bat *sid,
					   bool skip_nils, int scale, int tp,
					   BAT *(*grpfunc1)(BAT *, BAT *, BAT *, BAT *, int, bool),
					   gdk_return(*grpfunc2)(BAT **, BAT **, BAT *, BAT *,
											 BAT *, BAT *, int, bool, int),
					   BAT * (*quantilefunc)(BAT *, BAT *, BAT *, BAT *, int,
											 double, bool), const bat *quantile,
					   const double *quantile_val, const char *malfunc)
{
	BAT *b, *g = NULL, *e = NULL, *s = NULL, *bn = NULL, *cnts = NULL, *q = NULL;
	double qvalue;

	/* exactly one of grpfunc1, grpfunc2 and quantilefunc is non-NULL */
	assert((grpfunc1 != NULL) + (grpfunc2 != NULL) + (quantilefunc != NULL) == 1);

	/* if retval2 is non-NULL, we must have grpfunc2 */
	assert(retval2 == NULL || grpfunc2 != NULL);
	/* only quantiles need a quantile BAT */
	assert((quantilefunc == NULL) == (quantile == NULL && quantile_val == NULL));

	b = BATdescriptor(*bid);
	if (b == NULL ||
		(gid && !is_bat_nil(*gid) && (g = BATdescriptor(*gid)) == NULL) ||
		(eid && !is_bat_nil(*eid) && (e = BATdescriptor(*eid)) == NULL) ||
		(sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) ||
		(quantile && !is_bat_nil(*quantile) && (q = BATdescriptor(*quantile)) == NULL)) {
		BBPreclaim(b);
		BBPreclaim(g);
		BBPreclaim(e);
		BBPreclaim(s);
		BBPreclaim(q);
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (tp == TYPE_any &&
		(grpfunc1 == BATgroupmedian ||
		 grpfunc1 == BATgroupmedian_avg ||
		 quantilefunc == BATgroupquantile ||
		 quantilefunc == BATgroupquantile_avg))
		tp = b->ttype;

	if (grpfunc1) {
		bn = (*grpfunc1) (b, g, e, s, tp, skip_nils);
	} else if (quantilefunc) {
		if (!quantile_val) {
			assert(BATcount(q) > 0 || BATcount(b) == 0);
			assert(q->ttype == TYPE_dbl);
			if (BATcount(q) == 0) {
				qvalue = 0.5;
			} else {
				MT_lock_set(&q->theaplock);
				qvalue = ((const dbl *) Tloc(q, 0))[0];
				MT_lock_unset(&q->theaplock);
				if (qvalue < 0 || qvalue > 1) {
					BBPunfix(b->batCacheid);
					BBPreclaim(g);
					BBPreclaim(e);
					BBPreclaim(s);
					BBPunfix(q->batCacheid);
					throw(MAL, malfunc,
						  "quantile value of %f is not in range [0,1]", qvalue);
				}
			}
			BBPunfix(q->batCacheid);
		} else {
			qvalue = *(quantile_val);
		}
		bn = (*quantilefunc) (b, g, e, s, tp, qvalue, skip_nils);
	} else if ((*grpfunc2) (&bn, retval2 ? &cnts : NULL, b, g, e, s, tp,
							skip_nils, scale) != GDK_SUCCEED) {
		bn = NULL;
	}

	BBPunfix(b->batCacheid);
	BBPreclaim(g);
	BBPreclaim(e);
	BBPreclaim(s);
	if (bn == NULL)
		throw(MAL, malfunc, GDK_EXCEPTION);
	*retval1 = bn->batCacheid;
	BBPkeepref(bn);
	if (retval2) {
		*retval2 = cnts->batCacheid;
		BBPkeepref(cnts);
	}
	return MAL_SUCCEED;
}

static str
AGGRgrouped(bat *retval1, bat *retval2, const bat *bid, const bat *gid,
			const bat *eid, const bat *sid, bool skip_nils, int scale, int tp,
			BAT *(*grpfunc1)(BAT *, BAT *, BAT *, BAT *, int, bool),
			gdk_return(*grpfunc2)(BAT **, BAT **, BAT *, BAT *, BAT *, BAT *,
								  int, bool, int), BAT * (*quantilefunc)(BAT *,
																		 BAT *,
																		 BAT *,
																		 BAT *,
																		 int,
																		 double,
																		 bool),
			const bat *quantile, const char *malfunc)
{
	return AGGRgrouped_bat_or_val(retval1, retval2, bid, gid, eid, sid,
								  skip_nils, scale, tp, grpfunc1, grpfunc2,
								  quantilefunc, quantile, NULL, malfunc);
}

static str
AGGRsum3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_bte,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

static str
AGGRsum3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_sht,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

static str
AGGRsum3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_int,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

str
AGGRsum3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_lng,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

#ifdef HAVE_HGE
str
AGGRsum3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_hge,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}
#endif

static str
AGGRsum3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_flt,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

static str
AGGRsum3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   BATgroupsum, NULL, NULL, NULL, "aggr.sum");
}

static str
AGGRprod3_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_bte,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

static str
AGGRprod3_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_sht,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

static str
AGGRprod3_int(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_int,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

static str
AGGRprod3_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_lng,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

#ifdef HAVE_HGE
static str
AGGRprod3_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_hge,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}
#endif

static str
AGGRprod3_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_flt,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

static str
AGGRprod3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   BATgroupprod, NULL, NULL, NULL, "aggr.prod");
}

static str
AGGRavg13_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

static str
AGGRavg23_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid,
			  const bat *eid)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

static str
AGGRavg14_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			  int *scale)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, *scale,
					   TYPE_dbl, NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

static str
AGGRavg24_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid,
			  const bat *eid, int *scale)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, true, *scale,
					   TYPE_dbl, NULL, BATgroupavg, NULL, NULL, "aggr.avg");
}

static str
AGGRstdev3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   BATgroupstdev_sample, NULL, NULL, NULL, "aggr.stdev");
}

static str
AGGRstdevp3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   BATgroupstdev_population, NULL, NULL, NULL,
					   "aggr.stdevp");
}

static str
AGGRvariance3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   BATgroupvariance_sample, NULL, NULL, NULL,
					   "aggr.variance");
}

static str
AGGRvariancep3_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_dbl,
					   BATgroupvariance_population, NULL, NULL, NULL,
					   "aggr.variancep");
}

static str
AGGRcount3(bat *retval, const bat *bid, const bat *gid, const bat *eid,
		   const bit *ignorenils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *ignorenils, 0,
					   TYPE_lng, BATgroupcount, NULL, NULL, NULL, "aggr.count");
}

static str
AGGRcount3nonils(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, true, 0, TYPE_lng,
					   BATgroupcount, NULL, NULL, NULL, "aggr.count");
}

static str
AGGRcount3nils(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, false, 0, TYPE_lng,
					   BATgroupcount, NULL, NULL, NULL, "aggr.count");
}

#include "algebra.h"			/* for ALGprojection */
static str
AGGRmin3(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, NULL, false, 0, TYPE_oid,
					  BATgroupmin, NULL, NULL, NULL, "aggr.min");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

static str
AGGRmax3(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, NULL, false, 0, TYPE_oid,
					  BATgroupmax, NULL, NULL, NULL, "aggr.max");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

static str
AGGRsubsum_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_bte, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsum_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_sht, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsum_int(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_int, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsum_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_lng, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

#ifdef HAVE_HGE
static str
AGGRsubsum_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_hge, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}
#endif

static str
AGGRsubsum_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_flt, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsum_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsumcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_bte, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsumcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_sht, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsumcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_int, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsumcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_lng, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

#ifdef HAVE_HGE
static str
AGGRsubsumcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_hge, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}
#endif

static str
AGGRsubsumcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_flt, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubsumcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl, BATgroupsum, NULL,
					   NULL, NULL, "aggr.subsum");
}

static str
AGGRsubprod_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_bte, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprod_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_sht, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprod_int(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_int, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprod_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_lng, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

#ifdef HAVE_HGE
static str
AGGRsubprod_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_hge, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}
#endif

static str
AGGRsubprod_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_flt, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprod_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprodcand_bte(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_bte, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprodcand_sht(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_sht, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprodcand_int(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_int, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprodcand_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_lng, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

#ifdef HAVE_HGE
static str
AGGRsubprodcand_hge(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_hge, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}
#endif

static str
AGGRsubprodcand_flt(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_flt, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubprodcand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl, BATgroupprod, NULL,
					   NULL, NULL, "aggr.subprod");
}

static str
AGGRsubavg1_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRsubavg1cand_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRsubavg2_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid,
				const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRsubavg2cand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid,
					const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRsubavg1s_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				 const bit *skip_nils, const int *scale)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRsubavg1scand_dbl(bat *retval, const bat *bid, const bat *gid,
					 const bat *eid, const bat *sid, const bit *skip_nils,
					 const int *scale)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRsubavg2s_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid,
				 const bat *eid, const bit *skip_nils, const int *scale)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, NULL, *skip_nils,
					   *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRsubavg2scand_dbl(bat *retval1, bat *retval2, const bat *bid, const bat *gid,
					 const bat *eid, const bat *sid, const bit *skip_nils,
					 const int *scale)
{
	return AGGRgrouped(retval1, retval2, bid, gid, eid, sid, *skip_nils,
					   *scale, TYPE_dbl, NULL, BATgroupavg,
					   NULL, NULL, "aggr.subavg");
}

static str
AGGRavg3(bat *retval1, bat *retval2, bat *retval3, const bat *bid,
		 const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	BAT *b, *g, *e, *s, *avgs, *cnts, *rems;
	gdk_return rc;

	b = BATdescriptor(*bid);
	g = gid != NULL && !is_bat_nil(*gid) ? BATdescriptor(*gid) : NULL;
	e = eid != NULL && !is_bat_nil(*eid) ? BATdescriptor(*eid) : NULL;
	s = sid != NULL && !is_bat_nil(*sid) ? BATdescriptor(*sid) : NULL;

	if (b == NULL ||
		(gid != NULL && !is_bat_nil(*gid) && g == NULL) ||
		(eid != NULL && !is_bat_nil(*eid) && e == NULL) ||
		(sid != NULL && !is_bat_nil(*sid) && s == NULL)) {
		BBPreclaim(b);
		BBPreclaim(g);
		BBPreclaim(e);
		BBPreclaim(s);
		throw(MAL, "aggr.subavg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	rc = BATgroupavg3(&avgs, &rems, &cnts, b, g, e, s, *skip_nils);

	BBPunfix(b->batCacheid);
	BBPreclaim(g);
	BBPreclaim(e);
	BBPreclaim(s);
	if (rc != GDK_SUCCEED)
		throw(MAL, "aggr.subavg", GDK_EXCEPTION);
	*retval1 = avgs->batCacheid;
	BBPkeepref(avgs);
	*retval2 = rems->batCacheid;
	BBPkeepref(rems);
	*retval3 = cnts->batCacheid;
	BBPkeepref(cnts);
	return MAL_SUCCEED;
}

static str
AGGRavg3comb(bat *retval1, const bat *bid, const bat *rid, const bat *cid,
			 const bat *gid, const bat *eid, const bit *skip_nils)
{
	BAT *b, *r, *c, *g, *e, *bn;

	b = BATdescriptor(*bid);
	r = BATdescriptor(*rid);
	c = BATdescriptor(*cid);
	g = gid != NULL && !is_bat_nil(*gid) ? BATdescriptor(*gid) : NULL;
	e = eid != NULL && !is_bat_nil(*eid) ? BATdescriptor(*eid) : NULL;

	if (b == NULL ||
		r == NULL ||
		c == NULL ||
		(gid != NULL && !is_bat_nil(*gid) && g == NULL) ||
		(eid != NULL && !is_bat_nil(*eid) && e == NULL)) {
		BBPreclaim(b);
		BBPreclaim(r);
		BBPreclaim(c);
		BBPreclaim(g);
		BBPreclaim(e);
		throw(MAL, "aggr.subavg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	bn = BATgroupavg3combine(b, r, c, g, e, *skip_nils);

	BBPunfix(b->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(c->batCacheid);
	BBPreclaim(g);
	BBPreclaim(e);
	if (bn == NULL)
		throw(MAL, "aggr.subavg", GDK_EXCEPTION);
	*retval1 = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

static str
AGGRsubstdev_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				 const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl, BATgroupstdev_sample,
					   NULL, NULL, NULL, "aggr.substdev");
}

static str
AGGRsubstdevcand_dbl(bat *retval, const bat *bid, const bat *gid,
					 const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl, BATgroupstdev_sample,
					   NULL, NULL, NULL, "aggr.substdev");
}

static str
AGGRsubstdevp_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				  const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl,
					   BATgroupstdev_population, NULL, NULL, NULL,
					   "aggr.substdevp");
}

static str
AGGRsubstdevpcand_dbl(bat *retval, const bat *bid, const bat *gid,
					  const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl,
					   BATgroupstdev_population,
					   NULL, NULL, NULL, "aggr.substdevp");
}

static str
AGGRsubvariance_dbl(bat *retval, const bat *bid, const bat *gid, const bat *eid,
					const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl, BATgroupvariance_sample,
					   NULL, NULL, NULL, "aggr.subvariance");
}

static str
AGGRsubvariancecand_dbl(bat *retval, const bat *bid, const bat *gid,
						const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl, BATgroupvariance_sample,
					   NULL, NULL, NULL, "aggr.subvariance");
}

static str
AGGRsubvariancep_dbl(bat *retval, const bat *bid, const bat *gid,
					 const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_dbl,
					   BATgroupvariance_population, NULL,
					   NULL, NULL, "aggr.subvariancep");
}

static str
AGGRsubvariancepcand_dbl(bat *retval, const bat *bid, const bat *gid,
						 const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_dbl,
					   BATgroupvariance_population, NULL,
					   NULL, NULL, "aggr.subvariancep");
}

static str
AGGRsubcount(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			 const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_lng, BATgroupcount, NULL, NULL,
					   NULL, "aggr.subcount");
}

static str
AGGRsubcountcand(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				 const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_lng, BATgroupcount, NULL,
					   NULL, NULL, "aggr.subcount");
}

static str
AGGRsubmin(bat *retval, const bat *bid, const bat *gid, const bat *eid,
		   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_oid, BATgroupmin, NULL,
					   NULL, NULL, "aggr.submin");
}

static str
AGGRsubmincand(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_oid, BATgroupmin, NULL,
					   NULL, NULL, "aggr.submin");
}

static str
AGGRsubmax(bat *retval, const bat *bid, const bat *gid, const bat *eid,
		   const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_oid, BATgroupmax, NULL,
					   NULL, NULL, "aggr.submax");
}

static str
AGGRsubmaxcand(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_oid, BATgroupmax, NULL,
					   NULL, NULL, "aggr.submax");
}

static str
AGGRsubmincand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, sid, *skip_nils,
					  0, TYPE_oid, BATgroupmin, NULL, NULL, NULL,
					  "aggr.submin");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

static str
AGGRsubmin_val(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRsubmincand_val(retval, bid, gid, eid, NULL, skip_nils);
}

static str
AGGRsubmaxcand_val(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				   const bat *sid, const bit *skip_nils)
{
	bat tmpid;
	str err;

	err = AGGRgrouped(&tmpid, NULL, bid, gid, eid, sid, *skip_nils,
					  0, TYPE_oid, BATgroupmax, NULL, NULL, NULL,
					  "aggr.submax");
	if (err == MAL_SUCCEED) {
		err = ALGprojection(retval, &tmpid, bid);
		BBPrelease(tmpid);
	}
	return err;
}

static str
AGGRsubmax_val(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			   const bit *skip_nils)
{
	return AGGRsubmaxcand_val(retval, bid, gid, eid, NULL, skip_nils);
}

static str
AGGRmedian(void *retval, const bat *bid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, true,
						   0, TYPE_any, BATgroupmedian, NULL,
						   NULL, NULL, "aggr.submedian")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

static str
AGGRsubmedian(bat *retval, const bat *bid, const bat *gid, const bat *eid,
			  const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_any, BATgroupmedian, NULL,
					   NULL, NULL, "aggr.submedian");
}

static str
AGGRsubmediancand(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				  const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_any, BATgroupmedian, NULL,
					   NULL, NULL, "aggr.submedian");
}

/* quantile functions, could make median functions obsolete completely */
static str
AGGRquantile(void *retval, const bat *bid, const bat *qid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, true,
						   0, TYPE_any, NULL, NULL, BATgroupquantile,
						   qid, "aggr.subquantile")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

static str
AGGRquantile_cst(void *retval, const bat *bid, const dbl *q)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped_bat_or_val(&rval, NULL, bid, NULL, NULL, NULL, true,
									  0, TYPE_any, NULL, NULL, BATgroupquantile,
									  NULL, q,
									  "aggr.subquantile")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

static str
AGGRsubquantile(bat *retval, const bat *bid, const bat *quantile,
				const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_any, NULL, NULL, BATgroupquantile,
					   quantile, "aggr.subquantile");
}

static str
AGGRsubquantilecand(bat *retval, const bat *bid, const bat *quantile,
					const bat *gid, const bat *eid, const bat *sid,
					const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_any, NULL, NULL, BATgroupquantile,
					   quantile, "aggr.subquantile");
}

static str
AGGRmedian_avg(dbl *retval, const bat *bid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, true,
						   0, TYPE_any, BATgroupmedian_avg, NULL,
						   NULL, NULL, "aggr.submedian_avg")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

static str
AGGRsubmedian_avg(bat *retval, const bat *bid, const bat *gid, const bat *eid,
				  const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_any, BATgroupmedian_avg, NULL,
					   NULL, NULL, "aggr.submedian_avg");
}

static str
AGGRsubmediancand_avg(bat *retval, const bat *bid, const bat *gid,
					  const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_any, BATgroupmedian_avg, NULL,
					   NULL, NULL, "aggr.submedian_avg");
}

/* quantile functions, could make median functions obsolete completely */
static str
AGGRquantile_avg(dbl *retval, const bat *bid, const bat *qid)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped(&rval, NULL, bid, NULL, NULL, NULL, true,
						   0, TYPE_any, NULL, NULL, BATgroupquantile_avg,
						   qid, "aggr.subquantile_avg")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

static str
AGGRquantile_avg_cst(dbl *retval, const bat *bid, const dbl *q)
{
	str err;
	bat rval;
	if ((err = AGGRgrouped_bat_or_val(&rval, NULL, bid, NULL, NULL, NULL, true,
									  0, TYPE_any, NULL, NULL,
									  BATgroupquantile_avg, NULL, q,
									  "aggr.subquantile_avg")) == MAL_SUCCEED) {
		oid pos = 0;
		err = ALGfetchoid(retval, &rval, &pos);
		BBPrelease(rval);
	}
	return err;
}

static str
AGGRsubquantile_avg(bat *retval, const bat *bid, const bat *quantile,
					const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, NULL, *skip_nils,
					   0, TYPE_any, NULL, NULL, BATgroupquantile_avg,
					   quantile, "aggr.subquantile_avg");
}

static str
AGGRsubquantilecand_avg(bat *retval, const bat *bid, const bat *quantile,
						const bat *gid, const bat *eid, const bat *sid,
						const bit *skip_nils)
{
	return AGGRgrouped(retval, NULL, bid, gid, eid, sid, *skip_nils,
					   0, TYPE_any, NULL, NULL, BATgroupquantile_avg,
					   quantile, "aggr.subquantile_avg");
}

static str
AGGRgroup_str_concat(bat *retval1, const bat *bid, const bat *gid,
					 const bat *eid, const bat *sid, bool skip_nils,
					 const bat *sepid, const char *separator,
					 const char *malfunc)
{
	BAT *b, *g, *e, *s, *sep, *bn = NULL;

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	s = sid ? BATdescriptor(*sid) : NULL;
	sep = sepid ? BATdescriptor(*sepid) : NULL;

	if (b == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL) ||
		(sid != NULL && s == NULL) || (sepid != NULL && sep == NULL)) {
		BBPreclaim(b);
		BBPreclaim(g);
		BBPreclaim(e);
		BBPreclaim(s);
		BBPreclaim(sep);
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	bn = BATgroupstr_group_concat(b, g, e, s, sep, skip_nils, separator);

	BBPunfix(b->batCacheid);
	BBPreclaim(g);
	BBPreclaim(e);
	BBPreclaim(s);
	BBPreclaim(sep);
	if (bn == NULL)
		throw(MAL, malfunc, GDK_EXCEPTION);
	*retval1 = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

#define DEFAULT_SEPARATOR ","

static str
AGGRstr_group_concat(bat *retval, const bat *bid, const bat *gid,
					 const bat *eid)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, true, NULL,
								DEFAULT_SEPARATOR, "aggr.str_group_concat");
}

static str
AGGRsubstr_group_concat(bat *retval, const bat *bid, const bat *gid,
						const bat *eid, const bit *skip_nils)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, *skip_nils, NULL,
								DEFAULT_SEPARATOR, "aggr.substr_group_concat");
}

static str
AGGRsubstr_group_concatcand(bat *retval, const bat *bid, const bat *gid,
							const bat *eid, const bat *sid,
							const bit *skip_nils)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, sid, *skip_nils, NULL,
								DEFAULT_SEPARATOR, "aggr.substr_group_concat");
}

static str
AGGRstr_group_concat_sep(bat *retval, const bat *bid, const bat *sep,
						 const bat *gid, const bat *eid)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, true, sep, NULL,
								"aggr.str_group_concat_sep");;
}

static str
AGGRsubstr_group_concat_sep(bat *retval, const bat *bid, const bat *sep,
							const bat *gid, const bat *eid,
							const bit *skip_nils)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, NULL, *skip_nils, sep,
								NULL, "aggr.substr_group_concat_sep");
}

static str
AGGRsubstr_group_concatcand_sep(bat *retval, const bat *bid, const bat *sep,
								const bat *gid, const bat *eid, const bat *sid,
								const bit *skip_nils)
{
	return AGGRgroup_str_concat(retval, bid, gid, eid, sid, *skip_nils, sep,
								NULL, "aggr.substr_group_concat_sep");
}

static str
AGGRgrouped2(bat *retval, const bat *bid1, const bat *bid2, const bat *gid,
			 const bat *eid, const bat *sid, bool skip_nils, int tp,
			 BAT *(*func)(BAT *, BAT *, BAT *, BAT *, BAT *, int tp,
						  bool skip_nils), const char *malfunc)
{
	BAT *b1, *b2, *g, *e, *s, *bn = NULL;

	assert(func != NULL);

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	s = sid ? BATdescriptor(*sid) : NULL;

	if (b1 == NULL || b2 == NULL || (gid != NULL && g == NULL)
		|| (eid != NULL && e == NULL) || (sid != NULL && s == NULL)) {
		BBPreclaim(b1);
		BBPreclaim(b2);
		BBPreclaim(g);
		BBPreclaim(e);
		BBPreclaim(s);
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (b1->ttype != b2->ttype) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		BBPreclaim(g);
		BBPreclaim(e);
		BBPreclaim(s);
		throw(MAL, malfunc,
			  SQLSTATE(42000) "%s requires both arguments of the same type",
			  malfunc);
	}

	bn = (*func) (b1, b2, g, e, s, tp, skip_nils);

	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPreclaim(g);
	BBPreclaim(e);
	BBPreclaim(s);
	if (bn == NULL)
		throw(MAL, malfunc, GDK_EXCEPTION);
	*retval = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

static str
AGGRcovariance(bat *retval, const bat *b1, const bat *b2, const bat *gid,
			   const bat *eid)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, true, TYPE_dbl,
						BATgroupcovariance_sample, "aggr.covariance");
}

static str
AGGRsubcovariance(bat *retval, const bat *b1, const bat *b2, const bat *gid,
				  const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, *skip_nils, TYPE_dbl,
						BATgroupcovariance_sample, "aggr.subcovariance");
}

static str
AGGRsubcovariancecand(bat *retval, const bat *b1, const bat *b2, const bat *gid,
					  const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, sid, *skip_nils, TYPE_dbl,
						BATgroupcovariance_sample, "aggr.subcovariance");
}

static str
AGGRcovariancep(bat *retval, const bat *b1, const bat *b2, const bat *gid,
				const bat *eid)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, true, TYPE_dbl,
						BATgroupcovariance_population, "aggr.covariancep");
}

static str
AGGRsubcovariancep(bat *retval, const bat *b1, const bat *b2, const bat *gid,
				   const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, *skip_nils, TYPE_dbl,
						BATgroupcovariance_population, "aggr.subcovariancep");
}

static str
AGGRsubcovariancepcand(bat *retval, const bat *b1, const bat *b2,
					   const bat *gid, const bat *eid, const bat *sid,
					   const bit *skip_nils)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, sid, *skip_nils, TYPE_dbl,
						BATgroupcovariance_population, "aggr.subcovariancep");
}

static str
AGGRcorr(bat *retval, const bat *b1, const bat *b2, const bat *gid,
		 const bat *eid)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, true, TYPE_dbl,
						BATgroupcorrelation, "aggr.corr");
}

static str
AGGRsubcorr(bat *retval, const bat *b1, const bat *b2, const bat *gid,
			const bat *eid, const bit *skip_nils)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, NULL, *skip_nils, TYPE_dbl,
						BATgroupcorrelation, "aggr.subcorr");
}

static str
AGGRsubcorrcand(bat *retval, const bat *b1, const bat *b2, const bat *gid,
				const bat *eid, const bat *sid, const bit *skip_nils)
{
	return AGGRgrouped2(retval, b1, b2, gid, eid, sid, *skip_nils, TYPE_dbl,
						BATgroupcorrelation, "aggr.subcorr");
}

#include "mel.h"
mel_func aggr_init_funcs[] = {
 command("aggr", "sum", AGGRsum3_dbl, false, "Grouped tail sum on bte", args(1,4, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "sum", AGGRsum3_bte, false, "Grouped tail sum on bte", args(1,4, batarg("",bte),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_bte, false, "Grouped sum aggregate", args(1,5, batarg("",bte),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_bte, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",bte),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_bte, false, "Grouped tail product on bte", args(1,4, batarg("",bte),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_bte, false, "Grouped product aggregate", args(1,5, batarg("",bte),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_bte, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",bte),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_sht, false, "Grouped tail sum on bte", args(1,4, batarg("",sht),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_sht, false, "Grouped sum aggregate", args(1,5, batarg("",sht),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_sht, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",sht),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_sht, false, "Grouped tail product on bte", args(1,4, batarg("",sht),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_sht, false, "Grouped product aggregate", args(1,5, batarg("",sht),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_sht, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",sht),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_int, false, "Grouped tail sum on bte", args(1,4, batarg("",int),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_int, false, "Grouped sum aggregate", args(1,5, batarg("",int),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_int, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",int),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_int, false, "Grouped tail product on bte", args(1,4, batarg("",int),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_int, false, "Grouped product aggregate", args(1,5, batarg("",int),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_int, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",int),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_lng, false, "Grouped tail sum on bte", args(1,4, batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_lng, false, "Grouped sum aggregate", args(1,5, batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_lng, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_lng, false, "Grouped tail product on bte", args(1,4, batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_lng, false, "Grouped product aggregate", args(1,5, batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_lng, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_dbl, false, "Grouped tail sum on sht", args(1,4, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "sum", AGGRsum3_sht, false, "Grouped tail sum on sht", args(1,4, batarg("",sht),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_sht, false, "Grouped sum aggregate", args(1,5, batarg("",sht),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_sht, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",sht),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_sht, false, "Grouped tail product on sht", args(1,4, batarg("",sht),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_sht, false, "Grouped product aggregate", args(1,5, batarg("",sht),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_sht, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",sht),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_int, false, "Grouped tail sum on sht", args(1,4, batarg("",int),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_int, false, "Grouped sum aggregate", args(1,5, batarg("",int),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_int, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",int),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_int, false, "Grouped tail product on sht", args(1,4, batarg("",int),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_int, false, "Grouped product aggregate", args(1,5, batarg("",int),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_int, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",int),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_lng, false, "Grouped tail sum on sht", args(1,4, batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_lng, false, "Grouped sum aggregate", args(1,5, batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_lng, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_lng, false, "Grouped tail product on sht", args(1,4, batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_lng, false, "Grouped product aggregate", args(1,5, batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_lng, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_dbl, false, "Grouped tail sum on int", args(1,4, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "sum", AGGRsum3_int, false, "Grouped tail sum on int", args(1,4, batarg("",int),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_int, false, "Grouped sum aggregate", args(1,5, batarg("",int),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_int, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",int),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_int, false, "Grouped tail product on int", args(1,4, batarg("",int),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_int, false, "Grouped product aggregate", args(1,5, batarg("",int),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_int, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",int),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_lng, false, "Grouped tail sum on int", args(1,4, batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_lng, false, "Grouped sum aggregate", args(1,5, batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_lng, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_lng, false, "Grouped tail product on int", args(1,4, batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_lng, false, "Grouped product aggregate", args(1,5, batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_lng, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_dbl, false, "Grouped tail sum on lng", args(1,4, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "sum", AGGRsum3_lng, false, "Grouped tail sum on lng", args(1,4, batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_lng, false, "Grouped sum aggregate", args(1,5, batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_lng, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_lng, false, "Grouped tail product on lng", args(1,4, batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_lng, false, "Grouped product aggregate", args(1,5, batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_lng, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_flt, false, "Grouped tail sum on flt", args(1,4, batarg("",flt),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_flt, false, "Grouped sum aggregate", args(1,5, batarg("",flt),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_flt, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",flt),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_flt, false, "Grouped tail product on flt", args(1,4, batarg("",flt),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_flt, false, "Grouped product aggregate", args(1,5, batarg("",flt),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_flt, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",flt),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_dbl, false, "Grouped tail sum on flt", args(1,4, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_dbl, false, "Grouped sum aggregate", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_dbl, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_dbl, false, "Grouped tail product on flt", args(1,4, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_dbl, false, "Grouped product aggregate", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_dbl, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_dbl, false, "Grouped tail sum on dbl", args(1,4, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_dbl, false, "Grouped sum aggregate", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_dbl, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_dbl, false, "Grouped tail product on dbl", args(1,4, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_dbl, false, "Grouped product aggregate", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_dbl, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "avg", AGGRavg13_dbl, false, "Grouped tail average on bte", args(1,4, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg23_dbl, false, "Grouped tail average on bte, also returns count", args(2,5, batarg("",dbl),batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg14_dbl, false, "Grouped tail average on bte", args(1,5, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "avg", AGGRavg24_dbl, false, "Grouped tail average on bte, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1_dbl, false, "Grouped average aggregate", args(1,5, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1cand_dbl, false, "Grouped average aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2_dbl, false, "Grouped average aggregate, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2cand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1s_dbl, false, "Grouped average aggregate", args(1,6, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1scand_dbl, false, "Grouped average aggregate with candidates list", args(1,7, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2s_dbl, false, "Grouped average aggregate, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2scand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,8, batarg("",dbl),batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "stdev", AGGRstdev3_dbl, false, "Grouped tail standard deviation (sample/non-biased) on bte", args(1,4, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdev", AGGRsubstdev_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdev", AGGRsubstdevcand_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdevp", AGGRstdevp3_dbl, false, "Grouped tail standard deviation (population/biased) on bte", args(1,4, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdevp", AGGRsubstdevp_dbl, false, "Grouped standard deviation (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdevp", AGGRsubstdevpcand_dbl, false, "Grouped standard deviation (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variance", AGGRvariance3_dbl, false, "Grouped tail variance (sample/non-biased) on bte", args(1,4, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariance", AGGRsubvariance_dbl, false, "Grouped variance (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariance", AGGRsubvariancecand_dbl, false, "Grouped variance (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variancep", AGGRvariancep3_dbl, false, "Grouped tail variance (population/biased) on bte", args(1,4, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariancep", AGGRsubvariancep_dbl, false, "Grouped variance (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariancep", AGGRsubvariancepcand_dbl, false, "Grouped variance (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariance", AGGRcovariance, false, "Covariance sample aggregate", args(1,5, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariance", AGGRsubcovariance, false, "Grouped covariance sample aggregate", args(1,6, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariance", AGGRsubcovariancecand, false, "Grouped covariance sample aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariancep", AGGRcovariancep, false, "Covariance population aggregate", args(1,5, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariancep", AGGRsubcovariancep, false, "Grouped covariance population aggregate", args(1,6, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariancep", AGGRsubcovariancepcand, false, "Grouped covariance population aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "corr", AGGRcorr, false, "Correlation aggregate", args(1,5, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcorr", AGGRsubcorr, false, "Grouped correlation aggregate", args(1,6, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcorr", AGGRsubcorrcand, false, "Grouped correlation aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",bte),batarg("b2",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "avg", AGGRavg13_dbl, false, "Grouped tail average on sht", args(1,4, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg23_dbl, false, "Grouped tail average on sht, also returns count", args(2,5, batarg("",dbl),batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg14_dbl, false, "Grouped tail average on sht", args(1,5, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "avg", AGGRavg24_dbl, false, "Grouped tail average on sht, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1_dbl, false, "Grouped average aggregate", args(1,5, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1cand_dbl, false, "Grouped average aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2_dbl, false, "Grouped average aggregate, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2cand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1s_dbl, false, "Grouped average aggregate", args(1,6, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1scand_dbl, false, "Grouped average aggregate with candidates list", args(1,7, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2s_dbl, false, "Grouped average aggregate, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2scand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,8, batarg("",dbl),batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "stdev", AGGRstdev3_dbl, false, "Grouped tail standard deviation (sample/non-biased) on sht", args(1,4, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdev", AGGRsubstdev_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdev", AGGRsubstdevcand_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdevp", AGGRstdevp3_dbl, false, "Grouped tail standard deviation (population/biased) on sht", args(1,4, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdevp", AGGRsubstdevp_dbl, false, "Grouped standard deviation (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdevp", AGGRsubstdevpcand_dbl, false, "Grouped standard deviation (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variance", AGGRvariance3_dbl, false, "Grouped tail variance (sample/non-biased) on sht", args(1,4, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariance", AGGRsubvariance_dbl, false, "Grouped variance (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariance", AGGRsubvariancecand_dbl, false, "Grouped variance (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variancep", AGGRvariancep3_dbl, false, "Grouped tail variance (population/biased) on sht", args(1,4, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariancep", AGGRsubvariancep_dbl, false, "Grouped variance (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariancep", AGGRsubvariancepcand_dbl, false, "Grouped variance (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariance", AGGRcovariance, false, "Covariance sample aggregate", args(1,5, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariance", AGGRsubcovariance, false, "Grouped covariance sample aggregate", args(1,6, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariance", AGGRsubcovariancecand, false, "Grouped covariance sample aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariancep", AGGRcovariancep, false, "Covariance population aggregate", args(1,5, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariancep", AGGRsubcovariancep, false, "Grouped covariance population aggregate", args(1,6, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariancep", AGGRsubcovariancepcand, false, "Grouped covariance population aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "corr", AGGRcorr, false, "Correlation aggregate", args(1,5, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcorr", AGGRsubcorr, false, "Grouped correlation aggregate", args(1,6, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcorr", AGGRsubcorrcand, false, "Grouped correlation aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",sht),batarg("b2",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "avg", AGGRavg13_dbl, false, "Grouped tail average on int", args(1,4, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg23_dbl, false, "Grouped tail average on int, also returns count", args(2,5, batarg("",dbl),batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg14_dbl, false, "Grouped tail average on int", args(1,5, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "avg", AGGRavg24_dbl, false, "Grouped tail average on int, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1_dbl, false, "Grouped average aggregate", args(1,5, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1cand_dbl, false, "Grouped average aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2_dbl, false, "Grouped average aggregate, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2cand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1s_dbl, false, "Grouped average aggregate", args(1,6, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1scand_dbl, false, "Grouped average aggregate with candidates list", args(1,7, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2s_dbl, false, "Grouped average aggregate, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2scand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,8, batarg("",dbl),batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "stdev", AGGRstdev3_dbl, false, "Grouped tail standard deviation (sample/non-biased) on int", args(1,4, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdev", AGGRsubstdev_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdev", AGGRsubstdevcand_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdevp", AGGRstdevp3_dbl, false, "Grouped tail standard deviation (population/biased) on int", args(1,4, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdevp", AGGRsubstdevp_dbl, false, "Grouped standard deviation (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdevp", AGGRsubstdevpcand_dbl, false, "Grouped standard deviation (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variance", AGGRvariance3_dbl, false, "Grouped tail variance (sample/non-biased) on int", args(1,4, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariance", AGGRsubvariance_dbl, false, "Grouped variance (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariance", AGGRsubvariancecand_dbl, false, "Grouped variance (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variancep", AGGRvariancep3_dbl, false, "Grouped tail variance (population/biased) on int", args(1,4, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariancep", AGGRsubvariancep_dbl, false, "Grouped variance (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariancep", AGGRsubvariancepcand_dbl, false, "Grouped variance (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariance", AGGRcovariance, false, "Covariance sample aggregate", args(1,5, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariance", AGGRsubcovariance, false, "Grouped covariance sample aggregate", args(1,6, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariance", AGGRsubcovariancecand, false, "Grouped covariance sample aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariancep", AGGRcovariancep, false, "Covariance population aggregate", args(1,5, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariancep", AGGRsubcovariancep, false, "Grouped covariance population aggregate", args(1,6, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariancep", AGGRsubcovariancepcand, false, "Grouped covariance population aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "corr", AGGRcorr, false, "Correlation aggregate", args(1,5, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcorr", AGGRsubcorr, false, "Grouped correlation aggregate", args(1,6, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcorr", AGGRsubcorrcand, false, "Grouped correlation aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",int),batarg("b2",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "avg", AGGRavg13_dbl, false, "Grouped tail average on lng", args(1,4, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg23_dbl, false, "Grouped tail average on lng, also returns count", args(2,5, batarg("",dbl),batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg14_dbl, false, "Grouped tail average on lng", args(1,5, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "avg", AGGRavg24_dbl, false, "Grouped tail average on lng, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1_dbl, false, "Grouped average aggregate", args(1,5, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1cand_dbl, false, "Grouped average aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2_dbl, false, "Grouped average aggregate, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2cand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1s_dbl, false, "Grouped average aggregate", args(1,6, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1scand_dbl, false, "Grouped average aggregate with candidates list", args(1,7, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2s_dbl, false, "Grouped average aggregate, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2scand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,8, batarg("",dbl),batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "stdev", AGGRstdev3_dbl, false, "Grouped tail standard deviation (sample/non-biased) on lng", args(1,4, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdev", AGGRsubstdev_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdev", AGGRsubstdevcand_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdevp", AGGRstdevp3_dbl, false, "Grouped tail standard deviation (population/biased) on lng", args(1,4, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdevp", AGGRsubstdevp_dbl, false, "Grouped standard deviation (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdevp", AGGRsubstdevpcand_dbl, false, "Grouped standard deviation (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variance", AGGRvariance3_dbl, false, "Grouped tail variance (sample/non-biased) on lng", args(1,4, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariance", AGGRsubvariance_dbl, false, "Grouped variance (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariance", AGGRsubvariancecand_dbl, false, "Grouped variance (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variancep", AGGRvariancep3_dbl, false, "Grouped tail variance (population/biased) on lng", args(1,4, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariancep", AGGRsubvariancep_dbl, false, "Grouped variance (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariancep", AGGRsubvariancepcand_dbl, false, "Grouped variance (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariance", AGGRcovariance, false, "Covariance sample aggregate", args(1,5, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariance", AGGRsubcovariance, false, "Grouped covariance sample aggregate", args(1,6, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariance", AGGRsubcovariancecand, false, "Grouped covariance sample aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariancep", AGGRcovariancep, false, "Covariance population aggregate", args(1,5, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariancep", AGGRsubcovariancep, false, "Grouped covariance population aggregate", args(1,6, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariancep", AGGRsubcovariancepcand, false, "Grouped covariance population aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "corr", AGGRcorr, false, "Correlation aggregate", args(1,5, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcorr", AGGRsubcorr, false, "Grouped correlation aggregate", args(1,6, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcorr", AGGRsubcorrcand, false, "Grouped correlation aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",lng),batarg("b2",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "avg", AGGRavg13_dbl, false, "Grouped tail average on flt", args(1,4, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg23_dbl, false, "Grouped tail average on flt, also returns count", args(2,5, batarg("",dbl),batarg("",lng),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg14_dbl, false, "Grouped tail average on flt", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "avg", AGGRavg24_dbl, false, "Grouped tail average on flt, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1_dbl, false, "Grouped average aggregate", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1cand_dbl, false, "Grouped average aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2_dbl, false, "Grouped average aggregate, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2cand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1s_dbl, false, "Grouped average aggregate", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1scand_dbl, false, "Grouped average aggregate with candidates list", args(1,7, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2s_dbl, false, "Grouped average aggregate, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2scand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,8, batarg("",dbl),batarg("",lng),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "stdev", AGGRstdev3_dbl, false, "Grouped tail standard deviation (sample/non-biased) on flt", args(1,4, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdev", AGGRsubstdev_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdev", AGGRsubstdevcand_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdevp", AGGRstdevp3_dbl, false, "Grouped tail standard deviation (population/biased) on flt", args(1,4, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdevp", AGGRsubstdevp_dbl, false, "Grouped standard deviation (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdevp", AGGRsubstdevpcand_dbl, false, "Grouped standard deviation (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variance", AGGRvariance3_dbl, false, "Grouped tail variance (sample/non-biased) on flt", args(1,4, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariance", AGGRsubvariance_dbl, false, "Grouped variance (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariance", AGGRsubvariancecand_dbl, false, "Grouped variance (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variancep", AGGRvariancep3_dbl, false, "Grouped tail variance (population/biased) on flt", args(1,4, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariancep", AGGRsubvariancep_dbl, false, "Grouped variance (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariancep", AGGRsubvariancepcand_dbl, false, "Grouped variance (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariance", AGGRcovariance, false, "Covariance sample aggregate", args(1,5, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariance", AGGRsubcovariance, false, "Grouped covariance sample aggregate", args(1,6, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariance", AGGRsubcovariancecand, false, "Grouped covariance sample aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariancep", AGGRcovariancep, false, "Covariance population aggregate", args(1,5, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariancep", AGGRsubcovariancep, false, "Grouped covariance population aggregate", args(1,6, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariancep", AGGRsubcovariancepcand, false, "Grouped covariance population aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "corr", AGGRcorr, false, "Correlation aggregate", args(1,5, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcorr", AGGRsubcorr, false, "Grouped correlation aggregate", args(1,6, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcorr", AGGRsubcorrcand, false, "Grouped correlation aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",flt),batarg("b2",flt),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "avg", AGGRavg13_dbl, false, "Grouped tail average on dbl", args(1,4, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg23_dbl, false, "Grouped tail average on dbl, also returns count", args(2,5, batarg("",dbl),batarg("",lng),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg14_dbl, false, "Grouped tail average on dbl", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "avg", AGGRavg24_dbl, false, "Grouped tail average on dbl, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1_dbl, false, "Grouped average aggregate", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1cand_dbl, false, "Grouped average aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2_dbl, false, "Grouped average aggregate, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2cand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1s_dbl, false, "Grouped average aggregate", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg1scand_dbl, false, "Grouped average aggregate with candidates list", args(1,7, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2s_dbl, false, "Grouped average aggregate, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "subavg", AGGRsubavg2scand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,8, batarg("",dbl),batarg("",lng),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit),arg("scale",int))),
 command("aggr", "stdev", AGGRstdev3_dbl, false, "Grouped tail standard deviation (sample/non-biased) on dbl", args(1,4, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdev", AGGRsubstdev_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdev", AGGRsubstdevcand_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdevp", AGGRstdevp3_dbl, false, "Grouped tail standard deviation (population/biased) on dbl", args(1,4, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdevp", AGGRsubstdevp_dbl, false, "Grouped standard deviation (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdevp", AGGRsubstdevpcand_dbl, false, "Grouped standard deviation (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variance", AGGRvariance3_dbl, false, "Grouped tail variance (sample/non-biased) on dbl", args(1,4, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariance", AGGRsubvariance_dbl, false, "Grouped variance (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariance", AGGRsubvariancecand_dbl, false, "Grouped variance (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variancep", AGGRvariancep3_dbl, false, "Grouped tail variance (population/biased) on dbl", args(1,4, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariancep", AGGRsubvariancep_dbl, false, "Grouped variance (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariancep", AGGRsubvariancepcand_dbl, false, "Grouped variance (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariance", AGGRcovariance, false, "Covariance sample aggregate", args(1,5, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariance", AGGRsubcovariance, false, "Grouped covariance sample aggregate", args(1,6, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariance", AGGRsubcovariancecand, false, "Grouped covariance sample aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariancep", AGGRcovariancep, false, "Covariance population aggregate", args(1,5, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariancep", AGGRsubcovariancep, false, "Grouped covariance population aggregate", args(1,6, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariancep", AGGRsubcovariancepcand, false, "Grouped covariance population aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "corr", AGGRcorr, false, "Correlation aggregate", args(1,5, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcorr", AGGRsubcorr, false, "Grouped correlation aggregate", args(1,6, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcorr", AGGRsubcorrcand, false, "Grouped correlation aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",dbl),batarg("b2",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "min", AGGRmin3, false, "", args(1,4, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2))),
 command("aggr", "max", AGGRmax3, false, "", args(1,4, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2))),
 command("aggr", "submin", AGGRsubmin, false, "Grouped minimum aggregate", args(1,5, batarg("",oid),batargany("b",1),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "submin", AGGRsubmincand, false, "Grouped minimum aggregate with candidates list", args(1,6, batarg("",oid),batargany("b",1),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "submax", AGGRsubmax, false, "Grouped maximum aggregate", args(1,5, batarg("",oid),batargany("b",1),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "submax", AGGRsubmaxcand, false, "Grouped maximum aggregate with candidates list", args(1,6, batarg("",oid),batargany("b",1),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "submin", AGGRsubmin_val, false, "Grouped minimum aggregate", args(1,5, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "submin", AGGRsubmincand_val, false, "Grouped minimum aggregate with candidates list", args(1,6, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "submax", AGGRsubmax_val, false, "Grouped maximum aggregate", args(1,5, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "submax", AGGRsubmaxcand_val, false, "Grouped maximum aggregate with candidates list", args(1,6, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "count", AGGRcount3, false, "", args(1,5, batarg("",lng),batargany("b",1),batarg("g",oid),batargany("e",2),arg("ignorenils",bit))),
 command("aggr", "count", AGGRcount3nils, false, "Grouped count", args(1,4, batarg("",lng),batargany("b",1),batarg("g",oid),batargany("e",2))),
 command("aggr", "count_no_nil", AGGRcount3nonils, false, "", args(1,4, batarg("",lng),batargany("b",1),batarg("g",oid),batargany("e",2))),
 command("aggr", "subcount", AGGRsubcount, false, "Grouped count aggregate", args(1,5, batarg("",lng),batargany("b",1),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "subcount", AGGRsubcountcand, false, "Grouped count aggregate with candidates list", args(1,6, batarg("",lng),batargany("b",1),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "median", AGGRmedian, false, "Median aggregate", args(1,2, argany("",1),batargany("b",1))),
 command("aggr", "submedian", AGGRsubmedian, false, "Grouped median aggregate", args(1,5, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "submedian", AGGRsubmediancand, false, "Grouped median aggregate with candidate list", args(1,6, batargany("",1),batargany("b",1),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "quantile", AGGRquantile, false, "Quantile aggregate", args(1,3, argany("",1),batargany("b",1),batarg("q",dbl))),
 command("aggr", "quantile", AGGRquantile_cst, false, "Quantile aggregate", args(1,3, argany("",1),batargany("b",1),arg("q",dbl))),
 command("aggr", "subquantile", AGGRsubquantile, false, "Grouped quantile aggregate", args(1,6, batargany("",1),batargany("b",1),batarg("q",dbl),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "subquantile", AGGRsubquantilecand, false, "Grouped quantile aggregate with candidate list", args(1,7, batargany("",1),batargany("b",1),batarg("q",dbl),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "median_avg", AGGRmedian_avg, false, "Median aggregate", args(1,2, arg("",dbl),batargany("b",1))),
 command("aggr", "submedian_avg", AGGRsubmedian_avg, false, "Grouped median aggregate", args(1,5, batarg("",dbl),batargany("b",1),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "submedian_avg", AGGRsubmediancand_avg, false, "Grouped median aggregate with candidate list", args(1,6, batarg("",dbl),batargany("b",1),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "quantile_avg", AGGRquantile_avg, false, "Quantile aggregate", args(1,3, arg("",dbl),batargany("b",1),batarg("q",dbl))),
 command("aggr", "quantile_avg", AGGRquantile_avg_cst, false, "Quantile aggregate", args(1,3, arg("",dbl),batargany("b",1),arg("q",dbl))),
 command("aggr", "subquantile_avg", AGGRsubquantile_avg, false, "Grouped quantile aggregate", args(1,6, batarg("",dbl),batargany("b",1),batarg("q",dbl),batarg("g",oid),batargany("e",2),arg("skip_nils",bit))),
 command("aggr", "subquantile_avg", AGGRsubquantilecand_avg, false, "Grouped quantile aggregate with candidate list", args(1,7, batarg("",dbl),batargany("b",1),batarg("q",dbl),batarg("g",oid),batargany("e",2),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "str_group_concat", AGGRstr_group_concat, false, "Grouped string tail concat", args(1,4, batarg("",str),batarg("b",str),batarg("g",oid),batargany("e",1))),
 command("aggr", "substr_group_concat", AGGRsubstr_group_concat, false, "Grouped string concat", args(1,5, batarg("",str),batarg("b",str),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substr_group_concat", AGGRsubstr_group_concatcand, false, "Grouped string concat with candidates list", args(1,6, batarg("",str),batarg("b",str),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "str_group_concat", AGGRstr_group_concat_sep, false, "Grouped string tail concat with custom separator", args(1,5, batarg("",str),batarg("b",str),batarg("sep",str),batarg("g",oid),batargany("e",1))),
 command("aggr", "substr_group_concat", AGGRsubstr_group_concat_sep, false, "Grouped string concat with custom separator", args(1,6, batarg("",str),batarg("b",str),batarg("sep",str),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substr_group_concat", AGGRsubstr_group_concatcand_sep, false, "Grouped string concat with candidates list with custom separator", args(1,7, batarg("",str),batarg("b",str),batarg("sep",str),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),

 command("aggr", "subavg", AGGRavg3, false, "Grouped average aggregation", args(3,8, batarg("",bte),batarg("",lng),batarg("",lng),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRavg3, false, "Grouped average aggregation", args(3,8, batarg("",sht),batarg("",lng),batarg("",lng),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRavg3, false, "Grouped average aggregation", args(3,8, batarg("",int),batarg("",lng),batarg("",lng),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRavg3, false, "Grouped average aggregation", args(3,8, batarg("",lng),batarg("",lng),batarg("",lng),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
#ifdef HAVE_HGE
 command("aggr", "subavg", AGGRavg3, false, "Grouped average aggregation", args(3,8, batarg("",hge),batarg("",lng),batarg("",lng),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
#endif
 command("aggr", "subavg", AGGRavg3comb, false, "Grouped average aggregation combiner", args(1,7, batarg("",bte),batarg("b",bte),batarg("r",lng),batarg("c",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRavg3comb, false, "Grouped average aggregation combiner", args(1,7, batarg("",sht),batarg("b",sht),batarg("r",lng),batarg("c",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRavg3comb, false, "Grouped average aggregation combiner", args(1,7, batarg("",int),batarg("b",int),batarg("r",lng),batarg("c",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRavg3comb, false, "Grouped average aggregation combiner", args(1,7, batarg("",lng),batarg("b",lng),batarg("r",lng),batarg("c",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
#ifdef HAVE_HGE
 command("aggr", "subavg", AGGRavg3comb, false, "Grouped average aggregation combiner", args(1,7, batarg("",hge),batarg("b",hge),batarg("r",lng),batarg("c",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
#endif

#ifdef HAVE_HGE
 command("aggr", "sum", AGGRsum3_hge, false, "Grouped tail sum on bte", args(1,4, batarg("",hge),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_hge, false, "Grouped sum aggregate", args(1,5, batarg("",hge),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_hge, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_hge, false, "Grouped tail product on bte", args(1,4, batarg("",hge),batarg("b",bte),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_hge, false, "Grouped product aggregate", args(1,5, batarg("",hge),batarg("b",bte),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_hge, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",bte),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_hge, false, "Grouped tail sum on sht", args(1,4, batarg("",hge),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_hge, false, "Grouped sum aggregate", args(1,5, batarg("",hge),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_hge, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_hge, false, "Grouped tail product on sht", args(1,4, batarg("",hge),batarg("b",sht),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_hge, false, "Grouped product aggregate", args(1,5, batarg("",hge),batarg("b",sht),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_hge, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",sht),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_hge, false, "Grouped tail sum on int", args(1,4, batarg("",hge),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_hge, false, "Grouped sum aggregate", args(1,5, batarg("",hge),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_hge, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_hge, false, "Grouped tail product on int", args(1,4, batarg("",hge),batarg("b",int),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_hge, false, "Grouped product aggregate", args(1,5, batarg("",hge),batarg("b",int),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_hge, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",int),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_hge, false, "Grouped tail sum on lng", args(1,4, batarg("",hge),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_hge, false, "Grouped sum aggregate", args(1,5, batarg("",hge),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_hge, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_hge, false, "Grouped tail product on lng", args(1,4, batarg("",hge),batarg("b",lng),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_hge, false, "Grouped product aggregate", args(1,5, batarg("",hge),batarg("b",lng),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_hge, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",lng),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "sum", AGGRsum3_hge, false, "Grouped tail sum on hge", args(1,4, batarg("",hge),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subsum", AGGRsubsum_hge, false, "Grouped sum aggregate", args(1,5, batarg("",hge),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subsum", AGGRsubsumcand_hge, false, "Grouped sum aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "prod", AGGRprod3_hge, false, "Grouped tail product on hge", args(1,4, batarg("",hge),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subprod", AGGRsubprod_hge, false, "Grouped product aggregate", args(1,5, batarg("",hge),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subprod", AGGRsubprodcand_hge, false, "Grouped product aggregate with candidates list", args(1,6, batarg("",hge),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "avg", AGGRavg13_dbl, false, "Grouped tail average on hge", args(1,4, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "avg", AGGRavg23_dbl, false, "Grouped tail average on hge, also returns count", args(2,5, batarg("",dbl),batarg("",lng),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subavg", AGGRsubavg1_dbl, false, "Grouped average aggregate", args(1,5, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg1cand_dbl, false, "Grouped average aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2_dbl, false, "Grouped average aggregate, also returns count", args(2,6, batarg("",dbl),batarg("",lng),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subavg", AGGRsubavg2cand_dbl, false, "Grouped average aggregate with candidates list, also returns count", args(2,7, batarg("",dbl),batarg("",lng),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdev", AGGRstdev3_dbl, false, "Grouped tail standard deviation (sample/non-biased) on hge", args(1,4, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdev", AGGRsubstdev_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdev", AGGRsubstdevcand_dbl, false, "Grouped standard deviation (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "stdevp", AGGRstdevp3_dbl, false, "Grouped tail standard deviation (population/biased) on hge", args(1,4, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "substdevp", AGGRsubstdevp_dbl, false, "Grouped standard deviation (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "substdevp", AGGRsubstdevpcand_dbl, false, "Grouped standard deviation (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variance", AGGRvariance3_dbl, false, "Grouped tail variance (sample/non-biased) on hge", args(1,4, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariance", AGGRsubvariance_dbl, false, "Grouped variance (sample/non-biased) aggregate", args(1,5, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariance", AGGRsubvariancecand_dbl, false, "Grouped variance (sample/non-biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "variancep", AGGRvariancep3_dbl, false, "Grouped tail variance (population/biased) on hge", args(1,4, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subvariancep", AGGRsubvariancep_dbl, false, "Grouped variance (population/biased) aggregate", args(1,5, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subvariancep", AGGRsubvariancepcand_dbl, false, "Grouped variance (population/biased) aggregate with candidates list", args(1,6, batarg("",dbl),batarg("b",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariance", AGGRcovariance, false, "Covariance sample aggregate", args(1,5, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariance", AGGRsubcovariance, false, "Grouped covariance sample aggregate", args(1,6, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariance", AGGRsubcovariancecand, false, "Grouped covariance sample aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "covariancep", AGGRcovariancep, false, "Covariance population aggregate", args(1,5, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcovariancep", AGGRsubcovariancep, false, "Grouped covariance population aggregate", args(1,6, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcovariancep", AGGRsubcovariancepcand, false, "Grouped covariance population aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "corr", AGGRcorr, false, "Correlation aggregate", args(1,5, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1))),
 command("aggr", "subcorr", AGGRsubcorr, false, "Grouped correlation aggregate", args(1,6, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subcorr", AGGRsubcorrcand, false, "Grouped correlation aggregate with candidate list", args(1,7, batarg("",dbl),batarg("b1",hge),batarg("b2",hge),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_aggr_mal)
{ mal_module("aggr", NULL, aggr_init_funcs); }
