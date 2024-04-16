/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * Foteini Alvanaki
 */

#include "geom.h"
#include "geod.h"
#include "geom_atoms.h"

/********** Geo Update Start **********/
#ifdef HAVE_RTREE
static str
filterSelectRTree(bat* outid, const bat *bid , const bat *sid, GEOSGeom const_geom, mbr *const_mbr, double distance, bit anti, char (*func) (GEOSContextHandle_t handle, const GEOSGeometry *, const GEOSGeometry *, double), const char *name)
{
	BAT *out = NULL, *b = NULL, *s = NULL;
	BATiter b_iter;
	struct canditer ci;
	GEOSGeom col_geom;

	//Get BAT, BATiter and candidate list
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	canditer_init(&ci, b, s);
	b_iter = bat_iterator(b);

	//Result BAT
	if ((out = COLnew(0, ATOMindex("oid"), ci.ncand, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//Get a candidate list from searching on the rtree with the constant mbr
	BUN* results_rtree = RTREEsearch(b, const_mbr, b->batCount);
	if (results_rtree == NULL) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		BBPreclaim(out);
		throw(MAL, name, "RTreesearch failed, returned NULL candidates");
	}

	//Cycle through rtree candidates
	//If there is a original candidate list, make sure the rtree cand is in there
	//Then do the actual calculation for the predicate using the GEOS function
	for (int i = 0; results_rtree[i] != BUN_NONE && i < (int) b->batCount; i++) {
		oid cand = results_rtree[i];
		//If we have a candidate list that is not dense, we need to check if the rtree candidate is also on the original candidate list
		if (ci.tpe != cand_dense) {
			//If the original candidate list does not contain the rtree cand, move on to next one
			if (!canditer_contains(&ci,cand))
				continue;
		}
		const wkb *col_wkb = BUNtvar(b_iter, cand - b->hseqbase);
		if ((col_geom = wkb2geos(col_wkb)) == NULL)
			throw(MAL, name, SQLSTATE(38000) "WKB2Geos operation failed");
		if (GEOSGetSRID_r(geoshandle, col_geom) != GEOSGetSRID_r(geoshandle, const_geom)) {
			GEOSGeom_destroy_r(geoshandle, col_geom);
			GEOSGeom_destroy_r(geoshandle, const_geom);
			bat_iterator_end(&b_iter);
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			BBPreclaim(out);
			throw(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
		}
		//GEOS function returns 1 on true, 0 on false and 2 on exception
		bit cond = ((*func)(geoshandle, col_geom, const_geom, distance) == 1);
		if (cond != anti) {
			if (BUNappend(out, (oid*) &cand, false) != GDK_SUCCEED) {
				GEOSGeom_destroy_r(geoshandle, col_geom);
				GEOSGeom_destroy_r(geoshandle, const_geom);
				bat_iterator_end(&b_iter);
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				BBPreclaim(out);
				throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		GEOSGeom_destroy_r(geoshandle, col_geom);
	}
	GEOSGeom_destroy_r(geoshandle, const_geom);
	bat_iterator_end(&b_iter);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	*outid = out->batCacheid;
	BBPkeepref(out);
	return MAL_SUCCEED;
}
#endif

static str
filterSelectNoIndex(bat* outid, const bat *bid , const bat *sid, wkb *wkb_const, double distance, bit anti, char (*func) (GEOSContextHandle_t handle, const GEOSGeometry *, const GEOSGeometry *, double), const char *name)
{
	BAT *out = NULL, *b = NULL, *s = NULL;
	BATiter b_iter;
	struct canditer ci;
	GEOSGeom col_geom, const_geom;

	//WKB constant is NULL
	if ((const_geom = wkb2geos(wkb_const)) == NULL) {
		if ((out = BATdense(0, 0, 0)) == NULL)
			throw(MAL, name, GDK_EXCEPTION);
		*outid = out->batCacheid;
		BBPkeepref(out);
		return MAL_SUCCEED;
	}

	//Get BAT, BATiter and candidate list
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	canditer_init(&ci, b, s);
	b_iter = bat_iterator(b);

	//Result BAT
	if ((out = COLnew(0, ATOMindex("oid"), ci.ncand, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (BUN i = 0; i < ci.ncand; i++) {
		BUN cand = canditer_next(&ci);
		const wkb *col_wkb = BUNtvar(b_iter, cand - b->hseqbase);
		if ((col_geom = wkb2geos(col_wkb)) == NULL)
			throw(MAL, name, SQLSTATE(38000) "WKB2Geos operation failed");
		if (GEOSGetSRID_r(geoshandle, col_geom) != GEOSGetSRID_r(geoshandle, const_geom)) {
			GEOSGeom_destroy_r(geoshandle, col_geom);
			GEOSGeom_destroy_r(geoshandle, const_geom);
			bat_iterator_end(&b_iter);
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			BBPreclaim(out);
			throw(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
		}
		//GEOS function returns 1 on true, 0 on false and 2 on exception
		bit cond = ((*func)(geoshandle, col_geom, const_geom, distance) == 1);
		if (cond != anti) {
			if (BUNappend(out, (oid*) &cand, false) != GDK_SUCCEED) {
				if (col_geom)
					GEOSGeom_destroy_r(geoshandle, col_geom);
				if (const_geom)
					GEOSGeom_destroy_r(geoshandle, const_geom);
				bat_iterator_end(&b_iter);
				BBPunfix(b->batCacheid);
				if (s)
					BBPunfix(s->batCacheid);
				BBPreclaim(out);
				throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		GEOSGeom_destroy_r(geoshandle, col_geom);
	}

	GEOSGeom_destroy_r(geoshandle, const_geom);
	bat_iterator_end(&b_iter);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	*outid = out->batCacheid;
	BBPkeepref(out);
	return MAL_SUCCEED;
}

str
wkbIntersectsSelectRTree(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti) {
#ifdef HAVE_RTREE
	//If there is an RTree on memory or on file, use the RTree method. Otherwise, use the no index version.
	if (RTREEexists_bid(*bid)) {
		//Calculate MBR of constant geometry first
		GEOSGeom const_geom;
		if ((const_geom = wkb2geos(*wkb_const)) == NULL) {
			BAT *out = NULL;
			if ((out = BATdense(0, 0, 0)) == NULL)
				throw(MAL, "geom.wkbIntersectsSelectRTree", GDK_EXCEPTION);
			*outid = out->batCacheid;
			BBPkeepref(out);
			return MAL_SUCCEED;
		}
		//Calculate the MBR for the constant geometry
		mbr *const_mbr = NULL;
		wkbMBR(&const_mbr,wkb_const);

		return filterSelectRTree(outid,bid,sid,const_geom,const_mbr,0,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsSelectRTree");
	}
	else
		return filterSelectNoIndex(outid,bid,sid,*wkb_const,0,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsSelectNoIndex");
#else
	return filterSelectNoIndex(outid,bid,sid,*wkb_const,0,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsSelectNoIndex");
#endif
}

str
wkbDWithinSelectRTree(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, dbl* distance, bit *anti) {
#ifdef HAVE_RTREE
	//If there is an RTree on memory or on file, use the RTree method. Otherwise, use the no index version.
	if (RTREEexists_bid(*bid)) {
		//Calculate MBR of constant geometry first
		GEOSGeom const_geom;
		if ((const_geom = wkb2geos(*wkb_const)) == NULL) {
			BAT *out = NULL;
			if ((out = BATdense(0, 0, 0)) == NULL)
				throw(MAL, "geom.wkbDWithinSelectRTree", GDK_EXCEPTION);
			*outid = out->batCacheid;
			BBPkeepref(out);
			return MAL_SUCCEED;
		}
		//Calculate the MBR for the constant geometry
		mbr *const_mbr = NULL;
		wkbMBR(&const_mbr,wkb_const);

		//We expand the bounding box to cover the "distance within" area
		//And use GEOSIntersects with the expanded bounding box
		//This expands the box too much
		//But better to get more false candidates than not getting a true candidate
		const_mbr->xmin -=(*distance);
		const_mbr->ymin -=(*distance);
		const_mbr->xmax +=(*distance);
		const_mbr->ymax +=(*distance);

		return filterSelectRTree(outid,bid,sid,const_geom,const_mbr,*distance,*anti,GEOSDistanceWithin_r,"geom.wkbDWithinSelectRTree");
	}
	else
		return filterSelectNoIndex(outid,bid,sid,*wkb_const,*distance,*anti,GEOSDistanceWithin_r,"geom.wkbDWithinSelectNoIndex");
#else
	return filterSelectNoIndex(outid,bid,sid,*wkb_const,*distance,*anti,GEOSDistanceWithin_r,"geom.wkbDWithinSelectNoIndex");
#endif
}

str
wkbIntersectsSelectNoIndex(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti) {
	return filterSelectNoIndex(outid,bid,sid,*wkb_const,0,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsSelectNoIndex");
}

str
wkbDWithinSelectNoIndex(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, double *distance, bit *anti) {
	return filterSelectNoIndex(outid,bid,sid,*wkb_const,*distance,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsSelectNoIndex");
}

static str
filterJoinNoIndex(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, double double_flag, const bat *ls_id, const bat *rs_id, bit nil_matches, lng estimate, bit anti, char (*func) (GEOSContextHandle_t handle, const GEOSGeometry *, const GEOSGeometry *, double), const char *name)
{
	BAT *lres = NULL, *rres = NULL, *l = NULL, *r = NULL, *ls = NULL, *rs = NULL;
	BUN estimate_safe;
	BATiter l_iter, r_iter;
	str msg = MAL_SUCCEED;
	struct canditer l_ci, r_ci;
	GEOSGeom l_geom, r_geom;
	GEOSGeom *l_geoms = NULL, *r_geoms = NULL;

	//get the input BATs
	if ((l = BATdescriptor(*l_id)) == NULL || (r = BATdescriptor(*r_id)) == NULL) {
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	//get the candidate lists
	if (ls_id && !is_bat_nil(*ls_id) && (ls = BATdescriptor(*ls_id)) == NULL && rs_id && !is_bat_nil(*rs_id) && (rs = BATdescriptor(*rs_id)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto free;
	}
	canditer_init(&l_ci, l, ls);
	canditer_init(&r_ci, r, rs);

	// properly handle the estimate
	if (is_lng_nil(estimate) || estimate == 0) {
		if (l_ci.ncand > r_ci.ncand)
			estimate = l_ci.ncand;
		else
			estimate = r_ci.ncand;
	}

	if (estimate < 0 || is_lng_nil(estimate) || estimate > (lng) BUN_MAX)
		estimate_safe = BUN_NONE;
	else
		estimate_safe = (BUN) estimate;

	// create new BATs for the output
	if ((lres = COLnew(0, ATOMindex("oid"), estimate_safe, TRANSIENT)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}
	if ((rres = COLnew(0, ATOMindex("oid"), estimate_safe, TRANSIENT)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	//Allocate arrays for reutilizing GEOS type conversion
	if ((l_geoms = GDKmalloc(l_ci.ncand * sizeof(GEOSGeometry *))) == NULL || (r_geoms = GDKmalloc(r_ci.ncand * sizeof(GEOSGeometry *))) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	l_iter = bat_iterator(l);
	r_iter = bat_iterator(r);

	//Convert wkb to GEOS only once
	for (BUN i = 0; i < l_ci.ncand; i++) {
		oid l_oid = canditer_next(&l_ci);
		l_geoms[i] = wkb2geos((const wkb*) BUNtvar(l_iter, l_oid - l->hseqbase));
	}
	for (BUN j = 0; j < r_ci.ncand; j++) {
		oid r_oid = canditer_next(&r_ci);
		r_geoms[j] = wkb2geos((const wkb*)BUNtvar(r_iter, r_oid - r->hseqbase));
	}

	canditer_reset(&l_ci);
	for (BUN i = 0; i < l_ci.ncand; i++) {
		oid l_oid = canditer_next(&l_ci);
		l_geom = l_geoms[i];
		if (!nil_matches && l_geom == NULL)
			continue;
		canditer_reset(&r_ci);
		for (BUN j = 0; j < r_ci.ncand; j++) {
			oid r_oid = canditer_next(&r_ci);
			r_geom = r_geoms[j];
			//Null handling
			if (r_geom == NULL) {
				if (nil_matches && l_geom == NULL) {
					if (BUNappend(lres, &l_oid, false) != GDK_SUCCEED || BUNappend(rres, &r_oid, false) != GDK_SUCCEED) {
						msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
						bat_iterator_end(&l_iter);
						bat_iterator_end(&r_iter);
						goto free;
					}
				}
				else
					continue;
			}
			if (GEOSGetSRID_r(geoshandle, l_geom) != GEOSGetSRID_r(geoshandle, r_geom)) {
				msg = createException(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
				bat_iterator_end(&l_iter);
				bat_iterator_end(&r_iter);
				goto free;
			}
			//Apply the (Geom, Geom, double) -> bit function
			bit cond = (*func)(geoshandle, l_geom, r_geom, double_flag) == 1;
			if (cond != anti) {
				if (BUNappend(lres, &l_oid, false) != GDK_SUCCEED || BUNappend(rres, &r_oid, false) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					bat_iterator_end(&l_iter);
					bat_iterator_end(&r_iter);
					goto free;
				}
			}
		}
	}
	if (l_geoms) {
		for (BUN i = 0; i < l_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, l_geoms[i]);
		}
		GDKfree(l_geoms);
	}
	if (r_geoms) {
		for (BUN i = 0; i < r_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, r_geoms[i]);
		}
		GDKfree(r_geoms);
	}
	bat_iterator_end(&l_iter);
	bat_iterator_end(&r_iter);
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	if (ls)
		BBPunfix(ls->batCacheid);
	if (rs)
		BBPunfix(rs->batCacheid);
	*lres_id = lres->batCacheid;
	BBPkeepref(lres);
	*rres_id = rres->batCacheid;
	BBPkeepref(rres);
	return MAL_SUCCEED;
free:
	if (l_geoms) {
		for (BUN i = 0; i < l_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, l_geoms[i]);
		}
		GDKfree(l_geoms);
	}
	if (r_geoms) {
		for (BUN i = 0; i < r_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, r_geoms[i]);
		}
		GDKfree(r_geoms);
	}
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	if (ls)
		BBPunfix(ls->batCacheid);
	if (rs)
		BBPunfix(rs->batCacheid);
	if (lres)
		BBPreclaim(lres);
	if (rres)
		BBPreclaim(rres);
	return msg;
}

#ifdef HAVE_RTREE
static str
filterJoinRTree(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, double double_flag, const bat *ls_id, const bat *rs_id, bit nil_matches, lng estimate, bit anti, char (*func) (GEOSContextHandle_t handle, const GEOSGeometry *, const GEOSGeometry *, double), const char *name) {
	BAT *lres = NULL, *rres = NULL, *l = NULL, *r = NULL, *ls = NULL, *rs = NULL, *inner_res = NULL, *outer_res = NULL, *inner_b = NULL;
	BUN estimate_safe;
	BATiter l_iter, r_iter;
	str msg = MAL_SUCCEED;
	struct canditer l_ci, r_ci, outer_ci, inner_ci;
	GEOSGeom outer_geom, inner_geom;
	GEOSGeom *l_geoms = NULL, *r_geoms = NULL, *outer_geoms = NULL, *inner_geoms = NULL;

	//get the input BATs
	if ((l = BATdescriptor(*l_id)) == NULL || (r = BATdescriptor(*r_id)) == NULL) {
		if (l)
			BBPunfix(l->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	//get the candidate lists
	if (ls_id && !is_bat_nil(*ls_id) && (ls = BATdescriptor(*ls_id)) == NULL && rs_id && !is_bat_nil(*rs_id) && (rs = BATdescriptor(*rs_id)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto free;
	}
	canditer_init(&l_ci, l, ls);
	canditer_init(&r_ci, r, rs);

	// properly handle the estimate
	if (is_lng_nil(estimate) || estimate == 0) {
		if (l_ci.ncand > r_ci.ncand)
			estimate = l_ci.ncand;
		else
			estimate = r_ci.ncand;
	}

	if (estimate < 0 || is_lng_nil(estimate) || estimate > (lng) BUN_MAX)
		estimate_safe = BUN_NONE;
	else
		estimate_safe = (BUN) estimate;

	// create new BATs for the output
	if ((lres = COLnew(0, ATOMindex("oid"), estimate_safe, TRANSIENT)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}
	if ((rres = COLnew(0, ATOMindex("oid"), estimate_safe, TRANSIENT)) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	//Allocate arrays for reutilizing GEOS type conversion
	if ((l_geoms = GDKmalloc(l_ci.ncand * sizeof(GEOSGeometry *))) == NULL || (r_geoms = GDKmalloc(r_ci.ncand * sizeof(GEOSGeometry *))) == NULL) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	l_iter = bat_iterator(l);
	r_iter = bat_iterator(r);

	//Convert wkb to GEOS only once
	for (BUN i = 0; i < l_ci.ncand; i++) {
		oid l_oid = canditer_next(&l_ci);
		l_geoms[i] = wkb2geos((const wkb*) BUNtvar(l_iter, l_oid - l->hseqbase));
	}
	for (BUN j = 0; j < r_ci.ncand; j++) {
		oid r_oid = canditer_next(&r_ci);
		r_geoms[j] = wkb2geos((const wkb*)BUNtvar(r_iter, r_oid - r->hseqbase));
	}

	bat_iterator_end(&l_iter);
	bat_iterator_end(&r_iter);

	if (l_ci.ncand > r_ci.ncand) {
		inner_b = l;
		inner_res = lres;
		inner_ci = l_ci;
		inner_geoms = l_geoms;
		outer_res = rres;
		outer_ci = r_ci;
		outer_geoms = r_geoms;
	}
	else {
		inner_b = r;
		inner_res = rres;
		inner_ci = r_ci;
		inner_geoms = r_geoms;
		outer_res = lres;
		outer_ci = l_ci;
		outer_geoms = l_geoms;
	}

	canditer_reset(&outer_ci);
	for (BUN i = 0; i < outer_ci.ncand; i++) {
		oid outer_oid = canditer_next(&outer_ci);
		outer_geom = outer_geoms[i];
		if (!nil_matches && outer_geom == NULL)
			continue;

		//Calculate the MBR for the constant geometry
		mbr *outer_mbr = mbrFromGeos(outer_geom);
		BUN* results_rtree = RTREEsearch(inner_b, outer_mbr, outer_ci.ncand);
		if (results_rtree == NULL) {
			msg = createException(MAL, name, "RTreesearch failed, returned NULL candidates");
			goto free;
		}

		canditer_reset(&inner_ci);
		for (BUN j = 0; results_rtree[j] != BUN_NONE && j < inner_ci.ncand; j++) {
			//oid inner_oid = canditer_next(&inner_ci);
			oid inner_oid = results_rtree[j];
			inner_geom = inner_geoms[j - inner_b->hseqbase];

			if (inner_ci.tpe != cand_dense) {
				//If the original candidate list does not contain the rtree cand, move on to next one
				if (!canditer_contains(&inner_ci,inner_oid))
					continue;
			}

			//Null handling
			if (inner_geom == NULL) {
				if (nil_matches && outer_geom == NULL) {
					if (BUNappend(outer_res, &outer_oid, false) != GDK_SUCCEED || BUNappend(inner_res, &inner_oid, false) != GDK_SUCCEED) {
						msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto free;
					}
				}
				else
					continue;
			}
			if (GEOSGetSRID_r(geoshandle, outer_geom) != GEOSGetSRID_r(geoshandle, inner_geom)) {
				msg = createException(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
				goto free;
			}
			//Apply the (Geom, Geom, double) -> bit function
			bit cond = (*func)(geoshandle, inner_geom, outer_geom, double_flag) == 1;
			if (cond != anti) {
				if (BUNappend(inner_res, &inner_oid, false) != GDK_SUCCEED || BUNappend(outer_res, &outer_oid, false) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto free;
				}
			}
		}
	}
	if (l_geoms) {
		for (BUN i = 0; i < l_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, l_geoms[i]);
		}
		GDKfree(l_geoms);
	}
	if (r_geoms) {
		for (BUN i = 0; i < r_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, r_geoms[i]);
		}
		GDKfree(r_geoms);
	}
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	if (ls)
		BBPunfix(ls->batCacheid);
	if (rs)
		BBPunfix(rs->batCacheid);
	*lres_id = lres->batCacheid;
	BBPkeepref(lres);
	*rres_id = rres->batCacheid;
	BBPkeepref(rres);
	return MAL_SUCCEED;
free:
	if (l_geoms) {
		for (BUN i = 0; i < l_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, l_geoms[i]);
		}
		GDKfree(l_geoms);
	}
	if (r_geoms) {
		for (BUN i = 0; i < r_ci.ncand; i++) {
			GEOSGeom_destroy_r(geoshandle, r_geoms[i]);
		}
		GDKfree(r_geoms);
	}
	BBPunfix(l->batCacheid);
	BBPunfix(r->batCacheid);
	if (ls)
		BBPunfix(ls->batCacheid);
	if (rs)
		BBPunfix(rs->batCacheid);
	if (lres)
		BBPreclaim(lres);
	if (rres)
		BBPreclaim(rres);
	return msg;
}
#endif

str
wkbIntersectsJoinRTree(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti) {
#ifdef HAVE_RTREE
	//If there is an RTree on memory or on file, use the RTree method. Otherwise, use the no index version.
	if (RTREEexists_bid(*l_id) && RTREEexists_bid(*r_id))
		return filterJoinRTree(lres_id,rres_id,l_id,r_id,0,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsJoinRTree");
	else
		return filterJoinNoIndex(lres_id,rres_id,l_id,r_id,0,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsJoinNoIndex");

#else
	return filterJoinNoIndex(lres_id,rres_id,l_id,r_id,0,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsJoinNoIndex");
#endif
}

str
wkbDWithinJoinRTree(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, dbl *distance, bit *nil_matches, lng *estimate, bit *anti) {
#ifdef HAVE_RTREE
	if (RTREEexists_bid(*l_id) && RTREEexists_bid(*r_id))
		return filterJoinRTree(lres_id,rres_id,l_id,r_id,*distance,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbDWithinJoinRTree");
	else
		return filterJoinNoIndex(lres_id,rres_id,l_id,r_id,*distance,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbDWithinJoinNoIndex");
#else
	return filterJoinNoIndex(lres_id,rres_id,l_id,r_id,*distance,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbDWithinJoinNoIndex");
#endif
}

str
wkbIntersectsJoinNoIndex(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti) {
	return filterJoinNoIndex(lres_id,rres_id,l_id,r_id,0,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbIntersectsJoinNoIndex");
}

str
wkbDWithinJoinNoIndex(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, double *distance, bit *nil_matches, lng *estimate, bit *anti) {
	return filterJoinNoIndex(lres_id,rres_id,l_id,r_id,*distance,ls_id,rs_id,*nil_matches,*estimate,*anti,GEOSDistanceWithin_r,"geom.wkbDWithinJoinNoIndex");
}

//MBR bulk function
//Creates the BAT with MBRs from the input BAT with WKB geometries
//Also creates the RTree structure and saves it on the WKB input BAT
str
wkbMBR_bat(bat *outBAT_id, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	mbr *outMBR = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.mbr", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("mbr"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.mbr", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = wkbMBR(&outMBR, &inWKB)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outMBR, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outMBR);
			throw(MAL, "batgeom.mbr", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outMBR);
		outMBR = NULL;
	}
	bat_iterator_end(&inBAT_iter);
	BBPunfix(inBAT->batCacheid);

	*outBAT_id = outBAT->batCacheid;

#ifdef HAVE_RTREE
	//Build RTree index using the mbr's we just calculated, and save it on the wkb BAT
	BATrtree(inBAT,outBAT);
#endif
	BBPkeepref(outBAT);
	return MAL_SUCCEED;
}

/* ST_Transform Bulk function */
str
wkbTransform_bat(bat *outBAT_id, bat *inBAT_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
{
	return wkbTransform_bat_cand(outBAT_id,inBAT_id,NULL,srid_src,srid_dst,proj4_src_str,proj4_dst_str);
}

str
wkbTransform_bat_cand(bat *outBAT_id, bat *inBAT_id, bat *s_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
{
#ifndef HAVE_PROJ
	*outBAT_id = 0;
	(void) inBAT_id;
	(void) s_id;
	(void) srid_src;
	(void) srid_dst;
	(void) proj4_src_str;
	(void) proj4_dst_str;
	throw(MAL, "geom.Transform", SQLSTATE(38000) "PROJ library not found");
#else
	BAT *outBAT = NULL, *inBAT = NULL, *s = NULL;;
	BATiter inBAT_iter;
	str err = MAL_SUCCEED;
	struct canditer ci;

	PJ *P;
	GEOSGeom geosGeometry, transformedGeosGeometry;
	const wkb *geomWKB = NULL;
	wkb *transformedWKB;
	int geometryType = -1;

	if (is_int_nil(*srid_src) ||
	    is_int_nil(*srid_dst) ||
	    strNil(*proj4_src_str) ||
	    strNil(*proj4_dst_str)) {
		//TODO: What do we return here?
		return MAL_SUCCEED;
	}

	if (strcmp(*proj4_src_str, "null") == 0)
		throw(MAL, "batgeom.Transform", SQLSTATE(38000) "Cannot find in spatial_ref_sys srid %d\n", *srid_src);
	if (strcmp(*proj4_dst_str, "null") == 0)
		throw(MAL, "batgeom.Transform", SQLSTATE(38000) "Cannot find in spatial_ref_sys srid %d\n", *srid_dst);
	if (strcmp(*proj4_src_str, *proj4_dst_str) == 0) {
		//TODO: Return a copy of the input BAT
		return MAL_SUCCEED;
	}

	//Create PROJ transformation object with PROJ strings passed as argument
	P = proj_create_crs_to_crs(PJ_DEFAULT_CTX,
                               *proj4_src_str,
                               *proj4_dst_str,
                               NULL);
	if (P==0)
        throw(MAL, "batgeom.Transform", SQLSTATE(38000) "PROJ initialization failed");

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		proj_destroy(P);
		throw(MAL, "batgeom.Transform", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		proj_destroy(P);
		throw(MAL, "batgeom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//check for candidate lists
	if (s_id && !is_bat_nil(*s_id) && (s = BATdescriptor(*s_id)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		BBPunfix(outBAT->batCacheid);
		proj_destroy(P);
		throw(MAL, "batgeom.Transform", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	canditer_init(&ci, inBAT, s);

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	for (BUN i = 0; i < ci.ncand && err == MAL_SUCCEED; i++) {
		oid p = (canditer_next(&ci) - inBAT->hseqbase);
		geomWKB = (wkb *) BUNtvar(inBAT_iter, p);

		/* get the geosGeometry from the wkb */
		geosGeometry = wkb2geos(geomWKB);
		/* get the type of the geometry */
		geometryType = GEOSGeomTypeId_r(geoshandle, geosGeometry) + 1;

		//TODO: No collection?
		switch (geometryType) {
		case wkbPoint_mdb:
			err = transformPoint(&transformedGeosGeometry, geosGeometry, P);
			break;
		case wkbLineString_mdb:
			err = transformLineString(&transformedGeosGeometry, geosGeometry, P);
			break;
		case wkbLinearRing_mdb:
			err = transformLinearRing(&transformedGeosGeometry, geosGeometry, P);
			break;
		case wkbPolygon_mdb:
			err = transformPolygon(&transformedGeosGeometry, geosGeometry, P, *srid_dst);
			break;
		case wkbMultiPoint_mdb:
		case wkbMultiLineString_mdb:
		case wkbMultiPolygon_mdb:
			err = transformMultiGeometry(&transformedGeosGeometry, geosGeometry, P, *srid_dst, geometryType);
			break;
		default:
			transformedGeosGeometry = NULL;
			err = createException(MAL, "batgeom.Transform", SQLSTATE(38000) "Geos unknown geometry type");
		}

		if (err == MAL_SUCCEED && transformedGeosGeometry) {
			/* set the new srid */
			GEOSSetSRID_r(geoshandle, transformedGeosGeometry, *srid_dst);
			/* get the wkb */
			if ((transformedWKB = geos2wkb(transformedGeosGeometry)) == NULL)
				throw(MAL, "batgeom.Transform", SQLSTATE(38000) "Geos operation geos2wkb failed");
			else {
				if (BUNappend(outBAT, transformedWKB, false) != GDK_SUCCEED) {
					throw(MAL, "batgeom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}

			/* destroy the geos geometries */
			GEOSGeom_destroy_r(geoshandle, transformedGeosGeometry);

		}
		GEOSGeom_destroy_r(geoshandle, geosGeometry);
	}
	proj_destroy(P);
	bat_iterator_end(&inBAT_iter);

	BBPunfix(inBAT->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;
#endif
}

/* ST_DistanceGeographic Bulk function */
str
wkbDistanceGeographic_bat(bat *out_id, const bat *a_id, const bat *b_id)
{
	return wkbDistanceGeographic_bat_cand(out_id,a_id,b_id,NULL,NULL);
}

str
wkbDistanceGeographic_bat_cand(bat *out_id, const bat *a_id, const bat *b_id, const bat *s1_id, const bat *s2_id)
{
	BAT *out = NULL, *a = NULL, *b = NULL, *s1 = NULL, *s2 = NULL;
	BATiter a_iter, b_iter;
	str msg = MAL_SUCCEED;
	struct canditer ci1, ci2;

	//get the BATs
	if ((a = BATdescriptor(*a_id)) == NULL || (b = BATdescriptor(*b_id)) == NULL) {
		msg = createException(MAL, "batgeom.DistanceGeographic", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	//check if the BATs are aligned
	if (a->hseqbase != b->hseqbase || BATcount(a) != BATcount(b)) {
		msg = createException(MAL, "batgeom.DistanceGeographic", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//check for candidate lists
	if (s1_id && !is_bat_nil(*s1_id) && (s1 = BATdescriptor(*s1_id)) == NULL) {
		msg = createException(MAL, "batgeom.DistanceGeographic", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	if (s2_id && !is_bat_nil(*s2_id) && (s2 = BATdescriptor(*s2_id)) == NULL) {
		msg = createException(MAL, "batgeom.DistanceGeographic", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		if (s1)
			BBPunfix(s1->batCacheid);
		goto clean;
	}
	canditer_init(&ci1, a, s1);
	canditer_init(&ci2, b, s2);

	//create a new BAT for the output
	if ((out = COLnew(0, ATOMindex("dbl"), ci1.ncand, TRANSIENT)) == NULL) {
		msg = createException(MAL, "batgeom.DistanceGeographic", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (s1)
			BBPunfix(s1->batCacheid);
		if (s2)
			BBPunfix(s2->batCacheid);
		goto clean;
	}
	//iterator over the BATs
	a_iter = bat_iterator(a);
	b_iter = bat_iterator(b);

	for (BUN i = 0; i < ci1.ncand; i++) {
		double distanceVal = 0;
		oid p1 = (canditer_next(&ci1) - a->hseqbase);
		oid p2 = (canditer_next(&ci2) - b->hseqbase);
		wkb *aWKB = (wkb *) BUNtvar(a_iter, p1);
		wkb *bWKB = (wkb *) BUNtvar(b_iter, p2);

		if ((msg = wkbDistanceGeographic(&distanceVal, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPreclaim(out);
			goto bailout;
		}
		if (BUNappend(out, &distanceVal, false) != GDK_SUCCEED) {
			BBPreclaim(out);
			msg = createException(MAL, "batgeom.DistanceGeographic", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}
	*out_id = out->batCacheid;
	BBPkeepref(out);
bailout:
	bat_iterator_end(&a_iter);
	bat_iterator_end(&b_iter);
	BBPreclaim(s1);
	BBPreclaim(s2);
clean:
	BBPreclaim(a);
	BBPreclaim(b);
	BBPreclaim(out);
	return msg;
}

/********** Geo Update End **********/

/*******************************/
/********** One input **********/
/*******************************/

str
geom_2_geom_bat(bat *outBAT_id, bat *inBAT_id, bat *cand, int *columnType, int *columnSRID)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	str msg = MAL_SUCCEED;
	struct canditer ci;
	oid off = 0;
	bool nils = false;
	wkb *inWKB = NULL, *outWKB = NULL;

	//get the descriptor of the BAT
	if ((b = BATdescriptor(*inBAT_id)) == NULL) {
		msg = createException(MAL, "batcalc.wkb", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	bi = bat_iterator(b);
	if (cand && !is_bat_nil(*cand) && (s = BATdescriptor(*cand)) == NULL) {
		msg = createException(MAL, "batcalc.wkb", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	//create a new BAT, aligned with input BAT
	if ((dst = COLnew(ci.hseq, ATOMindex("wkb"), ci.ncand, TRANSIENT)) == NULL) {
		msg = createException(MAL, "batcalc.wkb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			inWKB = (wkb *) BUNtvar(bi, p);

			if ((msg = geom_2_geom(&outWKB, &inWKB, columnType, columnSRID)) != MAL_SUCCEED)	//check type
				goto bailout;
			if (tfastins_nocheckVAR(dst, i, outWKB) != GDK_SUCCEED) {
				GDKfree(outWKB);
				msg = createException(MAL, "batcalc.wkb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= is_wkb_nil(outWKB);
			GDKfree(outWKB);
			outWKB = NULL;
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			inWKB = (wkb *) BUNtvar(bi, p);

			if ((msg = geom_2_geom(&outWKB, &inWKB, columnType, columnSRID)) != MAL_SUCCEED)	//check type
				goto bailout;
			if (tfastins_nocheckVAR(dst, i, outWKB) != GDK_SUCCEED) {
				GDKfree(outWKB);
				msg = createException(MAL, "batcalc.wkb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= is_wkb_nil(outWKB);
			GDKfree(outWKB);
			outWKB = NULL;
		}
	}

bailout:
	if (b) {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
	}
	BBPreclaim(s);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = BATcount(dst) <= 1;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*outBAT_id = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

/*create WKB from WKT */
str
wkbFromText_bat(bat *outBAT_id, bat *inBAT_id, int *srid, int *tpe)
{
	return wkbFromText_bat_cand(outBAT_id, inBAT_id, NULL, srid, tpe);
}

str
wkbFromText_bat_cand(bat *outBAT_id, bat *inBAT_id, bat *cand, int *srid, int *tpe)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	str msg = MAL_SUCCEED;
	struct canditer ci;
	oid off = 0;
	bool nils = false;

	//get the descriptor of the BAT
	if ((b = BATdescriptor(*inBAT_id)) == NULL) {
		msg = createException(MAL, "batgeom.wkbFromText", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	bi = bat_iterator(b);
	if (cand && !is_bat_nil(*cand) && (s = BATdescriptor(*cand)) == NULL) {
		msg = createException(MAL, "batgeom.wkbFromText", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	//create a new BAT, aligned with input BAT
	if ((dst = COLnew(ci.hseq, ATOMindex("wkb"), ci.ncand, TRANSIENT)) == NULL) {
		msg = createException(MAL, "batgeom.wkbFromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			str inWKB = (str) BUNtvar(bi, p);
			wkb *outSingle;

			if ((msg = wkbFromText(&outSingle, &inWKB, srid, tpe)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(dst, i, outSingle) != GDK_SUCCEED) {
				GDKfree(outSingle);
				msg = createException(MAL, "batgeom.wkbFromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= is_wkb_nil(outSingle);
			GDKfree(outSingle);
			outSingle = NULL;
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			str inWKB = (str) BUNtvar(bi, p);
			wkb *outSingle;

			if ((msg = wkbFromText(&outSingle, &inWKB, srid, tpe)) != MAL_SUCCEED)
				goto bailout;
			if (tfastins_nocheckVAR(dst, i, outSingle) != GDK_SUCCEED) {
				GDKfree(outSingle);
				msg = createException(MAL, "batgeom.wkbFromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= is_wkb_nil(outSingle);
			GDKfree(outSingle);
			outSingle = NULL;
		}
	}

bailout:
	if (b) {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
	}
	BBPreclaim(s);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = BATcount(dst) <= 1;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*outBAT_id = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

/*****************************************************************************/
/********************* IN: mbr - OUT: double - FLAG :int *********************/
/*****************************************************************************/
str
wkbCoordinateFromMBR_bat(bat *outBAT_id, bat *inBAT_id, int *coordinateIdx)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	mbr *inMBR = NULL;
	double outDbl = 0.0;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.coordinateFromMBR", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.coordinateFromMBR", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;

		inMBR = (mbr *) BUNtloc(inBAT_iter, p);
		if ((err = wkbCoordinateFromMBR(&outDbl, &inMBR, coordinateIdx)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outDbl, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.coordinateFromMBR", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);
	return MAL_SUCCEED;

}

/**************************************************************************/
/********************* IN: wkb - OUT: str - FLAG :int *********************/
/**************************************************************************/
static str
WKBtoSTRflagINT_bat(bat *outBAT_id, bat *inBAT_id, int *flag, str (*func) (char **, wkb **, int *), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("str"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		char *outSingle;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outSingle, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outSingle);
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outSingle);
		outSingle = NULL;
	}
	bat_iterator_end(&inBAT_iter);

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;
}

/*create textual representation of the wkb */
str
wkbAsText_bat(bat *outBAT_id, bat *inBAT_id, int *withSRID)
{
	return WKBtoSTRflagINT_bat(outBAT_id, inBAT_id, withSRID, wkbAsText, "batgeom.wkbAsText");
}

str
wkbGeometryType_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	return WKBtoSTRflagINT_bat(outBAT_id, inBAT_id, flag, wkbGeometryType, "batgeom.wkbGeometryType");
}

/***************************************************************************/
/*************************** IN: wkb - OUT: wkb ****************************/
/***************************************************************************/

static str
WKBtoWKB_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (wkb **, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		wkb *outSingle;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outSingle, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outSingle);
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outSingle);
		outSingle = NULL;
	}
	bat_iterator_end(&inBAT_iter);

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;
}

str
wkbBoundary_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoWKB_bat(outBAT_id, inBAT_id, wkbBoundary, "batgeom.wkbBoundary");
}


/**************************************************************************************/
/*************************** IN: wkb - OUT: wkb - FLAG:int ****************************/
/**************************************************************************************/

static str
WKBtoWKBflagINT_bat(bat *outBAT_id, bat *inBAT_id, const int *flag, str (*func) (wkb **, wkb **, const int *), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		wkb *outSingle;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outSingle, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outSingle);
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outSingle);
		outSingle = NULL;
	}
	bat_iterator_end(&inBAT_iter);

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;
}

str
wkbGeometryN_bat(bat *outBAT_id, bat *inBAT_id, const int *flag)
{
	return WKBtoWKBflagINT_bat(outBAT_id, inBAT_id, flag, wkbGeometryN, "batgeom.wkbGeometryN");
}

/***************************************************************************/
/*************************** IN: wkb - OUT: bit ****************************/
/***************************************************************************/

static str
WKBtoBIT_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (bit *, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		bit outSingle;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;

}

str
wkbIsClosed_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsClosed, "batgeom.wkbIsClosed");
}

str
wkbIsEmpty_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsEmpty, "batgeom.wkbIsEmpty");
}

str
wkbIsSimple_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsSimple, "batgeom.wkbIsSimple");
}

str
wkbIsRing_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsRing, "batgeom.wkbIsRing");
}

str
wkbIsValid_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoBIT_bat(outBAT_id, inBAT_id, wkbIsValid, "batgeom.wkbIsValid");
}


/***************************************************************************/
/*************************** IN: wkb - OUT: int ****************************/
/***************************************************************************/

static str
WKBtoINT_bat(bat *outBAT_id, bat *inBAT_id, str (*func) (int *, wkb **), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("int"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		int outSingle;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;

}

str
wkbDimension_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoINT_bat(outBAT_id, inBAT_id, wkbDimension, "batgeom.wkbDimension");
}

str
wkbNumGeometries_bat(bat *outBAT_id, bat *inBAT_id)
{
	return WKBtoINT_bat(outBAT_id, inBAT_id, wkbNumGeometries, "batgeom.wkbNumGeometries");
}

/***************************************************************************************/
/*************************** IN: wkb - OUT: int - FLAG: int ****************************/
/***************************************************************************************/

static str
WKBtoINTflagINT_bat(bat *outBAT_id, bat *inBAT_id, int *flag, str (*func) (int *, wkb **, int *), const char *name)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("int"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		int outSingle;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = (*func) (&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;

}

str
wkbNumPoints_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	return WKBtoINTflagINT_bat(outBAT_id, inBAT_id, flag, wkbNumPoints, "batgeom.wkbNumPoints");
}

str
wkbNumRings_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	return WKBtoINTflagINT_bat(outBAT_id, inBAT_id, flag, wkbNumRings, "batgeom.wkbNumRings");
}

/******************************************************************************************/
/*************************** IN: wkb - OUT: double - FLAG: int ****************************/
/******************************************************************************************/

str
wkbGetCoordinate_bat(bat *outBAT_id, bat *inBAT_id, int *flag)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkbGetCoordinate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new for the output BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkbGetCoordinate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the input BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {	//iterate over all valid elements
		str err = NULL;
		double outSingle;

		inWKB = (wkb *) BUNtvar(inBAT_iter, p);
		if ((err = wkbGetCoordinate(&outSingle, &inWKB, flag)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outSingle, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.wkbGetCoordinate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	//set the number of elements in the outBAT
	BATsetcount(outBAT, BATcount(inBAT));

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;

}

/*******************************/
/********* Two inputs **********/
/*******************************/

str
wkbBox2D_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;
	str ret = MAL_SUCCEED;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("mbr"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		mbr *outSingle;

		wkb *aWKB = (wkb *) BUNtvar(aBAT_iter, i);
		wkb *bWKB = (wkb *) BUNtvar(bBAT_iter, i);

		if ((ret = wkbBox2D(&outSingle, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPreclaim(outBAT);
			goto bailout;
		}
		if (BUNappend(outBAT, outSingle, false) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			GDKfree(outSingle);
			ret = createException(MAL, "batgeom.wkbBox2D", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		GDKfree(outSingle);
	}

	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);
  bailout:
	bat_iterator_end(&aBAT_iter);
	bat_iterator_end(&bBAT_iter);

  clean:
	BBPreclaim(aBAT);
	BBPreclaim(bBAT);

	return ret;
}

str
wkbContains_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;
	str ret = MAL_SUCCEED;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", SQLSTATE(38000) "Problem retrieving BATs");
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.Contains", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("bit"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		bit outBIT;

		wkb *aWKB = (wkb *) BUNtvar(aBAT_iter, i);
		wkb *bWKB = (wkb *) BUNtvar(bBAT_iter, i);

		if ((ret = wkbContains(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) {
			BBPreclaim(outBAT);
			goto bailout;
		}
		if (BUNappend(outBAT, &outBIT, false) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			ret = createException(MAL, "batgeom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}

	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

  bailout:
	bat_iterator_end(&aBAT_iter);
	bat_iterator_end(&bBAT_iter);

  clean:
	BBPreclaim(aBAT);
	BBPreclaim(bBAT);

	return ret;
}

str
wkbContains_geom_bat(bat *outBAT_id, wkb **geomWKB, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BATiter inBAT_iter;
	BUN p = 0, q = 0;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.Contains", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		bit outBIT;

		wkb *inWKB = (wkb *) BUNtvar(inBAT_iter, p);

		if ((err = wkbContains(&outBIT, geomWKB, &inWKB)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outBIT, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;

}

str
wkbContains_bat_geom(bat *outBAT_id, bat *inBAT_id, wkb **geomWKB)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BATiter inBAT_iter;
	BUN p = 0, q = 0;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.Contains", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("bit"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		bit outBIT;

		wkb *inWKB = (wkb *) BUNtvar(inBAT_iter, p);

		if ((err = wkbContains(&outBIT, &inWKB, geomWKB)) != MAL_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &outBIT, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;
}



/*
str
wkbFromWKB_bat(bat *outBAT_id, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb **inWKB = NULL, *outWKB = NULL;
	BUN i;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkb", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT))) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.wkb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//pointers to the first valid elements of the x and y BATS
	BATiter inBATi = bat_iterator(inBAT);
	inWKB = (wkb **) inBATi.base;
	for (i = 0; i < BATcount(inBAT); i++) {	//iterate over all valid elements
		str err = NULL;
		if ((err = wkbFromWKB(&outWKB, &inWKB[i])) != MAL_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, false) != GDK_SUCCEED) {
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.wkb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outWKB);
		outWKB = NULL;
	}
	bat_iterator_end(&inBATi);

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);
	return MAL_SUCCEED;

}
*/

/************************************/
/********* Multiple inputs **********/
/************************************/
str
wkbMakePoint_bat(bat *outBAT_id, bat *xBAT_id, bat *yBAT_id, bat *zBAT_id, bat *mBAT_id, int *zmFlag)
{
	BAT *outBAT = NULL, *xBAT = NULL, *yBAT = NULL, *zBAT = NULL, *mBAT = NULL;
	BATiter xBAT_iter, yBAT_iter, zBAT_iter, mBAT_iter;
	BUN i;
	str ret = MAL_SUCCEED;

	if (*zmFlag == 11)
		throw(MAL, "batgeom.wkbMakePoint", SQLSTATE(38000) "POINTZM is not supported");

	//get the BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL || (yBAT = BATdescriptor(*yBAT_id)) == NULL || (*zmFlag == 10 && (zBAT = BATdescriptor(*zBAT_id)) == NULL)
	    || (*zmFlag == 1 && (mBAT = BATdescriptor(*mBAT_id)) == NULL)) {

		ret = createException(MAL, "batgeom.wkbMakePoint", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	//check if the BATs are aligned
	if (xBAT->hseqbase != yBAT->hseqbase ||
	    BATcount(xBAT) != BATcount(yBAT) ||
	    (zBAT && (xBAT->hseqbase != zBAT->hseqbase || BATcount(xBAT) != BATcount(zBAT))) ||
	    (mBAT && (xBAT->hseqbase != mBAT->hseqbase || BATcount(xBAT) != BATcount(mBAT)))) {
		ret = createException(MAL, "batgeom.wkbMakePoint", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(xBAT->hseqbase, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.wkbMakePoint", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	xBAT_iter = bat_iterator(xBAT);
	yBAT_iter = bat_iterator(yBAT);
	if (zBAT)
		zBAT_iter = bat_iterator(zBAT);
	if (mBAT)
		mBAT_iter = bat_iterator(mBAT);

	for (i = 0; i < BATcount(xBAT); i++) {
		wkb *pointWKB = NULL;

		double x = *((double *) BUNtloc(xBAT_iter, i));
		double y = *((double *) BUNtloc(yBAT_iter, i));
		double z = 0.0;
		double m = 0.0;

		if (zBAT)
			z = *((double *) BUNtloc(zBAT_iter, i));
		if (mBAT)
			m = *((double *) BUNtloc(mBAT_iter, i));

		if ((ret = wkbMakePoint(&pointWKB, &x, &y, &z, &m, zmFlag)) != MAL_SUCCEED) {	//check

			BBPreclaim(outBAT);
			goto bailout;
		}
		if (BUNappend(outBAT, pointWKB, false) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			GDKfree(pointWKB);
			ret = createException(MAL, "batgeom.WkbMakePoint", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		GDKfree(pointWKB);
	}

	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

  bailout:
	bat_iterator_end(&xBAT_iter);
	bat_iterator_end(&yBAT_iter);
	if (zBAT)
		bat_iterator_end(&zBAT_iter);
	if (mBAT)
		bat_iterator_end(&mBAT_iter);
  clean:
	BBPreclaim(xBAT);
	BBPreclaim(yBAT);
	BBPreclaim(zBAT);
	BBPreclaim(mBAT);

	return ret;
}


/* sets the srid of the geometry - BULK version*/
str
wkbSetSRID_bat(bat *outBAT_id, bat *inBAT_id, int *srid)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BUN p = 0, q = 0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.SetSRID", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.SetSRID", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		wkb *outWKB = NULL;

		wkb *inWKB = (wkb *) BUNtvar(inBAT_iter, p);

		if ((err = wkbSetSRID(&outWKB, &inWKB, srid)) != MAL_SUCCEED) {	//set SRID
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.SetSRID", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outWKB);
		outWKB = NULL;
	}
	bat_iterator_end(&inBAT_iter);

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;
}

str
wkbDistance_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;
	str ret = MAL_SUCCEED;

	//get the BATs
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL || (bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto clean;
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		ret = createException(MAL, "batgeom.Distance", SQLSTATE(38000) "Columns must be aligned");
		goto clean;
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("dbl"), BATcount(aBAT), TRANSIENT)) == NULL) {
		ret = createException(MAL, "batgeom.Distance", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto clean;
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		double distanceVal = 0;

		wkb *aWKB = (wkb *) BUNtvar(aBAT_iter, i);
		wkb *bWKB = (wkb *) BUNtvar(bBAT_iter, i);

		if ((ret = wkbDistance(&distanceVal, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check

			BBPreclaim(outBAT);
			goto bailout;
		}
		if (BUNappend(outBAT, &distanceVal, false) != GDK_SUCCEED) {
			BBPreclaim(outBAT);
			ret = createException(MAL, "batgeom.Distance", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}

	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

  bailout:
	bat_iterator_end(&aBAT_iter);
	bat_iterator_end(&bBAT_iter);
  clean:
	BBPreclaim(aBAT);
	BBPreclaim(bBAT);

	return ret;

}

str
wkbDistance_geom_bat(bat *outBAT_id, wkb **geomWKB, bat *inBAT_id)
{
	BAT *outBAT = NULL, *inBAT = NULL;
	BATiter inBAT_iter;
	BUN p = 0, q = 0;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.Distance", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create a new BAT for the output
	if ((outBAT = COLnew(inBAT->hseqbase, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPunfix(inBAT->batCacheid);
		throw(MAL, "batgeom.Distance", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BAT
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) {
		str err = NULL;
		double distanceVal = 0;

		wkb *inWKB = (wkb *) BUNtvar(inBAT_iter, p);

		if ((err = wkbDistance(&distanceVal, geomWKB, &inWKB)) != MAL_SUCCEED) {	//check
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, &distanceVal, false) != GDK_SUCCEED) {
			bat_iterator_end(&inBAT_iter);
			BBPunfix(inBAT->batCacheid);
			BBPunfix(outBAT->batCacheid);
			throw(MAL, "batgeom.Distance", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&inBAT_iter);

	BBPunfix(inBAT->batCacheid);
	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);

	return MAL_SUCCEED;
}

str
wkbDistance_bat_geom(bat *outBAT_id, bat *inBAT_id, wkb **geomWKB)
{
	return wkbDistance_geom_bat(outBAT_id, geomWKB, inBAT_id);
}

/**
 * It filters the geometry in the second BAT with respect to the MBR of the geometry in the first BAT.
 **/
/*
str
wkbFilter_bat(bat *aBATfiltered_id, bat *bBATfiltered_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *aBATfiltered = NULL, *bBATfiltered = NULL, *aBAT = NULL, *bBAT = NULL;
	wkb *aWKB = NULL, *bWKB = NULL;
	bit outBIT;
	BATiter aBAT_iter, bBAT_iter;
	BUN i = 0;

	//get the descriptor of the BAT
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (aBAT->hseqbase != bBAT->hseqbase ||	//the idxs of the headers of the BATs are not the same
	    BATcount(aBAT) != BATcount(bBAT)) {	//the number of valid elements in the BATs are not the same
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(38000) "The arguments must have dense and aligned heads");
	}
	//create two new BATs for the output
	if ((aBATfiltered = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if ((bBATfiltered = COLnew(bBAT->hseqbase, ATOMindex("wkb"), BATcount(bBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		str err = NULL;
		aWKB = (wkb *) BUNtvar(aBAT_iter, i);
		bWKB = (wkb *) BUNtvar(bBAT_iter, i);

		//check the containment of the MBRs
		if ((err = mbrOverlaps_wkb(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) {
			bat_iterator_end(&aBAT_iter);
			bat_iterator_end(&bBAT_iter);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			BBPunfix(aBATfiltered->batCacheid);
			BBPunfix(bBATfiltered->batCacheid);
			return err;
		}
		if (outBIT) {
			if (BUNappend(aBATfiltered, aWKB, false) != GDK_SUCCEED ||
			    BUNappend(bBATfiltered, bWKB, false) != GDK_SUCCEED) {
				bat_iterator_end(&aBAT_iter);
				bat_iterator_end(&bBAT_iter);
				BBPunfix(aBAT->batCacheid);
				BBPunfix(bBAT->batCacheid);
				BBPunfix(aBATfiltered->batCacheid);
				BBPunfix(bBATfiltered->batCacheid);
				throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	}
	bat_iterator_end(&aBAT_iter);
	bat_iterator_end(&bBAT_iter);

	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);
	*aBATfiltered_id = aBATfiltered->batCacheid;
	BBPkeepref(aBATfiltered);
	*bBATfiltered_id = bBATfiltered->batCacheid;
	BBPkeepref(bBATfiltered);

	return MAL_SUCCEED;


}
*/

/**
 * It filters the geometry in the second BAT with respect to the MBR of the geometry in the first BAT.
 **/
str
wkbFilter_geom_bat(bat *BATfiltered_id, wkb **geomWKB, bat *BAToriginal_id)
{
	BAT *BATfiltered = NULL, *BAToriginal = NULL;
	wkb *WKBoriginal = NULL;
	BATiter BAToriginal_iter;
	BUN i = 0;
	mbr *geomMBR;
	str err = NULL;

	//get the descriptor of the BAT
	if ((BAToriginal = BATdescriptor(*BAToriginal_id)) == NULL) {
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	//create the new BAT
	if ((BATfiltered = COLnew(BAToriginal->hseqbase, ATOMindex("wkb"), BATcount(BAToriginal), TRANSIENT)) == NULL) {
		BBPunfix(BAToriginal->batCacheid);
		throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//create the MBR of the geom
	if ((err = wkbMBR(&geomMBR, geomWKB)) != MAL_SUCCEED) {
		BBPunfix(BAToriginal->batCacheid);
		BBPunfix(BATfiltered->batCacheid);
		return err;
	}

	//iterator over the BAT
	BAToriginal_iter = bat_iterator(BAToriginal);

	for (i = 0; i < BATcount(BAToriginal); i++) {
		str err = NULL;
		mbr *MBRoriginal;
		bit outBIT = 0;

		WKBoriginal = (wkb *) BUNtvar(BAToriginal_iter, i);

		//create the MBR for each geometry in the BAT
		if ((err = wkbMBR(&MBRoriginal, &WKBoriginal)) != MAL_SUCCEED) {
			bat_iterator_end(&BAToriginal_iter);
			BBPunfix(BAToriginal->batCacheid);
			BBPunfix(BATfiltered->batCacheid);
			GDKfree(geomMBR);
			return err;
		}
		//check the containment of the MBRs
		if ((err = mbrOverlaps(&outBIT, &geomMBR, &MBRoriginal)) != MAL_SUCCEED) {
			bat_iterator_end(&BAToriginal_iter);
			BBPunfix(BAToriginal->batCacheid);
			BBPunfix(BATfiltered->batCacheid);
			GDKfree(geomMBR);
			GDKfree(MBRoriginal);
			return err;
		}

		if (outBIT) {
			if (BUNappend(BATfiltered, WKBoriginal, false) != GDK_SUCCEED) {
				bat_iterator_end(&BAToriginal_iter);
				BBPunfix(BAToriginal->batCacheid);
				BBPunfix(BATfiltered->batCacheid);
				GDKfree(geomMBR);
				GDKfree(MBRoriginal);
				throw(MAL, "batgeom.MBRfilter", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}

		GDKfree(MBRoriginal);
	}
	bat_iterator_end(&BAToriginal_iter);

	GDKfree(geomMBR);
	BBPunfix(BAToriginal->batCacheid);
	*BATfiltered_id = BATfiltered->batCacheid;
	BBPkeepref(BATfiltered);

	return MAL_SUCCEED;

}

str
wkbFilter_bat_geom(bat *BATfiltered_id, bat *BAToriginal_id, wkb **geomWKB)
{
	return wkbFilter_geom_bat(BATfiltered_id, geomWKB, BAToriginal_id);
}

/* MBR */
str
wkbCoordinateFromWKB_bat(bat *outBAT_id, bat *inBAT_id, int *coordinateIdx)
{
	str err = NULL;
	bat inBAT_mbr_id = 0;	//the id of the bat with the mbrs

	if ((err = wkbMBR_bat(&inBAT_mbr_id, inBAT_id)) != MAL_SUCCEED) {
		return err;
	}
	//call the bulk version of wkbCoordinateFromMBR
	err = wkbCoordinateFromMBR_bat(outBAT_id, &inBAT_mbr_id, coordinateIdx);
	BBPrelease(inBAT_mbr_id);
	return err;
}

str
wkbMakeLine_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i;

	//get the BATs
	aBAT = BATdescriptor(*aBAT_id);
	bBAT = BATdescriptor(*bBAT_id);
	if (aBAT == NULL || bBAT == NULL) {
		BBPreclaim(aBAT);
		BBPreclaim(bBAT);
		throw(MAL, "batgeom.MakeLine", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", SQLSTATE(38000) "Columns must be aligned");
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		str err = NULL;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aWKB = (wkb *) BUNtvar(aBAT_iter, i);
		bWKB = (wkb *) BUNtvar(bBAT_iter, i);

		if ((err = wkbMakeLine(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check
			bat_iterator_end(&aBAT_iter);
			bat_iterator_end(&bBAT_iter);
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, false) != GDK_SUCCEED) {
			bat_iterator_end(&aBAT_iter);
			bat_iterator_end(&bBAT_iter);
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outWKB);
		outWKB = NULL;
	}
	bat_iterator_end(&aBAT_iter);
	bat_iterator_end(&bBAT_iter);

	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);
	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);

	return MAL_SUCCEED;
}

str
wkbUnion_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id)
{
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	BATiter aBAT_iter, bBAT_iter;
	BUN i;

	//get the BATs
	aBAT = BATdescriptor(*aBAT_id);
	bBAT = BATdescriptor(*bBAT_id);
	if (aBAT == NULL || bBAT == NULL) {
		BBPreclaim(aBAT);
		BBPreclaim(bBAT);
		throw(MAL, "batgeom.Union", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	//check if the BATs are aligned
	if (aBAT->hseqbase != bBAT->hseqbase || BATcount(aBAT) != BATcount(bBAT)) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", SQLSTATE(38000) "Columns must be aligned");
	}
	//create a new BAT for the output
	if ((outBAT = COLnew(aBAT->hseqbase, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPunfix(aBAT->batCacheid);
		BBPunfix(bBAT->batCacheid);
		throw(MAL, "batgeom.Union", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	//iterator over the BATs
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = 0; i < BATcount(aBAT); i++) {
		str err = NULL;
		wkb *aWKB = NULL, *bWKB = NULL, *outWKB = NULL;

		aWKB = (wkb *) BUNtvar(aBAT_iter, i);
		bWKB = (wkb *) BUNtvar(bBAT_iter, i);

		if ((err = wkbUnion(&outWKB, &aWKB, &bWKB)) != MAL_SUCCEED) {	//check
			bat_iterator_end(&aBAT_iter);
			bat_iterator_end(&bBAT_iter);
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			return err;
		}
		if (BUNappend(outBAT, outWKB, false) != GDK_SUCCEED) {
			bat_iterator_end(&aBAT_iter);
			bat_iterator_end(&bBAT_iter);
			BBPunfix(outBAT->batCacheid);
			BBPunfix(aBAT->batCacheid);
			BBPunfix(bBAT->batCacheid);
			GDKfree(outWKB);
			throw(MAL, "batgeom.Union", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(outWKB);
		outWKB = NULL;
	}
	bat_iterator_end(&aBAT_iter);
	bat_iterator_end(&bBAT_iter);

	*outBAT_id = outBAT->batCacheid;
	BBPkeepref(outBAT);
	BBPunfix(aBAT->batCacheid);
	BBPunfix(bBAT->batCacheid);

	return MAL_SUCCEED;
}
