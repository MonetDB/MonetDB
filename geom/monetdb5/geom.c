/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @a Wouter Scherphof, Niels Nes, Foteini Alvanaki
 * @* The simple geom module
 */

#include "geom.h"
#include "geod.h"
#include "geom_atoms.h"
#include "gdk_logger.h"
#include "mal_exception.h"

mbr mbrNIL = {0}; // will be initialized properly by geom prelude

/**
 * AGGREGATES
 **/

/**
* Collect (Group By implementation)
*
**/
//Gets the type of collection a single geometry should belong to
static int
GEOSGeom_getCollectionType (int GEOSGeom_type) {

	//TODO Remove
	(void) geodeticEdgeBoundingBox;

	//Single geometries get collected into a Multi* geometry
	if (GEOSGeom_type == GEOS_POINT)
		return GEOS_MULTIPOINT;
	else if (GEOSGeom_type == GEOS_LINESTRING || GEOSGeom_type == GEOS_LINEARRING)
		return GEOS_MULTILINESTRING;
	else if (GEOSGeom_type == GEOS_POLYGON)
		return GEOS_MULTIPOLYGON;
	//Multi* or GeometryCollections get collected into GeometryCollections
	else
		return GEOS_GEOMETRYCOLLECTION;
}

/* Group By operation. Joins geometries together in the same group into a MultiGeometry */
str
wkbCollectAggrSubGroupedCand(bat *outid, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	BAT *b = NULL, *g = NULL, *s = NULL, *out = NULL;
	BAT *sortedgroups, *sortedorder, *sortedinput;
	BATiter bi;
	const oid *gids = NULL;
	str msg = MAL_SUCCEED;
	const char *err;
	//SRID for collection
	int srid = 0;

	oid min, max;
	BUN ngrp;
	struct canditer ci;

	oid lastGrp = -1;
	int geomCollectionType = -1;
	BUN geomCount = 0;
	wkb **unions = NULL;
	GEOSGeom *unionGroup = NULL, collection;

	//Not using these variables
	(void)skip_nils;
	(void)eid;

	//Get the BAT descriptors for the value, group and candidate bats
	if ((b = BATdescriptor(*bid)) == NULL ||
		(gid && !is_bat_nil(*gid) && (g = BATdescriptor(*gid)) == NULL) ||
		(sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL)) {
		msg = createException(MAL, "geom.Collect", RUNTIME_OBJECT_MISSING);
		goto free;
	}

	if ((BATsort(&sortedgroups, &sortedorder, NULL, g, NULL, NULL, false, false, true)) != GDK_SUCCEED) {
		msg = createException(MAL, "geom.Collect", "BAT sort failed.");
		goto free;
	}

	//Project new order onto input bat IF the sortedorder isn't dense (in which case, the original input order is correct)
	if (!BATtdense(sortedorder)) {
		sortedinput = BATproject(sortedorder,b);
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		b = sortedinput;
		g = sortedgroups;
		BBPunfix(sortedorder->batCacheid);
	}
	else {
		BBPunfix(sortedgroups->batCacheid);
		BBPunfix(sortedorder->batCacheid);
	}

	//Fill in the values of the group aggregate operation
	if ((err = BATgroupaggrinit(b, g, NULL, s, &min, &max, &ngrp, &ci)) != NULL) {
		msg = createException(MAL, "geom.Collect", "%s", err);
		goto free;
	}

	//Create a new BAT column of wkb type, with lenght equal to the number of groups
	if ((out = COLnew(min, ATOMindex("wkb"), ngrp, TRANSIENT)) == NULL) {
		msg = createException(MAL, "geom.Collect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	if (ngrp) {
		//All unions for output BAT
		if ((unions = GDKzalloc(sizeof(wkb *) * ngrp)) == NULL) {
			msg = createException(MAL, "geom.Collect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			BBPreclaim(out);
			goto free;
		}

		//Intermediate array for all the geometries in a group
		if ((unionGroup = GDKzalloc(sizeof(GEOSGeom) * ci.ncand)) == NULL) {
			msg = createException(MAL, "geom.Collect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			BBPreclaim(out);
			if (unions)
				GDKfree(unions);
			goto free;
		}

		if (g && !BATtdense(g))
			gids = (const oid *)Tloc(g, 0);
		bi = bat_iterator(b);

		for (BUN i = 0; i < ci.ncand; i++) {
			oid o = canditer_next(&ci);
			BUN p = o - b->hseqbase;
			oid grp = gids ? gids[p] : g ? min + (oid)p : 0;
			wkb *inWKB = (wkb *)BUNtvar(bi, p);
			GEOSGeom inGEOM = wkb2geos(inWKB);


			if (grp != lastGrp) {
				if (lastGrp != (oid)-1) {
					//Finish the previous group, move on to the next one
					collection = GEOSGeom_createCollection(geomCollectionType, unionGroup, (unsigned int) geomCount);
					GEOSSetSRID(collection,srid);
					//Save collection to unions array as wkb
					unions[lastGrp] = geos2wkb(collection);

					GEOSGeom_destroy(collection);
					GDKfree(unionGroup);

					if ((unionGroup = GDKzalloc(sizeof(GEOSGeom) * ci.ncand)) == NULL) {
						msg = createException(MAL, "geom.Collect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						//Frees
						bat_iterator_end(&bi);
						if (unions) {
							for (BUN i = 0; i < ngrp; i++)
								GDKfree(unions[i]);
							GDKfree(unions);
						}
						if (unionGroup) {
							for (BUN i = 0; i < geomCount; i++)
								if (unionGroup[i])
									GEOSGeom_destroy(unionGroup[i]);
							GDKfree(unionGroup);
						}
						goto free;
					}
				}
				geomCount = 0;
				lastGrp = grp;
				geomCollectionType = GEOSGeom_getCollectionType(GEOSGeomTypeId(inGEOM));
				srid = GEOSGetSRID(inGEOM);
			}
			unionGroup[geomCount] = inGEOM;
			geomCount += 1;
			if (geomCollectionType != GEOS_GEOMETRYCOLLECTION && GEOSGeom_getCollectionType(GEOSGeomTypeId(inGEOM)) != geomCollectionType)
				geomCollectionType = GEOS_GEOMETRYCOLLECTION;
		}
		//Last collection
		collection = GEOSGeom_createCollection(geomCollectionType, unionGroup, (unsigned int) geomCount);
		GEOSSetSRID(collection,srid);
		unions[lastGrp] = geos2wkb(collection);

		GEOSGeom_destroy(collection);
		GDKfree(unionGroup);

		if (BUNappendmulti(out, unions, ngrp, false) != GDK_SUCCEED) {
			msg = createException(MAL, "geom.Union", SQLSTATE(38000) "BUNappend operation failed");
			bat_iterator_end(&bi);
			if (unions) {
				for (BUN i = 0; i < ngrp; i++)
					GDKfree(unions[i]);
				GDKfree(unions);
			}
			goto free;
		}

		bat_iterator_end(&bi);
		for (BUN i = 0; i < ngrp; i++)
			GDKfree(unions[i]);
		GDKfree(unions);

	}
	*outid = out->batCacheid;
	BBPkeepref(out);
	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	return MAL_SUCCEED;
free:
	if (b)
		BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	BBPreclaim(out);
	return msg;
}

str
wkbCollectAggrSubGrouped(bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return wkbCollectAggrSubGroupedCand(out, bid, gid, eid, NULL, skip_nils);
}

str
wkbCollectAggr (wkb **out, const bat *bid) {
	str msg = MAL_SUCCEED;
	BAT *b = NULL;
	GEOSGeom *unionGroup = NULL, collection;
	int geomCollectionType = -1;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(MAL, "geom.Collect", RUNTIME_OBJECT_MISSING);
		if (b)
			BBPunfix(b->batCacheid);
		return msg;
	}

	BUN count = BATcount(b);

	if ((unionGroup = GDKzalloc(sizeof(GEOSGeom) * count)) == NULL) {
		msg = createException(MAL, "geom.Collect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		BBPunfix(b->batCacheid);
		return msg;
	}
	int srid = -1;

	BATiter bi = bat_iterator(b);
	for (BUN i = 0; i < count; i++) {
		oid p = i + b->hseqbase;
		wkb *inWKB = (wkb *)BUNtvar(bi, p);
		unionGroup[i] = wkb2geos(inWKB);
		if (srid == -1)
			srid = GEOSGetSRID(unionGroup[i]);

		//Set collection type on first geometry
		if (geomCollectionType == -1)
			geomCollectionType = GEOSGeom_getCollectionType(GEOSGeomTypeId(unionGroup[i]));
		//Then, check if we need to change it a Geometry collection (if the current geometry is different from the previous type)
		if (geomCollectionType != GEOS_GEOMETRYCOLLECTION && GEOSGeom_getCollectionType(GEOSGeomTypeId(unionGroup[i])) != geomCollectionType)
			geomCollectionType = GEOS_GEOMETRYCOLLECTION;
	}
	collection = GEOSGeom_createCollection(geomCollectionType, unionGroup, (unsigned int) count);
	GEOSSetSRID(collection,srid);
	//Result
	(*out) = geos2wkb(collection);
	if (*out == NULL)
		msg = createException(MAL, "geom.ConvexHull", SQLSTATE(38000) "Geos operation geos2wkb failed");

    // Cleanup
    // Data ownership has been transfered from unionGroup elements to
    // collection. Check libgeos GEOSGeom_createCollection() for more.
    bat_iterator_end(&bi);
    GEOSGeom_destroy(collection);
    if (unionGroup)
        GDKfree(unionGroup);
    BBPunfix(b->batCacheid);

	return msg;
}

static str
wkbCollect (wkb **out, wkb * const *a, wkb * const *b) {
	str err = MAL_SUCCEED;
	GEOSGeom collection;
	/* geom_a and geom_b */
	GEOSGeom geoms[2] = {NULL, NULL};
	int type_a, type_b;

	if ((err = wkbGetCompatibleGeometries(a, b, &geoms[0], &geoms[1])) != MAL_SUCCEED)
		throw(MAL,"geom.Collect", "%s", err);

	//int srid = GEOSGetSRID(ga);
	type_a = GEOSGeomTypeId(geoms[0]);
	type_b = GEOSGeomTypeId(geoms[1]);

	/* NOTE: geoms will be moved to collection. no need for cleanup */
	if (type_a == type_b)
		collection = GEOSGeom_createCollection(GEOSGeom_getCollectionType(type_a), geoms, (unsigned int) 2);
	else
		collection = GEOSGeom_createCollection(GEOS_GEOMETRYCOLLECTION, geoms, (unsigned int) 2);

	if ((*out = geos2wkb(collection)) == NULL)
		err = createException(MAL, "geom.Collect", SQLSTATE(38000) "Geos operation geos2wkb failed");

	GEOSGeom_destroy(collection);

	return err;
}
/**
 * Start of old geom module
 **/

int TYPE_mbr;

static inline int
geometryHasZ(int info)
{
	return (info & 0x02);
}

static inline int
geometryHasM(int info)
{
	return (info & 0x01);
}

/* the first argument in the functions is the return variable */

#ifdef HAVE_PROJ

/* math.h files do not have M_PI defined */
#ifndef M_PI
#define M_PI	((double) 3.14159265358979323846)	/* pi */
#endif

//TODO Remove?
/** convert degrees to radians */
/*static inline void
degrees2radians(double *x, double *y, double *z)
{
	double val = M_PI / 180.0;
	*x *= val;
	*y *= val;
	*z *= val;
}*/

//TODO Remove?
/** convert radians to degrees */
/*static inline void
radians2degrees(double *x, double *y, double *z)
{
	double val = 180.0 / M_PI;
	*x *= val;
	*y *= val;
	*z *= val;
}*/

static str
transformCoordSeq(int idx, int coordinatesNum, PJ *P, const GEOSCoordSequence *gcs_old, GEOSCoordSeq gcs_new)
{
	double x = 0, y = 0, z = 0;
	int errorNum = 0;
	PJ_COORD c, c_out;

	if (!GEOSCoordSeq_getX(gcs_old, idx, &x) ||
	    !GEOSCoordSeq_getY(gcs_old, idx, &y) ||
	    (coordinatesNum > 2 && !GEOSCoordSeq_getZ(gcs_old, idx, &z)))
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot get coordinates");

	c.lpzt.lam=x;
	c.lpzt.phi=y;
	c.lpzt.z=z;
	c.lpzt.t = HUGE_VAL;

	c_out = proj_trans(P, PJ_FWD, c);

	errorNum = proj_errno(P);
	if (errorNum != 0) {
		if (coordinatesNum > 2)
			throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot transform point (%f %f %f): %s\n", x, y, z, proj_errno_string(errorNum));
		else
			throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot transform point (%f %f): %s\n", x, y, proj_errno_string(errorNum));
	}

	if (!GEOSCoordSeq_setX(gcs_new, idx,c_out.xy.x) ||
	    !GEOSCoordSeq_setY(gcs_new, idx,c_out.xy.y) ||
	    (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, idx, z)))
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos cannot set coordinates");

	return MAL_SUCCEED;
}

str
transformPoint(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	str ret = MAL_SUCCEED;

	*transformedGeometry = NULL;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* create the coordinates sequence for the transformed geometry */
	gcs_new = GEOSCoordSeq_create(1, coordinatesNum);
	if (gcs_new == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* create the transformed coordinates */
	ret = transformCoordSeq(0, coordinatesNum, P, gcs_old, gcs_new);
	if (ret != MAL_SUCCEED) {
		GEOSCoordSeq_destroy(gcs_new);
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createPoint(gcs_new);
	if (*transformedGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
	}

	return MAL_SUCCEED;
}

str
transformLine(GEOSCoordSeq *gcs_new, const GEOSGeometry *geosGeometry, PJ *P)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	unsigned int pointsNum = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum))
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getSize failed");

	/* create the coordinates sequence for the transformed geometry */
	*gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	if (*gcs_new == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");

	/* create the transformed coordinates */
	for (i = 0; i < pointsNum; i++) {
		ret = transformCoordSeq(i, coordinatesNum, P, gcs_old, *gcs_new);
		if (ret != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(*gcs_new);
			*gcs_new = NULL;
			return ret;
		}
	}

	return MAL_SUCCEED;
}

str
transformLineString(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
{
	GEOSCoordSeq coordSeq;
	str ret = MAL_SUCCEED;

	ret = transformLine(&coordSeq, geosGeometry, P);
	if (ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createLineString(coordSeq);
	if (*transformedGeometry == NULL) {
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
	}

	return ret;
}

str
transformLinearRing(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
{
	GEOSCoordSeq coordSeq = NULL;
	str ret = MAL_SUCCEED;

	ret = transformLine(&coordSeq, geosGeometry, P);
	if (ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createLinearRing(coordSeq);
	if (*transformedGeometry == NULL) {
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
	}

	return ret;
}

str
transformPolygon(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (exteriorRingGeometry == NULL) {
		*transformedGeometry = NULL;
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");
	}

	ret = transformLinearRing(&transformedExteriorRingGeometry, exteriorRingGeometry, P);
	if (ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}
	GEOSSetSRID(transformedExteriorRingGeometry, srid);

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*transformedGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed.");
	}

	if(numInteriorRings > 0)
	{
		/* iterate over the interiorRing and transform each one of them */
		transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
		if (transformedInteriorRingGeometries == NULL) {
			*transformedGeometry = NULL;
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			throw(MAL, "geom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		for (i = 0; i < numInteriorRings; i++) {
			ret = transformLinearRing(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), P);
			if (ret != MAL_SUCCEED) {
				while (--i >= 0)
					GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
				GDKfree(transformedInteriorRingGeometries);
				GEOSGeom_destroy(transformedExteriorRingGeometry);
				*transformedGeometry = NULL;
				return ret;
			}
			GEOSSetSRID(transformedInteriorRingGeometries[i], srid);
		}
	}

	*transformedGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);

	if (*transformedGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	//GEOSGeom_destroy(transformedExteriorRingGeometry);
	return ret;
}

str
transformMultiGeometry(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid, int geometryType)
{
	int geometriesNum, subGeometryType, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	const GEOSGeometry *multiGeometry = NULL;
	str ret = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	if (geometriesNum == -1)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetNumGeometries failed");
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < geometriesNum; i++) {
		if ((multiGeometry = GEOSGetGeometryN(geosGeometry, i)) == NULL)
			ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGetGeometryN failed");
		else if ((subGeometryType = GEOSGeomTypeId(multiGeometry) + 1) == 0)
			ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeomTypeId failed");
		else {
			switch (subGeometryType) {
			case wkbPoint_mdb:
				ret = transformPoint(&transformedMultiGeometries[i], multiGeometry, P);
				break;
			case wkbLineString_mdb:
				ret = transformLineString(&transformedMultiGeometries[i], multiGeometry, P);
				break;
			case wkbLinearRing_mdb:
				ret = transformLinearRing(&transformedMultiGeometries[i], multiGeometry, P);
				break;
			case wkbPolygon_mdb:
				ret = transformPolygon(&transformedMultiGeometries[i], multiGeometry, P, srid);
				break;
			default:
				ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos unknown geometry type");
				break;
			}
		}

		if (ret != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*transformedGeometry = NULL;
			return ret;
		}

		GEOSSetSRID(transformedMultiGeometries[i], srid);
	}

	*transformedGeometry = GEOSGeom_createCollection(geometryType - 1, transformedMultiGeometries, geometriesNum);
	if (*transformedGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return ret;
}
#endif

/* It gets a geometry and transforms its coordinates to the provided srid */
str
wkbTransform(wkb **transformedWKB, wkb **geomWKB, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
{
#ifndef HAVE_PROJ
	*transformedWKB = NULL;
	(void) geomWKB;
	(void) srid_src;
	(void) srid_dst;
	(void) proj4_src_str;
	(void) proj4_dst_str;
	throw(MAL, "geom.Transform", SQLSTATE(38000) "PROJ library not found");
#else
	PJ *P;
	GEOSGeom geosGeometry, transformedGeosGeometry;
	int geometryType = -1;

	str ret = MAL_SUCCEED;

	if (*geomWKB == NULL)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Geos wkb format is null");

	if (is_wkb_nil(*geomWKB) ||
	    is_int_nil(*srid_src) ||
	    is_int_nil(*srid_dst) ||
	    strNil(*proj4_src_str) ||
	    strNil(*proj4_dst_str)) {
		if ((*transformedWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if (strcmp(*proj4_src_str, "null") == 0)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Cannot find in spatial_ref_sys srid %d\n", *srid_src);
	if (strcmp(*proj4_dst_str, "null") == 0)
		throw(MAL, "geom.Transform", SQLSTATE(38000) "Cannot find in spatial_ref_sys srid %d\n", *srid_dst);
	if (strcmp(*proj4_src_str, *proj4_dst_str) == 0) {
		if ((*transformedWKB = wkbCopy(*geomWKB)) == NULL)
			throw(MAL, "geom.Transform", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	//Create PROJ transformation object with PROJ strings passed as argument
	P = proj_create_crs_to_crs(PJ_DEFAULT_CTX,
                               *proj4_src_str,
                               *proj4_dst_str,
                               NULL);
	if (P==0)
        throw(MAL, "geom.Transform", SQLSTATE(38000) "PROJ initialization failed");

	/* get the geosGeometry from the wkb */
	geosGeometry = wkb2geos(*geomWKB);
	/* get the type of the geometry */
	geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//TODO: No collection?
	switch (geometryType) {
	case wkbPoint_mdb:
		ret = transformPoint(&transformedGeosGeometry, geosGeometry, P);
		break;
	case wkbLineString_mdb:
		ret = transformLineString(&transformedGeosGeometry, geosGeometry, P);
		break;
	case wkbLinearRing_mdb:
		ret = transformLinearRing(&transformedGeosGeometry, geosGeometry, P);
		break;
	case wkbPolygon_mdb:
		ret = transformPolygon(&transformedGeosGeometry, geosGeometry, P, *srid_dst);
		break;
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
		ret = transformMultiGeometry(&transformedGeosGeometry, geosGeometry, P, *srid_dst, geometryType);
		break;
	default:
		transformedGeosGeometry = NULL;
		ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos unknown geometry type");
	}

	if (ret == MAL_SUCCEED && transformedGeosGeometry) {
		/* set the new srid */
		GEOSSetSRID(transformedGeosGeometry, *srid_dst);
		/* get the wkb */
		if ((*transformedWKB = geos2wkb(transformedGeosGeometry)) == NULL)
			ret = createException(MAL, "geom.Transform", SQLSTATE(38000) "Geos operation geos2wkb failed");
		/* destroy the geos geometries */
		GEOSGeom_destroy(transformedGeosGeometry);
	}
	proj_destroy(P);
	GEOSGeom_destroy(geosGeometry);

	return ret;
#endif
}

//gets a coord seq and forces it to have dim dimensions adding or removing extra dimensions
static str
forceDimCoordSeq(int idx, int coordinatesNum, int dim, const GEOSCoordSequence *gcs_old, GEOSCoordSeq gcs_new)
{
	double x = 0, y = 0, z = 0;

	//get the coordinates
	if (!GEOSCoordSeq_getX(gcs_old, idx, &x))
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getX failed");
	if (!GEOSCoordSeq_getY(gcs_old, idx, &y))
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getY failed");
	if (coordinatesNum > 2 && dim > 2 &&	//read it only if needed (dim >2)
	    !GEOSCoordSeq_getZ(gcs_old, idx, &z))
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getZ failed");

	//create the new coordinates
	if (!GEOSCoordSeq_setX(gcs_new, idx, x))
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setX failed");
	if (!GEOSCoordSeq_setY(gcs_new, idx, y))
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setY failed");
	if (dim > 2)
		if (!GEOSCoordSeq_setZ(gcs_new, idx, z))
			throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setZ failed");
	return MAL_SUCCEED;
}

static str
forceDimPoint(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
	}

	/* create the coordinates sequence for the translated geometry */
	if ((gcs_new = GEOSCoordSeq_create(1, dim)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	ret = forceDimCoordSeq(0, coordinatesNum, dim, gcs_old, gcs_new);
	if (ret != MAL_SUCCEED) {
		*outGeometry = NULL;
		GEOSCoordSeq_destroy(gcs_new);
		return ret;
	}

	/* create the geometry from the coordinates sequence */
	*outGeometry = GEOSGeom_createPoint(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_createPoint failed");
	}

	return MAL_SUCCEED;
}

static str
forceDimLineString(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum))
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getSize failed");

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, dim);
	if (gcs_new == NULL)
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		ret = forceDimCoordSeq(i, coordinatesNum, dim, gcs_old, gcs_new);
		if (ret != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return ret;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLineString(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
	}

	return MAL_SUCCEED;

}

//Although linestring and linearRing are essentially the same we need to distinguish that when creating polygon from the rings
static str
forceDimLinearRing(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum))
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getSize failed");

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, dim);
	if (gcs_new == NULL)
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		ret = forceDimCoordSeq(i, coordinatesNum, dim, gcs_old, gcs_new);
		if (ret != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return ret;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLinearRing(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_createLinearRing failed");
	}

	return MAL_SUCCEED;
}

static str
forceDimPolygon(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str ret = MAL_SUCCEED;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (!exteriorRingGeometry) {
		*outGeometry = NULL;
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");
	}

	if ((ret = forceDimLinearRing(&transformedExteriorRingGeometry, exteriorRingGeometry, dim)) != MAL_SUCCEED) {
		*outGeometry = NULL;
		return ret;
	}

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed.");
	}

	/* iterate over the interiorRing and translate each one of them */
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
	if (transformedInteriorRingGeometries == NULL) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.ForceDim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	for (i = 0; i < numInteriorRings; i++) {
		if ((ret = forceDimLinearRing(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), dim)) != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
			GDKfree(transformedInteriorRingGeometries);
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			*outGeometry = NULL;
			return ret;
		}
	}

	*outGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*outGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		ret = createException(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	GEOSGeom_destroy(transformedExteriorRingGeometry);

	return ret;
}

static str forceDimGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim);
static str
forceDimMultiGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int geometriesNum, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	str err = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.ForceDim", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	//In order to have the geometries in the output in the same order as in the input
	//we should read them and put them in the area in reverse order
	for (i = geometriesNum - 1; i >= 0; i--) {
		const GEOSGeometry *multiGeometry = GEOSGetGeometryN(geosGeometry, i);

		if ((err = forceDimGeometry(&transformedMultiGeometries[i], multiGeometry, dim)) != MAL_SUCCEED) {
			while (++i < geometriesNum)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createCollection(GEOSGeomTypeId(geosGeometry), transformedMultiGeometries, geometriesNum);
	if (*outGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		err = createException(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return err;
}

static str
forceDimGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, int dim)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return forceDimPoint(outGeometry, geosGeometry, dim);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return forceDimLineString(outGeometry, geosGeometry, dim);
	case wkbPolygon_mdb:
		return forceDimPolygon(outGeometry, geosGeometry, dim);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return forceDimMultiGeometry(outGeometry, geosGeometry, dim);
	default:
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation %s unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbForceDim(wkb **outWKB, wkb **geomWKB, int *dim)
{
	GEOSGeometry *outGeometry;
	GEOSGeom geosGeometry;
	str err;

	if (is_wkb_nil(*geomWKB) || is_int_nil(*dim)) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.ForceDim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if ((err = forceDimGeometry(&outGeometry, geosGeometry, *dim)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		*outWKB = NULL;
		return err;
	}

	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));

	*outWKB = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.ForceDim", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

static str
segmentizePoint(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry)
{
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;

	//nothing much to do. Just create a copy of the point
	//get the coordinates
	if ((gcs_old = GEOSGeom_getCoordSeq(geosGeometry)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
	}
	//create a copy of it
	if ((gcs_new = GEOSCoordSeq_clone(gcs_old)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_clone failed");
	}
	//create the geometry from the coordinates sequence
	*outGeometry = GEOSGeom_createPoint(gcs_new);
	if (*outGeometry == NULL) {
		GEOSCoordSeq_destroy(gcs_new);
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGeom_createPoint failed");
	}

	return MAL_SUCCEED;
}

static str
segmentizeLineString(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz, int isRing)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, additionalPoints = 0, i = 0, j = 0;
	double xl = 0.0, yl = 0.0, zl = 0.0;
	double *xCoords_org, *yCoords_org, *zCoords_org;
	str err = MAL_SUCCEED;

	//get the number of coordinates the geometry has
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	//get the coordinates of the points comprising the geometry
	if ((gcs_old = GEOSGeom_getCoordSeq(geosGeometry)) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
	}
	//get the number of points in the geometry
	if (!GEOSCoordSeq_getSize(gcs_old, &pointsNum)) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getSize failed");
	}
	//store the points so that I do not have to read them multiple times using geos
	if ((xCoords_org = GDKmalloc(pointsNum * sizeof(double))) == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(HY013) MAL_MALLOC_FAIL " for %u double values", pointsNum);
	}
	if ((yCoords_org = GDKmalloc(pointsNum * sizeof(double))) == NULL) {
		GDKfree(xCoords_org);
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(HY013) MAL_MALLOC_FAIL " for %u double values", pointsNum);
	}
	if ((zCoords_org = GDKmalloc(pointsNum * sizeof(double))) == NULL) {
		GDKfree(xCoords_org);
		GDKfree(yCoords_org);
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(HY013) MAL_MALLOC_FAIL " for %u double values", pointsNum);
	}

	if (!GEOSCoordSeq_getX(gcs_old, 0, &xCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getX failed");
		goto bailout;
	}
	if (!GEOSCoordSeq_getY(gcs_old, 0, &yCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getY failed");
		goto bailout;
	}
	if (coordinatesNum > 2 && !GEOSCoordSeq_getZ(gcs_old, 0, &zCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getZ failed");
		goto bailout;
	}

	xl = xCoords_org[0];
	yl = yCoords_org[0];
	zl = zCoords_org[0];

	//check how many new points should be added
	for (i = 1; i < pointsNum; i++) {
		double dist;

		if (!GEOSCoordSeq_getX(gcs_old, i, &xCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getX failed");
			goto bailout;
		}
		if (!GEOSCoordSeq_getY(gcs_old, i, &yCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getY failed");
			goto bailout;
		}
		if (coordinatesNum > 2 && !GEOSCoordSeq_getZ(gcs_old, i, &zCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getZ failed");
			goto bailout;
		}

		//compute the distance of the current point to the last added one
		while ((dist = sqrt(pow(xl - xCoords_org[i], 2) + pow(yl - yCoords_org[i], 2) + pow(zl - zCoords_org[i], 2))) > sz) {
			TRC_DEBUG(GEOM, "Old : (%f, %f, %f) vs (%f, %f, %f) = %f\n", xl, yl, zl, xCoords_org[i], yCoords_org[i], zCoords_org[i], dist);

			additionalPoints++;
			//compute the point
			xl = xl + (xCoords_org[i] - xl) * sz / dist;
			yl = yl + (yCoords_org[i] - yl) * sz / dist;
			zl = zl + (zCoords_org[i] - zl) * sz / dist;
		}

		xl = xCoords_org[i];
		yl = yCoords_org[i];
		zl = zCoords_org[i];

	}

	TRC_DEBUG(GEOM, "Adding %u\n", additionalPoints);

	//create the coordinates sequence for the translated geometry
	if ((gcs_new = GEOSCoordSeq_create(pointsNum + additionalPoints, coordinatesNum)) == NULL) {
		*outGeometry = NULL;
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");
		goto bailout;
	}
	//add the first point
	if (!GEOSCoordSeq_setX(gcs_new, 0, xCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setX failed");
		GEOSCoordSeq_destroy(gcs_new);
		goto bailout;
	}
	if (!GEOSCoordSeq_setY(gcs_new, 0, yCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setY failed");
		GEOSCoordSeq_destroy(gcs_new);
		goto bailout;
	}
	if (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, 0, zCoords_org[0])) {
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setZ failed");
		GEOSCoordSeq_destroy(gcs_new);
		goto bailout;
	}

	xl = xCoords_org[0];
	yl = yCoords_org[0];
	zl = zCoords_org[0];

	//check and add the rest of the points
	for (i = 1; i < pointsNum; i++) {
		//compute the distance of the current point to the last added one
		double dist;
		while ((dist = sqrt(pow(xl - xCoords_org[i], 2) + pow(yl - yCoords_org[i], 2) + pow(zl - zCoords_org[i], 2))) > sz) {
			TRC_DEBUG(GEOM, "Old: (%f, %f, %f) vs (%f, %f, %f) = %f\n", xl, yl, zl, xCoords_org[i], yCoords_org[i], zCoords_org[i], dist);

			assert(j < additionalPoints);

			//compute intermediate point
			xl = xl + (xCoords_org[i] - xl) * sz / dist;
			yl = yl + (yCoords_org[i] - yl) * sz / dist;
			zl = zl + (zCoords_org[i] - zl) * sz / dist;

			//add the intermediate point
			if (!GEOSCoordSeq_setX(gcs_new, i + j, xl)) {
				err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setX failed");
				GEOSCoordSeq_destroy(gcs_new);
				goto bailout;
			}
			if (!GEOSCoordSeq_setY(gcs_new, i + j, yl)) {
				err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setY failed");
				GEOSCoordSeq_destroy(gcs_new);
				goto bailout;
			}
			if (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, i + j, zl)) {
				err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setZ failed");
				GEOSCoordSeq_destroy(gcs_new);
				goto bailout;
			}

			j++;
		}

		//add the original point
		if (!GEOSCoordSeq_setX(gcs_new, i + j, xCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setX failed");
			GEOSCoordSeq_destroy(gcs_new);
			goto bailout;
		}
		if (!GEOSCoordSeq_setY(gcs_new, i + j, yCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setY failed");
			GEOSCoordSeq_destroy(gcs_new);
			goto bailout;
		}
		if (coordinatesNum > 2 && !GEOSCoordSeq_setZ(gcs_new, i + j, zCoords_org[i])) {
			err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setZ failed");
			GEOSCoordSeq_destroy(gcs_new);
			goto bailout;
		}

		xl = xCoords_org[i];
		yl = yCoords_org[i];
		zl = zCoords_org[i];

	}

	//create the geometry from the translated coordinates sequence
	if (isRing)
		*outGeometry = GEOSGeom_createLinearRing(gcs_new);
	else
		*outGeometry = GEOSGeom_createLineString(gcs_new);

	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGeom_%s failed", isRing ? "LinearRing" : "LineString");
		GEOSCoordSeq_destroy(gcs_new);
	}

  bailout:
	GDKfree(xCoords_org);
	GDKfree(yCoords_org);
	GDKfree(zCoords_org);

	return err;
}

static str
segmentizePolygon(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str err;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (exteriorRingGeometry == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");
	}

	if ((err = segmentizeLineString(&transformedExteriorRingGeometry, exteriorRingGeometry, sz, 1)) != MAL_SUCCEED) {
		*outGeometry = NULL;
		return err;
	}

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed.");
	}
	//iterate over the interiorRing and segmentize each one of them
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
	if (transformedInteriorRingGeometries == NULL) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Segmentize", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	for (i = 0; i < numInteriorRings; i++) {
		if ((err = segmentizeLineString(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), sz, 1)) != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
			GDKfree(transformedInteriorRingGeometries);
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*outGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	GEOSGeom_destroy(transformedExteriorRingGeometry);

	return err;
}

static str segmentizeGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz);
static str
segmentizeMultiGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz)
{
	int geometriesNum, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	str err = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.Segmentize", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	//In order to have the geometries in the output in the same order as in the input
	//we should read them and put them in the area in reverse order
	for (i = geometriesNum - 1; i >= 0; i--) {
		const GEOSGeometry *multiGeometry = GEOSGetGeometryN(geosGeometry, i);

		if ((err = segmentizeGeometry(&transformedMultiGeometries[i], multiGeometry, sz)) != MAL_SUCCEED) {
			while (++i < geometriesNum)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createCollection(GEOSGeomTypeId(geosGeometry), transformedMultiGeometries, geometriesNum);
	if (*outGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		err = createException(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return err;
}

static str
segmentizeGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double sz)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return segmentizePoint(outGeometry, geosGeometry);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return segmentizeLineString(outGeometry, geosGeometry, sz, 0);
	case wkbPolygon_mdb:
		return segmentizePolygon(outGeometry, geosGeometry, sz);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return segmentizeMultiGeometry(outGeometry, geosGeometry, sz);
	default:
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) " Geos %s Unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbSegmentize(wkb **outWKB, wkb **geomWKB, dbl *sz)
{
	GEOSGeometry *outGeometry;
	GEOSGeom geosGeometry;
	str err;

	if (is_wkb_nil(*geomWKB) || is_dbl_nil(*sz)) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Segmentize", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if ((err = segmentizeGeometry(&outGeometry, geosGeometry, *sz)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		*outWKB = NULL;
		return err;
	}

	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));

	*outWKB = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.Segmentize", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

//gets a coord seq and moves it dx, dy, dz
static str
translateCoordSeq(int idx, int coordinatesNum, double dx, double dy, double dz, const GEOSCoordSequence *gcs_old, GEOSCoordSeq gcs_new)
{
	double x = 0, y = 0, z = 0;

	//get the coordinates
	if (!GEOSCoordSeq_getX(gcs_old, idx, &x))
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getX failed");
	if (!GEOSCoordSeq_getY(gcs_old, idx, &y))
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getY failed");
	if (coordinatesNum > 2)
		if (!GEOSCoordSeq_getZ(gcs_old, idx, &z))
			throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getZ failed");

	//create new coordinates moved by dx, dy, dz
	if (!GEOSCoordSeq_setX(gcs_new, idx, (x + dx)))
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setX failed");
	if (!GEOSCoordSeq_setY(gcs_new, idx, (y + dy)))
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setY failed");
	if (coordinatesNum > 2)
		if (!GEOSCoordSeq_setZ(gcs_new, idx, (z + dz)))
			throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setZ failed");

	return MAL_SUCCEED;
}

static str
translatePoint(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	str err;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
	}

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(1, coordinatesNum);
	if (gcs_new == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	if ((err = translateCoordSeq(0, coordinatesNum, dx, dy, dz, gcs_old, gcs_new)) != MAL_SUCCEED) {
		GEOSCoordSeq_destroy(gcs_new);
		*outGeometry = NULL;
		return err;
	}

	/* create the geometry from the coordinates sequence */
	*outGeometry = GEOSGeom_createPoint(gcs_new);
	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_createPoint failed");
		GEOSCoordSeq_destroy(gcs_new);
	}

	return err;
}

static str
translateLineString(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str err;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	GEOSCoordSeq_getSize(gcs_old, &pointsNum);

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	if (gcs_new == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		if ((err = translateCoordSeq(i, coordinatesNum, dx, dy, dz, gcs_old, gcs_new)) != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return err;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLineString(gcs_new);
	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
		GEOSCoordSeq_destroy(gcs_new);
	}

	return err;
}

//Necessary for composing a polygon from rings
static str
translateLinearRing(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int coordinatesNum = 0;
	const GEOSCoordSequence *gcs_old;
	GEOSCoordSeq gcs_new;
	unsigned int pointsNum = 0, i = 0;
	str err;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);

	if (gcs_old == NULL)
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	GEOSCoordSeq_getSize(gcs_old, &pointsNum);

	/* create the coordinates sequence for the translated geometry */
	gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	if (gcs_new == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");
	}

	/* create the translated coordinates */
	for (i = 0; i < pointsNum; i++) {
		if ((err = translateCoordSeq(i, coordinatesNum, dx, dy, dz, gcs_old, gcs_new)) != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(gcs_new);
			return err;
		}
	}

	//create the geometry from the translated coordinates sequence
	*outGeometry = GEOSGeom_createLinearRing(gcs_new);
	if (*outGeometry == NULL) {
		err = createException(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_createLinearRing failed");
		GEOSCoordSeq_destroy(gcs_new);
	}

	return err;
}

static str
translatePolygon(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	const GEOSGeometry *exteriorRingGeometry;
	GEOSGeometry *transformedExteriorRingGeometry = NULL;
	GEOSGeometry **transformedInteriorRingGeometries = NULL;
	int numInteriorRings = 0, i = 0;
	str err = MAL_SUCCEED;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (exteriorRingGeometry == NULL) {
		*outGeometry = NULL;
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");
	}

	if ((err = translateLinearRing(&transformedExteriorRingGeometry, exteriorRingGeometry, dx, dy, dz)) != MAL_SUCCEED) {
		*outGeometry = NULL;
		return err;
	}

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed.");
	}

	/* iterate over the interiorRing and translate each one of them */
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings * sizeof(GEOSGeometry *));
	if (transformedInteriorRingGeometries == NULL) {
		*outGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		throw(MAL, "geom.Translate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	for (i = 0; i < numInteriorRings; i++) {
		if ((err = translateLinearRing(&transformedInteriorRingGeometries[i], GEOSGetInteriorRingN(geosGeometry, i), dx, dy, dz)) != MAL_SUCCEED) {
			while (--i >= 0)
				GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
			GDKfree(transformedInteriorRingGeometries);
			GEOSGeom_destroy(transformedExteriorRingGeometry);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	if (*outGeometry == NULL) {
		for (i = 0; i < numInteriorRings; i++)
			GEOSGeom_destroy(transformedInteriorRingGeometries[i]);
		err = createException(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_createPolygon failed");
	}
	GDKfree(transformedInteriorRingGeometries);
	GEOSGeom_destroy(transformedExteriorRingGeometry);

	return err;
}

static str translateGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz);
static str
translateMultiGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int geometriesNum, i;
	GEOSGeometry **transformedMultiGeometries = NULL;
	str err = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	transformedMultiGeometries = GDKmalloc(geometriesNum * sizeof(GEOSGeometry *));
	if (transformedMultiGeometries == NULL)
		throw(MAL, "geom.Translate", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	//In order to have the geometries in the output in the same order as in the input
	//we should read them and put them in the area in reverse order
	for (i = geometriesNum - 1; i >= 0; i--) {
		const GEOSGeometry *multiGeometry = GEOSGetGeometryN(geosGeometry, i);

		if ((err = translateGeometry(&transformedMultiGeometries[i], multiGeometry, dx, dy, dz)) != MAL_SUCCEED) {
			while (i++ < geometriesNum)
				GEOSGeom_destroy(transformedMultiGeometries[i]);
			GDKfree(transformedMultiGeometries);
			*outGeometry = NULL;
			return err;
		}
	}

	*outGeometry = GEOSGeom_createCollection(GEOSGeomTypeId(geosGeometry), transformedMultiGeometries, geometriesNum);
	if (*outGeometry == NULL) {
		for (i = 0; i < geometriesNum; i++)
			GEOSGeom_destroy(transformedMultiGeometries[i]);
		err = createException(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation GEOSGeom_createCollection failed");
	}
	GDKfree(transformedMultiGeometries);

	return err;
}

static str
translateGeometry(GEOSGeometry **outGeometry, const GEOSGeometry *geosGeometry, double dx, double dy, double dz)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return translatePoint(outGeometry, geosGeometry, dx, dy, dz);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return translateLineString(outGeometry, geosGeometry, dx, dy, dz);
	case wkbPolygon_mdb:
		return translatePolygon(outGeometry, geosGeometry, dx, dy, dz);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return translateMultiGeometry(outGeometry, geosGeometry, dx, dy, dz);
	default:
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos %s unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbTranslate(wkb **outWKB, wkb **geomWKB, dbl *dx, dbl *dy, dbl *dz)
{
	GEOSGeometry *outGeometry;
	GEOSGeom geosGeometry;
	str err;

	if (is_wkb_nil(*geomWKB) || is_dbl_nil(*dx) || is_dbl_nil(*dy) || is_dbl_nil(*dz)) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Translate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if ((err = translateGeometry(&outGeometry, geosGeometry, *dx, *dy, *dz)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		*outWKB = NULL;
		return err;
	}

	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));

	*outWKB = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.Translate", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

//It creates a Delaunay triangulation
//flag = 0 => returns a collection of polygons
//flag = 1 => returns a multilinestring
str
wkbDelaunayTriangles(wkb **outWKB, wkb **geomWKB, dbl *tolerance, int *flag)
{
	GEOSGeom outGeometry;
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geomWKB) || is_dbl_nil(*tolerance) || is_int_nil(*flag)) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.DelaunayTriangles", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	outGeometry = GEOSDelaunayTriangulation(geosGeometry, *tolerance, *flag);
	GEOSGeom_destroy(geosGeometry);
	if (outGeometry == NULL) {
		*outWKB = NULL;
		throw(MAL, "geom.DelaunayTriangles", SQLSTATE(38000) "Geos operation GEOSDelaunayTriangulation failed");
	}

	*outWKB = geos2wkb(outGeometry);
	GEOSGeom_destroy(outGeometry);

	if (*outWKB == NULL)
		throw(MAL, "geom.DelaunayTriangles", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

str
wkbPointOnSurface(wkb **resWKB, wkb **geomWKB)
{
	GEOSGeom geosGeometry, resGeosGeometry;

	if (is_wkb_nil(*geomWKB)) {
		if ((*resWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.PointOnSurface", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*resWKB = NULL;
		throw(MAL, "geom.PointOnSurface", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	resGeosGeometry = GEOSPointOnSurface(geosGeometry);
	if (resGeosGeometry == NULL) {
		*resWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointOnSurface", SQLSTATE(38000) "Geos operation GEOSPointOnSurface failed");
	}
	//set the srid of the point the same as the srid of the input geometry
	GEOSSetSRID(resGeosGeometry, GEOSGetSRID(geosGeometry));

	*resWKB = geos2wkb(resGeosGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(resGeosGeometry);

	if (*resWKB == NULL)
		throw(MAL, "geom.PointOnSurface", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

static str
dumpGeometriesSingle(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, unsigned int *lvl, const char *path)
{
	char *newPath = NULL;
	size_t pathLength = strlen(path);
	wkb *singleWKB = geos2wkb(geosGeometry);
	str err = MAL_SUCCEED;

	if (singleWKB == NULL)
		throw(MAL, "geom.Dump", SQLSTATE(38000) "Geos operation geos2wkb failed");

	//change the path only if it is empty
	if (pathLength == 0) {
		const int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295

		(*lvl)++;

		newPath = GDKmalloc(lvlDigitsNum + 1);
		if (newPath == NULL) {
			GDKfree(singleWKB);
			throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		snprintf(newPath, lvlDigitsNum + 1, "%u", *lvl);
	} else {
		//remove the comma at the end of the path
#ifdef __COVERITY__
		/* coverity complains about the allocated space being
		 * too small, but we just want to reduce the length of
		 * the string by one, so the length in the #else part
		 * is exactly what we need */
		newPath = GDKmalloc(pathLength + 1);
#else
		newPath = GDKmalloc(pathLength);
#endif
		if (newPath == NULL) {
			GDKfree(singleWKB);
			throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		strcpy_len(newPath, path, pathLength);
	}
	if (BUNappend(idBAT, newPath, false) != GDK_SUCCEED ||
	    BUNappend(geomBAT, singleWKB, false) != GDK_SUCCEED)
		err = createException(MAL, "geom.Dump", SQLSTATE(38000) "Geos operation BUNappend failed");

	GDKfree(newPath);
	GDKfree(singleWKB);

	return err;
}

static str dumpGeometriesGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path);
static str
dumpGeometriesMulti(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int i;
	const GEOSGeometry *multiGeometry = NULL;
	unsigned int lvl = 0;
	size_t pathLength = strlen(path);
	char *newPath;
	str err = MAL_SUCCEED;

	int geometriesNum = GEOSGetNumGeometries(geosGeometry);

	pathLength += 10 + 1 + 1; /* 10 for lvl, 1 for ",", 1 for NULL byte */
	newPath = GDKmalloc(pathLength);
	if (newPath == NULL)
		throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < geometriesNum; i++) {
		multiGeometry = GEOSGetGeometryN(geosGeometry, i);
		if (multiGeometry == NULL) {
			err = createException(MAL, "geom.Dump", SQLSTATE(38000) "Geos operation GEOSGetGeometryN failed");
			break;
		}
		lvl++;

		snprintf(newPath, pathLength, "%s%u,", path, lvl);

		//*secondLevel = 0;
		err = dumpGeometriesGeometry(idBAT, geomBAT, multiGeometry, newPath);
		if (err != MAL_SUCCEED)
			break;
	}
	GDKfree(newPath);
	return err;
}

static str
dumpGeometriesGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;
	unsigned int lvl = 0;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
	case wkbPolygon_mdb:
		//Single Geometry
		return dumpGeometriesSingle(idBAT, geomBAT, geosGeometry, &lvl, path);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		//Multi Geometry
		//check if the geometry was empty
		if (GEOSisEmpty(geosGeometry) == 1) {
			str err;
			//handle it as single
			if ((err = dumpGeometriesSingle(idBAT, geomBAT, geosGeometry, &lvl, path)) != MAL_SUCCEED)
				return err;
		}

		return dumpGeometriesMulti(idBAT, geomBAT, geosGeometry, path);
	default:
		throw(MAL, "geom.Dump", SQLSTATE(38000) "Geos %s unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbDump(bat *idBAT_id, bat *geomBAT_id, wkb **geomWKB)
{
	BAT *idBAT = NULL, *geomBAT = NULL;
	GEOSGeom geosGeometry;
	unsigned int geometriesNum;
	str err;

	if (is_wkb_nil(*geomWKB)) {

		//create new empty BAT for the output
		if ((idBAT = COLnew(0, TYPE_str, 0, TRANSIENT)) == NULL) {
			*idBAT_id = bat_nil;
			throw(MAL, "geom.DumpPoints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		if ((geomBAT = COLnew(0, ATOMindex("wkb"), 0, TRANSIENT)) == NULL) {
			BBPunfix(idBAT->batCacheid);
			*geomBAT_id = bat_nil;
			throw(MAL, "geom.DumpPoints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		*idBAT_id = idBAT->batCacheid;
		BBPkeepref(idBAT);

		*geomBAT_id = geomBAT->batCacheid;
		BBPkeepref(geomBAT);

		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);

	//count the number of geometries
	geometriesNum = GEOSGetNumGeometries(geosGeometry);

	if ((idBAT = COLnew(0, TYPE_str, geometriesNum, TRANSIENT)) == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if ((geomBAT = COLnew(0, ATOMindex("wkb"), geometriesNum, TRANSIENT)) == NULL) {
		BBPunfix(idBAT->batCacheid);
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	err = dumpGeometriesGeometry(idBAT, geomBAT, geosGeometry, "");
	GEOSGeom_destroy(geosGeometry);
	if (err != MAL_SUCCEED) {
		BBPunfix(idBAT->batCacheid);
		BBPunfix(geomBAT->batCacheid);
		return err;
	}

	*idBAT_id = idBAT->batCacheid;
	BBPkeepref(idBAT);
	*geomBAT_id = geomBAT->batCacheid;
	BBPkeepref(geomBAT);
	return MAL_SUCCEED;
}

static str
dumpPointsPoint(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, unsigned int *lvl, const char *path)
{
	char *newPath = NULL;
	size_t pathLength = strlen(path);
	wkb *pointWKB = geos2wkb(geosGeometry);
	const int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295
	str err = MAL_SUCCEED;

	if (pointWKB == NULL)
		throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	(*lvl)++;
	newPath = GDKmalloc(pathLength + lvlDigitsNum + 1);
	if (newPath == NULL) {
		GDKfree(pointWKB);
		throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	sprintf(newPath, "%s%u", path, *lvl);

	if (BUNappend(idBAT, newPath, false) != GDK_SUCCEED ||
	    BUNappend(geomBAT, pointWKB, false) != GDK_SUCCEED)
		err = createException(MAL, "geom.Dump", SQLSTATE(38000) "Geos operation BUNappend failed");

	GDKfree(newPath);
	GDKfree(pointWKB);

	return err;
}

static str
dumpPointsLineString(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int pointsNum = 0;
	str err;
	int i = 0;
	int check = 0;
	unsigned int lvl = 0;
	wkb *geomWKB = geos2wkb(geosGeometry);

	err = wkbNumPoints(&pointsNum, &geomWKB, &check);
	GDKfree(geomWKB);
	if (err != MAL_SUCCEED)
		return err;

	for (i = 0; i < pointsNum && err == MAL_SUCCEED; i++) {
		GEOSGeometry *pointGeometry = GEOSGeomGetPointN(geosGeometry, i);

		if (pointGeometry == NULL)
			throw(MAL, "geom.DumpPoints", SQLSTATE(38000) "Geos operation GEOSGeomGetPointN failed");

		err = dumpPointsPoint(idBAT, geomBAT, pointGeometry, &lvl, path);
		GEOSGeom_destroy(pointGeometry);
	}

	return err;
}

static str
dumpPointsPolygon(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, unsigned int *lvl, const char *path)
{
	const GEOSGeometry *exteriorRingGeometry;
	int numInteriorRings = 0, i = 0;
	str err;
	const int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295
	size_t pathLength = strlen(path);
	char *newPath;
	char *extraStr = ",";
	int extraLength = 1;

	//get the exterior ring of the polygon
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (exteriorRingGeometry == NULL)
		throw(MAL, "geom.DumpPoints", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");

	(*lvl)++;
	newPath = GDKmalloc(pathLength + lvlDigitsNum + extraLength + 1);
	if (newPath == NULL)
		throw(MAL, "geom.DumpPoints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	snprintf(newPath, pathLength + lvlDigitsNum + extraLength + 1,
			 "%s%u%s", path, *lvl, extraStr);

	//get the points in the exterior ring
	err = dumpPointsLineString(idBAT, geomBAT, exteriorRingGeometry, newPath);
	GDKfree(newPath);
	if (err != MAL_SUCCEED)
		return err;

	//check the interior rings
	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1)
		throw(MAL, "geom.DumpPoints", SQLSTATE(38000) "Geos operation GEOSGetNumInteriorRings failed");

	// iterate over the interiorRing and transform each one of them
	for (i = 0; i < numInteriorRings; i++) {
		(*lvl)++;

		newPath = GDKmalloc(pathLength + lvlDigitsNum + extraLength + 1);
		if (newPath == NULL)
			throw(MAL, "geom.DumpPoints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(newPath, pathLength + lvlDigitsNum + extraLength + 1,
				 "%s%u%s", path, *lvl, extraStr);

		err = dumpPointsLineString(idBAT, geomBAT, GEOSGetInteriorRingN(geosGeometry, i), newPath);
		GDKfree(newPath);
		if (err != MAL_SUCCEED)
			return err;
	}

	return MAL_SUCCEED;
}

static str dumpPointsGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path);
static str
dumpPointsMultiGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int geometriesNum, i;
	const GEOSGeometry *multiGeometry = NULL;
	str err;
	unsigned int lvl = 0;
	size_t pathLength = strlen(path);
	char *newPath = NULL;
	char *extraStr = ",";
	int extraLength = 1;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);

	for (i = 0; i < geometriesNum; i++) {
		const int lvlDigitsNum = 10;	//MAX_UNIT = 4,294,967,295

		multiGeometry = GEOSGetGeometryN(geosGeometry, i);
		lvl++;

		newPath = GDKmalloc(pathLength + lvlDigitsNum + extraLength + 1);
		if (newPath == NULL)
			throw(MAL, "geom.DumpPoints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(newPath, pathLength + lvlDigitsNum + extraLength + 1,
				 "%s%u%s", path, lvl, extraStr);

		//*secondLevel = 0;
		err = dumpPointsGeometry(idBAT, geomBAT, multiGeometry, newPath);
		GDKfree(newPath);
		if (err != MAL_SUCCEED)
			return err;
	}

	return MAL_SUCCEED;
}

static str
dumpPointsGeometry(BAT *idBAT, BAT *geomBAT, const GEOSGeometry *geosGeometry, const char *path)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;
	unsigned int lvl = 0;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
		return dumpPointsPoint(idBAT, geomBAT, geosGeometry, &lvl, path);
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return dumpPointsLineString(idBAT, geomBAT, geosGeometry, path);
	case wkbPolygon_mdb:
		return dumpPointsPolygon(idBAT, geomBAT, geosGeometry, &lvl, path);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return dumpPointsMultiGeometry(idBAT, geomBAT, geosGeometry, path);
	default:
		throw(MAL, "geom.DumpPoints", SQLSTATE(38000) "Geoes %s unknown geometry type", geom_type2str(geometryType, 0));
	}
}

str
wkbDumpPoints(bat *idBAT_id, bat *geomBAT_id, wkb **geomWKB)
{
	BAT *idBAT = NULL, *geomBAT = NULL;
	GEOSGeom geosGeometry;
	int check = 0;
	int pointsNum;
	str err;

	if (is_wkb_nil(*geomWKB)) {

		//create new empty BAT for the output
		if ((idBAT = COLnew(0, TYPE_str, 0, TRANSIENT)) == NULL) {
			*idBAT_id = int_nil;
			throw(MAL, "geom.DumpPoints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		if ((geomBAT = COLnew(0, ATOMindex("wkb"), 0, TRANSIENT)) == NULL) {
			BBPunfix(idBAT->batCacheid);
			*geomBAT_id = int_nil;
			throw(MAL, "geom.DumpPoints", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}

		*idBAT_id = idBAT->batCacheid;
		BBPkeepref(idBAT);

		*geomBAT_id = geomBAT->batCacheid;
		BBPkeepref(geomBAT);

		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);

	if ((err = wkbNumPoints(&pointsNum, geomWKB, &check)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosGeometry);
		return err;
	}

	if ((idBAT = COLnew(0, TYPE_str, pointsNum, TRANSIENT)) == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if ((geomBAT = COLnew(0, ATOMindex("wkb"), pointsNum, TRANSIENT)) == NULL) {
		BBPunfix(idBAT->batCacheid);
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	err = dumpPointsGeometry(idBAT, geomBAT, geosGeometry, "");
	GEOSGeom_destroy(geosGeometry);
	if (err != MAL_SUCCEED) {
		BBPunfix(idBAT->batCacheid);
		BBPunfix(geomBAT->batCacheid);
		return err;
	}

	*idBAT_id = idBAT->batCacheid;
	BBPkeepref(idBAT);
	*geomBAT_id = geomBAT->batCacheid;
	BBPkeepref(geomBAT);
	return MAL_SUCCEED;
}

str
geom_2_geom(wkb **resWKB, wkb **valueWKB, int *columnType, int *columnSRID)
{
	GEOSGeom geosGeometry;
	int geoCoordinatesNum = 2;
	int valueType = 0;

	int valueSRID = (*valueWKB)->srid;

	if (is_wkb_nil(*valueWKB) || is_int_nil(*columnType) || is_int_nil(*columnSRID)) {
		*resWKB = wkbNULLcopy();
		if (*resWKB == NULL)
			throw(MAL, "calc.wkb", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	/* get the geosGeometry from the wkb */
	geosGeometry = wkb2geos(*valueWKB);
	if (geosGeometry == NULL)
		throw(MAL, "calc.wkb", SQLSTATE(38000) "Geos operation wkb2geos failed");

	/* get the number of coordinates the geometry has */
	geoCoordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the type of the geometry */
	valueType = (GEOSGeomTypeId(geosGeometry) + 1) << 2;

	if (geoCoordinatesNum > 2)
		valueType += (1 << 1);
	if (geoCoordinatesNum > 3)
		valueType += 1;

	if (valueSRID != *columnSRID || valueType != *columnType) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "calc.wkb", SQLSTATE(38000) "Geos column needs geometry(%d, %d) and value is geometry(%d, %d)\n", *columnType, *columnSRID, valueType, valueSRID);
	}

	/* get the wkb from the geosGeometry */
	*resWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (*resWKB == NULL)
		throw(MAL, "calc.wkb", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

/*check if the geometry has z coordinate*/
str
geoHasZ(int *res, int *info)
{
	if (is_int_nil(*info))
		*res = int_nil;
	else if (geometryHasZ(*info))
		*res = 1;
	else
		*res = 0;
	return MAL_SUCCEED;

}

/*check if the geometry has m coordinate*/
str
geoHasM(int *res, int *info)
{
	if (is_int_nil(*info))
		*res = int_nil;
	else if (geometryHasM(*info))
		*res = 1;
	else
		*res = 0;
	return MAL_SUCCEED;
}

/*check the geometry subtype*/
/*returns the length of the resulting string*/
str
geoGetType(char **res, int *info, int *flag)
{
	if (is_int_nil(*info) || is_int_nil(*flag)) {
		if ((*res = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.getType", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((*res = GDKstrdup(geom_type2str(*info >> 2, *flag))) == NULL)
		throw(MAL, "geom.getType", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* initialize geos */

static str
geom_prelude(void)
{
	mbrNIL.xmin = flt_nil;
	mbrNIL.xmax = flt_nil;
	mbrNIL.ymin = flt_nil;
	mbrNIL.ymax = flt_nil;
	libgeom_init();
	TYPE_mbr = malAtomSize(sizeof(mbr), "mbr");

	return MAL_SUCCEED;
}

/* clean geos */
str
geom_epilogue(void *ret)
{
	(void) ret;
	libgeom_exit();
	return MAL_SUCCEED;
}

/* create the WKB out of the GEOSGeometry
 * It makes sure to make all checks before returning
 * the input geosGeometry should not be altered by this function
 * return NULL on error */
wkb *
geos2wkb(const GEOSGeometry *geosGeometry)
{
	size_t wkbLen = 0;
	unsigned char *w = NULL;
	wkb *geomWKB;

	// if the geosGeometry is NULL create a NULL WKB
	if (geosGeometry == NULL) {
		return wkbNULLcopy();
	}

	GEOS_setWKBOutputDims(GEOSGeom_getCoordinateDimension(geosGeometry));
	w = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);

	if (w == NULL)
		return NULL;

	assert(wkbLen <= GDK_int_max);

	geomWKB = GDKmalloc(wkb_size(wkbLen));
	//If malloc failed create a NULL wkb
	if (geomWKB == NULL) {
		GEOSFree(w);
		return NULL;
	}

	geomWKB->len = (int) wkbLen;
	geomWKB->srid = GEOSGetSRID(geosGeometry);
	memcpy(&geomWKB->data, w, wkbLen);
	GEOSFree(w);

	return geomWKB;
}

/* gets the mbr from the geometry */
mbr *
mbrFromGeos(const GEOSGeom geosGeometry)
{
	GEOSGeom envelope;
	mbr *geomMBR;
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;

	geomMBR = GDKmalloc(sizeof(mbr));
	if (geomMBR == NULL)	//problem in reserving space
		return NULL;

	/* if input is null or GEOSEnvelope created exception then create a nill mbr */
	if (!geosGeometry || (envelope = GEOSEnvelope(geosGeometry)) == NULL) {
		*geomMBR = mbrNIL;
		return geomMBR;
	}

	if ((GEOSGeomTypeId(envelope) + 1) == wkbPoint_mdb) {
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
		const GEOSCoordSequence *coords = GEOSGeom_getCoordSeq(envelope);
#else
		const GEOSCoordSeq coords = GEOSGeom_getCoordSeq(envelope);
#endif
		GEOSCoordSeq_getX(coords, 0, &xmin);
		GEOSCoordSeq_getY(coords, 0, &ymin);
		assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
		assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
		geomMBR->xmin = (float) xmin;
		geomMBR->ymin = (float) ymin;
		geomMBR->xmax = (float) xmin;
		geomMBR->ymax = (float) ymin;
	} else {		// GEOSGeomTypeId(envelope) == GEOS_POLYGON
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
		const GEOSGeometry *ring = GEOSGetExteriorRing(envelope);
#else
		const GEOSGeom ring = GEOSGetExteriorRing(envelope);
#endif
		if (ring) {
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
			const GEOSCoordSequence *coords = GEOSGeom_getCoordSeq(ring);
#else
			const GEOSCoordSeq coords = GEOSGeom_getCoordSeq(ring);
#endif
			GEOSCoordSeq_getX(coords, 0, &xmin);	//left-lower corner
			GEOSCoordSeq_getY(coords, 0, &ymin);
			GEOSCoordSeq_getX(coords, 2, &xmax);	//right-upper corner
			GEOSCoordSeq_getY(coords, 2, &ymax);
			assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
			assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
			assert(GDK_flt_min <= xmax && xmax <= GDK_flt_max);
			assert(GDK_flt_min <= ymax && ymax <= GDK_flt_max);
			geomMBR->xmin = (float) xmin;
			geomMBR->ymin = (float) ymin;
			geomMBR->xmax = (float) xmax;
			geomMBR->ymax = (float) ymax;
		}
	}
	GEOSGeom_destroy(envelope);
	return geomMBR;
}

//Returns the wkb in a hex representation */
static char hexit[] = "0123456789ABCDEF";

str
wkbAsBinary(char **toStr, wkb **geomWKB)
{
	char *s;
	int i;

	if (is_wkb_nil(*geomWKB)) {
		if ((*toStr = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.AsBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((*toStr = GDKmalloc(1 + (*geomWKB)->len * 2)) == NULL)
		throw(MAL, "geom.AsBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	s = *toStr;
	for (i = 0; i < (*geomWKB)->len; i++) {
		int val = ((*geomWKB)->data[i] >> 4) & 0xf;
		*s++ = hexit[val];
		val = (*geomWKB)->data[i] & 0xf;
		*s++ = hexit[val];
		TRC_DEBUG(GEOM, "%d: First: %c - Second: %c ==> Original %c (%d)\n", i, *(s-2), *(s-1), (*geomWKB)->data[i], (int)((*geomWKB)->data[i]));
	}
	*s = '\0';
	return MAL_SUCCEED;
}

static int
decit(char hex)
{
	switch (hex) {
	case '0':
		return 0;
	case '1':
		return 1;
	case '2':
		return 2;
	case '3':
		return 3;
	case '4':
		return 4;
	case '5':
		return 5;
	case '6':
		return 6;
	case '7':
		return 7;
	case '8':
		return 8;
	case '9':
		return 9;
	case 'A':
	case 'a':
		return 10;
	case 'B':
	case 'b':
		return 11;
	case 'C':
	case 'c':
		return 12;
	case 'D':
	case 'd':
		return 13;
	case 'E':
	case 'e':
		return 14;
	case 'F':
	case 'f':
		return 15;
	default:
		return -1;
	}
}

str
wkbFromBinary(wkb **geomWKB, const char **inStr)
{
	size_t strLength, wkbLength, i;
	wkb *w;

	if (strNil(*inStr)) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.FromBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	strLength = strlen(*inStr);
	if (strLength & 1)
		throw(MAL, "geom.FromBinary", SQLSTATE(38000) "Geos odd length input string");

	wkbLength = strLength / 2;
	assert(wkbLength <= GDK_int_max);

	w = GDKmalloc(wkb_size(wkbLength));
	if (w == NULL)
		throw(MAL, "geom.FromBinary", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	//compute the value for s
	for (i = 0; i < strLength; i += 2) {
		int firstHalf = decit((*inStr)[i]);
		int secondHalf = decit((*inStr)[i + 1]);
		if (firstHalf == -1 || secondHalf == -1) {
			GDKfree(w);
			throw(MAL, "geom.FromBinary", SQLSTATE(38000) "Geos incorrectly formatted input string");
		}
		w->data[i / 2] = (firstHalf << 4) | secondHalf;
	}

	w->len = (int) wkbLength;
	w->srid = 0;
	*geomWKB = w;

	return MAL_SUCCEED;
}

str
mbrFromMBR(mbr **w, mbr **src)
{
	*w = GDKmalloc(sizeof(mbr));
	if (*w == NULL)
		throw(MAL, "calc.mbr", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	**w = **src;
	return MAL_SUCCEED;
}

str
wkbFromWKB(wkb **w, wkb **src)
{
	*w = GDKmalloc(wkb_size((*src)->len));
	if (*w == NULL)
		throw(MAL, "calc.wkb", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (is_wkb_nil(*src)) {
		**w = wkb_nil;
	} else {
		(*w)->len = (*src)->len;
		(*w)->srid = (*src)->srid;
		memcpy((*w)->data, (*src)->data, (*src)->len);
	}
	return MAL_SUCCEED;
}

/* creates a wkb from the given textual representation */
/* int* tpe is needed to verify that the type of the FromText function used is the
 * same with the type of the geometry created from the wkt representation */
str
wkbFromText(wkb **geomWKB, str *geomWKT, int *srid, int *tpe)
{
	size_t len = 0;
	int te = 0;
	str err;
	size_t parsedBytes;

	*geomWKB = NULL;
	if (strNil(*geomWKT) || is_int_nil(*srid) || is_int_nil(*tpe)) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "wkb.FromText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	err = wkbFROMSTR_withSRID(*geomWKT, &len, geomWKB, *srid, &parsedBytes);
	if (err != MAL_SUCCEED)
		return err;

	if (is_wkb_nil(*geomWKB) || *tpe == 0 ||
	    *tpe == wkbGeometryCollection_mdb ||
	    ((te = *((*geomWKB)->data + 1) & 0x0f) + (*tpe > 2)) == *tpe) {
		return MAL_SUCCEED;
	}

	GDKfree(*geomWKB);
	*geomWKB = NULL;

	te += (te > 2);
	if (*tpe > 0 && te != *tpe)
		throw(SQL, "wkb.FromText", SQLSTATE(38000) "Geometry not type '%d: %s' but '%d: %s' instead", *tpe, geom_type2str(*tpe, 0), te, geom_type2str(te, 0));
	throw(MAL, "wkb.FromText", SQLSTATE(38000) "%s", "cannot parse string");
}

/* create textual representation of the wkb */
str
wkbAsText(char **txt, wkb **geomWKB, int *withSRID)
{
	size_t len = 0;
	char *wkt = NULL;
	const char *sridTxt = "SRID:";

	if (is_wkb_nil(*geomWKB) || (withSRID && is_int_nil(*withSRID))) {
		if ((*txt = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.AsText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if ((*geomWKB)->srid < 0)
		throw(MAL, "geom.AsText", SQLSTATE(38000) "Geod negative SRID");

	if (wkbTOSTR(&wkt, &len, *geomWKB, false) < 0)
		throw(MAL, "geom.AsText", SQLSTATE(38000) "Geos failed to create Text from Well Known Format");

	if (withSRID == NULL || *withSRID == 0) {	//accepting NULL withSRID to make internal use of it easier
		*txt = wkt;
		return MAL_SUCCEED;
	}

	/* 10 for maximum number of digits to represent an INT */
	len = strlen(wkt) + 10 + strlen(sridTxt) + 2;
	*txt = GDKmalloc(len);
	if (*txt == NULL) {
		GDKfree(wkt);
		throw(MAL, "geom.AsText", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	snprintf(*txt, len, "%s%d;%s", sridTxt, (*geomWKB)->srid, wkt);

	GDKfree(wkt);
	return MAL_SUCCEED;
}

str
wkbMLineStringToPolygon(wkb **geomWKB, str *geomWKT, int *srid, int *flag)
{
	int itemsNum = 0, i, type = wkbMultiLineString_mdb;
	str ret = MAL_SUCCEED;
	wkb *inputWKB = NULL;

	wkb **linestringsWKB;
	double *linestringsArea;
	bit ordered = 0;

	if (strNil(*geomWKT) || is_int_nil(*srid) || is_int_nil(*flag)) {
		if ((*geomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MLineStringToPolygon", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	*geomWKB = NULL;

	//make wkb from wkt
	ret = wkbFromText(&inputWKB, geomWKT, srid, &type);
	if (ret != MAL_SUCCEED)
		return ret;

	//read the number of linestrings in the input
	ret = wkbNumGeometries(&itemsNum, &inputWKB);
	if (ret != MAL_SUCCEED) {
		GDKfree(inputWKB);
		return ret;
	}

	linestringsWKB = GDKmalloc(itemsNum * sizeof(wkb *));
	linestringsArea = GDKmalloc(itemsNum * sizeof(double));
	if (linestringsWKB == NULL || linestringsArea == NULL) {
		itemsNum = 0;
		ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	//create one polygon for each lineString and compute the area of each of them
	for (i = 1; i <= itemsNum; i++) {
		wkb *polygonWKB;

		ret = wkbGeometryN(&linestringsWKB[i - 1], &inputWKB, &i);
		if (ret != MAL_SUCCEED || linestringsWKB[i - 1] == NULL) {
			itemsNum = i - 1;
			goto bailout;
		}

		ret = wkbMakePolygon(&polygonWKB, &linestringsWKB[i - 1], NULL, srid);
		if (ret != MAL_SUCCEED) {
			itemsNum = i;
			goto bailout;
		}

		ret = wkbArea(&linestringsArea[i - 1], &polygonWKB);
		GDKfree(polygonWKB);
		if (ret != MAL_SUCCEED) {
			itemsNum = i;
			goto bailout;
		}
	}

	GDKfree(inputWKB);
	inputWKB = NULL;

	//order the linestrings with decreasing (polygons) area
	while (!ordered) {
		ordered = 1;

		for (i = 0; i < itemsNum - 1; i++) {
			if (linestringsArea[i + 1] > linestringsArea[i]) {
				//switch
				wkb *linestringWKB = linestringsWKB[i];
				double linestringArea = linestringsArea[i];

				linestringsWKB[i] = linestringsWKB[i + 1];
				linestringsArea[i] = linestringsArea[i + 1];

				linestringsWKB[i + 1] = linestringWKB;
				linestringsArea[i + 1] = linestringArea;

				ordered = 0;
			}
		}
	}

	if (*flag == 0) {
		//the biggest polygon is the external shell
		GEOSCoordSeq coordSeq_external;
		GEOSGeom externalGeometry, linearRingExternalGeometry, *internalGeometries, finalGeometry;

		externalGeometry = wkb2geos(linestringsWKB[0]);
		if (externalGeometry == NULL) {
			ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos operation wkb2geos failed");
			goto bailout;
		}

		coordSeq_external = GEOSCoordSeq_clone(GEOSGeom_getCoordSeq(externalGeometry));
		GEOSGeom_destroy(externalGeometry);
		if (coordSeq_external == NULL) {
			ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos operation GEOSCoordSeq_clone failed");
			goto bailout;
		}
		linearRingExternalGeometry = GEOSGeom_createLinearRing(coordSeq_external);
		if (linearRingExternalGeometry == NULL) {
			GEOSCoordSeq_destroy(coordSeq_external);
			ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos operation GEOSGeom_createLinearRing failed");
			goto bailout;
		}

		//all remaining should be internal
		internalGeometries = GDKmalloc((itemsNum - 1) * sizeof(GEOSGeom));
		if (internalGeometries == NULL) {
			GEOSGeom_destroy(linearRingExternalGeometry);
			ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		for (i = 1; i < itemsNum; i++) {
			GEOSCoordSeq coordSeq_internal;
			GEOSGeom internalGeometry;

			internalGeometry = wkb2geos(linestringsWKB[i]);
			if (internalGeometry == NULL) {
				GEOSGeom_destroy(linearRingExternalGeometry);
				while (--i >= 1)
					GEOSGeom_destroy(internalGeometries[i - 1]);
				GDKfree(internalGeometries);
				ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos operation wkb2geos failed");
				goto bailout;
			}

			coordSeq_internal = GEOSCoordSeq_clone(GEOSGeom_getCoordSeq(internalGeometry));
			GEOSGeom_destroy(internalGeometry);
			if (coordSeq_internal == NULL) {
				GEOSGeom_destroy(linearRingExternalGeometry);
				while (--i >= 1)
					GEOSGeom_destroy(internalGeometries[i - 1]);
				GDKfree(internalGeometries);
				ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos operation wkb2geos failed");
				goto bailout;
			}
			internalGeometries[i - 1] = GEOSGeom_createLinearRing(coordSeq_internal);
			if (internalGeometries[i - 1] == NULL) {
				GEOSGeom_destroy(linearRingExternalGeometry);
				GEOSCoordSeq_destroy(coordSeq_internal);
				while (--i >= 1)
					GEOSGeom_destroy(internalGeometries[i - 1]);
				GDKfree(internalGeometries);
				ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos operation GEOSGeom_createLinearRing failed");
				goto bailout;
			}
		}

		finalGeometry = GEOSGeom_createPolygon(linearRingExternalGeometry, internalGeometries, itemsNum - 1);
		GEOSGeom_destroy(linearRingExternalGeometry);
		if (finalGeometry == NULL) {
			for (i = 0; i < itemsNum - 1; i++)
				GEOSGeom_destroy(internalGeometries[i]);
			GDKfree(internalGeometries);
			ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos error creating Polygon from LinearRing");
			goto bailout;
		}
		GDKfree(internalGeometries);
		//check of the created polygon is valid
		if (GEOSisValid(finalGeometry) != 1) {
			//suppress the GEOS message
			GDKclrerr();

			GEOSGeom_destroy(finalGeometry);

			ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos the provided MultiLineString does not create a valid Polygon");
			goto bailout;
		}

		GEOSSetSRID(finalGeometry, *srid);
		*geomWKB = geos2wkb(finalGeometry);
		GEOSGeom_destroy(finalGeometry);
		if (*geomWKB == NULL)
			ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos operation geos2wkb failed");
	} else if (*flag == 1) {
		ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos multipolygon from string has not been defined");
	} else {
		ret = createException(MAL, "geom.MLineStringToPolygon", SQLSTATE(38000) "Geos unknown flag");
	}

  bailout:
	GDKfree(inputWKB);
	for (i = 0; i < itemsNum; i++)
		GDKfree(linestringsWKB[i]);
	GDKfree(linestringsWKB);
	GDKfree(linestringsArea);
	return ret;
}

str
wkbMakePoint(wkb **out, dbl *x, dbl *y, dbl *z, dbl *m, int *zmFlag)
{
	GEOSGeom geosGeometry;
	GEOSCoordSeq seq;

	if (is_dbl_nil(*x) || is_dbl_nil(*y) || is_dbl_nil(*z) || is_dbl_nil(*m) || is_int_nil(*zmFlag)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakePoint", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	//create the point from the coordinates
	switch (*zmFlag) {
	case 0:			/* x, y */
		seq = GEOSCoordSeq_create(1, 2);
		break;
	case 1:			/* x, y, m */
	case 10:		/* x, y, z */
		seq = GEOSCoordSeq_create(1, 3);
		break;
	case 11:		/* x, y, z, m */
		throw(MAL, "geom.MakePoint", SQLSTATE(38000) "Geos POINTZM is not supported");
	default:
		throw(MAL, "geom.MakePoint", SQLSTATE(38000) "Geos "ILLEGAL_ARGUMENT);
	}

	if (seq == NULL)
		throw(MAL, "geom.MakePoint", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");

	if (!GEOSCoordSeq_setOrdinate(seq, 0, 0, *x) ||
	    !GEOSCoordSeq_setOrdinate(seq, 0, 1, *y) ||
	    (*zmFlag == 1 && !GEOSCoordSeq_setOrdinate(seq, 0, 2, *m)) ||
	    (*zmFlag == 10 && !GEOSCoordSeq_setOrdinate(seq, 0, 2, *z))) {
		GEOSCoordSeq_destroy(seq);
		throw(MAL, "geom.MakePoint", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setOrdinate failed");
	}

	if ((geosGeometry = GEOSGeom_createPoint(seq)) == NULL) {
		GEOSCoordSeq_destroy(seq);
		throw(MAL, "geom.MakePoint", SQLSTATE(38000) "Geos opertion GEOSGeometry failed");
	}

	*out = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (is_wkb_nil(*out)) {
		GDKfree(*out);
		*out = NULL;
		throw(MAL, "geom.MakePoint", SQLSTATE(38000) "Geos to create WKB from GEOSGeometry failed");
	}

	return MAL_SUCCEED;
}

/* common code for functions that return integer */
static str
wkbBasicInt(int *out, wkb *geom, int (*func) (const GEOSGeometry *), const char *name)
{
	GEOSGeom geosGeometry;
	str ret = MAL_SUCCEED;

	if (is_wkb_nil(geom)) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	if ((geosGeometry = wkb2geos(geom)) == NULL)
		throw(MAL, name, SQLSTATE(38000) "Geos operation wkb2geos failed");

	*out = (*func) (geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	//if there was an error returned by geos
	if (GDKerrbuf && GDKerrbuf[0]) {
		//create an exception with this name
		ret = createException(MAL, name, SQLSTATE(38000) "Geos operation %s", GDKerrbuf);

		//clear the error buffer
		GDKclrerr();
	}

	return ret;
}

/* returns the type of the geometry as a string*/
str
wkbGeometryType(char **out, wkb **geomWKB, int *flag)
{
	int typeId = 0;
	str ret = MAL_SUCCEED;

	ret = wkbBasicInt(&typeId, *geomWKB, GEOSGeomTypeId, "geom.GeometryType");
	if (ret != MAL_SUCCEED)
		return ret;
	if (!is_int_nil(typeId))	/* geoGetType deals with nil */
		typeId = (typeId + 1) << 2;
	return geoGetType(out, &typeId, flag);
}

/* returns the number of dimensions of the geometry */
str
wkbCoordDim(int *out, wkb **geom)
{
	return wkbBasicInt(out, *geom, GEOSGeom_getCoordinateDimension, "geom.CoordDim");
}

/* returns the inherent dimension of the geometry, e.g 0 for point */
str
wkbDimension(int *dimension, wkb **geomWKB)
{
	return wkbBasicInt(dimension, *geomWKB, GEOSGeom_getDimensions, "geom.Dimension");
}

/* returns the srid of the geometry */
str
wkbGetSRID(int *out, wkb **geomWKB)
{
	return wkbBasicInt(out, *geomWKB, GEOSGetSRID, "geom.GetSRID");
}

/* sets the srid of the geometry */
str
wkbSetSRID(wkb **resultGeomWKB, wkb **geomWKB, int *srid)
{
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geomWKB) || is_int_nil(*srid)) {
		if ((*resultGeomWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.setSRID", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((geosGeometry = wkb2geos(*geomWKB)) == NULL)
		throw(MAL, "geom.setSRID", SQLSTATE(38000) "Geos operation wkb2geos failed");

	GEOSSetSRID(geosGeometry, *srid);
	*resultGeomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (*resultGeomWKB == NULL)
		throw(MAL, "geom.setSRID", SQLSTATE(38000) "Geos operation geos2wkb failed");

	return MAL_SUCCEED;
}

/* depending on the specific function it returns the X,Y or Z coordinate of a point */
str
wkbGetCoordinate(dbl *out, wkb **geom, int *dimNum)
{
	GEOSGeom geosGeometry;
	const GEOSCoordSequence *gcs;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*geom) || is_int_nil(*dimNum)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = dbl_nil;
		throw(MAL, "geom.GetCoordinate", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if ((GEOSGeomTypeId(geosGeometry) + 1) != wkbPoint_mdb) {
		char *geomSTR;

		GEOSGeom_destroy(geosGeometry);
		if ((err = wkbAsText(&geomSTR, geom, NULL)) != MAL_SUCCEED)
			return err;
		err = createException(MAL, "geom.GetCoordinate", SQLSTATE(38000) "Geometry \"%s\" not a Point", geomSTR);
		GDKfree(geomSTR);
		return err;
	}

	gcs = GEOSGeom_getCoordSeq(geosGeometry);
	/* gcs shouldn't be freed, it's internal to the GEOSGeom */

	if (gcs == NULL) {
		err = createException(MAL, "geom.GetCoordinate", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
	} else if (!GEOSCoordSeq_getOrdinate(gcs, 0, *dimNum, out))
		err = createException(MAL, "geom.GetCoordinate", SQLSTATE(38000) "Geos operation GEOSCoordSeq_getOrdinate failed");
	else if (isnan(*out))
		*out = dbl_nil;
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/*common code for functions that return geometry */
static str
wkbBasic(wkb **out, wkb **geom, GEOSGeometry *(*func) (const GEOSGeometry *), const char *name)
{
	GEOSGeom geosGeometry, outGeometry;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((geosGeometry = wkb2geos(*geom)) == NULL) {
		*out = NULL;
		throw(MAL, name, SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if ((outGeometry = (*func) (geosGeometry)) == NULL) {
		err = createException(MAL, name, SQLSTATE(38000) "Geos operation GEOS%s failed", name + 5);
	} else {
		//set the srid equal to the srid of the initial geometry
		if ((*geom)->srid)	//GEOSSetSRID has assertion for srid != 0
			GEOSSetSRID(outGeometry, (*geom)->srid);

		if ((*out = geos2wkb(outGeometry)) == NULL)
			err = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);

		GEOSGeom_destroy(outGeometry);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

str
wkbBoundary(wkb **boundaryWKB, wkb **geomWKB)
{
	return wkbBasic(boundaryWKB, geomWKB, GEOSBoundary, "geom.Boundary");
}

str
wkbEnvelope(wkb **out, wkb **geom)
{
	return wkbBasic(out, geom, GEOSEnvelope, "geom.Envelope");
}

str
wkbEnvelopeFromCoordinates(wkb **out, dbl *xmin, dbl *ymin, dbl *xmax, dbl *ymax, int *srid)
{
	GEOSGeom geosGeometry, linearRingGeometry;
	GEOSCoordSeq coordSeq;

	if (is_dbl_nil(*xmin) || is_dbl_nil(*ymin) || is_dbl_nil(*xmax) || is_dbl_nil(*ymax) || is_int_nil(*srid)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakeEnvelope", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	//create the coordinates sequence
	if ((coordSeq = GEOSCoordSeq_create(5, 2)) == NULL)
		throw(MAL, "geom.MakeEnvelope", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");

	//set the values
	if (!GEOSCoordSeq_setX(coordSeq, 0, *xmin) ||
	    !GEOSCoordSeq_setY(coordSeq, 0, *ymin) ||
	    !GEOSCoordSeq_setX(coordSeq, 1, *xmin) ||
	    !GEOSCoordSeq_setY(coordSeq, 1, *ymax) ||
	    !GEOSCoordSeq_setX(coordSeq, 2, *xmax) ||
	    !GEOSCoordSeq_setY(coordSeq, 2, *ymax) ||
	    !GEOSCoordSeq_setX(coordSeq, 3, *xmax) ||
	    !GEOSCoordSeq_setY(coordSeq, 3, *ymin) ||
	    !GEOSCoordSeq_setX(coordSeq, 4, *xmin) ||
	    !GEOSCoordSeq_setY(coordSeq, 4, *ymin)) {
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.MakeEnvelope", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setX/Y failed");
	}

	linearRingGeometry = GEOSGeom_createLinearRing(coordSeq);
	if (linearRingGeometry == NULL) {
		//Gives segmentation fault GEOSCoordSeq_destroy(coordSeq);
		GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.MakeEnvelope", SQLSTATE(38000) "Geos error creating LinearRing from coordinates");
	}

	geosGeometry = GEOSGeom_createPolygon(linearRingGeometry, NULL, 0);
	if (geosGeometry == NULL) {
		GEOSGeom_destroy(linearRingGeometry);
		throw(MAL, "geom.MakeEnvelope", SQLSTATE(38000) "Geos error creating Polygon from LinearRing");
	}

	GEOSSetSRID(geosGeometry, *srid);

	*out = geos2wkb(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

str
wkbMakePolygon(wkb **out, wkb **external, bat *internalBAT_id, int *srid)
{
	GEOSGeom geosGeometry, externalGeometry, linearRingGeometry;
	bit closed = 0;
	GEOSCoordSeq coordSeq_copy;
	str err;

	if (is_wkb_nil(*external) || is_int_nil(*srid)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Polygon", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	externalGeometry = wkb2geos(*external);
	if (externalGeometry == NULL)
		throw(MAL, "geom.Polygon", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	//check the type of the external geometry
	if ((GEOSGeomTypeId(externalGeometry) + 1) != wkbLineString_mdb) {
		*out = NULL;
		GEOSGeom_destroy(externalGeometry);
		throw(MAL, "geom.Polygon", SQLSTATE(38000) "Geometries should be LineString");
	}
	//check whether the linestring is closed
	if ((err = wkbIsClosed(&closed, external)) != MAL_SUCCEED) {
		GEOSGeom_destroy(externalGeometry);
		return err;
	}
	if (!closed) {
		*out = NULL;
		GEOSGeom_destroy(externalGeometry);
		throw(MAL, "geom.Polygon", SQLSTATE(38000) "Geos lineString should be closed");
	}
	//create a copy of the coordinates
	coordSeq_copy = GEOSCoordSeq_clone(GEOSGeom_getCoordSeq(externalGeometry));
	GEOSGeom_destroy(externalGeometry);
	if (coordSeq_copy == NULL)
		throw(MAL, "geom.Polygon", SQLSTATE(38000) "Geos operation GEOSCoordSeq_clone failed");

	//create a linearRing using the copy of the coordinates
	linearRingGeometry = GEOSGeom_createLinearRing(coordSeq_copy);
	if (linearRingGeometry == NULL) {
		GEOSCoordSeq_destroy(coordSeq_copy);
		throw(MAL, "geom.Polygon", SQLSTATE(38000) "Geos operation GEOSGeom_createLinearRing failed");
	}

	//create a polygon using the linearRing
	if (internalBAT_id == NULL) {
		geosGeometry = GEOSGeom_createPolygon(linearRingGeometry, NULL, 0);
		if (geosGeometry == NULL) {
			*out = NULL;
			GEOSGeom_destroy(linearRingGeometry);
			throw(MAL, "geom.Polygon", SQLSTATE(38000) "Geos error creating Polygon from LinearRing");
		}
	} else {
		/* TODO: Looks like incomplete code: what should be
		 * done with internalBAT_id? --sjoerd */
		geosGeometry = NULL;
	}

	GEOSSetSRID(geosGeometry, *srid);

	*out = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

//Gets two Point or LineString geometries and returns a line
str
wkbMakeLine(wkb **out, wkb **geom1WKB, wkb **geom2WKB)
{
	GEOSGeom outGeometry, geom1Geometry, geom2Geometry;
	GEOSCoordSeq outCoordSeq = NULL;
	const GEOSCoordSequence *geom1CoordSeq = NULL, *geom2CoordSeq = NULL;
	unsigned int i = 0, geom1Size = 0, geom2Size = 0;
	unsigned geom1Dimension = 0, geom2Dimension = 0;
	double x, y, z;
	str err = MAL_SUCCEED;

	*out = NULL;
	if (is_wkb_nil(*geom1WKB) || is_wkb_nil(*geom2WKB)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geom1Geometry = wkb2geos(*geom1WKB);
	if (!geom1Geometry) {
		*out = NULL;
		throw(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	geom2Geometry = wkb2geos(*geom2WKB);
	if (!geom2Geometry) {
		*out = NULL;
		GEOSGeom_destroy(geom1Geometry);
		throw(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}
	//make sure the geometries are of the same srid
	if (GEOSGetSRID(geom1Geometry) != GEOSGetSRID(geom2Geometry)) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geometries of different SRID");
		goto bailout;
	}
	//check the types of the geometries
	if (GEOSGeomTypeId(geom1Geometry) + 1 != wkbPoint_mdb &&
		 GEOSGeomTypeId(geom1Geometry) + 1 != wkbLineString_mdb &&
		 GEOSGeomTypeId(geom2Geometry) + 1 != wkbPoint_mdb &&
		 GEOSGeomTypeId(geom2Geometry) + 1 != wkbLineString_mdb) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geometries should be Point or LineString");
		goto bailout;
	}
	//get the coordinate sequences of the geometries
	if ((geom1CoordSeq = GEOSGeom_getCoordSeq(geom1Geometry)) == NULL ||
		 (geom2CoordSeq = GEOSGeom_getCoordSeq(geom2Geometry)) == NULL) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");
		goto bailout;
	}
	//make sure that the dimensions of the geometries are the same
	if (!GEOSCoordSeq_getDimensions(geom1CoordSeq, &geom1Dimension) ||
		 !GEOSCoordSeq_getDimensions(geom2CoordSeq, &geom2Dimension)) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSGeom_getDimensions failed");
		goto bailout;
	}
	if (geom1Dimension != geom2Dimension) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geometries should be of the same dimension");
		goto bailout;
	}
	//get the number of coordinates in the two geometries
	if (!GEOSCoordSeq_getSize(geom1CoordSeq, &geom1Size) ||
		 !GEOSCoordSeq_getSize(geom2CoordSeq, &geom2Size)) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSGeom_getSize failed");
		goto bailout;
	}
	//create the coordSeq for the new geometry
	if ((outCoordSeq = GEOSCoordSeq_create(geom1Size + geom2Size, geom1Dimension)) == NULL) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSCoordSeq_create failed");
		goto bailout;
	}
	for (i = 0; i < geom1Size; i++) {
		GEOSCoordSeq_getX(geom1CoordSeq, i, &x);
		GEOSCoordSeq_getY(geom1CoordSeq, i, &y);
		if (!GEOSCoordSeq_setX(outCoordSeq, i, x) ||
		    !GEOSCoordSeq_setY(outCoordSeq, i, y)) {
			err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSCoordSeq_set[XY] failed");
			goto bailout;
		}
		if (geom1Dimension > 2) {
			GEOSCoordSeq_getZ(geom1CoordSeq, i, &z);
			if (!GEOSCoordSeq_setZ(outCoordSeq, i, z)) {
				err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setZ failed");
				goto bailout;
			}
		}
	}
	for (i = 0; i < geom2Size; i++) {
		GEOSCoordSeq_getX(geom2CoordSeq, i, &x);
		GEOSCoordSeq_getY(geom2CoordSeq, i, &y);
		if (!GEOSCoordSeq_setX(outCoordSeq, i + geom1Size, x) ||
		    !GEOSCoordSeq_setY(outCoordSeq, i + geom1Size, y)) {
			err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSCoordSeq_set[XY] failed");
			goto bailout;
		}
		if (geom2Dimension > 2) {
			GEOSCoordSeq_getZ(geom2CoordSeq, i, &z);
			if (!GEOSCoordSeq_setZ(outCoordSeq, i + geom1Size, z)) {
				err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSCoordSeq_setZ failed");
				goto bailout;
			}
		}
	}

	if ((outGeometry = GEOSGeom_createLineString(outCoordSeq)) == NULL) {
		err = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
		goto bailout;
	}
	outCoordSeq = NULL;

	GEOSSetSRID(outGeometry, GEOSGetSRID(geom1Geometry));
	*out = geos2wkb(outGeometry);
	GEOSGeom_destroy(outGeometry);

  bailout:
	if (outCoordSeq)
		GEOSCoordSeq_destroy(outCoordSeq);
	GEOSGeom_destroy(geom1Geometry);
	GEOSGeom_destroy(geom2Geometry);
	return err;
}

//Gets a BAT with geometries and returns a single LineString
str
wkbMakeLineAggr(wkb **outWKB, bat *bid)
{
	BAT *inBAT = NULL;
	BATiter inBAT_iter;
	BUN i;
	wkb *aWKB, *bWKB;
	str err;

	//get the BATs
	if ((inBAT = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "geom.MakeLine", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	/* TODO: what should be returned if the input BAT is less than
	 * two rows? --sjoerd */
	if (BATcount(inBAT) == 0) {
		BBPunfix(inBAT->batCacheid);
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);
	aWKB = (wkb *) BUNtvar(inBAT_iter, 0);
	if (BATcount(inBAT) == 1) {
		bat_iterator_end(&inBAT_iter);
		err = wkbFromWKB(outWKB, &aWKB);
		BBPunfix(inBAT->batCacheid);
		if (err) {
			freeException(err);
			throw(MAL, "geom.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		return MAL_SUCCEED;
	}
	bWKB = (wkb *) BUNtvar(inBAT_iter, 1);
	//create the first line using the first two geometries
	err = wkbMakeLine(outWKB, &aWKB, &bWKB);

	// add one more segment for each following row
	for (i = 2; err == MAL_SUCCEED && i < BATcount(inBAT); i++) {
		aWKB = *outWKB;
		bWKB = (wkb *) BUNtvar(inBAT_iter, i);
		*outWKB = NULL;

		err = wkbMakeLine(outWKB, &aWKB, &bWKB);
		GDKfree(aWKB);
	}

	bat_iterator_end(&inBAT_iter);
	BBPunfix(inBAT->batCacheid);

	return err;
}

static str
wkbExtractPointToCoordSeq(GEOSCoordSeq *outCoordSeq, wkb *inWKB, int index) {
	double x,y;
	str msg = MAL_SUCCEED;
	GEOSGeom inGeometry;
	const GEOSCoordSequence *inCoordSeq = NULL;

	inGeometry = wkb2geos(inWKB);
	if (!inGeometry) {
		throw(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}
	inCoordSeq = GEOSGeom_getCoordSeq(inGeometry);
	GEOSCoordSeq_getX(inCoordSeq, 0, &x);
	GEOSCoordSeq_getY(inCoordSeq, 0, &y);
	if (!GEOSCoordSeq_setX(*outCoordSeq, index, x) ||
	    !GEOSCoordSeq_setY(*outCoordSeq, index, y)) {
		GEOSGeom_destroy(inGeometry);
		throw(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSCoordSeq_set[XY] failed");
	}
	GEOSGeom_destroy(inGeometry);
	return msg;
}

static str
wkbMakeLineAggrArray(wkb **outWKB, wkb **inWKB_array, int size) {
	str msg = MAL_SUCCEED;
	int i;
	wkb *aWKB, *bWKB;
	GEOSGeom outGeometry;
	GEOSCoordSeq outCoordSeq = NULL;

	/* TODO: what should be returned if the input is less than
	 * two rows? --sjoerd */
	if (size == 0) {
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "aggr.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	aWKB = inWKB_array[0];
	if (size == 1) {
		msg = wkbFromWKB(outWKB, &aWKB);
		if (msg) {
			freeException(msg);
			throw(MAL, "aggr.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		return MAL_SUCCEED;
	}
	bWKB = inWKB_array[1];
	//create the first line using the first two geometries
	outCoordSeq = GEOSCoordSeq_create(size, 2);

	msg = wkbExtractPointToCoordSeq(&outCoordSeq, aWKB, 0);
	msg = wkbExtractPointToCoordSeq(&outCoordSeq, bWKB, 1);

	// add one more segment for each following row
	for (i = 2; msg == MAL_SUCCEED && i < size; i++) {
		msg = wkbExtractPointToCoordSeq(&outCoordSeq, inWKB_array[i], i);
	}
	if ((outGeometry = GEOSGeom_createLineString(outCoordSeq)) == NULL) {
		msg = createException(MAL, "geom.MakeLine", SQLSTATE(38000) "Geos operation GEOSGeom_createLineString failed");
	}
	*outWKB = geos2wkb(outGeometry);
	GEOSGeom_destroy(outGeometry);
	/* no need to clean outCoordSeq. it is destroyed via outGeometry */
	return msg;
}

//TODO Check SRID
//TODO Check if the input geometries are points
str
wkbMakeLineAggrSubGroupedCand(bat *outid, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	BAT *b = NULL, *g = NULL, *s = NULL, *out = NULL;
	BAT *sortedgroups, *sortedorder, *sortedinput;
	BATiter bi;
	const oid *gids = NULL;
	str msg = MAL_SUCCEED;

	oid min, max;
	BUN ngrp;
	struct canditer ci;

	oid lastGrp = -1;
	wkb **lines = NULL, **lineGroup = NULL;
	int position = 0;

	//Not using these variables
	(void) skip_nils;
	(void) eid;

	//Get the BAT descriptors for the value, group and candidate bats
	if ((b = BATdescriptor(*bid)) == NULL ||
		(gid && !is_bat_nil(*gid) && (g = BATdescriptor(*gid)) == NULL) ||
		(sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL)) {
		msg = createException(MAL, "aggr.MakeLine", RUNTIME_OBJECT_MISSING);
		goto free;
	}

	if ((BATsort(&sortedgroups, &sortedorder, NULL, g, NULL, NULL, false, false, true)) != GDK_SUCCEED) {
		msg = createException(MAL, "aggr.MakeLine", "BAT sort failed.");
		goto free;
	}

	//Project new order onto input bat IF the sortedorder isn't dense (in which case, the original input order is correct)
	if (!BATtdense(sortedorder)) {
		sortedinput = BATproject(sortedorder,b);
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		b = sortedinput;
		g = sortedgroups;
		BBPunfix(sortedorder->batCacheid);
	}
	else {
		BBPunfix(sortedgroups->batCacheid);
		BBPunfix(sortedorder->batCacheid);
	}

	//Fill in the values of the group aggregate operation
	if ((msg = (str) BATgroupaggrinit(b, g, NULL, s, &min, &max, &ngrp, &ci)) != NULL) {
		msg = createException(MAL, "aggr.MakeLine", "%s", msg);
		goto free;
	}

	//Create a new BAT column of wkb type, with lenght equal to the number of groups
	if ((out = COLnew(min, ATOMindex("wkb"), ngrp, TRANSIENT)) == NULL) {
		msg = createException(MAL, "aggr.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto free;
	}

	//Create an array of WKB to hold the results of the MakeLine
	if ((lines = GDKzalloc(sizeof(wkb *) * ngrp)) == NULL) {
		msg = createException(MAL, "aggr.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		BBPreclaim(out);
		goto free;
	}

	//Create an array of WKB to hold the points to be made into a line (for one group at a time)
	if ((lineGroup = GDKzalloc(sizeof(wkb*) * ci.ncand)) == NULL) {
		msg = createException(MAL, "aggr.MakeLine", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		BBPreclaim(out);
		goto free;
	}

	if (g && !BATtdense(g))
		gids = (const oid *)Tloc(g, 0);
	bi = bat_iterator(b);

	for (BUN i = 0; i < ci.ncand; i++) {
		oid o = canditer_next(&ci);
		BUN p = o - b->hseqbase;
		oid grp = gids ? gids[p] : g ? min + (oid)p : 0;
		wkb *inWKB = (wkb *)BUNtvar(bi, p);

		if (grp != lastGrp) {
			if (lastGrp != (oid)-1) {
				msg = wkbMakeLineAggrArray(&lines[lastGrp], lineGroup, position);
				position = 0;
			}
			lastGrp = grp;
		}
		lineGroup[position++] = inWKB;
	}
	msg = wkbMakeLineAggrArray(&lines[lastGrp], lineGroup, position);
	GDKfree(lineGroup);

	if (BUNappendmulti(out, lines, ngrp, false) != GDK_SUCCEED) {
		msg = createException(MAL, "geom.Union", SQLSTATE(38000) "BUNappend operation failed");
		for (BUN i = 0; i < ngrp; i++)
			GDKfree(lines[i]);
		GDKfree(lines);
		bat_iterator_end(&bi);
		goto free;
	}

	for (BUN i = 0; i < ngrp; i++)
		GDKfree(lines[i]);
	GDKfree(lines);
	bat_iterator_end(&bi);

	*outid = out->batCacheid;
	BBPkeepref(out);
	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	return MAL_SUCCEED;
free:
	if (b)
		BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	BBPreclaim(out);
	return msg;
}

str
wkbMakeLineAggrSubGrouped (bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils) {
	return wkbMakeLineAggrSubGroupedCand(out,bid,gid,eid,NULL,skip_nils);
}

/* Returns the first or last point of a linestring */
static str
wkbBorderPoint(wkb **out, wkb **geom, GEOSGeometry *(*func) (const GEOSGeometry *), const char *name)
{
	GEOSGeom geosGeometry;
	GEOSGeom new;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	*out = NULL;
	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		throw(MAL, name, SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		err = createException(MAL, name, SQLSTATE(38000) "Geometry not a LineString");
	} else {
		new = (*func) (geosGeometry);
		if (new == NULL) {
			err = createException(MAL, name, SQLSTATE(38000) "Geos operation GEOSGeomGet%s failed", name + 5);
		} else {
			*out = geos2wkb(new);
			GEOSGeom_destroy(new);
		}
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns the first point in a linestring */
str
wkbStartPoint(wkb **out, wkb **geom)
{
	return wkbBorderPoint(out, geom, GEOSGeomGetStartPoint, "geom.StartPoint");
}

/* Returns the last point in a linestring */
str
wkbEndPoint(wkb **out, wkb **geom)
{
	return wkbBorderPoint(out, geom, GEOSGeomGetEndPoint, "geom.EndPoint");
}

static str
numPointsLineString(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	/* get the coordinates of the points comprising the geometry */
	const GEOSCoordSequence *coordSeq = GEOSGeom_getCoordSeq(geosGeometry);

	if (coordSeq == NULL)
		throw(MAL, "geom.NumPoints", SQLSTATE(38000) "Geos operation GEOSGeom_getCoordSeq failed");

	/* get the number of points in the geometry */
	if (!GEOSCoordSeq_getSize(coordSeq, out)) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", SQLSTATE(38000) "Geos operation GEOSGeomGetNumPoints failed");
	}

	return MAL_SUCCEED;
}

static str
numPointsPolygon(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	const GEOSGeometry *exteriorRingGeometry;
	int numInteriorRings = 0, i = 0;
	str err;
	unsigned int pointsN = 0;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if (!exteriorRingGeometry) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");
	}
	//get the points in the exterior ring
	if ((err = numPointsLineString(out, exteriorRingGeometry)) != MAL_SUCCEED) {
		*out = int_nil;
		return err;
	}
	pointsN = *out;

	//check the interior rings
	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", SQLSTATE(38000) "Geos operation GEOSGetNumInteriorRings failed");
	}
	// iterate over the interiorRing and transform each one of them
	for (i = 0; i < numInteriorRings; i++) {
		if ((err = numPointsLineString(out, GEOSGetInteriorRingN(geosGeometry, i))) != MAL_SUCCEED) {
			*out = int_nil;
			return err;
		}
		pointsN += *out;
	}

	*out = pointsN;
	return MAL_SUCCEED;
}

static str numPointsGeometry(unsigned int *out, const GEOSGeometry *geosGeometry);
static str
numPointsMultiGeometry(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	int geometriesNum, i;
	const GEOSGeometry *multiGeometry = NULL;
	str err;
	unsigned int pointsN = 0;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);

	for (i = 0; i < geometriesNum; i++) {
		multiGeometry = GEOSGetGeometryN(geosGeometry, i);
		if ((err = numPointsGeometry(out, multiGeometry)) != MAL_SUCCEED) {
			*out = int_nil;
			return err;
		}
		pointsN += *out;
	}

	*out = pointsN;
	return MAL_SUCCEED;
}

static str
numPointsGeometry(unsigned int *out, const GEOSGeometry *geosGeometry)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	//check the type of the geometry
	switch (geometryType) {
	case wkbPoint_mdb:
	case wkbLineString_mdb:
	case wkbLinearRing_mdb:
		return numPointsLineString(out, geosGeometry);
	case wkbPolygon_mdb:
		return numPointsPolygon(out, geosGeometry);
	case wkbMultiPoint_mdb:
	case wkbMultiLineString_mdb:
	case wkbMultiPolygon_mdb:
	case wkbGeometryCollection_mdb:
		return numPointsMultiGeometry(out, geosGeometry);
	default:
		throw(MAL, "geom.NumPoints", SQLSTATE(38000) "Geos geometry type %s unknown", geom_type2str(geometryType, 0));
	}
}

/* Returns the number of points in a geometry */
str
wkbNumPoints(int *out, wkb **geom, int *check)
{
	GEOSGeom geosGeometry;
	int geometryType = 0;
	str err = MAL_SUCCEED;
	char *geomSTR = NULL;
	unsigned int pointsNum;

	if (is_wkb_nil(*geom) || is_int_nil(*check)) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	geometryType = GEOSGeomTypeId(geosGeometry) + 1;

	if (*check && geometryType != wkbLineString_mdb) {
		*out = int_nil;
		GEOSGeom_destroy(geosGeometry);

		if ((err = wkbAsText(&geomSTR, geom, NULL)) == MAL_SUCCEED) {
			err = createException(MAL, "geom.NumPoints", SQLSTATE(38000) "Geometry \"%s\" not a LineString", geomSTR);
			GDKfree(geomSTR);
		}
		return err;
	}

	if ((err = numPointsGeometry(&pointsNum, geosGeometry)) != MAL_SUCCEED) {
		*out = int_nil;
		GEOSGeom_destroy(geosGeometry);
		return err;
	}

	if (pointsNum > INT_MAX) {
		GEOSGeom_destroy(geosGeometry);
		*out = int_nil;
		throw(MAL, "geom.NumPoints", SQLSTATE(38000) "Geos operation Overflow");
	}

	*out = pointsNum;
	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* Returns the n-th point of the geometry */
str
wkbPointN(wkb **out, wkb **geom, int *n)
{
	int rN = -1;
	GEOSGeom geosGeometry;
	GEOSGeom new;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*geom) || is_int_nil(*n)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.PointN", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = NULL;
		throw(MAL, "geom.PointN", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointN", SQLSTATE(38000) "Geometry not a LineString");
	}
	//check number of points
	rN = GEOSGeomGetNumPoints(geosGeometry);
	if (rN == -1) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointN", SQLSTATE(38000) "Geos operation GEOSGeomGetNumPoints failed");
	}

	if (rN <= *n || *n < 0) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointN", SQLSTATE(38000) "Geos unable to retrieve point %d (not enough points)", *n);
	}

	if ((new = GEOSGeomGetPointN(geosGeometry, *n)) == NULL) {
		err = createException(MAL, "geom.PointN", SQLSTATE(38000) "Geos operation GEOSGeomGetPointN failed");
	} else {
		if ((*out = geos2wkb(new)) == NULL)
			err = createException(MAL, "geom.PointN", SQLSTATE(38000) "Geos operation GEOSGeomGetPointN failed");
		GEOSGeom_destroy(new);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns the exterior ring of the polygon*/
str
wkbExteriorRing(wkb **exteriorRingWKB, wkb **geom)
{
	GEOSGeom geosGeometry;
	const GEOSGeometry *exteriorRingGeometry;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*geom)) {
		if ((*exteriorRingWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.ExteriorRing", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*exteriorRingWKB = NULL;
		throw(MAL, "geom.ExteriorRing", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_POLYGON) {
		*exteriorRingWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.ExteriorRing", SQLSTATE(38000) "Geometry not a Polygon");

	}
	/* get the exterior ring of the geometry */
	if ((exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry)) == NULL)
		err = createException(MAL, "geom.ExteriorRing", SQLSTATE(38000) "Geos operation GEOSGetExteriorRing failed");
	else {
		/* get the wkb representation of it */
		if ((*exteriorRingWKB = geos2wkb(exteriorRingGeometry)) == NULL)
			err = createException(MAL, "geom.ExteriorRing", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns the n-th interior ring of a polygon */
str
wkbInteriorRingN(wkb **interiorRingWKB, wkb **geom, int *ringNum)
{
	GEOSGeom geosGeometry = NULL;
	const GEOSGeometry *interiorRingGeometry;
	int rN = -1;
	str err = MAL_SUCCEED;

	//initialize to NULL
	*interiorRingWKB = NULL;

	if (is_wkb_nil(*geom) || is_int_nil(*ringNum)) {
		if ((*interiorRingWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.InteriorRingN", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*interiorRingWKB = NULL;
		throw(MAL, "geom.InteriorRingN", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if ((GEOSGeomTypeId(geosGeometry) + 1) != wkbPolygon_mdb) {
		*interiorRingWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.InteriorRingN", SQLSTATE(38000) "Geometry not a Polygon");

	}
	//check number of internal rings
	rN = GEOSGetNumInteriorRings(geosGeometry);
	if (rN == -1) {
		*interiorRingWKB = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.InteriorRingN", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed.");
	}

	if (rN < *ringNum || *ringNum <= 0) {
		GEOSGeom_destroy(geosGeometry);
		//NOT AN ERROR throw(MAL, "geom.interiorRingN", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed. Not enough interior rings");
		if ((*interiorRingWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.InteriorRingN", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	/* get the interior ring of the geometry */
	if ((interiorRingGeometry = GEOSGetInteriorRingN(geosGeometry, *ringNum - 1)) == NULL) {
		err = createException(MAL, "geom.InteriorRingN", SQLSTATE(38000) "Geos operation GEOSGetInteriorRingN failed");
	} else {
		/* get the wkb representation of it */
		if ((*interiorRingWKB = geos2wkb(interiorRingGeometry)) == NULL)
			err = createException(MAL, "geom.InteriorRingN", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns the number of interior rings in the first polygon of the provided geometry
 * plus the exterior ring depending on the value of exteriorRing*/
str
wkbNumRings(int *out, wkb **geom, int *exteriorRing)
{
	str ret = MAL_SUCCEED;
	bit empty;
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geom) || is_int_nil(*exteriorRing)) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	//check if the geometry is empty
	if ((ret = wkbIsEmpty(&empty, geom)) != MAL_SUCCEED) {
		return ret;
	}
	if (empty) {
		//the geometry is empty
		*out = 0;
		return MAL_SUCCEED;
	}
	//check the type of the geometry
	geosGeometry = wkb2geos(*geom);

	if (geosGeometry == NULL)
		throw(MAL, "geom.NumRings", SQLSTATE(38000) "Geos problem converting WKB to GEOS");

	if (GEOSGeomTypeId(geosGeometry) + 1 == wkbMultiPolygon_mdb) {
		//use the first polygon as done by PostGIS
		wkb *new = geos2wkb(GEOSGetGeometryN(geosGeometry, 0));
		if (new == NULL) {
			ret = createException(MAL, "geom.NumRings", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			ret = wkbBasicInt(out, new, GEOSGetNumInteriorRings, "geom.NumRings");
			GDKfree(new);
		}
	} else if (GEOSGeomTypeId(geosGeometry) + 1 == wkbPolygon_mdb) {
		ret = wkbBasicInt(out, *geom, GEOSGetNumInteriorRings, "geom.NumRings");
	} else {
		//It is not a polygon so the number of rings is 0
		*out = -*exteriorRing; /* compensate for += later */
	}

	GEOSGeom_destroy(geosGeometry);

	if (ret != MAL_SUCCEED)
		return ret;

	*out += *exteriorRing;

	return MAL_SUCCEED;
}

/* it handles functions that take as input a single geometry and return Boolean */
static str
wkbBasicBoolean(bit *out, wkb **geom, char (*func) (const GEOSGeometry *), const char *name)
{
	int ret;
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geom)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL)
		throw(MAL, name, SQLSTATE(38000) "Geos operation wkb2geom failed");

	ret = (*func) (geosGeometry);	//it is supposed to return char but treating it as such gives wrong results
	GEOSGeom_destroy(geosGeometry);

	if (ret == 2) {
		GDKclrerr();
		ret = 0;
	}

	*out = ret;

	return MAL_SUCCEED;
}

/* the function checks whether the geometry is closed. GEOS works only with
 * linestring geometries but PostGIS returns true in any geometry that is not
 * a linestring. I made it to be like PostGIS */
static str
geosIsClosed(bit *out, const GEOSGeometry *geosGeometry)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;
	int i = 0;
	str err;
	int geometriesNum;

	*out = bit_nil;

	switch (geometryType) {
	case -1:
		throw(MAL, "geom.IsClosed", SQLSTATE(38000) "Geos operation GEOSGeomTypeId failed");
	case wkbPoint_mdb:
	case wkbPolygon_mdb:
	case wkbMultiPoint_mdb:
	case wkbMultiPolygon_mdb:
		//In all these case it is always true
		*out = 1;
		break;
	case wkbLineString_mdb:
		//check
		if ((i = GEOSisClosed(geosGeometry)) == 2)
			throw(MAL, "geom.IsClosed", SQLSTATE(38000) "Geos operation GEOSisClosed failed");
		*out = i;
		break;
	case wkbMultiLineString_mdb:
	case wkbGeometryCollection_mdb:
		//check each one separately
		geometriesNum = GEOSGetNumGeometries(geosGeometry);
		if (geometriesNum < 0)
			throw(MAL, "geom.IsClosed", SQLSTATE(38000) "Geos operation GEOSGetNumGeometries failed");

		for (i = 0; i < geometriesNum; i++) {
			const GEOSGeometry *gN = GEOSGetGeometryN(geosGeometry, i);
			if (!gN)
				throw(MAL, "geom.IsClosed", SQLSTATE(38000) "Geos operation GEOSGetGeometryN failed");

			if ((err = geosIsClosed(out, gN)) != MAL_SUCCEED) {
				return err;
			}

			if (!*out)	//no reason to check further logical AND will always be 0
				return MAL_SUCCEED;
		}

		break;
	default:
		throw(MAL, "geom.IsClosed", SQLSTATE(38000) "Geos geometry type unknown");
	}

	return MAL_SUCCEED;
}

str
wkbIsClosed(bit *out, wkb **geomWKB)
{
	str err;
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geomWKB)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	//if empty geometry return false
	if ((err = wkbIsEmpty(out, geomWKB)) != MAL_SUCCEED) {
		return err;
	}
	if (*out) {
		*out = 0;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL)
		throw(MAL, "geom.IsClosed", SQLSTATE(38000) "Geos operation wkb2geos failed");

	err = geosIsClosed(out, geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return err;
}

str
wkbIsEmpty(bit *out, wkb **geomWKB)
{
	return wkbBasicBoolean(out, geomWKB, GEOSisEmpty, "geom.IsEmpty");
}

str
wkbIsRing(bit *out, wkb **geomWKB)
{
	return wkbBasicBoolean(out, geomWKB, GEOSisRing, "geom.IsRing");
}

str
wkbIsSimple(bit *out, wkb **geomWKB)
{
	return wkbBasicBoolean(out, geomWKB, GEOSisSimple, "geom.IsSimple");
}

/*geom prints a message saying the reason why the geometry is not valid but
 * since there is also isValidReason I skip this here */
str
wkbIsValid(bit *out, wkb **geomWKB)
{
	str err = wkbBasicBoolean(out, geomWKB, GEOSisValid, "geom.IsValid");
	/* GOESisValid may cause GDKerror to be called: ignore it */
	if (err == MAL_SUCCEED)
		GDKclrerr();
	return err;
}

str
wkbIsValidReason(char **reason, wkb **geomWKB)
{
	GEOSGeom geosGeometry;
	char *GEOSReason = NULL;

	if (is_wkb_nil(*geomWKB)) {
		if ((*reason = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.IsValidReason", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL)
		throw(MAL, "geom.IsValidReason", SQLSTATE(38000) "Geos operation wkb2geom failed");

	GEOSReason = GEOSisValidReason(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GEOSReason == NULL)
		throw(MAL, "geom.IsValidReason", SQLSTATE(38000) "Geos operation GEOSisValidReason failed");

	*reason = GDKstrdup(GEOSReason);
	GEOSFree(GEOSReason);
	if (*reason == NULL)
		throw(MAL, "geom.IsValidReason", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}

/* I should check it since it does not work */
str
wkbIsValidDetail(char **out, wkb **geom)
{
	int res = -1;
	char *GEOSreason = NULL;
	GEOSGeom GEOSlocation = NULL;
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geom)) {
		if ((*out = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "geom.IsValidReason", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	if ((geosGeometry = wkb2geos(*geom)) == NULL) {
		*out = NULL;
		throw(MAL, "geom.IsValidDetail", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	res = GEOSisValidDetail(geosGeometry, 1, &GEOSreason, &GEOSlocation);

	GEOSGeom_destroy(geosGeometry);

	if (res == 2) {
		throw(MAL, "geom.IsValidDetail", SQLSTATE(38000) "Geos operation GEOSisValidDetail failed");
	}

	*out = GDKstrdup(GEOSreason);

	GEOSFree(GEOSreason);
	GEOSGeom_destroy(GEOSlocation);

	if (*out == NULL)
		throw(MAL, "geom.IsValidReason", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}

/* returns the area of the geometry */
str
wkbArea(dbl *out, wkb **geomWKB)
{
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geomWKB)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geomWKB);
	if (geosGeometry == NULL) {
		*out = dbl_nil;
		throw(MAL, "geom.Area", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (!GEOSArea(geosGeometry, out)) {
		GEOSGeom_destroy(geosGeometry);
		*out = dbl_nil;
		throw(MAL, "geom.Area", SQLSTATE(38000) "Geos operation GEOSArea failed");
	}

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* returns the centroid of the geometry */
str
wkbCentroid(wkb **out, wkb **geom)
{
	GEOSGeom geosGeometry;
	GEOSGeom outGeometry;

	if (is_wkb_nil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Centroid", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL)
		throw(MAL, "geom.Centroid", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	outGeometry = GEOSGetCentroid(geosGeometry);
	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry));	//the centroid has the same SRID with the the input geometry
	*out = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	return MAL_SUCCEED;

}

/*  Returns the 2-dimensional Cartesian minimum distance (based on spatial ref) between two geometries in projected units */
str
wkbDistance(dbl *out, wkb **a, wkb **b)
{
	GEOSGeom ga, gb;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*a) || is_wkb_nil(*b)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	ga = wkb2geos(*a);
	gb = wkb2geos(*b);
	if (ga == NULL || gb == NULL) {
		if (ga)
			GEOSGeom_destroy(ga);
		if (gb)
			GEOSGeom_destroy(gb);
		*out = dbl_nil;
		throw(MAL, "geom.Distance", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (GEOSGetSRID(ga) != GEOSGetSRID(gb)) {
		err = createException(MAL, "geom.Distance", SQLSTATE(38000) "Geometries of different SRID");
	} else if (!GEOSDistance(ga, gb, out)) {
		err = createException(MAL, "geom.Distance", SQLSTATE(38000) "Geos operation GEOSDistance failed");
	}

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return err;
}

/* Returns the 2d length of the geometry if it is a linestring or multilinestring */
str
wkbLength(dbl *out, wkb **a)
{
	GEOSGeom geosGeometry;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*a)) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*a);
	if (geosGeometry == NULL) {
		throw(MAL, "geom.Length", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (!GEOSLength(geosGeometry, out))
		err = createException(MAL, "geom.Length", SQLSTATE(38000) "Geos operation GEOSLength failed");

	GEOSGeom_destroy(geosGeometry);

	return err;
}

/* Returns a geometry that represents the convex hull of this geometry.
 * The convex hull of a geometry represents the minimum convex geometry
 * that encloses all geometries within the set. */
str
wkbConvexHull(wkb **out, wkb **geom)
{
	str ret = MAL_SUCCEED;
	GEOSGeom geosGeometry;
	GEOSGeom convexHullGeometry = NULL;

	if (is_wkb_nil(*geom)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.ConvexHull", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if ((geosGeometry = wkb2geos(*geom)) == NULL)
		throw(MAL, "geom.ConvexHull", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if ((convexHullGeometry = GEOSConvexHull(geosGeometry)) == NULL) {
		ret = createException(MAL, "geom.ConvexHull", SQLSTATE(38000) "Geos operation GEOSConvexHull failed");
	} else {
		GEOSSetSRID(convexHullGeometry, (*geom)->srid);
		*out = geos2wkb(convexHullGeometry);
		GEOSGeom_destroy(convexHullGeometry);
		if (*out == NULL)
			ret = createException(MAL, "geom.ConvexHull", SQLSTATE(38000) "Geos operation geos2wkb failed");
	}
	GEOSGeom_destroy(geosGeometry);

	return ret;

}

/* Gets two geometries and returns a new geometry */
static str
wkbanalysis(wkb **out, wkb **geom1WKB, wkb **geom2WKB, GEOSGeometry *(*func) (const GEOSGeometry *, const GEOSGeometry *), const char *name)
{
	GEOSGeom outGeometry, geom1Geometry, geom2Geometry;
	str err = MAL_SUCCEED;

	if (is_wkb_nil(*geom1WKB) || is_wkb_nil(*geom2WKB)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geom1Geometry = wkb2geos(*geom1WKB);
	geom2Geometry = wkb2geos(*geom2WKB);
	if (geom1Geometry == NULL || geom2Geometry == NULL) {
		*out = NULL;
		if (geom1Geometry)
			GEOSGeom_destroy(geom1Geometry);
		if (geom2Geometry)
			GEOSGeom_destroy(geom2Geometry);
		throw(MAL, name, SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	//make sure the geometries are of the same srid
	if (GEOSGetSRID(geom1Geometry) != GEOSGetSRID(geom2Geometry)) {
		err = createException(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
	} else if ((outGeometry = (*func) (geom1Geometry, geom2Geometry)) == NULL) {
		err = createException(MAL, name, SQLSTATE(38000) "Geos operation GEOS%s failed", name + 5);
	} else {
		GEOSSetSRID(outGeometry, GEOSGetSRID(geom1Geometry));
		*out = geos2wkb(outGeometry);
		GEOSGeom_destroy(outGeometry);
	}
	GEOSGeom_destroy(geom1Geometry);
	GEOSGeom_destroy(geom2Geometry);

	return err;
}

str
wkbIntersection(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSIntersection, "geom.Intersection");
}

str
wkbUnion(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSUnion, "geom.Union");
}

//Gets a BAT with geometries and returns a single LineString
str
wkbUnionAggr(wkb **outWKB, bat *inBAT_id)
{
	BAT *inBAT = NULL;
	BATiter inBAT_iter;
	BUN i;
	str err;
	wkb *aWKB, *bWKB;

	//get the BATs
	if (!(inBAT = BATdescriptor(*inBAT_id))) {
		throw(MAL, "geom.Union", SQLSTATE(38000) "Geos problem retrieving columns");
	}

	if (BATcount(inBAT) == 0) {
		BBPunfix(inBAT->batCacheid);
		if ((*outWKB = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Union", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	//iterator over the BATs
	inBAT_iter = bat_iterator(inBAT);

	aWKB = (wkb *) BUNtvar(inBAT_iter, 0);
	if (BATcount(inBAT) == 1) {
		bat_iterator_end(&inBAT_iter);
		err = wkbFromWKB(outWKB, &aWKB);
		BBPunfix(inBAT->batCacheid);
		if (err) {
			freeException(err);
			throw(MAL, "geom.Union", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		return MAL_SUCCEED;
	}
	bWKB = (wkb *) BUNtvar(inBAT_iter, 1);
	//create the first union using the first two geometries
	err = wkbUnion(outWKB, &aWKB, &bWKB);
	for (i = 2; err == MAL_SUCCEED && i < BATcount(inBAT); i++) {
		aWKB = *outWKB;
		bWKB = (wkb *) BUNtvar(inBAT_iter, i);
		*outWKB = NULL;

		err = wkbUnion(outWKB, &aWKB, &bWKB);
		GDKfree(aWKB);
	}

	bat_iterator_end(&inBAT_iter);
	BBPunfix(inBAT->batCacheid);

	return err;

}

str
wkbDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSDifference, "geom.Difference");
}

str
wkbSymDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSSymDifference, "geom.SymDifference");
}

/* Returns a geometry that represents all points whose distance from this Geometry is less than or equal to distance. */
str
wkbBuffer(wkb **out, wkb **geom, dbl *distance)
{
	GEOSGeom geosGeometry;
	GEOSGeom new;

	if (is_wkb_nil(*geom) || is_dbl_nil(*distance)) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.Buffer", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		throw(MAL, "geom.Buffer", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if ((new = GEOSBuffer(geosGeometry, *distance, 18)) == NULL) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Buffer", SQLSTATE(38000) "Geos operation GEOSBuffer failed");
	}
	*out = geos2wkb(new);
	GEOSGeom_destroy(new);
	GEOSGeom_destroy(geosGeometry);

	if (*out == NULL)
		throw(MAL, "geom.Buffer", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	(*out)->srid = (*geom)->srid;

	return MAL_SUCCEED;
}

/* Gets two geometries and returns a Boolean by comparing them */
static str
wkbspatial(bit *out, wkb **geomWKB_a, wkb **geomWKB_b, char (*func) (const GEOSGeometry *, const GEOSGeometry *), const char *name)
{
	int res;
	GEOSGeom geosGeometry_a, geosGeometry_b;

	if (is_wkb_nil(*geomWKB_a) || is_wkb_nil(*geomWKB_b)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	geosGeometry_a = wkb2geos(*geomWKB_a);
	geosGeometry_b = wkb2geos(*geomWKB_b);
	if (geosGeometry_a == NULL || geosGeometry_b == NULL) {
		if (geosGeometry_a)
			GEOSGeom_destroy(geosGeometry_a);
		if (geosGeometry_b)
			GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, name, SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (GEOSGetSRID(geosGeometry_a) != GEOSGetSRID(geosGeometry_b)) {
		GEOSGeom_destroy(geosGeometry_a);
		GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, name, SQLSTATE(38000) "Geometries of different SRID");
	}

	res = (*func) (geosGeometry_a, geosGeometry_b);

	GEOSGeom_destroy(geosGeometry_a);
	GEOSGeom_destroy(geosGeometry_b);

	if (res == 2)
		throw(MAL, name, SQLSTATE(38000) "Geos operation GEOS%s failed", name + 5);

	*out = res;

	return MAL_SUCCEED;
}

str
wkbContains(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSContains, "geom.Contains");
}

str
wkbCrosses(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSCrosses, "geom.Crosses");
}

str
wkbDisjoint(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSDisjoint, "geom.Disjoint");
}

str
wkbEquals(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSEquals, "geom.Equals");
}

str
wkbIntersects(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSIntersects, "geom.Intersects");
}

str
wkbOverlaps(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSOverlaps, "geom.Overlaps");
}

str
wkbRelate(bit *out, wkb **geomWKB_a, wkb **geomWKB_b, str *pattern)
{
	int res;
	GEOSGeom geosGeometry_a, geosGeometry_b;

	if (is_wkb_nil(*geomWKB_a) || is_wkb_nil(*geomWKB_b) || strNil(*pattern)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	geosGeometry_a = wkb2geos(*geomWKB_a);
	geosGeometry_b = wkb2geos(*geomWKB_b);
	if (geosGeometry_a == NULL || geosGeometry_b == NULL) {
		if (geosGeometry_a)
			GEOSGeom_destroy(geosGeometry_a);
		if (geosGeometry_b)
			GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, "geom.RelatePattern", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	if (GEOSGetSRID(geosGeometry_a) != GEOSGetSRID(geosGeometry_b)) {
		GEOSGeom_destroy(geosGeometry_a);
		GEOSGeom_destroy(geosGeometry_b);
		throw(MAL, "geom.RelatePattern", SQLSTATE(38000) "Geometries of different SRID");
	}

	res = GEOSRelatePattern(geosGeometry_a, geosGeometry_b, *pattern);

	GEOSGeom_destroy(geosGeometry_a);
	GEOSGeom_destroy(geosGeometry_b);

	if (res == 2)
		throw(MAL, "geom.RelatePattern", SQLSTATE(38000) "Geos operation GEOSRelatePattern failed");

	*out = res;

	return MAL_SUCCEED;
}

str
wkbTouches(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSTouches, "geom.Touches");
}

str
wkbWithin(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSWithin, "geom.Within");
}

str
wkbCovers(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSCovers, "geom.Covers");
}

str
wkbCoveredBy(bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
{
	return wkbspatial(out, geomWKB_a, geomWKB_b, GEOSCoveredBy, "geom.CoveredBy");
}

str
wkbDWithin(bit *out, wkb **geomWKB_a, wkb **geomWKB_b, dbl *distance)
{
	double distanceComputed;
	str err;

	if (is_wkb_nil(*geomWKB_a) || is_wkb_nil(*geomWKB_b) || is_dbl_nil(*distance)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if ((err = wkbDistance(&distanceComputed, geomWKB_a, geomWKB_b)) != MAL_SUCCEED) {
		return err;
	}

	*out = (distanceComputed <= *distance);

	return MAL_SUCCEED;
}

str
wkbDWithinMbr(bit *out, wkb **a, wkb **b, mbr **mbr_a, mbr **mbr_b, dbl *distance)
{
	double actualDistance, bboxDistance;
	double halfd_a, halfd_b; // halfed diagonals of the a and b bounding boxes
	double ambiguous_zone_min, ambiguous_zone_max; // see comments
	str err;

	if (is_wkb_nil(*a) || is_wkb_nil(*b) || is_dbl_nil(*distance)) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	// if there are no mbr(s) fallback to wkbDWithin
	if (is_mbr_nil(*mbr_a) || is_mbr_nil(*mbr_b))
		return wkbDWithin(out, a, b, distance);

	// first calculate the distance of the bounding boxes (mbrs)
	if ((err = mbrDistance(&bboxDistance, mbr_a, mbr_b)) != MAL_SUCCEED)
		return err;

	if ((err = mbrDiagonal(&halfd_a, mbr_a)) != MAL_SUCCEED)
		return err;
	halfd_a *= .5;

	if ((err = mbrDiagonal(&halfd_b, mbr_b)) != MAL_SUCCEED)
		return err;
	halfd_b *= .5;

	// Every bounding box can be inscribed in a circle. When calculating the distance
	// between two mbrs we do so by their centroids which are actually the origins of
	// their circumscribed circles. Then, independently of the bounded geometry, we can
	// find two rough distance limits which are giving us a zone outside of which the
	// questions 'distance within' can be answered only by the bounding box geometry.
	// If the 'distance within' check is done over distance value in this zone then we
	// actually need to perform the underlying geometry distance calculation.
	ambiguous_zone_max = bboxDistance;
	ambiguous_zone_min = bboxDistance - halfd_a - halfd_b;

	if (*distance < ambiguous_zone_min) {
		*out = false;
	} else if (*distance > ambiguous_zone_max) {
		*out = true;
	} else {
		// if we are not sure still calculate the actual distance of the geometries
		if ((err = wkbDistance(&actualDistance, a, b)) != MAL_SUCCEED)
			return err;
		*out = (actualDistance <= *distance);
	}

	return MAL_SUCCEED;
}

/*returns the n-th geometry in a multi-geometry */
str
wkbGeometryN(wkb **out, wkb **geom, const int *geometryNum)
{
	int geometriesNum = -1;
	GEOSGeom geosGeometry = NULL;

	//no geometry at this position
	if (is_wkb_nil(*geom) || is_int_nil(*geometryNum) || *geometryNum <= 0) {
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.GeometryN", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);

	if (geosGeometry == NULL) {
		*out = NULL;
		throw(MAL, "geom.GeometryN", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	if (geometriesNum < 0) {
		*out = NULL;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.GeometryN", SQLSTATE(38000) "Geos operation GEOSGetNumGeometries failed");
	}
	if (geometriesNum == 1 || //geometry is not a multi geometry
	    geometriesNum < *geometryNum) { //no geometry at this position
		GEOSGeom_destroy(geosGeometry);
		if ((*out = wkbNULLcopy()) == NULL)
			throw(MAL, "geom.GeometryN", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSGetGeometryN(geosGeometry, *geometryNum - 1));
	GEOSGeom_destroy(geosGeometry);
	if (*out == NULL)
		throw(MAL, "geom.GeometryN", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	return MAL_SUCCEED;
}

/* returns the number of geometries */
str
wkbNumGeometries(int *out, wkb **geom)
{
	GEOSGeom geosGeometry;

	if (is_wkb_nil(*geom)) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);
	if (geosGeometry == NULL) {
		*out = int_nil;
		throw(MAL, "geom.NumGeometries", SQLSTATE(38000) "Geos operation wkb2geos failed");
	}

	*out = GEOSGetNumGeometries(geosGeometry);
	GEOSGeom_destroy(geosGeometry);
	if (*out < 0) {
		*out = int_nil;
		throw(MAL, "geom.GeometryN", SQLSTATE(38000) "Geos operation GEOSGetNumGeometries failed");
	}

	return MAL_SUCCEED;
}


/* TODO: Analyze these functions below (what's the dif from normal contain, is it unfinished?) */

geom_export str wkbContains_point_bat(bat *out, wkb **a, bat *point_x, bat *point_y);
geom_export str wkbContains_point(bit *out, wkb **a, dbl *point_x, dbl *point_y);

static inline double
isLeft(double P0x, double P0y, double P1x, double P1y, double P2x, double P2y)
{
	return ((P1x - P0x) * (P2y - P0y)
		- (P2x - P0x) * (P1y - P0y));
}

static str
pnpoly(int *out, int nvert, dbl *vx, dbl *vy, bat *point_x, bat *point_y)
{
	BAT *bo = NULL, *bpx = NULL, *bpy;
	dbl *px = NULL, *py = NULL;
	BUN i = 0, cnt;
	int j = 0, nv;
	bit *cs = NULL;

	/*Get the BATs */
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
		throw(MAL, "geom.point", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}

	if ((bpy = BATdescriptor(*point_y)) == NULL) {
		BBPunfix(bpx->batCacheid);
		throw(MAL, "geom.point", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}

	/*Check BATs alignment */
	if (bpx->hseqbase != bpy->hseqbase || BATcount(bpx) != BATcount(bpy)) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", SQLSTATE(38000) "both point bats must have dense and aligned heads");
	}

	/*Create output BAT */
	if ((bo = COLnew(bpx->hseqbase, TYPE_bit, BATcount(bpx), TRANSIENT)) == NULL) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs */
	BATiter bpxi = bat_iterator(bpx);
	BATiter bpyi = bat_iterator(bpy);
	px = (dbl *) bpxi.base;
	py = (dbl *) bpyi.base;

	nv = nvert - 1;
	cnt = BATcount(bpx);
	cs = (bit *) Tloc(bo, 0);
	for (i = 0; i < cnt; i++) {
		int wn = 0;
		for (j = 0; j < nv; j++) {
			if (vy[j] <= py[i]) {
				if (vy[j + 1] > py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) > 0)
						++wn;
			} else {
				if (vy[j + 1] <= py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) < 0)
						--wn;
			}
		}
		*cs++ = wn & 1;
	}
	bat_iterator_end(&bpxi);
	bat_iterator_end(&bpyi);
	BATsetcount(bo, cnt);
	bo->tsorted = bo->trevsorted = false;
	bo->tkey = false;
	BBPunfix(bpx->batCacheid);
	BBPunfix(bpy->batCacheid);
	*out = bo->batCacheid;
	BBPkeepref(bo);
	return MAL_SUCCEED;
}

static str
pnpolyWithHoles(bat *out, int nvert, dbl *vx, dbl *vy, int nholes, dbl **hx, dbl **hy, int *hn, bat *point_x, bat *point_y)
{
	BAT *bo = NULL, *bpx = NULL, *bpy;
	dbl *px = NULL, *py = NULL;
	BUN i = 0, cnt = 0;
	int j = 0, h = 0;
	bit *cs = NULL;

	/*Get the BATs */
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
		throw(MAL, "geom.point", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}
	if ((bpy = BATdescriptor(*point_y)) == NULL) {
		BBPunfix(bpx->batCacheid);
		throw(MAL, "geom.point", SQLSTATE(38000) RUNTIME_OBJECT_MISSING);
	}

	/*Check BATs alignment */
	if (bpx->hseqbase != bpy->hseqbase || BATcount(bpx) != BATcount(bpy)) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", SQLSTATE(38000) "Geos both point bats must have dense and aligned heads");
	}

	/*Create output BAT */
	if ((bo = COLnew(bpx->hseqbase, TYPE_bit, BATcount(bpx), TRANSIENT)) == NULL) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs */
	BATiter bpxi = bat_iterator(bpx);
	BATiter bpyi = bat_iterator(bpy);
	px = (dbl *) bpxi.base;
	py = (dbl *) bpyi.base;
	cnt = BATcount(bpx);
	cs = (bit *) Tloc(bo, 0);
	for (i = 0; i < cnt; i++) {
		int wn = 0;

		/*First check the holes */
		for (h = 0; h < nholes; h++) {
			int nv = hn[h] - 1;
			wn = 0;
			for (j = 0; j < nv; j++) {
				if (hy[h][j] <= py[i]) {
					if (hy[h][j + 1] > py[i])
						if (isLeft(hx[h][j], hy[h][j], hx[h][j + 1], hy[h][j + 1], px[i], py[i]) > 0)
							++wn;
				} else {
					if (hy[h][j + 1] <= py[i])
						if (isLeft(hx[h][j], hy[h][j], hx[h][j + 1], hy[h][j + 1], px[i], py[i]) < 0)
							--wn;
				}
			}

			/*It is in one of the holes */
			if (wn) {
				break;
			}
		}

		if (wn)
			continue;

		/*If not in any of the holes, check inside the Polygon */
		for (j = 0; j < nvert - 1; j++) {
			if (vy[j] <= py[i]) {
				if (vy[j + 1] > py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) > 0)
						++wn;
			} else {
				if (vy[j + 1] <= py[i])
					if (isLeft(vx[j], vy[j], vx[j + 1], vy[j + 1], px[i], py[i]) < 0)
						--wn;
			}
		}
		*cs++ = wn & 1;
	}
	bat_iterator_end(&bpxi);
	bat_iterator_end(&bpyi);
	BATsetcount(bo, cnt);
	bo->tsorted = bo->trevsorted = false;
	bo->tkey = false;
	BBPunfix(bpx->batCacheid);
	BBPunfix(bpy->batCacheid);
	*out = bo->batCacheid;
	BBPkeepref(bo);
	return MAL_SUCCEED;
}

#define POLY_NUM_VERT 120
#define POLY_NUM_HOLE 10

str
wkbContains_point_bat(bat *out, wkb **a, bat *point_x, bat *point_y)
{
	double *vert_x, *vert_y, **holes_x = NULL, **holes_y = NULL;
	int *holes_n = NULL, j;
	wkb *geom = NULL;
	str err = MAL_SUCCEED;
	str geom_str = NULL;
	char *str2, *token, *subtoken;
	char *saveptr1 = NULL, *saveptr2 = NULL;
	int nvert = 0, nholes = 0;

	geom = (wkb *) *a;

	if ((err = wkbAsText(&geom_str, &geom, NULL)) != MAL_SUCCEED) {
		return err;
	}
	token = strchr(geom_str, '(');
	token += 2;

	/*Lets get the polygon */
	token = strtok_r(token, ")", &saveptr1);
	vert_x = GDKmalloc(POLY_NUM_VERT * sizeof(double));
	vert_y = GDKmalloc(POLY_NUM_VERT * sizeof(double));
	if (vert_x == NULL || vert_y == NULL) {
		err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	for (str2 = token;; str2 = NULL) {
		subtoken = strtok_r(str2, ",", &saveptr2);
		if (subtoken == NULL)
			break;
		sscanf(subtoken, "%lf %lf", &vert_x[nvert], &vert_y[nvert]);
		nvert++;
		if ((nvert % POLY_NUM_VERT) == 0) {
			double *tmp;
			tmp = GDKrealloc(vert_x, nvert * 2 * sizeof(double));
			if (tmp == NULL) {
				err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			vert_x = tmp;
			tmp = GDKrealloc(vert_y, nvert * 2 * sizeof(double));
			if (tmp == NULL) {
				err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			vert_y = tmp;
		}
	}

	token = strtok_r(NULL, ")", &saveptr1);
	if (token) {
		holes_x = GDKzalloc(POLY_NUM_HOLE * sizeof(double *));
		holes_y = GDKzalloc(POLY_NUM_HOLE * sizeof(double *));
		holes_n = GDKzalloc(POLY_NUM_HOLE * sizeof(int));
		if (holes_x == NULL || holes_y == NULL || holes_n == NULL) {
			err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
	}
	/*Lets get all the holes */
	while (token) {
		int nhole = 0;
		token = strchr(token, '(');
		if (!token)
			break;
		token++;

		if (holes_x[nholes] == NULL &&
		    (holes_x[nholes] = GDKzalloc(POLY_NUM_VERT * sizeof(double))) == NULL) {
			err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		if (holes_y[nholes] == NULL &&
		    (holes_y[nholes] = GDKzalloc(POLY_NUM_VERT * sizeof(double))) == NULL) {
			err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}

		for (str2 = token;; str2 = NULL) {
			subtoken = strtok_r(str2, ",", &saveptr2);
			if (subtoken == NULL)
				break;
			sscanf(subtoken, "%lf %lf", &holes_x[nholes][nhole], &holes_y[nholes][nhole]);
			nhole++;
			if ((nhole % POLY_NUM_VERT) == 0) {
				double *tmp;
				tmp = GDKrealloc(holes_x[nholes], nhole * 2 * sizeof(double));
				if (tmp == NULL) {
					err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				holes_x[nholes] = tmp;
				tmp = GDKrealloc(holes_y[nholes], nhole * 2 * sizeof(double));
				if (tmp == NULL) {
					err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				holes_y[nholes] = tmp;
			}
		}

		holes_n[nholes] = nhole;
		nholes++;
		if ((nholes % POLY_NUM_HOLE) == 0) {
			double **tmp;
			int *itmp;
			tmp = GDKrealloc(holes_x, nholes * 2 * sizeof(double *));
			if (tmp == NULL) {
				err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			holes_x = tmp;
			tmp = GDKrealloc(holes_y, nholes * 2 * sizeof(double *));
			if (tmp == NULL) {
				err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			holes_y = tmp;
			itmp = GDKrealloc(holes_n, nholes * 2 * sizeof(int));
			if (itmp == NULL) {
				err = createException(MAL, "geom.Contains", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			holes_n = itmp;
		}
		token = strtok_r(NULL, ")", &saveptr1);
	}

	if (nholes)
		err = pnpolyWithHoles(out, nvert, vert_x, vert_y, nholes, holes_x, holes_y, holes_n, point_x, point_y);
	else {
		err = pnpoly(out, nvert, vert_x, vert_y, point_x, point_y);
	}

  bailout:
	GDKfree(geom_str);
	GDKfree(vert_x);
	GDKfree(vert_y);
	if (holes_x && holes_y && holes_n) {
		for (j = 0; j < nholes; j++) {
			GDKfree(holes_x[j]);
			GDKfree(holes_y[j]);
		}
	}
	GDKfree(holes_x);
	GDKfree(holes_y);
	GDKfree(holes_n);

	return err;
}

str
wkbContains_point(bit *out, wkb **a, dbl *point_x, dbl *point_y)
{
	(void) a;
	(void) point_x;
	(void) point_y;
	*out = TRUE;
	return MAL_SUCCEED;
}

static str
geom_AsText_wkb(char **ret, wkb **w)
{
	return wkbAsText(ret, w, &(int){0});
}
static str
geom_AsEWKT_wkb(char **ret, wkb **w)
{
	return wkbAsText(ret, w, &(int){1});
}
static str
geom_GeomFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){0});
}
static str
geom_PointFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){1});
}
static str
geom_LineFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){2});
}
static str
geom_PolygonFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){4});
}
static str
geom_MPointFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){5});
}
static str
geom_MLineFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){6});
}
static str
geom_MPolyFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){7});
}
static str
geom_GeomCollFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbFromText(ret, wkt, srid, &(int){8});
}
static str
geom_GeomFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){0});
}
static str
geom_PointFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){1});
}
static str
geom_LineFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){2});
}
static str
geom_PolygonFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){4});
}
static str
geom_MPointFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){5});
}
static str
geom_MLineFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){6});
}
static str
geom_MPolyFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){7});
}
static str
geom_GeomCollFromText_str(wkb **ret, char **wkt)
{
	return wkbFromText(ret, wkt, &(int){0}, &(int){8});
}
static str
geom_NumInteriorRings_wkb(int *ret, wkb **w)
{
	return wkbNumRings(ret, w, &(int){0});
}
static str
geom_NRings_wkb(int *ret, wkb **w)
{
	return wkbNumRings(ret, w, &(int){1});
}
static str
geom_BdPolyFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbMLineStringToPolygon(ret, wkt, srid, &(int){0});
}
static str
geom_BdMPolyFromText_str_int(wkb **ret, char **wkt, int *srid)
{
	return wkbMLineStringToPolygon(ret, wkt, srid, &(int){1});
}
static str
geom_MakePoint_dbl_dbl(wkb **ret, dbl *x, dbl *y)
{
	return wkbMakePoint(ret, x, y, &(dbl){0}, &(dbl){0}, &(int){0});
}
static str
geom_MakePoint_dbl_dbl_dbl(wkb **ret, dbl *x, dbl *y, dbl *z)
{
	return wkbMakePoint(ret, x, y, z, &(dbl){0}, &(int){10});
}
static str
geom_MakePointM_dbl_dbl_dbl(wkb **ret, dbl *x, dbl *y, dbl *m)
{
	return wkbMakePoint(ret, x, y, &(dbl){0}, m, &(int){1});
}
static str
geom_MakePoint_dbl_dbl_dbl_dbl(wkb **ret, dbl *x, dbl *y, dbl *z, dbl *m)
{
	return wkbMakePoint(ret, x, y, z, m, &(int){11});
}
static str
geom_GeometryType1_wkb(char **ret, wkb **w)
{
	return wkbGeometryType(ret, w, &(int){0});
}
static str
geom_GeometryType2_wkb(char **ret, wkb **w)
{
	return wkbGeometryType(ret, w, &(int){1});
}
static str
geom_X_wkb(dbl *ret, wkb **w)
{
	return wkbGetCoordinate(ret, w, &(int){0});
}
static str
geom_Y_wkb(dbl *ret, wkb **w)
{
	return wkbGetCoordinate(ret, w, &(int){1});
}
static str
geom_Z_wkb(dbl *ret, wkb **w)
{
	return wkbGetCoordinate(ret, w, &(int){2});
}
static str
geom_Force2D_wkb(wkb **ret, wkb **g)
{
	return wkbForceDim(ret, g, &(int){2});
}
static str
geom_Force3D_wkb(wkb **ret, wkb **g)
{
	return wkbForceDim(ret, g, &(int){3});
}
static str
geom_Translate_wkb_dbl_dbl(wkb **ret, wkb **g, dbl *dx, dbl *dy)
{
	return wkbTranslate(ret, g, dx, dy, &(dbl){0});
}
static str
geom_Translate_wkb_dbl_dbl_dbl(wkb **ret, wkb **g, dbl *dx, dbl *dy, dbl *dz)
{
	return wkbTranslate(ret, g, dx, dy, dz);
}
static str
geom_NumPoints_wkb(int *ret, wkb **w)
{
	return wkbNumPoints(ret, w, &(int){1});
}
static str
geom_NPoints_wkb(int *ret, wkb **w)
{
	return wkbNumPoints(ret, w, &(int){0});
}
static str
geom_MakeEnvelope_dbl_dbl_dbl_dbl_int(wkb **ret, dbl *xmin, dbl *ymin, dbl *xmax, dbl *ymax, int *srid)
{
	return wkbEnvelopeFromCoordinates(ret, xmin, ymin, xmax, ymax, srid);
}
static str
geom_MakeEnvelope_dbl_dbl_dbl_dbl(wkb **ret, dbl *xmin, dbl *ymin, dbl *xmax, dbl *ymax)
{
	return wkbEnvelopeFromCoordinates(ret, xmin, ymin, xmax, ymax, &(int){0});
}
static str
geom_MakePolygon_wkb(wkb **ret, wkb **external)
{
	return wkbMakePolygon(ret, external, &(bat){bat_nil}, &(int){0});
}
static str
geom_MakePolygon_wkb_int(wkb **ret, wkb **external, int *srid)
{
	return wkbMakePolygon(ret, external, &(bat){bat_nil}, srid);
}
static str
geom_XMinFromWKB_wkb(dbl *ret, wkb **g)
{
	return wkbCoordinateFromWKB(ret, g, &(int){1});
}
static str
geom_YMinFromWKB_wkb(dbl *ret, wkb **g)
{
	return wkbCoordinateFromWKB(ret, g, &(int){2});
}
static str
geom_XMaxFromWKB_wkb(dbl *ret, wkb **g)
{
	return wkbCoordinateFromWKB(ret, g, &(int){3});
}
static str
geom_YMaxFromWKB_wkb(dbl *ret, wkb **g)
{
	return wkbCoordinateFromWKB(ret, g, &(int){4});
}
static str
geom_XMinFromMBR_mbr(dbl *ret, mbr **b)
{
	return wkbCoordinateFromMBR(ret, b, &(int){1});
}
static str
geom_YMinFromMBR_mbr(dbl *ret, mbr **b)
{
	return wkbCoordinateFromMBR(ret, b, &(int){2});
}
static str
geom_XMaxFromMBR_mbr(dbl *ret, mbr **b)
{
	return wkbCoordinateFromMBR(ret, b, &(int){3});
}
static str
geom_YMaxFromMBR_mbr(dbl *ret, mbr **b)
{
	return wkbCoordinateFromMBR(ret, b, &(int){4});
}
static str
calc_wkb_str_int_int(wkb **ret, str *wkt, int *srid, int *type)
{
	(void) srid;
	(void) type;
	return wkbFromText(ret, wkt, &(int){0}, &(int){0});
}

static str
batgeom_GeomFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){0});
}
static str
batgeom_PointFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){1});
}
static str
batgeom_LineFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){2});
}
static str
batgeom_PolygonFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){4});
}
static str
batgeom_MPointFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){5});
}
static str
batgeom_MLineFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){6});
}
static str
batgeom_MPolyFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){7});
}
static str
batgeom_GeomCollFromText_str_int(bat *ret, bat *wkt, int *srid)
{
	return wkbFromText_bat(ret, wkt, srid, &(int){8});
}
static str
batgeom_GeomFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){0});
}
static str
batgeom_PointFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){1});
}
static str
batgeom_LineFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){2});
}
static str
batgeom_PolygonFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){4});
}
static str
batgeom_MPointFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){5});
}
static str
batgeom_MLineFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){6});
}
static str
batgeom_MPolyFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){7});
}
static str
batgeom_GeomCollFromText_str(bat *ret, bat *wkt)
{
	return wkbFromText_bat(ret, wkt, &(int){0}, &(int){8});
}
static str
batgeom_AsText_wkb(bat *ret, bat *w)
{
	return wkbAsText_bat(ret, w, &(int){0});
}
static str
batgeom_AsEWKT_wkb(bat *ret, bat *w)
{
	return wkbAsText_bat(ret, w, &(int){1});
}
static str
batgeom_GeometryType1_wkb(bat *ret, bat *w)
{
	return wkbGeometryType_bat(ret, w, &(int){0});
}
static str
batgeom_GeometryType2_wkb(bat *ret, bat *w)
{
	return wkbGeometryType_bat(ret, w, &(int){1});
}
static str
batgeom_MakePoint_dbl_dbl(bat *ret, bat *x, bat *y)
{
	return wkbMakePoint_bat(ret, x, y, &(bat){bat_nil}, &(bat){bat_nil}, &(int){0});
}
static str
batgeom_MakePoint_dbl_dbl_dbl(bat *ret, bat *x, bat *y, bat *z)
{
	return wkbMakePoint_bat(ret, x, y, z, &(bat){bat_nil}, &(int){10});
}
static str
batgeom_MakePointM_dbl_dbl_dbl(bat *ret, bat *x, bat *y, bat *m)
{
	return wkbMakePoint_bat(ret, x, y, &(bat){bat_nil}, m, &(int){1});
}
static str
batgeom_MakePoint_dbl_dbl_dbl_dbl(bat *ret, bat *x, bat *y, bat *z, bat *m)
{
	return wkbMakePoint_bat(ret, x, y, z, m, &(int){11});
}
static str
batgeom_NumPoints_wkb(bat *ret, bat *w)
{
	return wkbNumPoints_bat(ret, w, &(int){1});
}
static str
batgeom_NPoints_wkb(bat *ret, bat *w)
{
	return wkbNumPoints_bat(ret, w, &(int){0});
}
static str
batgeom_X_wkb(bat *ret, bat *w)
{
	return wkbGetCoordinate_bat(ret, w, &(int){0});
}
static str
batgeom_Y_wkb(bat *ret, bat *w)
{
	return wkbGetCoordinate_bat(ret, w, &(int){1});
}
static str
batgeom_Z_wkb(bat *ret, bat *w)
{
	return wkbGetCoordinate_bat(ret, w, &(int){2});
}
static str
batgeom_NumInteriorRings_wkb(bat *ret, bat *w)
{
	return wkbNumRings_bat(ret, w, &(int){0});
}
static str
batgeom_NRings_wkb(bat *ret, bat *w)
{
	return wkbNumRings_bat(ret, w, &(int){1});
}
static str
batgeom_XMinFromWKB_wkb(bat *ret, bat *g)
{
	return wkbCoordinateFromWKB_bat(ret, g, &(int){1});
}
static str
batgeom_YMinFromWKB_wkb(bat *ret, bat *g)
{
	return wkbCoordinateFromWKB_bat(ret, g, &(int){2});
}
static str
batgeom_XMaxFromWKB_wkb(bat *ret, bat *g)
{
	return wkbCoordinateFromWKB_bat(ret, g, &(int){3});
}
static str
batgeom_YMaxFromWKB_wkb(bat *ret, bat *g)
{
	return wkbCoordinateFromWKB_bat(ret, g, &(int){4});
}
static str
batgeom_XMinFromMBR_mbr(bat *ret, bat *b)
{
	return wkbCoordinateFromMBR_bat(ret, b, &(int){1});
}
static str
batgeom_YMinFromMBR_mbr(bat *ret, bat *b)
{
	return wkbCoordinateFromMBR_bat(ret, b, &(int){2});
}
static str
batgeom_XMaxFromMBR_mbr(bat *ret, bat *b)
{
	return wkbCoordinateFromMBR_bat(ret, b, &(int){3});
}
static str
batgeom_YMaxFromMBR_mbr(bat *ret, bat *b)
{
	return wkbCoordinateFromMBR_bat(ret, b, &(int){4});
}

#include "mel.h"
static mel_atom geom_init_atoms[] = {
 { .name="mbr", .basetype="lng", .size=sizeof(mbr), .tostr=mbrTOSTR, .fromstr=mbrFROMSTR, .hash=mbrHASH, .null=mbrNULL, .cmp=mbrCOMP, .read=mbrREAD, .write=mbrWRITE, },
 { .name="wkb", .tostr=wkbTOSTR, .fromstr=wkbFROMSTR, .hash=wkbHASH, .null=wkbNULL, .cmp=wkbCOMP, .read=wkbREAD, .write=wkbWRITE, .put=wkbPUT, .del=wkbDEL, .length=wkbLENGTH, .heap=wkbHEAP, },
 { .name="wkba", .tostr=wkbaTOSTR, .fromstr=wkbaFROMSTR, .null=wkbaNULL, .hash=wkbaHASH, .cmp=wkbaCOMP, .read=wkbaREAD, .write=wkbaWRITE, .put=wkbaPUT, .del=wkbaDEL, .length=wkbaLENGTH, .heap=wkbaHEAP, },  { .cmp=NULL }
};
static mel_func geom_init_funcs[] = {
 //TODO Fill in descriptions
 command("geom", "CoversGeographic", wkbCoversGeographic, false, "TODO", args(1, 3, arg("", bit), arg("a", wkb), arg("b", wkb))),

 command("geom", "DistanceGeographic", wkbDistanceGeographic, false, "TODO", args(1, 3, arg("", dbl), arg("a", wkb), arg("b", wkb))),
 command("batgeom", "DistanceGeographic", wkbDistanceGeographic_bat, false, "TODO", args(1, 3, batarg("", dbl), batarg("a", wkb), batarg("b", wkb))),
 command("batgeom", "DistanceGeographic", wkbDistanceGeographic_bat_cand, false, "TODO", args(1, 5, batarg("", dbl), batarg("a", wkb), batarg("b", wkb), batarg("s1", oid), batarg("s2", oid))),

 //Filter functions
 command("geom", "DWithinGeographic", wkbDWithinGeographic, false, "TODO", args(1, 4, arg("", bit), arg("a", wkb), arg("b", wkb), arg("d", dbl))),
 command("geom", "DWithinGeographicselect", wkbDWithinGeographicSelect, false, "TODO", args(1, 6, batarg("", oid), batarg("b", wkb), batarg("s", oid), arg("c", wkb), arg("d", dbl),arg("anti",bit))),
 command("geom", "DWithinGeographicjoin", wkbDWithinGeographicJoin, false, "TODO", args(2, 10, batarg("lr",oid),batarg("rr",oid), batarg("a", wkb), batarg("b", wkb), batarg("d", dbl), batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 command("geom", "IntersectsGeographic", wkbIntersectsGeographic, false, "Returns true if the geographic Geometries intersect in any point", args(1, 3, arg("", bit), arg("a", wkb), arg("b", wkb))),
 command("geom", "IntersectsGeographicselect", wkbIntersectsGeographicSelect, false, "TODO", args(1, 5, batarg("", oid), batarg("b", wkb), batarg("s", oid), arg("c", wkb), arg("anti",bit))),
 command("geom", "IntersectsGeographicjoin", wkbIntersectsGeographicJoin, false, "TODO", args(2, 9, batarg("lr",oid),batarg("rr",oid), batarg("a", wkb), batarg("b", wkb), batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),

 command("rtree", "Intersects", wkbIntersects, false, "Returns true if these Geometries 'spatially intersect in 2D'", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("rtree", "Intersectsselect", wkbIntersectsSelectRTree, false, "TODO", args(1, 5, batarg("", oid), batarg("b", wkb), batarg("s", oid), arg("c", wkb), arg("anti",bit))),
 command("rtree", "Intersectsjoin", wkbIntersectsJoinRTree, false, "TODO", args(2, 9, batarg("lr",oid),batarg("rr",oid), batarg("a", wkb), batarg("b", wkb), batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),

 command("rtree", "DWithin", wkbDWithin, false, "Returns true if these Geometries 'spatially intersect in 2D'", args(1,4, arg("",bit),arg("a",wkb),arg("b",wkb),arg("dst",dbl))),
 command("rtree", "DWithinselect", wkbDWithinSelectRTree, false, "TODO", args(1, 6, batarg("", oid), batarg("b", wkb), batarg("s", oid), arg("c", wkb), arg("dst",dbl), arg("anti",bit))),
 command("rtree", "DWithinjoin", wkbDWithinJoinRTree, false, "TODO", args(2, 10, batarg("lr",oid),batarg("rr",oid), batarg("a", wkb), batarg("b", wkb), batarg("sl",oid),batarg("sr",oid), arg("dst",dbl),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),

 command("geom", "Intersects_noindex", wkbIntersects, false, "Returns true if these Geometries 'spatially intersect in 2D'", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Intersects_noindexselect", wkbIntersectsSelectNoIndex, false, "TODO", args(1, 5, batarg("", oid), batarg("b", wkb), batarg("s", oid), arg("c", wkb), arg("anti",bit))),
 command("geom", "Intersects_noindexjoin", wkbIntersectsJoinNoIndex, false, "TODO", args(2, 9, batarg("lr",oid),batarg("rr",oid), batarg("a", wkb), batarg("b", wkb), batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),

 command("geom", "DWithin_noindex", wkbDWithin, false, "Returns true if the two geometries are within the specifies distance from each other", args(1,4, arg("",bit),arg("a",wkb),arg("b",wkb),arg("dst",dbl))),
 command("geom", "DWithinselect_noindex", wkbDWithinSelectRTree, false, "TODO", args(1, 6, batarg("", oid), batarg("b", wkb), batarg("s", oid), arg("c", wkb), arg("dst",dbl), arg("anti",bit))),
 command("geom", "DWithinjoin_noindex", wkbDWithinJoinRTree, false, "TODO", args(2, 10, batarg("lr",oid),batarg("rr",oid), batarg("a", wkb), batarg("b", wkb), batarg("sl",oid),batarg("sr",oid), arg("dst",dbl),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),

 command("geom", "IntersectsMBR", mbrIntersects, false, "TODO", args(1,3, arg("",bit),arg("a",mbr),arg("b",mbr))),

 command("geom", "Collect", wkbCollect, false, "TODO", args(1,3, arg("",wkb),arg("a",wkb),arg("b",wkb))),

 command("aggr", "Collect", wkbCollectAggr, false, "TODO", args(1, 2, arg("", wkb), batarg("val", wkb))),
 command("aggr", "subCollect", wkbCollectAggrSubGrouped, false, "TODO", args(1, 5, batarg("", wkb), batarg("val", wkb), batarg("g", oid), batarg("e", oid), arg("skip_nils", bit))),
 command("aggr", "subCollect", wkbCollectAggrSubGroupedCand, false, "TODO", args(1, 6, batarg("", wkb), batarg("val", wkb), batarg("g", oid), batargany("e", 1), batarg("g", oid), arg("skip_nils", bit))),

 command("geom", "hasZ", geoHasZ, false, "returns 1 if the geometry has z coordinate", args(1,2, arg("",int),arg("flags",int))),
 command("geom", "hasM", geoHasM, false, "returns 1 if the geometry has m coordinate", args(1,2, arg("",int),arg("flags",int))),
 command("geom", "getType", geoGetType, false, "returns the str representation of the geometry type", args(1,3, arg("",str),arg("flags",int),arg("format",int))),

 command("geom", "MLineStringToPolygon", wkbMLineStringToPolygon, false, "Creates polygons using the MultiLineString provided as WKT. Depending on the flag creates one (flag=0) or multiple (flag=1) polygons", args(1,4, arg("",wkb),arg("wkt",str),arg("srid",int),arg("flag",int))),
 command("geom", "AsBinary", wkbAsBinary, false, "Returns the wkb representation into HEX format", args(1,2, arg("",str),arg("w",wkb))),
 command("geom", "FromBinary", wkbFromBinary, false, "Creates a wkb using the HEX representation", args(1,2, arg("",wkb),arg("w",str))),
 command("geom", "ToText", wkbAsText, false, "", args(1,3, arg("",str),arg("w",wkb),arg("withSRID",int))),
 command("geom", "FromText", wkbFromText, false, "", args(1,4, arg("",wkb),arg("wkt",str),arg("srid",int),arg("type",int))),
 command("geom", "NumRings", wkbNumRings, false, "Returns the number of interior rings+exterior on the first polygon of the geometry", args(1,3, arg("",int),arg("w",wkb),arg("exterior",int))),
 command("geom", "MakePointXYZM", wkbMakePoint, false, "creates a point using the coordinates", args(1,6, arg("",wkb),arg("x",dbl),arg("y",dbl),arg("z",dbl),arg("m",dbl),arg("zmFlag",int))),
 command("geom", "GeometryType", wkbGeometryType, false, "", args(1,3, arg("",str),arg("w",wkb),arg("flag",int))),
 command("geom", "GetCoordinate", wkbGetCoordinate, false, "Returns the coordinate at position idx of a point, or NULL if not available. idx=0 -> X, idx=1 -> Y, idx=2 -> Z. Input must be point", args(1,3, arg("",dbl),arg("w",wkb),arg("idx",int))),
 command("geom", "Boundary", wkbBoundary, false, "Returns the closure of the combinatorial boundary of the Geometry.", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "CoordDim", wkbCoordDim, false, "Return the coordinate dimension of the geometry", args(1,2, arg("",int),arg("w",wkb))),
 command("geom", "Dimension", wkbDimension, false, "The inherent dimension of this Geometry object, which must be less than or equal to the coordinate dimension.", args(1,2, arg("",int),arg("w",wkb))),
 command("geom", "getSRID", wkbGetSRID, false, "Returns the Spatial Reference System ID for this Geometry.", args(1,2, arg("",int),arg("w",wkb))),
 command("geom", "setSRID", wkbSetSRID, false, "Sets the Reference System ID for this Geometry.", args(1,3, arg("",wkb),arg("w",wkb),arg("srid",int))),
 command("geom", "StartPoint", wkbStartPoint, false, "Returns the first point of a LINESTRING geometry as a POINT or NULL if the input parameter is not a LINESTRING", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "EndPoint", wkbEndPoint, false, "Returns the last point of a LINESTRING geometry as a POINT or NULL if the input parameter is not a LINESTRING.", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "PointN", wkbPointN, false, "Returns the n-th point of the Geometry. Argument w should be Linestring.", args(1,3, arg("",wkb),arg("w",wkb),arg("n",int))),
 command("geom", "Envelope", wkbEnvelope, false, "The minimum bounding box for this Geometry, returned as a Geometry. The polygon is defined by the corner points of the bounding box ((MINX,MINY),(MAXX,MINY),(MAXX,MAXY),(MINX,MAXY)).", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "EnvelopeFromCoordinates", wkbEnvelopeFromCoordinates, false, "A polygon created by the provided coordinates", args(1,6, arg("",wkb),arg("",dbl),arg("",dbl),arg("",dbl),arg("",dbl),arg("",int))),
 command("geom", "Polygon", wkbMakePolygon, false, "Returns a Polygon created from the provided LineStrings", args(1,4, arg("",wkb),arg("",wkb),batarg("",wkb),arg("",int))),
 command("geom", "ExteriorRing", wkbExteriorRing, false, "Returns a line string representing the exterior ring of the POLYGON geometry. Return NULL if the geometry is not a polygon.", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "InteriorRingN", wkbInteriorRingN, false, "Return the Nth interior linestring ring of the polygon geometry. Return NULL if the geometry is not a polygon or the given N is out of range.", args(1,3, arg("",wkb),arg("w",wkb),arg("n",int))),
 command("geom", "InteriorRings", wkbInteriorRings, false, "Returns an 'array' with all the interior rings of the polygon", args(1,2, arg("",wkba),arg("w",wkb))),
 command("geom", "IsClosed", wkbIsClosed, false, "Returns TRUE if the LINESTRING's start and end points are coincident.", args(1,2, arg("",bit),arg("w",wkb))),
 command("geom", "IsEmpty", wkbIsEmpty, false, "Returns true if this Geometry is an empty geometry.", args(1,2, arg("",bit),arg("w",wkb))),
 command("geom", "IsRing", wkbIsRing, false, "Returns TRUE if this LINESTRING is both closed and simple.", args(1,2, arg("",bit),arg("w",wkb))),
 command("geom", "IsSimple", wkbIsSimple, false, "Returns (TRUE) if this Geometry has no anomalous geometric points, such as self intersection or self tangency.", args(1,2, arg("",bit),arg("w",wkb))),
 command("geom", "IsValid", wkbIsValid, false, "Returns true if the ST_Geometry is well formed.", args(1,2, arg("",bit),arg("w",wkb))),
 command("geom", "IsValidReason", wkbIsValidReason, false, "Returns text stating if a geometry is valid or not and if not valid, a reason why.", args(1,2, arg("",str),arg("w",wkb))),
 command("geom", "IsValidDetail", wkbIsValidDetail, false, "Returns a valid_detail (valid,reason,location) row stating if a geometry is valid or not and if not valid, a reason why and a location where.", args(1,2, arg("",str),arg("w",wkb))),
 command("geom", "Area", wkbArea, false, "Returns the area of the surface if it is a polygon or multi-polygon", args(1,2, arg("",dbl),arg("w",wkb))),
 command("geom", "Centroid", wkbCentroid, false, "Computes the geometric center of a geometry, or equivalently, the center of mass of the geometry as a POINT.", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "Distance", wkbDistance, false, "Returns the 2-dimensional minimum cartesian distance between the two geometries in projected units (spatial ref units.", args(1,3, arg("",dbl),arg("a",wkb),arg("b",wkb))),
 command("geom", "Length", wkbLength, false, "Returns the cartesian 2D length of the geometry if it is a linestring or multilinestring", args(1,2, arg("",dbl),arg("w",wkb))),
 command("geom", "ConvexHull", wkbConvexHull, false, "Returns a geometry that represents the convex hull of this geometry. The convex hull of a geometry represents the minimum convex geometry that encloses all geometries within the set.", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "Intersection", wkbIntersection, false, "Returns a geometry that represents the point set intersection of the Geometries a, b", args(1,3, arg("",wkb),arg("a",wkb),arg("b",wkb))),
 command("geom", "Union", wkbUnion, false, "Returns a geometry that represents the point set union of the Geometries a, b", args(1,3, arg("",wkb),arg("a",wkb),arg("b",wkb))),
 command("geom", "Union", wkbUnionAggr, false, "Gets a BAT with geometries and returns their union", args(1,2, arg("",wkb),batarg("a",wkb))),
 command("geom", "Difference", wkbDifference, false, "Returns a geometry that represents that part of geometry A that does not intersect with geometry B", args(1,3, arg("",wkb),arg("a",wkb),arg("b",wkb))),
 command("geom", "SymDifference", wkbSymDifference, false, "Returns a geometry that represents the portions of A and B that do not intersect", args(1,3, arg("",wkb),arg("a",wkb),arg("b",wkb))),
 command("geom", "Buffer", wkbBuffer, false, "Returns a geometry that represents all points whose distance from this geometry is less than or equal to distance. Calculations are in the Spatial Reference System of this Geometry.", args(1,3, arg("",wkb),arg("a",wkb),arg("distance",dbl))),
 command("geom", "Contains", wkbContains, false, "Returns true if and only if no points of B lie in the exterior of A, and at least one point of the interior of B lies in the interior of A.", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Crosses", wkbCrosses, false, "Returns TRUE if the supplied geometries have some, but not all, interior points in common.", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Disjoint", wkbDisjoint, false, "Returns true if these Geometries are 'spatially disjoint'", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Equals", wkbEquals, false, "Returns true if the given geometries represent the same geometry. Directionality is ignored.", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Overlaps", wkbOverlaps, false, "Returns TRUE if the Geometries intersect but are not completely contained by each other.", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Relate", wkbRelate, false, "Returns true if the Geometry a 'spatially related' to Geometry b, by testing for intersection between the Interior, Boundary and Exterior of the two geometries as specified by the values in the intersectionPatternMatrix.", args(1,4, arg("",bit),arg("a",wkb),arg("b",wkb),arg("intersection_matrix_pattern",str))),
 command("geom", "Touches", wkbTouches, false, "Returns TRUE if the geometries have at least one point in common, but their interiors do not intersect.", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Within", wkbWithin, false, "Returns TRUE if the geometry A is completely inside geometry B", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "Covers", wkbCovers, false, "Returns TRUE if no point of geometry B is outside geometry A", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "CoveredBy", wkbCoveredBy, false, "Returns TRUE if no point of geometry A is outside geometry B", args(1,3, arg("",bit),arg("a",wkb),arg("b",wkb))),
 command("geom", "DWithin2", wkbDWithinMbr, false, "" /* <<< desc TODO */, args(1,6, arg("",bit),arg("a",wkb),arg("b",wkb),arg("a_mbr",mbr),arg("b_mbr",mbr),arg("dst",dbl))),
 command("geom", "GeometryN", wkbGeometryN, false, "Returns the 1-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING, MULTICURVE or (MULTI)POLYGON. Otherwise, return NULL", args(1,3, arg("",wkb),arg("g",wkb),arg("n",int))),
 command("geom", "NumGeometries", wkbNumGeometries, false, "Returns the number of geometries", args(1,2, arg("",int),arg("g",wkb))),
 command("geom", "Transform", wkbTransform, false, "Transforms a geometry from one srid to another", args(1,6, arg("",wkb),arg("g",wkb),arg("srid_src",int),arg("srid_dst",int),arg("proj_src",str),arg("proj_dest",str))),
 command("geom", "DelaunayTriangles", wkbDelaunayTriangles, false, "Returns a Delaunay triangulation, flag=0 => collection of polygons, flag=1 => multilinestring", args(1,4, arg("",wkb),arg("a",wkb),arg("tolerance",dbl),arg("flag",int))),
 command("geom", "Dump", wkbDump, false, "Gets a MultiPolygon and returns the Polygons in it", args(2,3, batarg("id",str),batarg("geom",wkb),arg("a",wkb))),
 command("geom", "DumpPoints", wkbDumpPoints, false, "Gets a Geometry and returns the Points in it", args(2,3, batarg("id",str),batarg("geom",wkb),arg("a",wkb))),
 command("geom", "Segmentize", wkbSegmentize, false, "It creates a new geometry with all segments on it smaller or equal to sz", args(1,3, arg("",wkb),arg("g",wkb),arg("sz",dbl))),
 command("geom", "ForceDimensions", wkbForceDim, false, "Removes or Adds additional coordinates in the geometry to make it d dimensions", args(1,3, arg("",wkb),arg("g",wkb),arg("d",int))),
 command("geom", "Contains", wkbContains_point, false, "Returns true if the Geometry a 'spatially contains' Geometry b", args(1,4, arg("",bit),arg("a",wkb),arg("x",dbl),arg("y",dbl))),
 command("geom", "Translate3D", wkbTranslate, false, "Moves all points of the geometry by dx, dy, dz", args(1,5, arg("",wkb),arg("g",wkb),arg("dx",dbl),arg("dy",dbl),arg("dz",dbl))),
 command("geom", "Contains", wkbContains_point_bat, false, "Returns true if the Geometry-BAT a 'spatially contains' Geometry-B b", args(1,4, batarg("",bit),arg("a",wkb),batarg("px",dbl),batarg("py",dbl))),
 command("geom", "PointsNum", wkbNumPoints, false, "The number of points in the Geometry. If check=1, the geometry should be a linestring", args(1,3, arg("",int),arg("w",wkb),arg("check",int))),
 command("geom", "MakeLine", wkbMakeLine, false, "Gets two point or linestring geometries and returns a linestring geometry", args(1,3, arg("",wkb),arg("a",wkb),arg("b",wkb))),
 command("aggr", "MakeLine", wkbMakeLineAggr, false, "Gets a BAT with point or linestring geometries and returns a single linestring geometry", args(1,2, arg("",wkb),batarg("a",wkb))),
 command("aggr", "subMakeLine", wkbMakeLineAggrSubGrouped, false, "TODO", args(1, 5, batarg("", wkb), batarg("val", wkb), batarg("g", oid), batarg("e", oid), arg("skip_nils", bit))),
 command("aggr", "subMakeLine", wkbMakeLineAggrSubGroupedCand, false, "TODO", args(1, 6, batarg("", wkb), batarg("val", wkb), batarg("g", oid), batargany("e", 1), batarg("g", oid), arg("skip_nils", bit))),
 command("geom", "PointOnSurface", wkbPointOnSurface, false, "Returns a point guaranteed to lie on the surface. Similar to postGIS it works for points and lines in addition to surfaces and for 3d geometries.", args(1,2, arg("",wkb),arg("w",wkb))),
 command("geom", "mbr", wkbMBR, false, "Creates the mbr for the given wkb.", args(1,2, arg("",mbr),arg("",wkb))),
 command("geom", "MakeBox2D", wkbBox2D, false, "Creates an mbr from the two 2D points", args(1,3, arg("",mbr),arg("",wkb),arg("",wkb))),
 command("geom", "mbrOverlaps", mbrOverlaps_wkb, false, "Returns true if the mbr of geom1 overlaps the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrOverlaps", mbrOverlaps, false, "Returns true if box1 overlaps box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrOverlapOrLeft", mbrOverlapOrLeft_wkb, false, "Returns true if the mbr of geom1 overlaps or is to the left of thr mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrOverlapOrLeft", mbrOverlapOrLeft, false, "Returns true if box1 overlaps or is to the left of box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrOverlapOrBelow", mbrOverlapOrBelow_wkb, false, "Returns true if the mbr of geom1 overlaps or is below the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrOverlapOrBelow", mbrOverlapOrBelow, false, "Returns true if box1 overlaps or is below box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrOverlapOrRight", mbrOverlapOrRight_wkb, false, "Returns true if the mbr of geom1 overlalps or is right of the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrOverlapOrRight", mbrOverlapOrRight, false, "Returns true if box1 overlalps or is right of box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrLeft", mbrLeft_wkb, false, "Returns true if the mbr of geom1 is left of the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrLeft", mbrLeft, false, "Returns true if box1 is left of box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrBelow", mbrBelow_wkb, false, "Returns true if the mbr of geom1 is below the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrBelow", mbrBelow, false, "Returns true if box1 is below box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrEqual", mbrEqual_wkb, false, "Returns true if the mbr of geom1 is the same as the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrEqual", mbrEqual, false, "Returns true if box1 is the same as box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrRight", mbrRight_wkb, false, "Returns true if the mbr of geom1 is right of the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrRight", mbrRight, false, "Returns true if box1 is right of box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrContained", mbrContained_wkb, false, "Returns true if the mbr of geom1 is contained by the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrContained", mbrContained, false, "Returns true if box1 is contained by box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrOverlapOrAbove", mbrOverlapOrAbove_wkb, false, "Returns true if the mbr of geom1 overlaps or is above the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrOverlapOrAbove", mbrOverlapOrAbove, false, "Returns true if box1 overlaps or is above box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrAbove", mbrAbove_wkb, false, "Returns true if the mbr of geom1 is above the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrAbove", mbrAbove, false, "Returns true if box1 is above box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrContains", mbrContains_wkb, false, "Returns true if the mbr of geom1 contains the mbr of geom2", args(1,3, arg("",bit),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrContains", mbrContains, false, "Returns true if box1 contains box2", args(1,3, arg("",bit),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "mbrDistance", mbrDistance_wkb, false, "Returns the distance of the centroids of the mbrs of the two geometries", args(1,3, arg("",dbl),arg("geom1",wkb),arg("geom2",wkb))),
 command("geom", "mbrDistance", mbrDistance, false, "Returns the distance of the centroids of the two boxes", args(1,3, arg("",dbl),arg("box1",mbr),arg("box2",mbr))),
 command("geom", "coordinateFromWKB", wkbCoordinateFromWKB, false, "returns xmin (=1), ymin (=2), xmax (=3) or ymax(=4) of the provided geometry", args(1,3, arg("",dbl),arg("",wkb),arg("",int))),
 command("geom", "coordinateFromMBR", wkbCoordinateFromMBR, false, "returns xmin (=1), ymin (=2), xmax (=3) or ymax(=4) of the provided mbr", args(1,3, arg("",dbl),arg("",mbr),arg("",int))),
 command("geom", "epilogue", geom_epilogue, false, "", args(1,1, arg("",void))),
 command("batgeom", "FromText", wkbFromText_bat, false, "", args(1,4, batarg("",wkb),batarg("wkt",str),arg("srid",int),arg("type",int))),
 command("batgeom", "ToText", wkbAsText_bat, false, "", args(1,3, batarg("",str),batarg("w",wkb),arg("withSRID",int))),
 command("batgeom", "GeometryType", wkbGeometryType_bat, false, "", args(1,3, batarg("",str),batarg("w",wkb),arg("flag",int))),
 command("batgeom", "MakePointXYZM", wkbMakePoint_bat, false, "creates a point using the coordinates", args(1,6, batarg("",wkb),batarg("x",dbl),batarg("y",dbl),batarg("z",dbl),batarg("m",dbl),arg("zmFlag",int))),
 command("batgeom", "PointsNum", wkbNumPoints_bat, false, "The number of points in the Geometry. If check=1, the geometry should be a linestring", args(1,3, batarg("",int),batarg("w",wkb),arg("check",int))),
 command("batgeom", "GetCoordinate", wkbGetCoordinate_bat, false, "Returns the coordinate at position idx of a point, or NULL if not available. idx=0 -> X, idx=1 -> Y, idx=2 -> Z. Input must be point", args(1,3, batarg("",dbl),batarg("w",wkb),arg("idx",int))),
 command("batgeom", "GeometryN", wkbGeometryN_bat, false, "Returns the 1-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING, MULTICURVE or (MULTI)POLYGON. Otherwise, return NULL", args(1,3, batarg("",wkb),batarg("w",wkb),arg("n",int))),
 command("batgeom", "NumGeometries", wkbNumGeometries_bat, false, "Returns the number of geometries", args(1,2, batarg("",int),batarg("w",wkb))),
 command("batgeom", "NumRings", wkbNumRings_bat, false, "Returns the number of interior rings+exterior on the first polygon of the geometry", args(1,3, batarg("",int),batarg("w",wkb),arg("exterior",int))),
 command("batgeom", "Boundary", wkbBoundary_bat, false, "", args(1,2, batarg("",wkb),batarg("w",wkb))),
 command("batgeom", "IsClosed", wkbIsClosed_bat, false, "", args(1,2, batarg("",bit),batarg("w",wkb))),
 command("batgeom", "IsEmpty", wkbIsEmpty_bat, false, "", args(1,2, batarg("",bit),batarg("w",wkb))),
 command("batgeom", "IsSimple", wkbIsSimple_bat, false, "", args(1,2, batarg("",bit),batarg("w",wkb))),
 command("batgeom", "IsRing", wkbIsRing_bat, false, "", args(1,2, batarg("",bit),batarg("w",wkb))),
 command("batgeom", "IsValid", wkbIsValid_bat, false, "", args(1,2, batarg("",bit),batarg("w",wkb))),
 command("batgeom", "MakeBox2D", wkbBox2D_bat, false, "", args(1,3, batarg("",mbr),batarg("p1",wkb),batarg("p2",wkb))),
 command("batgeom", "Dimension", wkbDimension_bat, false, "", args(1,2, batarg("",int),batarg("w",wkb))),
 command("batgeom", "Distance", wkbDistance_bat, false, "", args(1,3, batarg("",dbl),batarg("a",wkb),batarg("b",wkb))),
 command("batgeom", "Distance", wkbDistance_geom_bat, false, "", args(1,3, batarg("",dbl),arg("a",wkb),batarg("b",wkb))),
 command("batgeom", "Distance", wkbDistance_bat_geom, false, "", args(1,3, batarg("",dbl),batarg("a",wkb),arg("b",wkb))),
 command("batgeom", "Contains", wkbContains_bat, false, "", args(1,3, batarg("",bit),batarg("a",wkb),batarg("b",wkb))),
 command("batgeom", "Contains", wkbContains_geom_bat, false, "", args(1,3, batarg("",bit),arg("a",wkb),batarg("b",wkb))),
 command("batgeom", "Contains", wkbContains_bat_geom, false, "", args(1,3, batarg("",bit),batarg("a",wkb),arg("b",wkb))),
 command("batgeom", "Filter", wkbFilter_geom_bat, false, "Filters the points in the bats according to the MBR of the other bat.", args(1,3, batarg("",wkb),arg("a",wkb),batarg("b",wkb))),
 command("batgeom", "Filter", wkbFilter_bat_geom, false, "", args(1,3, batarg("",wkb),batarg("a",wkb),arg("b",wkb))),
 command("batgeom", "setSRID", wkbSetSRID_bat, false, "Sets the Reference System ID for this Geometry.", args(1,3, batarg("",wkb),batarg("w",wkb),arg("srid",int))),
 command("batgeom", "MakeLine", wkbMakeLine_bat, false, "Gets two BATS of point or linestring geometries and returns a bat with linestring geometries", args(1,3, batarg("",wkb),batarg("a",wkb),batarg("b",wkb))),
 command("batgeom", "Union", wkbUnion_bat, false, "Gets two BATS of geometries and returns the pairwise unions", args(1,3, batarg("",wkb),batarg("a",wkb),batarg("b",wkb))),
 command("batgeom", "mbr", wkbMBR_bat, false, "Creates the mbr for the given wkb.", args(1,2, batarg("",mbr),batarg("",wkb))),
 command("batgeom", "coordinateFromWKB", wkbCoordinateFromWKB_bat, false, "returns xmin (=1), ymin (=2), xmax (=3) or ymax(=4) of the provided geometry", args(1,3, batarg("",dbl),batarg("",wkb),arg("",int))),
 command("batgeom", "coordinateFromMBR", wkbCoordinateFromMBR_bat, false, "returns xmin (=1), ymin (=2), xmax (=3) or ymax(=4) of the provided mbr", args(1,3, batarg("",dbl),batarg("",mbr),arg("",int))),
 command("batgeom", "Transform", wkbTransform_bat, false, "Transforms a bat of geometries from one srid to another", args(1,6, batarg("",wkb),batarg("g",wkb),arg("srid_src",int),arg("srid_dst",int),arg("proj_src",str),arg("proj_dest",str))),
 command("calc", "mbr", mbrFromString, false, "", args(1,2, arg("",mbr),arg("v",str))),
 command("calc", "mbr", mbrFromMBR, false, "", args(1,2, arg("",mbr),arg("v",mbr))),
 command("calc", "wkb", wkbFromWKB, false, "It is called when adding a new geometry column to an existing table", args(1,2, arg("",wkb),arg("v",wkb))),
 command("calc", "wkb", geom_2_geom, false, "Called when inserting values to a table in order to check if the inserted geometries are of the same type and srid required by the column definition", args(1,4, arg("",wkb),arg("geo",wkb),arg("columnType",int),arg("columnSRID",int))),
 command("batcalc", "wkb", geom_2_geom_bat, false, "Called when inserting values to a table in order to check if the inserted geometries are of the same type and srid required by the column definition", args(1,5, batarg("",wkb),batarg("geo",wkb),batarg("s",oid),arg("columnType",int),arg("columnSRID",int))),
 command("batcalc", "wkb", wkbFromText_bat_cand, false, "", args(1,5, batarg("",wkb),batarg("wkt",str),batarg("s",oid),arg("srid",int),arg("type",int))),
 command("geom", "AsText", geom_AsText_wkb, false, "", args(1,2, arg("",str),arg("w",wkb))),
 command("geom", "AsEWKT", geom_AsEWKT_wkb, false, "", args(1,2, arg("",str),arg("w",wkb))),
 command("geom", "GeomFromText", geom_GeomFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "PointFromText", geom_PointFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "LineFromText", geom_LineFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "PolygonFromText", geom_PolygonFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "MPointFromText", geom_MPointFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "MLineFromText", geom_MLineFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "MPolyFromText", geom_MPolyFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "GeomCollFromText", geom_GeomCollFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "GeomFromText", geom_GeomFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "PointFromText", geom_PointFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "LineFromText", geom_LineFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "PolygonFromText", geom_PolygonFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "MPointFromText", geom_MPointFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "MLineFromText", geom_MLineFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "MPolyFromText", geom_MPolyFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "GeomCollFromText", geom_GeomCollFromText_str, false, "", args(1,2, arg("",wkb),arg("wkt",str))),
 command("geom", "NumInteriorRings", geom_NumInteriorRings_wkb, false, "", args(1,2, arg("",int),arg("w",wkb))),
 command("geom", "NRings", geom_NRings_wkb, false, "", args(1,2, arg("",int),arg("w",wkb))),
 command("geom", "BdPolyFromText", geom_BdPolyFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "BdMPolyFromText", geom_BdMPolyFromText_str_int, false, "", args(1,3, arg("",wkb),arg("wkt",str),arg("srid",int))),
 command("geom", "MakePoint", geom_MakePoint_dbl_dbl, false, "", args(1,3, arg("",wkb),arg("x",dbl),arg("y",dbl))),
 command("geom", "MakePoint", geom_MakePoint_dbl_dbl_dbl, false, "", args(1,4, arg("",wkb),arg("x",dbl),arg("y",dbl),arg("z",dbl))),
 command("geom", "MakePointM", geom_MakePointM_dbl_dbl_dbl, false, "", args(1,4, arg("",wkb),arg("x",dbl),arg("y",dbl),arg("m",dbl))),
 command("geom", "MakePoint", geom_MakePoint_dbl_dbl_dbl_dbl, false, "", args(1,5, arg("",wkb),arg("x",dbl),arg("y",dbl),arg("z",dbl),arg("m",dbl))),
 command("geom", "GeometryType1", geom_GeometryType1_wkb, false, "", args(1,2, arg("",str),arg("w",wkb))),
 command("geom", "GeometryType2", geom_GeometryType2_wkb, false, "", args(1,2, arg("",str),arg("w",wkb))),
 command("geom", "X", geom_X_wkb, false, "", args(1,2, arg("",dbl),arg("w",wkb))),
 command("geom", "Y", geom_Y_wkb, false, "", args(1,2, arg("",dbl),arg("w",wkb))),
 command("geom", "Z", geom_Z_wkb, false, "", args(1,2, arg("",dbl),arg("w",wkb))),
 command("geom", "Force2D", geom_Force2D_wkb, false, "", args(1,2, arg("",wkb),arg("g",wkb))),
 command("geom", "Force3D", geom_Force3D_wkb, false, "", args(1,2, arg("",wkb),arg("g",wkb))),
 command("geom", "Translate", geom_Translate_wkb_dbl_dbl, false, "", args(1,4, arg("",wkb),arg("g",wkb),arg("dx",dbl),arg("dy",dbl))),
 command("geom", "Translate", geom_Translate_wkb_dbl_dbl_dbl, false, "", args(1,5, arg("",wkb),arg("g",wkb),arg("dx",dbl),arg("dy",dbl),arg("dz",dbl))),
 command("geom", "NumPoints", geom_NumPoints_wkb, false, "", args(1,2, arg("",int),arg("w",wkb))),
 command("geom", "NPoints", geom_NPoints_wkb, false, "", args(1,2, arg("",int),arg("w",wkb))),
 command("geom", "MakeEnvelope", geom_MakeEnvelope_dbl_dbl_dbl_dbl_int, false, "", args(1,6, arg("",wkb),arg("xmin",dbl),arg("ymin",dbl),arg("xmax",dbl),arg("ymax",dbl),arg("srid",int))),
 command("geom", "MakeEnvelope", geom_MakeEnvelope_dbl_dbl_dbl_dbl, false, "", args(1,5, arg("",wkb),arg("xmin",dbl),arg("ymin",dbl),arg("xmax",dbl),arg("ymax",dbl))),
 command("geom", "MakePolygon", geom_MakePolygon_wkb, false, "", args(1,2, arg("",wkb),arg("external",wkb))),
 command("geom", "MakePolygon", geom_MakePolygon_wkb_int, false, "", args(1,3, arg("",wkb),arg("external",wkb),arg("srid",int))),
 command("geom", "XMinFromWKB", geom_XMinFromWKB_wkb, false, "", args(1,2, arg("",dbl),arg("g",wkb))),
 command("geom", "YMinFromWKB", geom_YMinFromWKB_wkb, false, "", args(1,2, arg("",dbl),arg("g",wkb))),
 command("geom", "XMaxFromWKB", geom_XMaxFromWKB_wkb, false, "", args(1,2, arg("",dbl),arg("g",wkb))),
 command("geom", "YMaxFromWKB", geom_YMaxFromWKB_wkb, false, "", args(1,2, arg("",dbl),arg("g",wkb))),
 command("geom", "XMinFromMBR", geom_XMinFromMBR_mbr, false, "", args(1,2, arg("",dbl),arg("b",mbr))),
 command("geom", "YMinFromMBR", geom_YMinFromMBR_mbr, false, "", args(1,2, arg("",dbl),arg("b",mbr))),
 command("geom", "XMaxFromMBR", geom_XMaxFromMBR_mbr, false, "", args(1,2, arg("",dbl),arg("b",mbr))),
 command("geom", "YMaxFromMBR", geom_YMaxFromMBR_mbr, false, "", args(1,2, arg("",dbl),arg("b",mbr))),
 command("calc", "wkb", calc_wkb_str_int_int, false, "", args(1,4, arg("",wkb),arg("wkt",str),arg("srid",int),arg("type",int))),
 command("batgeom", "GeomFromText", batgeom_GeomFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "PointFromText", batgeom_PointFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "LineFromText", batgeom_LineFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "PolygonFromText", batgeom_PolygonFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "MPointFromText", batgeom_MPointFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "MLineFromText", batgeom_MLineFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "MPolyFromText", batgeom_MPolyFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "GeomCollFromText", batgeom_GeomCollFromText_str_int, false, "", args(1,3, batarg("",wkb),batarg("wkt",str),arg("srid",int))),
 command("batgeom", "GeomFromText", batgeom_GeomFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "PointFromText", batgeom_PointFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "LineFromText", batgeom_LineFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "PolygonFromText", batgeom_PolygonFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "MPointFromText", batgeom_MPointFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "MLineFromText", batgeom_MLineFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "MPolyFromText", batgeom_MPolyFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "GeomCollFromText", batgeom_GeomCollFromText_str, false, "", args(1,2, batarg("",wkb),batarg("wkt",str))),
 command("batgeom", "AsText", batgeom_AsText_wkb, false, "", args(1,2, batarg("",str),batarg("w",wkb))),
 command("batgeom", "AsEWKT", batgeom_AsEWKT_wkb, false, "", args(1,2, batarg("",str),batarg("w",wkb))),
 command("batgeom", "GeometryType1", batgeom_GeometryType1_wkb, false, "", args(1,2, batarg("",str),batarg("w",wkb))),
 command("batgeom", "GeometryType2", batgeom_GeometryType2_wkb, false, "", args(1,2, batarg("",str),batarg("w",wkb))),
 command("batgeom", "MakePoint", batgeom_MakePoint_dbl_dbl, false, "", args(1,3, batarg("",wkb),batarg("x",dbl),batarg("y",dbl))),
 command("batgeom", "MakePoint", batgeom_MakePoint_dbl_dbl_dbl, false, "", args(1,4, batarg("",wkb),batarg("x",dbl),batarg("y",dbl),batarg("z",dbl))),
 command("batgeom", "MakePointM", batgeom_MakePointM_dbl_dbl_dbl, false, "", args(1,4, batarg("",wkb),batarg("x",dbl),batarg("y",dbl),batarg("m",dbl))),
 command("batgeom", "MakePoint", batgeom_MakePoint_dbl_dbl_dbl_dbl, false, "", args(1,5, batarg("",wkb),batarg("x",dbl),batarg("y",dbl),batarg("z",dbl),batarg("m",dbl))),
 command("batgeom", "NumPoints", batgeom_NumPoints_wkb, false, "", args(1,2, batarg("",int),batarg("w",wkb))),
 command("batgeom", "NPoints", batgeom_NPoints_wkb, false, "", args(1,2, batarg("",int),batarg("w",wkb))),
 command("batgeom", "X", batgeom_X_wkb, false, "", args(1,2, batarg("",dbl),batarg("w",wkb))),
 command("batgeom", "Y", batgeom_Y_wkb, false, "", args(1,2, batarg("",dbl),batarg("w",wkb))),
 command("batgeom", "Z", batgeom_Z_wkb, false, "", args(1,2, batarg("",dbl),batarg("w",wkb))),
 command("batgeom", "NumInteriorRings", batgeom_NumInteriorRings_wkb, false, "", args(1,2, batarg("",int),batarg("w",wkb))),
 command("batgeom", "NRings", batgeom_NRings_wkb, false, "", args(1,2, batarg("",int),batarg("w",wkb))),
 command("batgeom", "XMinFromWKB", batgeom_XMinFromWKB_wkb, false, "", args(1,2, batarg("",dbl),batarg("g",wkb))),
 command("batgeom", "YMinFromWKB", batgeom_YMinFromWKB_wkb, false, "", args(1,2, batarg("",dbl),batarg("g",wkb))),
 command("batgeom", "XMaxFromWKB", batgeom_XMaxFromWKB_wkb, false, "", args(1,2, batarg("",dbl),batarg("g",wkb))),
 command("batgeom", "YMaxFromWKB", batgeom_YMaxFromWKB_wkb, false, "", args(1,2, batarg("",dbl),batarg("g",wkb))),
 command("batgeom", "XMinFromMBR", batgeom_XMinFromMBR_mbr, false, "", args(1,2, batarg("",dbl),batarg("b",mbr))),
 command("batgeom", "YMinFromMBR", batgeom_YMinFromMBR_mbr, false, "", args(1,2, batarg("",dbl),batarg("b",mbr))),
 command("batgeom", "XMaxFromMBR", batgeom_XMaxFromMBR_mbr, false, "", args(1,2, batarg("",dbl),batarg("b",mbr))),
 command("batgeom", "YMaxFromMBR", batgeom_YMaxFromMBR_mbr, false, "", args(1,2, batarg("",dbl),batarg("b",mbr))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_geom_mal)
{ mal_module2("geom", geom_init_atoms, geom_init_funcs, geom_prelude, NULL); }
