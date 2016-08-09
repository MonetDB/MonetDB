#include "geom.h"

static str geos_geom_verify(GEOSGeometry **res, const GEOSGeometry *geosGeometry);

static str
geos_ring_verify(GEOSGeometry **res, const GEOSGeometry *geosGeometry)
{
    int closed = 0;
	str err = MAL_SUCCEED, msg = MAL_SUCCEED;
	unsigned int pointsNum;
    int srid = 0, j = 0;

    /*Check if the ring is closed*/ 
    if ((closed = GEOSisClosed(geosGeometry)) == 2)
        throw(MAL, "geos_ring_verify", "GEOSisClosed failed");

    /*If it is not closed, close it*/
    if (closed != 1) {
        GEOSCoordSequence *gcs_new = NULL;
        const GEOSCoordSequence *gcs_old = NULL;
        srid = GEOSGetSRID(geosGeometry);
        double x, y, z;
        int lineDim = 0, i = 0;

        if ((err = numPointsLineString(&pointsNum, geosGeometry)) != MAL_SUCCEED) {
            *res = NULL;
            msg = createException(MAL, "geos_ring_verify", "numPointsLineString failed:%s", err);
            GDKfree(err);
            return msg;
        } else {
            assert(pointsNum < 4);
            //get the coordinate sequences of the LineString
            if (!(gcs_old = GEOSGeom_getCoordSeq(geosGeometry))) {
                *res = NULL;
                throw(MAL, "geom.AddPoint", "GEOSGeom_getCoordSeq failed");
            }

            //Get dimension of the LineString
            if (GEOSCoordSeq_getDimensions(gcs_old, &lineDim) == 0) {
                *res = NULL;
                throw(MAL, "geom.AddPoint", "GEOSGeom_getDimensions failed");
            }

            if ((gcs_new = GEOSCoordSeq_create(4, lineDim)) == NULL) {
                *res = NULL;
                throw(MAL, "geom.AddPoint", "GEOSCoordSeq_create failed");
            }

            if (!GEOSCoordSeq_getX(gcs_old, 0, &x))
                throw(MAL, "geom.AddPoint", "GEOSCoordSeq_getX failed");

            if (!GEOSCoordSeq_getY(gcs_old, 0, &y))
                throw(MAL, "geom.AddPoint", "GEOSCoordSeq_getY failed");


            if (lineDim > 2) {
                if (!GEOSCoordSeq_getZ(gcs_old, 0, &z))
                    throw(MAL, "geom.AddPoint", "GEOSCoordSeq_getZ failed");
            }

            for (j = pointsNum-1; j < 4; j++) {
                if (!GEOSCoordSeq_setX(gcs_new, j, x))
                    throw(MAL, "geom.AddPoint", "GEOSCoordSeq_setX failed");

                if (!GEOSCoordSeq_setY(gcs_new, j, y))
                    throw(MAL, "geom.AddPoint", "GEOSCoordSeq_setY failed");

                if (lineDim > 2) {
                    if (!GEOSCoordSeq_setZ(gcs_new, i, z))
                        throw(MAL, "geom.AddPoint", "GEOSCoordSeq_setZ failed");
                }
            }
            //Create new LineString
            if (!(*res = GEOSGeom_createLineString(gcs_new))) {
                throw(MAL, "geom.AddPoint", "GEOSGeom_createLineString failed");
            }

    	    GEOSSetSRID(*res, srid);
        }
    } else {
        *res = GEOSGeom_clone(geosGeometry);
    }

	return MAL_SUCCEED;
}

static str
geos_polygon_verify(GEOSGeometry **res, const GEOSGeometry *geosGeometry)
{
	const GEOSGeometry *extRing = NULL;
	GEOSGeometry *extRes = NULL, **intRings = NULL;
	int i = 0, j = 0, numIntRings = 0;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;
    bit untouched = 1;

	/* get the exterior ring of the polygon */
	extRing = GEOSGetExteriorRing(geosGeometry);
	if (extRing == NULL) {
		*res = NULL;
		throw(MAL, "geos_poly_verify", "GEOSGetExteriorRing failed");
	}

    /*verify exterior ring*/
    if ( (geos_ring_verify(&extRes, extRing)) != MAL_SUCCEED) {
    } else if (extRes != extRing)
        untouched = 0;

	numIntRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numIntRings == -1) {
		*res = NULL;
		throw(MAL, "geos_poly_verify", "GEOSGetNumInteriorRings failed.");
    } else if(numIntRings) {

        if ( (intRings = (GEOSGeometry **) GDKzalloc(sizeof(GEOSGeometry *)*numIntRings)) == NULL) {
            *res = NULL;
            throw(MAL, "geos_poly_verify", MAL_MALLOC_FAIL);
        }

        for (i = 0; i < numIntRings; i++) {
            const GEOSGeometry *intRing = NULL;
            if ((intRing = GEOSGetInteriorRingN(geosGeometry, i)) == NULL) {
                msg = createException(MAL, "geos_poly_verify", "GEOSGetInteriorRingN failed.");
                break;
            }
            if ( (msg = geos_ring_verify(&intRings[i], intRing)) != MAL_SUCCEED) {
                break;
            } else if (intRings[i] != intRing)
                untouched = 0;
        }
        if (msg != MAL_SUCCEED) {
            *res = NULL;
            /*TODO: You should only destroy if you have clonned it*/
            for (j = 0; j < i; j++)
                GEOSGeom_destroy(intRings[j]);
            GDKfree(intRings);
            return msg;
        }
    }

    /*Create a new geometry*/
    if (!untouched) {
        if ( (*res = GEOSGeom_createPolygon(extRes, intRings, numIntRings)) == NULL) {
            GEOSGeom_destroy(extRes);
            if (numIntRings) {
                /*TODO: You should only destroy if you have clonned it*/
                for (i = 0; i < numIntRings; i++)
                    GEOSGeom_destroy(intRings[i]);
                GDKfree(intRings);
            }
            return createException(MAL, "geos_poly_verify", "GEOSGeom_createPolygon failed");
        }
    } else {
        *res = GEOSGeom_clone(geosGeometry);
    }

    if (intRings)
        GDKfree(intRings);
        
	return MAL_SUCCEED;
}

/*Line has 0 or more than one point. If missing duplicate points*/
static str
geos_line_verify(GEOSGeometry **res, const GEOSGeometry *geosGeometry)
{
    unsigned int pointsN;
	GEOSGeom *ret;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;
    int srid = 0;


	//get the points in the exterior ring
	if ( (err = numPointsLineString(&pointsN, geosGeometry)) != MAL_SUCCEED) {
        msg = createException(MAL,"geos_line_verify", "numPointsLineString failed:%s", err);
        GDKfree(err);
        *res = NULL;
		return err;
	}

    if (pointsN == 1) {
        GEOSCoordSequence *gcs_new = NULL;
        const GEOSCoordSequence *gcs_old = NULL;
        srid = GEOSGetSRID(geosGeometry);
        double x, y, z;
        int lineDim = 0;

        if (!(gcs_old = GEOSGeom_getCoordSeq(geosGeometry))) {
            *res = NULL;
            throw(MAL, "geos_line_verify", "GEOSGeom_getCoordSeq failed");
        }

        //Get dimension of the LineString
        if (GEOSCoordSeq_getDimensions(gcs_old, &lineDim) == 0) {
            *res = NULL;
            throw(MAL, "geos_line_verify", "GEOSGeom_getDimensions failed");
        }

        if ((gcs_new = GEOSCoordSeq_create(2, lineDim)) == NULL) {
            *res = NULL;
            throw(MAL, "geos_line_verify", "GEOSCoordSeq_create failed");
        }

        if (!GEOSCoordSeq_getX(gcs_old, 0, &x))
            throw(MAL, "geos_line_verify", "GEOSCoordSeq_getX failed");

        if (!GEOSCoordSeq_getY(gcs_old, 0, &y))
            throw(MAL, "geos_line_verify", "GEOSCoordSeq_getY failed");

        if (lineDim > 2) {
            if (!GEOSCoordSeq_getZ(gcs_old, 0, &z))
                throw(MAL, "geos_line_verify", "GEOSCoordSeq_getZ failed");
        }

        if (!GEOSCoordSeq_setX(gcs_new, 1, x))
            throw(MAL, "geos_line_verify", "GEOSCoordSeq_setX failed");

        if (!GEOSCoordSeq_setY(gcs_new, 1, y))
            throw(MAL, "geos_line_verify", "GEOSCoordSeq_setY failed");

        if (lineDim > 2) {
            if (!GEOSCoordSeq_setZ(gcs_new, 1, z))
                throw(MAL, "geos_line_verify", "GEOSCoordSeq_setZ failed");
        }

        //Create new LineString
        if (!(*res = GEOSGeom_createLineString(gcs_new))) {
            throw(MAL, "geos_line_verify", "GEOSGeom_createLineString failed");
        }

        GEOSSetSRID(*res, srid);
    }
	else {
        *res = GEOSGeom_clone(geosGeometry);
	}

    return MAL_SUCCEED;
}

static str
geos_collection_verify(GEOSGeometry **res, const GEOSGeometry *geosGeometry)
{
	GEOSGeometry **newGeoms = NULL;
	uint32_t i, j, numGeoms=0;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;

    numGeoms = GEOSGetNumGeometries(geosGeometry);
    if (numGeoms < 0)
        throw(MAL, "geos_collection_verify", "GEOSGetNumGeometries failed");

    if ( (newGeoms = (GEOSGeometry **) GDKmalloc(sizeof(GEOSGeometry *)*numGeoms)) == NULL) {
        throw(MAL, "geos_collection_verify", MAL_MALLOC_FAIL);
    }

    for (i = 0; i < numGeoms; i++) {
        const GEOSGeometry *geom = GEOSGetGeometryN(geosGeometry, i);
        if (!geom)
            throw(MAL, "geos_collection_verify", "GEOSGetGeometryN failed");

        if ((err = geos_geom_verify(&newGeoms[i], geom)) != MAL_SUCCEED) {
            msg = createException(MAL, "geos_collection_verify", "geos_geom_verify failed:%s",err);
            for (j = 0; j < i; j++) {
                GEOSGeom_destroy(newGeoms[j]);
            }
            GDKfree(newGeoms);
            return msg;
        }
    }

	if ( (*res = GEOSGeom_createCollection(GEOS_GEOMETRYCOLLECTION, newGeoms, numGeoms)) == NULL ) {
        for (i = 0; i < numGeoms; i++)
            GEOSGeom_destroy(newGeoms[i]);
        GDKfree(newGeoms);
		throw(MAL, "geos_collection_verify", "GEOSGeom_createCollection failed");
    }

	return MAL_SUCCEED;
}

static str
geos_geom_verify(GEOSGeometry **res, const GEOSGeometry *geosGeometry)
{
	int geometryType = GEOSGeomTypeId(geosGeometry) + 1;
    str msg = MAL_SUCCEED;

    switch (geometryType)
    {
        case wkbPoint_mdb:
	    case wkbMultiPoint_mdb:
            *res = GEOSGeom_clone(geosGeometry);
            break;
	    case wkbLineString_mdb:
    	case wkbLinearRing_mdb:
            msg = geos_line_verify(res, geosGeometry);
            break;
	    case wkbPolygon_mdb:
            msg = geos_polygon_verify(res, geosGeometry);
            break;
	    case wkbMultiLineString_mdb:
    	case wkbMultiPolygon_mdb:
    	case wkbGeometryCollection_mdb:
            msg = geos_collection_verify(res, geosGeometry);
            break;
        default:
            *res = NULL;
            msg = createException(MAL, "geos_verify", "Unknown geometry type");
            break;
    }
    
    return msg;
}

static str
GEOSGeom_GEOS_nodeLines(GEOSGeometry **res, const GEOSGeometry* lines)
{
	GEOSGeometry *geom = NULL, *point = NULL;

	if ( (point = GEOSGeomGetPointN(lines, 0)) == NULL) {
        *res = NULL;
        return MAL_SUCCEED;
    }

	if ( (geom = GEOSUnion(lines, point)) == NULL ) {
		GEOSGeom_destroy(point);
        *res = NULL;
        return MAL_SUCCEED;
	}

    *res = geom;
	return MAL_SUCCEED;
}

/*TODO: Find a way to clone it*/
static GEOSGeometry*
GEOSGeom_GEOS_buildArea(const GEOSGeometry* geom_in) {
    return NULL;
}


static str
GEOSGeom_GEOS_makeValidPolygon(GEOSGeometry **res, const GEOSGeometry *poly)
{
	GEOSGeom geosBound, geosCutEdges, geosArea, collapsePoints;
	GEOSGeometry *vgeoms[3], *uniqPointsBound = NULL, *uniqPointsEdges = NULL;
	unsigned int nvgeoms=0;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;

	assert (GEOSGeomTypeId(geosGeometry) == GEOS_POLYGON || GEOSGeomTypeId(geosGeometry) == GEOS_MULTIPOLYGON);

	if ( (geosBound = GEOSBoundary(poly)) == NULL) {
        *res = NULL;
		throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSBoundary failed");
    }

	if ( (err = GEOSGeom_GEOS_nodeLines(&geosCutEdges, geosBound)) != MAL_SUCCEED) {
		GEOSGeom_destroy(geosBound);
        *res = NULL;
		msg = createException(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSGeom_GEOS_nodeLines failed");
        GDKfree(err);
        return msg;
	}

    if ( (uniqPointsBound = GEOSGeom_extractUniquePoints(geosBound)) == NULL) {
        GEOSGeom_destroy(geosCutEdges); //Check if needs to be removed
        GEOSGeom_destroy(geosBound);
        *res = NULL;
        throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSGeom_extractUniquePoints failed");
    }

    if ( (uniqPointsEdges = GEOSGeom_extractUniquePoints(geosCutEdges)) == NULL) {
        GEOSGeom_destroy(geosCutEdges); //Check if needs to be removed
        GEOSGeom_destroy(uniqPointsBound);
        GEOSGeom_destroy(geosBound);
        *res = NULL;
        throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSGeom_extractUniquePoints failed");
    }

    if ( (collapsePoints = GEOSDifference(uniqPointsBound, uniqPointsEdges)) == NULL) {
        GEOSGeom_destroy(geosCutEdges); //Check if needs to be removed
        GEOSGeom_destroy(uniqPointsEdges);
        GEOSGeom_destroy(uniqPointsBound);
        GEOSGeom_destroy(geosBound);
        *res = NULL;
        throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSDifference failed");
    }

    GEOSGeom_destroy(uniqPointsEdges);
    GEOSGeom_destroy(uniqPointsBound);
    GEOSGeom_destroy(geosBound);

	if ( (geosArea = GEOSGeom_createEmptyPolygon()) == NULL) {
        GEOSGeom_destroy(geosCutEdges);
        *res = NULL;
		throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSGeom_createEmptyPolygon failed");
	}

	while (GEOSGetNumGeometries(geosCutEdges)) {
		GEOSGeometry* newArea=0;
		GEOSGeometry* newAreaBound=0;
		GEOSGeometry* symdif=0;
		GEOSGeometry* newCutEdges=0;

		if ( (newArea = GEOSGeom_GEOS_buildArea(geosCutEdges)) == NULL) {
			GEOSGeom_destroy(geosCutEdges);
			GEOSGeom_destroy(geosArea);
            *res = NULL;
			throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSGeom_GEOS_buildArea failed");
		}

		if ( GEOSisEmpty(newArea) ) {
			GEOSGeom_destroy(newArea);
			break;
		}

		if ( (newAreaBound = GEOSBoundary(newArea)) == NULL ) {
			GEOSGeom_destroy(newArea);
			GEOSGeom_destroy(geosArea);
            *res = NULL;
			throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSBoundary failed");
		}

		if ( (symdif = GEOSSymDifference(geosArea, newArea)) == NULL) {
			GEOSGeom_destroy(geosCutEdges);
			GEOSGeom_destroy(newArea);
			GEOSGeom_destroy(newAreaBound);
			GEOSGeom_destroy(geosArea);
            *res = NULL;
			throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSSymDifference failed");
		}

		GEOSGeom_destroy(geosArea);
		GEOSGeom_destroy(newArea);
		geosArea = symdif;
		symdif = 0;

		if ( (newCutEdges = GEOSDifference(geosCutEdges, newAreaBound)) == NULL) {
		    GEOSGeom_destroy(newAreaBound);
			GEOSGeom_destroy(geosCutEdges);
			GEOSGeom_destroy(geosArea);
            *res = NULL;
			throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSDifference failed");
		}
		GEOSGeom_destroy(newAreaBound);
		GEOSGeom_destroy(geosCutEdges);
		geosCutEdges = newCutEdges;
	}

	if ( !GEOSisEmpty(geosArea) ) {
		vgeoms[nvgeoms++] = geosArea;
	}
	else {
		GEOSGeom_destroy(geosArea);
	}

	if ( ! GEOSisEmpty(geosCutEdges) ) {
		vgeoms[nvgeoms++] = geosCutEdges;
	}
	else {
		GEOSGeom_destroy(geosCutEdges);
	}

	if ( ! GEOSisEmpty(collapsePoints) ) {
		vgeoms[nvgeoms++] = collapsePoints;
	}
	else {
		GEOSGeom_destroy(collapsePoints);
	}

	if ( 1 == nvgeoms ) {
		*res = vgeoms[0];
	}
	else {
		if ( (*res = GEOSGeom_createCollection(GEOS_GEOMETRYCOLLECTION, vgeoms, nvgeoms)) == NULL) {
			/* TODO: cleanup! */
			throw(MAL, "GEOSGeom_GEOS_makeValidPolygon", "GEOSGeom_createCollection failed");
		}
	}

	return MAL_SUCCEED;
}

static str
GEOSGeom_GEOS_makeValidLine(GEOSGeometry **res, const GEOSGeometry *geosGeometry)
{
	return GEOSGeom_GEOS_nodeLines(res, geosGeometry);
}

static str
GEOSGeom_GEOS_makeValidMultiLine(GEOSGeometry **res, const GEOSGeometry* geomGeometry)
{
	GEOSGeometry **lines = NULL, **points = NULL;
	GEOSGeometry *lineOut = NULL, *pointOut = NULL;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;
	uint32_t numLines=0, numLinesAlloc=0, numPoints=0, numGeoms=0, numSubGeoms=0, i, j;

	numGeoms = GEOSGetNumGeometries(geomGeometry);
	numLinesAlloc = numGeoms;
	if ( (lines = GDKmalloc(sizeof(GEOSGeometry*)*numLinesAlloc)) == NULL) {
        throw(MAL, "GEOSGeom_GEOS_makeValidMultiLine", MAL_MALLOC_FAIL);
    }
	if ( (points = GDKmalloc(sizeof(GEOSGeometry*)*numGeoms)) == NULL) {
        GDKfree(lines);
        throw(MAL, "GEOSGeom_GEOS_makeValidMultiLine", MAL_MALLOC_FAIL);
    }

	for (i=0; i<numGeoms; ++i)
	{
		const GEOSGeometry* geom = GEOSGetGeometryN(geomGeometry, i);
		GEOSGeometry* vg;
		if ( (err = GEOSGeom_GEOS_makeValidLine(&vg, geom)) != MAL_SUCCEED ) {
            GDKfree(lines);
            GDKfree(points);
            msg = createException(MAL, "GEOSGeom_GEOS_makeValidMultiLine", "GEOSGeom_GEOS_makeValidLine failed: %s", err);
            GDKfree(err);
            return msg;
        }
		if ( GEOSisEmpty(vg) ) {
			GEOSGeom_destroy(vg);
		}
		if ( GEOSGeomTypeId(vg) == GEOS_POINT ) {
			points[numPoints++] = vg;
		}
		else if ( GEOSGeomTypeId(vg) == GEOS_LINESTRING ) {
			lines[numLines++] = vg;
		}
		else if ( GEOSGeomTypeId(vg) == GEOS_MULTILINESTRING ) {
			numSubGeoms=GEOSGetNumGeometries(vg);
			numLinesAlloc += numSubGeoms;
			lines = realloc(lines, sizeof(GEOSGeometry*)*numLinesAlloc);
			for (j=0; j<numSubGeoms; ++j)
			{
				const GEOSGeometry* gc = GEOSGetGeometryN(vg, j);
				lines[numLines++] = GEOSGeom_clone(gc);
			}
		}
		else {
            GDKfree(lines);
            GDKfree(points);
            throw(MAL, "GEOSGeom_GEOS_makeValidMultiLine", "%s Unknown geometry type", geom_type2str(GEOSGeomTypeId(vg), 0));
		}
	}

	if ( numPoints ) {
		if ( numPoints > 1 ) {
			pointOut = GEOSGeom_createCollection(GEOS_MULTIPOINT, points, numPoints);
		}
		else {
			pointOut = points[0];
		}
	}

	if ( numLines ) {
		if ( numLines > 1 ) {
			lineOut = GEOSGeom_createCollection(GEOS_MULTILINESTRING, lines, numLines);
		}
		else {
			lineOut = lines[0];
		}
	}

	GDKfree(lines);

	if ( lineOut && pointOut ) {
		points[0] = lineOut;
		points[1] = pointOut;
		*res = GEOSGeom_createCollection(GEOS_GEOMETRYCOLLECTION, points, 2);
	}
	else if ( lineOut ) {
		*res = lineOut;
	}
	else if ( pointOut ) {
		*res = pointOut;
	}

	GDKfree(points);

	return MAL_SUCCEED;
}

static str GEOSGeom_GEOS_makeValidCollection(GEOSGeometry **res, const GEOSGeometry* geosGeometry);

static str
GEOSGeom_GEOS_makeValid(GEOSGeometry **res, const GEOSGeometry *geosGeometry, int sanity_check)
{
	char out = 0;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;

	if ( (out = GEOSisValid(geosGeometry)) == 2) {
        *res = NULL;
		throw(MAL, "GEOSGeom_GEOS_makeValid", "GEOSisValid failed");
	}
	else if (out) {
        *res = GEOSGeom_clone(geosGeometry);
        return MAL_SUCCEED;
	}

    switch (GEOSGeomTypeId(geosGeometry))
    {
        case GEOS_MULTIPOINT:
        case GEOS_POINT:
            throw(MAL, "GEOSGeom_GEOS_makeValid", "Invalid Point, not clear how to make it valid");
        case GEOS_LINESTRING:
            if ( (err = GEOSGeom_GEOS_makeValidLine(res, geosGeometry)) != MAL_SUCCEED ) {
                msg = createException(MAL, "GEOSGeom_GEOS_makeValid", "GEOSGeom_GEOS_makeValidLine failed:%s", err);
                GDKfree(err);
            }
            break;
        case GEOS_MULTILINESTRING:
            if ( (err = GEOSGeom_GEOS_makeValidMultiLine(res, geosGeometry)) != MAL_SUCCEED ){
                msg = createException(MAL, "GEOSGeom_GEOS_makeValid", "GEOSGeom_GEOS_makeValidMultiLine failed:%s", err);
                GDKfree(err);
            }
            break;
        case GEOS_POLYGON:
        case GEOS_MULTIPOLYGON:
            if ( (err = GEOSGeom_GEOS_makeValidPolygon(res, geosGeometry)) != MAL_SUCCEED ) {
                msg = createException(MAL, "GEOSGeom_GEOS_makeValid", "GEOSGeom_GEOS_makeValidPolygon failed:%s", err);
                GDKfree(err);
            }
            break;
        case GEOS_GEOMETRYCOLLECTION:
            if ( (err = GEOSGeom_GEOS_makeValidCollection(res, geosGeometry)) != MAL_SUCCEED ) {
                throw(MAL, "GEOSGeom_GEOS_makeValid", "GEOSGeom_GEOS_makeValidCollection failed: %s", err);
                GDKfree(err);
            }
            break;
        default:
            {
                msg = createException(MAL, "GEOSGeom_GEOS_makeValid", "Geometry type unknown");
                break;
            }
    }

    if (msg == MAL_SUCCEED && sanity_check) {
        int loss;
        GEOSGeometry *pi = NULL, *po = NULL, *pd = NULL;
        if ( (pi = GEOSGeom_extractUniquePoints(geosGeometry)) == NULL) {
            *res = NULL;
            throw(MAL, "GEOSGeom_GEOS_makeValid", "GEOSGeom_extractUniquePoints");
        }
        if ( (po = GEOSGeom_extractUniquePoints(*res)) == NULL) {
            GEOSGeom_destroy(pi);
            *res = NULL;
            throw(MAL, "GEOSGeom_GEOS_makeValid", "GEOSGeom_extractUniquePoints");
        }
        if ( (pd = GEOSDifference(pi, po)) == NULL) {
            GEOSGeom_destroy(pi);
            GEOSGeom_destroy(po);
            *res = NULL;
            throw(MAL, "GEOSGeom_GEOS_makeValid", "GEOSDifference");
        }

        GEOSGeom_destroy(pi);
        GEOSGeom_destroy(po);
        loss = !GEOSisEmpty(pd);
        GEOSGeom_destroy(pd);
        if ( loss ) {
            *res = NULL;
            throw(MAL, "GEOSGeom_GEOS_makeValid", "Vertice were lost during the process of validation");
        }
    }

	return msg;
}

static str
GEOSGeom_GEOS_makeValidCollection(GEOSGeometry **res, const GEOSGeometry* geosGeometry)
{
	int nvgeoms;
	GEOSGeometry **vgeoms;
	unsigned int i;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;

	if ( (nvgeoms = GEOSGetNumGeometries(geosGeometry)) == -1) {
		throw(MAL, "GEOSGeom_GEOS_makeValidCollection", "GEOSGetNumGeometries failed.");
	}

	if ( (vgeoms = GDKmalloc( sizeof(GEOSGeometry*) * nvgeoms )) == NULL ) {
		throw(MAL, "GEOSGeom_GEOS_makeValidCollection", MAL_MALLOC_FAIL);
    }

	for ( i=0; i<nvgeoms; ++i ) {
		if ( (err = GEOSGeom_GEOS_makeValid(&vgeoms[i], GEOSGetGeometryN(geosGeometry, i), false)) != MAL_SUCCEED) {
			while (i--)
                GEOSGeom_destroy(vgeoms[i]);
			GDKfree(vgeoms);
			*res = NULL;
		    msg = createException(MAL, "GEOSGeom_GEOS_makeValidCollection", "GEOSGeom_GEOS_makeValid failed:%s", err);
            GDKfree(err);
            return msg;
		}
	}

	if ( (*res = GEOSGeom_createCollection(GEOS_GEOMETRYCOLLECTION, vgeoms, nvgeoms)) == NULL) {
		for ( i=0; i<nvgeoms; ++i )
            GEOSGeom_destroy(vgeoms[i]);
		GDKfree(vgeoms);
		throw(MAL, "GEOSGeom_GEOS_makeValidCollection", "GEOSGeom_createCollection failed");
	}

	GDKfree(vgeoms);

	return MAL_SUCCEED;
}

str
geom_make_valid(GEOSGeometry **res, const GEOSGeometry *geosGeometry)
{
	int hasZ;
	GEOSGeom geosGeom;
    GEOSGeometry *geosOut = NULL;
    str err = MAL_SUCCEED, msg = MAL_SUCCEED;

	hasZ = GEOSHasZ(geosGeometry);

    if ( (err = geos_geom_verify(&geosOut, geosGeometry)) != MAL_SUCCEED) {
        throw(MAL, "geom_make_valid", "It was not possible to make a valid geometry out of the input");
    }

	if ( (err = GEOSGeom_GEOS_makeValid(res, geosOut, false)) != MAL_SUCCEED ) {
	    GEOSGeom_destroy(geosOut);
        msg = createException(MAL, "geom_make_valid", "GEOSGeom_GEOS_makeValid failed: %s", err);
        GDKfree(err);
		*res = NULL;
        return msg;
    }
	GEOSGeom_destroy(geosOut);

	if ( (GEOSGeomTypeId(geosGeometry)+1) == wkbGeometryCollection_mdb && (GEOSGeomTypeId(*res) +1 ) != wkbGeometryCollection_mdb) {
		GEOSGeom *ogeoms = (GEOSGeom *) GDKmalloc(sizeof(GEOSGeom*));
        int geometryType = -1, type = -1;
		assert(geom_in != res);
        ogeoms[0] = *res;

        geometryType = GEOSGeomTypeId(*res);
            switch (geometryType + 1) {
                case wkbPoint_mdb:
                    type = wkbMultiPoint_mdb - 1;
                    break;
                case wkbLineString_mdb:
                case wkbLinearRing_mdb:
                    type = wkbMultiLineString_mdb - 1;
                    break;
                case wkbPolygon_mdb:
                    type = wkbMultiPolygon_mdb - 1;
                    break;
                case wkbMultiPoint_mdb:
                case wkbMultiLineString_mdb:
                case wkbMultiPolygon_mdb:
                    type = wkbGeometryCollection_mdb - 1;
                    break;
                default:
                    *res = NULL;
                    throw(MAL, "geom_make_valid", "Unknown geometry type");
            }
		if ( (*res = GEOSGeom_createCollection(type, ogeoms, 1)) == NULL ) {
            *res = NULL;
            throw(MAL, "geom_make_valid", "GEOSGeom_createCollection failed!!!");
        }
	}

	GEOSSetSRID(*res, GEOSGetSRID(geosGeometry));

	return MAL_SUCCEED;
}
