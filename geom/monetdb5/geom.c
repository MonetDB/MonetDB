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

/*
 * @a Wouter Scherphof, Niels Nes
 * @* The simple geom module
 */


#include <monetdb_config.h>
#include "libgeom.h"


#include <mal.h>
#include <mal_atom.h>
#include <mal_exception.h>
#include <mal_client.h>
#include <stream.h>


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#ifdef WIN32
#ifndef LIBGEOM
#define geom_export extern __declspec(dllimport)
#else
#define geom_export extern __declspec(dllexport)
#endif
#else
#define geom_export extern
#endif


static inline int geometryHasZ(int info){return (info & 0x02);}
static inline int geometryHasM(int info){return (info & 0x01);}
const double pi=3.14159265358979323846;

/* the first argument in the functions is the return variable */

int TYPE_mbr;

/* general functions */
geom_export void geoHasZ(int* res, int* info);
geom_export void geoHasM(int* res, int* info);
geom_export void geoGetType(char** res, int* info, int* flag);

geom_export bat *geom_prelude(void);
geom_export void geom_epilogue(void);

geom_export wkb *wkbNULL(void);
geom_export mbr *mbrNULL(void);

/* functions tha are used when a column is added to an existing table */
geom_export str mbrFromMBR(mbr **w, mbr **src);
geom_export str wkbFromWKB(wkb **w, wkb **src);
//Is it needed?? geom_export str wkbFromWKB_bat(int* outBAT_id, int* inBAT_id);

/* The WKB we use is the EWKB used also in PostGIS 
 * because we decided that it is easire to carry around
 * the SRID */
 
/* gets a GEOSGeometry and creates a WKB */
geom_export wkb* geos2wkb(const GEOSGeometry* geosGeometry);
/* gets a GEOSGeometry and returns the mbr of it 
 * works only for 2D geometries */
geom_export mbr* mbrFromGeos(const GEOSGeom geosGeometry);

/* the len argument is needed for correct storage and retrieval */
geom_export int mbrFROMSTR(char *src, int *len, mbr **atom);
geom_export int mbrTOSTR(char **dst, int *len, mbr *atom);
geom_export int wkbFROMSTR(char* geomWKT, int *len, wkb** geomWKB, int srid);
geom_export int wkbTOSTR(char **geomWKT, int *len, wkb *geomWKB);

/* read/write to/from disk */
geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
geom_export int mbrWRITE(mbr *c, stream *s, size_t cnt);
geom_export wkb *wkbREAD(wkb *a, stream *s, size_t cnt);
geom_export int wkbWRITE(wkb *a, stream *s, size_t cnt);


geom_export str wkbFromText(wkb **geomWKB, str *geomWKT, int* srid, int *tpe);

/* Basic Methods on Geometric objects (OGC) */
geom_export str wkbDimension(int*, wkb**);
geom_export str wkbGeometryType(char**, wkb**, int*);
geom_export str wkbGetSRID(int*, wkb**);
//Envelope
geom_export str wkbAsText(char**, wkb**, int*);
geom_export str wkbAsBinary(char**, wkb**);
geom_export str wkbFromBinary(wkb**, char**);
geom_export str wkbIsEmpty(bit*, wkb**);
geom_export str wkbIsSimple(bit*, wkb**);
//Is3D
//IsMeasured
geom_export str wkbBoundary(wkb**, wkb**);


/* Methods for testing spatial relatioships between geometris (OGC) */
geom_export str wkbEquals(bit*, wkb**, wkb**);
geom_export str wkbDisjoint(bit*, wkb**, wkb**);
geom_export str wkbIntersects(bit*, wkb**, wkb**);
geom_export str wkbTouches(bit*, wkb**, wkb**);
geom_export str wkbCrosses(bit*, wkb**, wkb**);
geom_export str wkbWithin(bit*, wkb**, wkb**);
geom_export str wkbContains(bit*, wkb**, wkb**);
//geom_export str wkbContains_firstScalar_bat(int* outBAT_id, wkb** aWKB, int* bBAT_id);
geom_export str wkbContains_bat_bat(int* outBAT_id, int* aBAT_id, int* bBAT_id);
geom_export str wkbOverlaps(bit*, wkb**, wkb**);
geom_export str wkbRelate(bit*, wkb**, wkb**, str*);
geom_export str wkbCovers(bit *out, wkb **geomWKB_a, wkb **geomWKB_b);
geom_export str wkbCoveredBy(bit *out, wkb **geomWKB_a, wkb **geomWKB_b);

geom_export str wkbContainsFilter_bat(int* aBATfiltered_id, int* bBATfiltered_id, int* aBAT_id, int* bBAT_id);

//LocateAlong
//LocateBetween

//geom_export str wkbFromString(wkb**, str*); 

geom_export str geomMakePoint2D(wkb**, double*, double*);
geom_export str geomMakePoint2D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id);
geom_export str geomMakePoint3D(wkb**, double*, double*, double*);
geom_export str geomMakePoint3D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* zBAT_id);
geom_export str geomMakePoint4D(wkb**, double*, double*, double*, double*);
geom_export str geomMakePoint4D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* zBAT_id, int* mBAT_id);
geom_export str geomMakePointM(wkb**, double*, double*, double*);
geom_export str geomMakePointM_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* mBAT_id);

geom_export str wkbCoordDim(int* , wkb**);
geom_export str wkbSetSRID(wkb**, wkb**, int*);
geom_export str wkbSetSRID_bat(int* outBAT_id, int* inBAT_id, int* srid);
geom_export str wkbGetCoordX(double*, wkb**);
geom_export str wkbGetCoordY(double*, wkb**);
geom_export str wkbGetCoordZ(double*, wkb**);
geom_export str wkbStartPoint(wkb **out, wkb **geom);
geom_export str wkbEndPoint(wkb **out, wkb **geom);
geom_export str wkbNumPoints(int *out, wkb **geom);
geom_export str wkbPointN(wkb **out, wkb **geom, int *n);
geom_export str wkbEnvelope(wkb **out, wkb **geom);
geom_export str wkbEnvelopeFromCoordinates(wkb** out, double* xmin, double* ymin, double* xmax, double* ymax, int* srid);
geom_export str wkbMakePolygon(wkb** out, wkb** external, int* internalBAT_id, int* srid);
geom_export str wkbExteriorRing(wkb**, wkb**);
geom_export str wkbInteriorRingN(wkb**, wkb**, short*);
geom_export str wkbNumRings(int*, wkb**, int*);
geom_export str wkbIsClosed(bit *out, wkb **geom);
geom_export str wkbIsRing(bit *out, wkb **geom);
geom_export str wkbIsValid(bit *out, wkb **geom);
geom_export str wkbIsValidReason(char** out, wkb **geom);
geom_export str wkbIsValidDetail(char** out, wkb **geom);

geom_export str wkbArea(dbl *out, wkb **a);
geom_export str wkbCentroid(wkb **out, wkb **geom);
geom_export str wkbDistance(dbl *out, wkb **a, wkb **b);
geom_export str wkbLength(dbl *out, wkb **a);
geom_export str wkbConvexHull(wkb **out, wkb **geom);
geom_export str wkbIntersection(wkb **out, wkb **a, wkb **b);
geom_export str wkbUnion(wkb **out, wkb **a, wkb **b);
geom_export str wkbDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbSymDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbBuffer(wkb **out, wkb **geom, dbl *distance);

geom_export str wkbGeometryN(wkb** out, wkb** geom, int* geometryNum); 
geom_export str wkbNumGeometries(int* out, wkb** geom);

geom_export str wkbTransform(wkb**, wkb**, int*, int*, char**, char**);
geom_export str wkbPointOnSurface(wkb**, wkb**);


geom_export str geom_2_geom(wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID); 
geom_export str geom_2_geom_bat(int* outBAT_id, int* inBAT_id, int* columnType, int* columnSRID);

geom_export str wkbMBR(mbr **res, wkb **geom);
geom_export str wkbMBR_bat(int* outBAT_id, int* inBAT_id);
geom_export str wkbBox2D(mbr** box, wkb** point1, wkb** point2);

geom_export str mbrOverlaps(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlaps_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrAbove(bit *out, mbr **b1, mbr **b2);
geom_export str mbrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrBelow(bit *out, mbr **b1, mbr **b2);
geom_export str mbrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrLeft(bit *out, mbr **b1, mbr **b2);
geom_export str mbrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrRight(bit *out, mbr **b1, mbr **b2);
geom_export str mbrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrAbove(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrBelow(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrLeft(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrRight(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrContains(bit *out, mbr **b1, mbr **b2);
geom_export str mbrContains_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrContained(bit *out, mbr **b1, mbr **b2);
geom_export str mbrContained_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrEqual(bit *out, mbr **b1, mbr **b2);
geom_export str mbrEqual_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrDistance(double *out, mbr **b1, mbr **b2);
geom_export str mbrDistance_wkb(double *out, wkb **geom1WKB, wkb **geom2WKB);

geom_export str wkbCoordinateFromWKB(dbl*, wkb**, int*);
geom_export str wkbCoordinateFromWKB_bat(int *outBAT_id, int *inBAT_id, int* coordinateIdx);

geom_export str wkbCoordinateFromMBR(dbl*, mbr**, int*);
geom_export str wkbCoordinateFromMBR_bat(int *outBAT_id, int *inBAT_id, int* coordinateIdx);

/** convert degrees to radians */
static void degrees2radians(double *x, double *y, double *z) {
	(*x) *= pi/180.0;
	(*y) *= pi/180.0;
	(*z) *= pi/180.0;
}

/** convert radians to degrees */
static void radians2degrees(double *x, double *y, double *z) {
	(*x) *= 180.0/pi;
	(*y) *= 180.0/pi;
	(*z) *= 180.0/pi;
}

#ifdef HAVE_PROJ
static str transformCoordSeq(int idx, int coordinatesNum, projPJ proj4_src, projPJ proj4_dst, const GEOSCoordSequence* gcs_old, GEOSCoordSequence** gcs_new){
	double x=0, y=0, z=0;
	int* errorNum =0 ;

	GEOSCoordSeq_getX(gcs_old, idx, &x);
	GEOSCoordSeq_getY(gcs_old, idx, &y);
				
	if(coordinatesNum > 2) 
		GEOSCoordSeq_getZ(gcs_old, idx, &z);

	/* check if the passed reference system is geographic (proj=latlong) 
 	* and change the degrees to radians because pj_transform works with radians*/
	if (pj_is_latlong(proj4_src)) degrees2radians(&x, &y, &z) ;

		
	pj_transform(proj4_src, proj4_dst, 1, 0, &x, &y, &z);

	errorNum = pj_get_errno_ref();
	if (*errorNum != 0){
		if(coordinatesNum >2)
			return createException(MAL, "geom.wkbTransform", "Couldn't transform point (%f %f %f): %s\n", x, y, z, pj_strerrno(*errorNum));
		else
			return createException(MAL, "geom.wkbTransform", "Couldn't transform point (%f %f): %s\n", x, y, pj_strerrno(*errorNum));
	}


	/* check if the destination reference system is geographic and change
 	* the destination coordinates from radians to degrees */
	if (pj_is_latlong(proj4_dst)) radians2degrees(&x, &y, &z);


	GEOSCoordSeq_setX(*gcs_new, idx, x);
	GEOSCoordSeq_setY(*gcs_new, idx, y);
	
	if(coordinatesNum > 2) 
		GEOSCoordSeq_setZ(*gcs_new, idx, z);



	return MAL_SUCCEED;
}

static str transformPoint(GEOSGeometry** transformedGeometry, const GEOSGeometry* geosGeometry, projPJ proj4_src, projPJ proj4_dst) {
	int coordinatesNum = 0;
	const GEOSCoordSequence* gcs_old;	
	GEOSCoordSeq gcs_new;
	str ret = MAL_SUCCEED;

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);
	
	if(gcs_old == NULL) {
		*transformedGeometry = NULL;
		return createException(MAL, "geom.wkbTransform", "GEOSGeom_getCoordSeq failed");
	}

	/* create the coordinates sequence for the transformed geometry */
	gcs_new = GEOSCoordSeq_create(1, coordinatesNum);

	/* create the transformed coordinates */
	ret = transformCoordSeq(0, coordinatesNum, proj4_src, proj4_dst, gcs_old, &gcs_new);
	if(ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}
	
	/* create the geometry from the coordinates seqience */
	*transformedGeometry = GEOSGeom_createPoint(gcs_new);

	return MAL_SUCCEED;
}

static str transformLine(GEOSCoordSeq *gcs_new, const GEOSGeometry* geosGeometry, projPJ proj4_src, projPJ proj4_dst) {
	int coordinatesNum = 0;
	const GEOSCoordSequence* gcs_old;	
	unsigned int pointsNum =0, i=0;
	str ret = MAL_SUCCEED;	

	/* get the number of coordinates the geometry has */
	coordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the coordinates of the points comprising the geometry */
	gcs_old = GEOSGeom_getCoordSeq(geosGeometry);
	
	if(gcs_old == NULL)
		return createException(MAL, "geom.wkbTransform", "GEOSGeom_getCoordSeq failed");
	
	/* get the number of points in the geometry */
	GEOSCoordSeq_getSize(gcs_old, &pointsNum);

	/* create the coordinates sequence for the transformed geometry */
	*gcs_new = GEOSCoordSeq_create(pointsNum, coordinatesNum);
	
	/* create the transformed coordinates */
	for(i=0; i<pointsNum; i++) {
		ret = transformCoordSeq(i, coordinatesNum, proj4_src, proj4_dst, gcs_old, gcs_new);
		if(ret != MAL_SUCCEED) {
			GEOSCoordSeq_destroy(*gcs_new);
			gcs_new = NULL;
			return ret;
		}
	}

		
	return MAL_SUCCEED;
}

static str transformLineString(GEOSGeometry** transformedGeometry, const GEOSGeometry* geosGeometry, projPJ proj4_src, projPJ proj4_dst) {
	GEOSCoordSeq coordSeq;
	str ret = MAL_SUCCEED;

	ret = transformLine(&coordSeq, geosGeometry, proj4_src, proj4_dst);

	if(ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}
	
	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createLineString(coordSeq);
	
	return ret;
}

static str transformLinearRing(GEOSGeometry** transformedGeometry, const GEOSGeometry* geosGeometry, projPJ proj4_src, projPJ proj4_dst) {
	GEOSCoordSeq coordSeq = NULL;
	str ret = MAL_SUCCEED;

	ret = transformLine(&coordSeq, geosGeometry, proj4_src, proj4_dst);

	if(ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}
	
	/* create the geometry from the coordinates sequence */
	*transformedGeometry = GEOSGeom_createLinearRing(coordSeq);
	
	return ret;
}

static str transformPolygon(GEOSGeometry** transformedGeometry, const GEOSGeometry* geosGeometry, projPJ proj4_src, projPJ proj4_dst, int srid) {
	const GEOSGeometry* exteriorRingGeometry;
	GEOSGeometry* transformedExteriorRingGeometry = NULL;
	GEOSGeometry** transformedInteriorRingGeometries = NULL;
	int numInteriorRings=0, i=0;
	str ret = MAL_SUCCEED;

	/* get the exterior ring of the polygon */
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	if(!exteriorRingGeometry) {
		*transformedGeometry = NULL;
		return createException(MAL, "geom.wkbTransform","GEOSGetExteriorRing failed");
	}	

	ret = transformLinearRing(&transformedExteriorRingGeometry, exteriorRingGeometry, proj4_src, proj4_dst);
	if(ret != MAL_SUCCEED) {
		*transformedGeometry = NULL;
		return ret;
	}
	GEOSSetSRID(transformedExteriorRingGeometry, srid);

	numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	if (numInteriorRings == -1 ) {
		*transformedGeometry = NULL;
		GEOSGeom_destroy(transformedExteriorRingGeometry);
		return createException(MAL, "geom.wkbTransform", "GEOSGetInteriorRingN failed.");
	}

	/* iterate over the interiorRing and transform each one of them */
	transformedInteriorRingGeometries = GDKmalloc(numInteriorRings*sizeof(GEOSGeometry*));
	for(i=0; i<numInteriorRings; i++) {
		ret = transformLinearRing(&(transformedInteriorRingGeometries[i]), GEOSGetInteriorRingN(geosGeometry, i), proj4_src, proj4_dst);
		if(ret != MAL_SUCCEED) {
			GDKfree(*transformedInteriorRingGeometries);
			*transformedGeometry = NULL;
			return ret;
		}
		GEOSSetSRID(transformedInteriorRingGeometries[i], srid);
	}

	*transformedGeometry = GEOSGeom_createPolygon(transformedExteriorRingGeometry, transformedInteriorRingGeometries, numInteriorRings);
	return MAL_SUCCEED;
}

static str transformMultiGeometry(GEOSGeometry** transformedGeometry, const GEOSGeometry* geosGeometry, projPJ proj4_src, projPJ proj4_dst, int srid, int geometryType) {
	int geometriesNum, subGeometryType, i;
	GEOSGeometry** transformedMultiGeometries = NULL;
	const GEOSGeometry* multiGeometry = NULL;
	str ret = MAL_SUCCEED;

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	transformedMultiGeometries = GDKmalloc(geometriesNum*sizeof(GEOSGeometry*));

	for(i=0; i<geometriesNum; i++) {
		multiGeometry = GEOSGetGeometryN(geosGeometry, i);

		subGeometryType = GEOSGeomTypeId(multiGeometry)+1;

		switch(subGeometryType) {
			case wkbPoint:
				ret = transformPoint(&(transformedMultiGeometries[i]), multiGeometry, proj4_src, proj4_dst);
				break;
			case wkbLineString:
				ret = transformLineString(&(transformedMultiGeometries[i]), multiGeometry, proj4_src, proj4_dst);
				break;
			case wkbLinearRing:
				ret = transformLinearRing(&(transformedMultiGeometries[i]), multiGeometry, proj4_src, proj4_dst);
				break;
			case wkbPolygon:
				ret = transformPolygon(&(transformedMultiGeometries[i]), multiGeometry, proj4_src, proj4_dst, srid);
				break; 
			default:
				transformedMultiGeometries[i] = NULL;
				ret = createException(MAL, "geom.Transform", "Unknown geometry type");
		}

		if(ret != MAL_SUCCEED) {
			GDKfree(*transformedMultiGeometries);
			*transformedGeometry = NULL;
			return ret;
		}
		
		GEOSSetSRID(transformedMultiGeometries[i], srid);
	}
	
	*transformedGeometry = GEOSGeom_createCollection(geometryType-1, transformedMultiGeometries, geometriesNum);

	return ret;
}

/* the following function is used in postgis to get projPJ from str.
 * it is necessary to do it ina detailed way like that because pj_init_plus 
 * does not set all parameters correctly and I cannot test whether the 
 * coordinate reference systems are geographic or not */
 static projPJ projFromStr(char* projStr) {
	int t;
	char *params[1024];  // one for each parameter
	char *loc;
	char *str;
	size_t slen;
	projPJ result;


	if (projStr == NULL) return NULL;

	slen = strlen(projStr);

	if (slen == 0) return NULL;

	str = GDKmalloc(slen+1);
	strcpy(str, projStr);

	// first we split the string into a bunch of smaller strings,
	// based on the " " separator

	params[0] = str; // 1st param, we'll null terminate at the " " soon

	loc = str;
	t = 1;
	while  ((loc != NULL) && (*loc != 0) )
	{
		loc = strchr(loc, ' ');
		if (loc != NULL)
		{
			*loc = 0; // null terminate
			params[t] = loc+1;
			loc++; // next char
			t++; //next param
		}
	}

	if (!(result=pj_init(t, params)))
	{
		GDKfree(str);
		return NULL;
	}
	GDKfree(str);
	return result;

}
#endif

/* It gets a geometry and transforms its coordinates to the provided srid */
str wkbTransform(wkb** transformedWKB, wkb** geomWKB, int* srid_src, int* srid_dst, char** proj4_src_str, char** proj4_dst_str) {
#ifndef HAVE_PROJ 
return createException(MAL, "geom.Transform", "Function Not Implemented");
#else
	projPJ proj4_src, proj4_dst;
	GEOSGeom geosGeometry, transformedGeosGeometry;
	int geometryType = -1;

	str ret = MAL_SUCCEED;



	if(!strcmp(*proj4_src_str, "null"))
		throw(MAL, "geom.wkbTransform", "Could not find in spatial_ref_sys srid %d\n", *srid_src);
	if(!strcmp(*proj4_dst_str, "null"))
		throw(MAL, "geom.wkbTransform", "Could not find in spatial_ref_sys srid %d\n", *srid_dst);
	proj4_src = /*pj_init_plus*/projFromStr(*proj4_src_str);
	proj4_dst = /*pj_init_plus*/projFromStr(*proj4_dst_str);
	
	
	if(*geomWKB == NULL) {
		*transformedWKB = wkb_nil;
		pj_free(proj4_src);
		pj_free(proj4_dst);
		throw(MAL, "geom.Transform", "wkb is null");
	}

	/* get the geosGeometry from the wkb */
	geosGeometry = wkb2geos(*geomWKB);
	/* get the type of the geometry */
	geometryType = GEOSGeomTypeId(geosGeometry)+1;

	switch(geometryType) {
		case wkbPoint:
			ret = transformPoint(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst);
			break;
		case wkbLineString:
			ret = transformLineString(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst);
			break;
		case wkbLinearRing:
			ret = transformLinearRing(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst);
			break;
		case wkbPolygon:
			ret = transformPolygon(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst, *srid_dst);
			break; 
		case wkbMultiPoint:
		case wkbMultiLineString:
		case wkbMultiPolygon:
			ret = transformMultiGeometry(&transformedGeosGeometry, geosGeometry, proj4_src, proj4_dst, *srid_dst, geometryType);
			break;
		default:
			transformedGeosGeometry = NULL;
			ret = createException(MAL, "geom.Transform", "Unknown geometry type");
	}

	if(transformedGeosGeometry) {
		/* set the new srid */
		GEOSSetSRID(transformedGeosGeometry, *srid_dst);
		/* get the wkb */
		*transformedWKB = geos2wkb(transformedGeosGeometry);
		/* destroy the geos geometries */
		GEOSGeom_destroy(transformedGeosGeometry);
	} else
		*transformedWKB = wkb_nil;
	
	pj_free(proj4_src);
	pj_free(proj4_dst);
	GEOSGeom_destroy(geosGeometry);

	return ret;
#endif
}


str wkbPointOnSurface(wkb** resWKB, wkb** geomWKB) {
	GEOSGeom geosGeometry, resGeosGeometry;

	if(wkb_isnil(*geomWKB)){
		*resWKB = wkb_nil;
		return MAL_SUCCEED;
	}
	
	geosGeometry = wkb2geos(*geomWKB);
	if(!geosGeometry) {
		*resWKB = wkb_nil;
		throw(MAL, "geom.PointOnSurface", "wkb2geos failed");
	}

	resGeosGeometry = GEOSPointOnSurface(geosGeometry);
	if(!resGeosGeometry) {
		*resWKB = wkb_nil;
		throw(MAL, "geom.PointOnSurface", "GEOSPointOnSurface failed");
	}

	*resWKB = geos2wkb(resGeosGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(resGeosGeometry);

	return MAL_SUCCEED;
}


str geom_2_geom(wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID) {
	GEOSGeom geosGeometry;
	int geoCoordinatesNum = 2;
	int valueType = 0;
	
	int valueSRID = (*valueWKB)->srid;

	/* get the geosGeometry from the wkb */
	geosGeometry = wkb2geos(*valueWKB);
	/* get the number of coordinates the geometry has */
	geoCoordinatesNum = GEOSGeom_getCoordinateDimension(geosGeometry);
	/* get the type of the geometry */
	(valueType) = (GEOSGeomTypeId(geosGeometry)+1) << 2;

	if(geoCoordinatesNum > 2)
		(valueType) += (1<<1);
	if(geoCoordinatesNum > 3)
		(valueType) += 1;
	
	if(valueSRID != *columnSRID || valueType != *columnType)
		throw(MAL, "calc.wkb", "column needs geometry(%d, %d) and value is geometry(%d, %d)\n", *columnType, *columnSRID, valueType, valueSRID);

	/* get the wkb from the geosGeometry */
	*resWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);
	return MAL_SUCCEED;
}

str geom_2_geom_bat(int* outBAT_id, int* inBAT_id, int* columnType, int* columnSRID) {
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL, *outWKB = NULL;
	BUN p =0, q =0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batcalc.wkb", RUNTIME_OBJECT_MISSING);
	}
	
	if ( inBAT->htype != TYPE_void ) { //header type of  BAT not void
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batcalc.wkb", "the arguments must have dense and aligned heads");
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batcalc.wkb", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the input BAT
	BATseqbase(outBAT, inBAT->hseqbase);

	//iterator over the BAT	
	inBAT_iter = bat_iterator(inBAT);
	//for (i = 0; i < BATcount(inBAT); i++) { 
	BATloop(inBAT, p, q) { //iterate over all valid elements
		str err = NULL;

		//if for used --> inWKB = (wkb *) BUNtail(inBATi, i + BUNfirst(inBAT));
		inWKB = (wkb*) BUNtail(inBAT_iter, p);
		if ((err = geom_2_geom(&outWKB, &inWKB, columnType, columnSRID)) != MAL_SUCCEED) { //check type
			str msg;
			BBPreleaseref(inBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batcalc.wkb", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,outWKB,TRUE); //add the point to the new BAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;

}

/*check if the geometry has z coordinate*/
void geoHasZ(int* res, int* info) {
	if(geometryHasZ(*info)) *res=1;
	else *res=0;

}
/*check if the geometry has m coordinate*/
void geoHasM(int* res, int* info) {
	if(geometryHasM(*info)) *res=1;
	else *res=0;
}
/*check the geometry subtype*/
/*returns the length of the resulting string*/
void geoGetType(char** res, int* info, int* flag) {
	int type = (*info >> 2);
	const char* typeStr=geom_type2str(type, *flag) ;

	*res=GDKmalloc(strlen(typeStr));
	strcpy(*res, typeStr);
}

/* initialise geos */
bat *geom_prelude(void) {
	libgeom_init();
	TYPE_mbr = malAtomSize(sizeof(mbr), sizeof(oid), "mbr");
	return NULL;
}

/* clean geos */
void geom_epilogue(void) {
	libgeom_exit();
}

/* Check if fixed-sized atom mbr is null */
static int mbr_isnil(mbr *m) {
	if (!m || m->xmin == flt_nil || m->ymin == flt_nil ||
	    m->xmax == flt_nil || m->ymax == flt_nil)
		return 1;
	return 0;
}

/* returns the size of variable-sized atom wkb */
static var_t wkb_size(size_t len) {
	if (len == ~(size_t) 0)
		len = 0;
	assert(sizeof(wkb) - 1 + len <= VAR_MAX);
	return (var_t) (sizeof(wkb) - 1 + len);
}

/* NULL: generic nil mbr. */
/* returns a pointer to a nil-mbr. */
mbr *mbrNULL(void) {
	static mbr mbrNIL;
	mbrNIL.xmin = flt_nil;
	mbrNIL.ymin = flt_nil;
	mbrNIL.xmax = flt_nil;
	mbrNIL.ymax = flt_nil;
	return (&mbrNIL);
}

/* returns a pointer to a null wkb */
wkb *wkbNULL(void) {
	static wkb nullval;

	nullval.len = ~(int) 0;
	return (&nullval);
}

/* create the WKB out of the GEOSGeometry 
 * It makes sure to make all checks before returning 
 * the input geosGeometry should not be altered by this function*/
wkb* geos2wkb(const GEOSGeometry* geosGeometry) {
	size_t wkbLen = 0;
	unsigned char *w = NULL;
	wkb *geomWKB;

	// if the geosGeometry is NULL create a NULL WKB
	if(geosGeometry == NULL) {
		geomWKB = GDKmalloc(sizeof(wkb));
		*geomWKB = *wkbNULL();
		return geomWKB;
	}

	GEOS_setWKBOutputDims(GEOSGeom_getCoordinateDimension(geosGeometry));
	w = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);

	//If the GEOSGeomToWKB_buf did not succeed create a NULL WKB	
	if(w == NULL) {
		geomWKB = GDKmalloc(sizeof(wkb));
		*geomWKB = *wkbNULL();
		return geomWKB;
	}
	assert(wkbLen <= GDK_int_max);

	geomWKB = GDKmalloc(wkb_size(wkbLen));
	//If malloc failed create a NULL wkb
	if (geomWKB == NULL) {
		GEOSFree(w);
		geomWKB = GDKmalloc(sizeof(wkb));
		*geomWKB = *wkbNULL();
		return geomWKB;
	}

	geomWKB->len = (int) wkbLen;
	geomWKB->srid = GEOSGetSRID(geosGeometry);
	memcpy(&geomWKB->data, w, wkbLen);
	GEOSFree(w);

	return geomWKB;
}

/* gets the mbr from the geometry */
mbr* mbrFromGeos(const GEOSGeom geosGeometry) {
	GEOSGeom envelope; 
	mbr* geomMBR;
	double xmin=0, ymin=0, xmax=0, ymax=0;

	geomMBR = (mbr*) GDKmalloc(sizeof(mbr));
	if(geomMBR == NULL) //problem in reserving space
		return NULL;

	/* if input is null or GEOSEnvelope created exception then create a nill mbr */
	if(!geosGeometry || (envelope = GEOSEnvelope(geosGeometry)) == NULL) {
		*geomMBR = *mbrNULL();
		return geomMBR;
	}
	
	if ((GEOSGeomTypeId(envelope)+1) == wkbPoint) {
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
			GEOSCoordSeq_getX(coords, 0, &xmin); //left-lower corner
			GEOSCoordSeq_getY(coords, 0, &ymin);
			GEOSCoordSeq_getX(coords, 2, &xmax); //right-upper corner
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


/*str wkbFromString(wkb **w, str *wkt) {
	int len = 0;
	char *errbuf;
	str ex;

	if (wkbFROMSTR(*wkt, 0, &len, w))
		return MAL_SUCCEED;
	errbuf = GDKerrbuf;
	if (errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
	} else {
		errbuf = "cannot parse string";
	}

	ex = createException(MAL, "wkb.FromString", "%s", errbuf);

	if (GDKerrbuf)
		GDKerrbuf[0] = '\0';

	return ex;
}*/


/* FROMSTR: parse string to mbr. */
/* return number of parsed characters. */
int mbrFROMSTR(char *src, int *len, mbr **atom) {
	int nil = 0;
	int nchars = 0;	/* The number of characters parsed; the return value. */
	GEOSGeom geosMbr = NULL; /* The geometry object that is parsed from the src string. */
	double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
	char *c;

	if (strcmp(src, str_nil) == 0)
		nil = 1;

	if (!nil && strstr(src,"BOX") ==  src && (c = strstr(src,"(")) != NULL) {
		/* Parse the mbr */
		if ((c - src) != 3 && (c - src) != 4) {
			GDKerror("ParseException: Expected a string like 'BOX(0 0,1 1)' or 'BOX (0 0,1 1)'\n");
			return 0;
		}

		if (sscanf(c,"(%lf %lf,%lf %lf)", &xmin, &ymin, &xmax, &ymax) != 4) {
			GDKerror("ParseException: Not enough coordinates.\n");
			return 0;
		}
	} else if (!nil && (geosMbr = GEOSGeomFromWKT(src)) == NULL)
		return 0;

	if (*len < (int) sizeof(mbr)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = sizeof(mbr));
	}
	if (nil) {
		nchars = 3;
		**atom = *mbrNULL();
	} else if (geosMbr == NULL) {
		size_t l;
		assert(GDK_flt_min <= xmin && xmin <= GDK_flt_max);
		assert(GDK_flt_min <= xmax && xmax <= GDK_flt_max);
		assert(GDK_flt_min <= ymin && ymin <= GDK_flt_max);
		assert(GDK_flt_min <= ymax && ymax <= GDK_flt_max);
		(*atom)->xmin = (float) xmin;
		(*atom)->ymin = (float) ymin;
		(*atom)->xmax = (float) xmax;
		(*atom)->ymax = (float) ymax;
		l = strlen(src);
		assert(l <= GDK_int_max);
		nchars = (int) l;
	}
	if (geosMbr)
		GEOSGeom_destroy(geosMbr);
	return nchars;
}

#define MBR_WKTLEN 256

/* TOSTR: print atom in a string. */
/* return length of resulting string. */
int mbrTOSTR(char **dst, int *len, mbr *atom) {
	static char tempWkt[MBR_WKTLEN];
	size_t dstStrLen = 3;

	if (!mbr_isnil(atom)) {
		snprintf(tempWkt, MBR_WKTLEN, "BOX (%f %f, %f %f)",
			 atom->xmin, atom->ymin, atom->xmax, atom->ymax);
		dstStrLen = strlen(tempWkt) + 2;
		assert(dstStrLen < GDK_int_max);
	}

	if (*len < (int) dstStrLen + 1) {
		if (*dst)
			GDKfree(*dst);
		*dst = GDKmalloc(*len = (int) dstStrLen + 1);
	}

	if (dstStrLen > 3)
		snprintf(*dst, *len, "\"%s\"", tempWkt);
	else
		strcpy(*dst, "nil");
	return (int) dstStrLen;
}

/* Creates WKB representation (including srid) from WKT representation */
/* return number of parsed characters. */
int wkbFROMSTR(char* geomWKT, int* len, wkb **geomWKB, int srid) {
	GEOSGeom geosGeometry = NULL;	/* The geometry object that is parsed from the src string. */
	GEOSWKTReader *WKT_reader;

	if (strcmp(geomWKT, str_nil) == 0) {
		*geomWKB = wkb_nil;
		return 0;
	}

	WKT_reader = GEOSWKTReader_create();
	geosGeometry = GEOSWKTReader_read(WKT_reader, geomWKT); 
	GEOSWKTReader_destroy(WKT_reader);

	if(geosGeometry == NULL){
		*geomWKB = wkb_nil;
		return 0;
	}

	if (GEOSGeomTypeId(geosGeometry) == -1) {
		GEOSGeom_destroy(geosGeometry);
		*geomWKB = wkb_nil;
		return 0;
	}

	GEOSSetSRID(geosGeometry, srid);
	/* the srid was lost with the transformation of the GEOSGeom to wkb
	* so we decided to store it in the wkb */ 
		
	/* we have a GEOSGeometry will number of coordinates and SRID and we 
 	* want to get the wkb out of it */
	*geomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	*len = (int) wkb_size((*geomWKB)->len);

	return (int)strlen(geomWKT);
}

//Returns the wkb in a hex representation */
static char hexit[] = "0123456789ABCDEF";

str wkbAsBinary(char **toStr, wkb **geomWKB) {
	char *s;
	int i;

	if(wkb_isnil(*geomWKB)) {
		*toStr = (str) GDKmalloc(1);
		**toStr = '\0';
		return MAL_SUCCEED;
	}
	*toStr = (str) GDKmalloc(1+((*geomWKB)->len)*2);

	s = *toStr;
	for (i = 0; i < (*geomWKB)->len; i++) {
		int val = ((*geomWKB)->data[i] >> 4) & 0xf;
		*s++ = hexit[val];
		val = (*geomWKB)->data[i] & 0xf;
		*s++ = hexit[val];
//fprintf(stderr, "%d First: %c - Second: %c ==> Original %c (%d)\n", i, *(s-2), *(s-1), (*geomWKB)->data[i], (int)((*geomWKB)->data[i]));
	}
	*s = '\0';
	return MAL_SUCCEED;
}

static int decit(char hex) {
	switch(hex) {
		case '0':
			return 0;
			break;
		case '1':
			return 1;
			break;
		case '2':
			return 2;
			break;
		case '3':
			return 3;
			break;
		case '4':
			return 4;
			break;
		case '5':
			return 5;
			break;
		case '6':
			return 6;
			break;
		case '7':
			return 7;
			break;
		case '8':
			return 8;
			break;
		case '9':
			return 9;
			break;
		case 'A':
			return 10;
			break;
		case 'B':
			return 11;
			break;
		case 'C':
			return 12;
			break;
		case 'D':
			return 13;
			break;
		case 'E':	
			return 14;
			break;
		case 'F':
			return 15;
			break;
		default:
			return -1;
	}
}
str wkbFromBinary(wkb** geomWKB, char **inStr) {
	size_t strLength = 0, wkbLength = 0, i;
	char* s;

	strLength = strlen(*inStr);
	
	wkbLength = strLength/2;
	assert(wkbLength <= GDK_int_max);

	s = (char*)GDKmalloc(wkbLength);

	//compute the value for s
	for(i=0; i<strLength; i+=2) {
		char firstHalf = (decit((*inStr)[i]) << 4) & 0xf0; //make sure that only the four most significant bits may be 1
		char secondHalf = decit((*inStr)[i+1]) & 0xf; //make sure that only the four least significant bits may be 1
		s[i/2] = firstHalf | secondHalf; //concatenate the two halfs to create the final byte 
//fprintf(stderr, "%zd First: %c - Second: %c ==> Final: %c (%d)\n", i, (*inStr)[i], (*inStr)[i+1], s[i/2], (int)s[i/2]);
	}

	*geomWKB = GDKmalloc(wkb_size(wkbLength));
	(*geomWKB)->len = (int) wkbLength;
	(*geomWKB)->srid = 0;
	memcpy(&(*geomWKB)->data, s, wkbLength);
	GDKfree(s);


	return MAL_SUCCEED;
}

/* Creates the string representation (WKT) of a WKB */
/* return length of resulting string. */
int wkbTOSTR(char **geomWKT, int* len, wkb *geomWKB) {
	char *wkt = NULL;
	int dstStrLen = 5;			/* "nil" */

	/* from WKB to GEOSGeometry */
	GEOSGeom geosGeometry = wkb2geos(geomWKB);

	
	if (geosGeometry) {
		size_t l;
		GEOSWKTWriter *WKT_wr = GEOSWKTWriter_create();
		//set the number of dimensions in the writer so that it can 
		//read correctly the geometry coordinates
		GEOSWKTWriter_setOutputDimension(WKT_wr, GEOSGeom_getCoordinateDimension(geosGeometry));
		wkt = GEOSWKTWriter_write(WKT_wr, geosGeometry);
		l = strlen(wkt);
		assert(l < GDK_int_max);
		dstStrLen = (int) l + 2;	/* add quotes */
		GEOSWKTWriter_destroy(WKT_wr);
		GEOSGeom_destroy(geosGeometry);
	}

	if (wkt) {
	//	if (*len < (int) dstStrLen + 1) 
			*len = dstStrLen+1;
		
		*geomWKT = GDKmalloc(*len);
		snprintf(*geomWKT, *len, "\"%s\"", wkt);
		GEOSFree(wkt);
	} else {
		strcpy(*geomWKT, "nil");
	}

	return (int) dstStrLen;
}



/* read mbr from disk */
mbr* mbrREAD(mbr *a, stream *s, size_t cnt) {
	mbr *c;
	size_t i;
	int v[4];
	flt vals[4];

	for (i = 0, c = a; i < cnt; i++, c++) {
		if (!mnstr_readIntArray(s, v, 4))
			return NULL;
		memcpy(vals, v, 4 * sizeof(int));
		c->xmin = vals[0];
		c->ymin = vals[1];
		c->xmax = vals[2];
		c->ymax = vals[3];
	}
	return a;
}

/* write mbr to disk */
int mbrWRITE(mbr *c, stream *s, size_t cnt) {
	size_t i;
	flt vals[4];
	int v[4];

	for (i = 0; i < cnt; i++, c++) {
		vals[0] = c->xmin;
		vals[1] = c->ymin;
		vals[2] = c->xmax;
		vals[3] = c->ymax;
		memcpy(v, vals, 4 * sizeof(int));
		if (!mnstr_writeIntArray(s, v, 4))
			return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* read wkb from disk */
wkb* wkbREAD(wkb *a, stream *s, size_t cnt) {
	int len;
	int srid;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1)
		return NULL;
	if (mnstr_readInt(s, &srid) != 1)
		return NULL;
	if ((a = GDKmalloc(wkb_size(len))) == NULL)
		return NULL;
	a->len = len;
	a->srid = srid;
	if (len > 0 && mnstr_read(s, (char *) a->data, len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	return a;
}

/* write wkb to disk */
int wkbWRITE(wkb *a, stream *s, size_t cnt) {
	int len = a->len;
	int srid = a->srid;

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, len))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (!mnstr_writeInt(s, srid))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (len > 0 &&			/* 64bit: check for overflow */
	    mnstr_write(s, (char *) a->data, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

str mbrFromMBR(mbr **w, mbr **src) {
	*w = (mbr *) GDKmalloc(sizeof(mbr));

	**w = **src;
	return MAL_SUCCEED;
}

str wkbFromWKB(wkb **w, wkb **src) {
	*w = (wkb *) GDKmalloc(wkb_size((*src)->len));

	if (wkb_isnil(*src)) {
		**w = *wkbNULL();
	} else {
		(*w)->len = (*src)->len;
		(*w)->srid = (*src)->srid;
		memcpy(&(*w)->data, &(*src)->data, (*src)->len);
	}
	return MAL_SUCCEED;
}
/*
str wkbFromWKB_bat(int* outBAT_id, int* inBAT_id) {
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb **inWKB = NULL, *outWKB = NULL;
	BUN i;
	
	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.wkb", RUNTIME_OBJECT_MISSING);
	}
	
	if ( inBAT->htype != TYPE_void ) { //header type of  BAT not void
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.wkb", "both arguments must have dense and aligned heads");
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(inBAT))) == NULL) {
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.wkb", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the input BAT
	BATseqbase(outBAT, inBAT->hseqbase);

	//pointers to the first valid elements of the x and y BATS
	inWKB = (wkb **) Tloc(inBAT, BUNfirst(inBAT));
	for (i = 0; i < BATcount(inBAT); i++) { //iterate over all valid elements
		str err = NULL;
		if ((err = wkbFromWKB(&outWKB, &inWKB[i])) != MAL_SUCCEED) { 
			str msg;
			BBPreleaseref(inBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.wkb", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,outWKB,TRUE); //add the point to the new BAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;

}*/

/* creates a wkb from the given textual representation */
/*int* tpe is needed to verify that the type of the FromText function used is the
 * same with the type of the geometry created from the wkt representation */
str wkbFromText(wkb **geomWKB, str *geomWKT, int* srid, int *tpe) {
	int len=0, te = *tpe;
	char *errbuf;
	str ex;

	*geomWKB = NULL;
	if (wkbFROMSTR(*geomWKT, &len, geomWKB, *srid) &&
	    (wkb_isnil(*geomWKB) || *tpe==0 || *tpe == wkbGeometryCollection || (te = *((*geomWKB)->data + 1) & 0x0f) == *tpe))
		return MAL_SUCCEED;
	if (*geomWKB == NULL) {
		*geomWKB = wkb_nil;
	}
	if (*tpe > 0 && te != *tpe)
		throw(MAL, "wkb.FromText", "Trying to read Geometry type '%s' with function for Geometry type '%s'", geom_type2str(te,0), geom_type2str(*tpe,0));
	errbuf = GDKerrbuf;
	if (errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
	} else {
		errbuf = "cannot parse string";
	}

	ex = createException(MAL, "wkb.FromText", "%s", errbuf);

	if (GDKerrbuf)
		GDKerrbuf[0] = '\0';

	return ex;
}

/*create textual representation of the wkb */
str wkbAsText(char **txt, wkb **geomWKB, int* withSRID) {
	int len =0;
	char* wkt;

	if(wkbTOSTR(&wkt, &len, *geomWKB)) {
		if(*withSRID == 0) {
			*txt = GDKmalloc(strlen(wkt));
			if(*txt == NULL) {
				GDKfree(wkt);
				throw(MAL, "geom.wkbAsText", MAL_MALLOC_FAIL);
			}
			strcpy(*txt, wkt);
		} else {
			char* sridTxt = "SRID:";
			char* sridIntToString = NULL;
			size_t len2 = 0;

			//count the number of digits in srid
			int tmp = (*geomWKB)->srid;
			int digitsNum =0;
			while(tmp > 0) {
				tmp/=10;
				digitsNum++;
			}

			sridIntToString = GDKmalloc(digitsNum+1);
			if(sridIntToString == NULL) {
				GDKfree(wkt);
				throw(MAL, "geom.wkbAsText", MAL_MALLOC_FAIL);
			}
			sprintf(sridIntToString, "%d", (*geomWKB)->srid);

			len2 = strlen(wkt)+strlen(sridIntToString)+strlen(sridTxt)+2;
			*txt = GDKmalloc(len2);
			if(*txt == NULL) {
				GDKfree(wkt);
				GDKfree(sridIntToString);
				throw(MAL, "geom.wkbAsText", MAL_MALLOC_FAIL);
			}

			memcpy(*txt, sridTxt, strlen(sridTxt));
			memcpy(*txt+strlen(sridTxt), sridIntToString, strlen(sridIntToString));
			(*txt)[strlen(sridTxt)+strlen(sridIntToString)] = ';';
			memcpy(*txt+strlen(sridTxt)+strlen(sridIntToString)+1, wkt, strlen(wkt));
			(*txt)[len2-1] = '\0';

			GDKfree(sridIntToString);
		}

		GDKfree(wkt);
		return MAL_SUCCEED;
	}
	throw(MAL, "geom.AsText", "Failed to create Text from Well Known Format");
}

static str geomMakePoint(wkb **geomWKB, GEOSGeom geosGeometry) {
	
	*geomWKB = geos2wkb(geosGeometry);
	
	if(wkb_isnil(*geomWKB)) {
		*geomWKB = wkb_nil;
		throw(MAL, "geom.MakePoint", "Failed to crete WKB from GEOSGeometry");
	}

	return MAL_SUCCEED;
}

/* creates a point using the x, y coordinates */
str geomMakePoint2D(wkb** out, double* x, double* y) {
	GEOSGeom geosGeometry = NULL;
	str ret = MAL_SUCCEED;
	GEOSCoordSequence *seq = NULL;

	if (*x == dbl_nil || *y == dbl_nil) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	//create the point from the coordinates
	seq = GEOSCoordSeq_create(1, 2);
	GEOSCoordSeq_setX(seq, 0, *x);
	GEOSCoordSeq_setY(seq, 0, *y);
	geosGeometry = GEOSGeom_createPoint(seq);
	GEOSSetSRID(geosGeometry, 0);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geom.MakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	ret = geomMakePoint(out, geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return ret;
}

/* the bat version of geomMakePoint2D */
str geomMakePoint2D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id) {
	BAT *outBAT = NULL, *xBAT = NULL, *yBAT = NULL;
	dbl *x = NULL, *y = NULL;
	BUN i;
	wkb *p = NULL;
	
	//get the descriptors of the x and y BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ( xBAT->htype != TYPE_void || //header type of x BAT not void
		 yBAT->htype != TYPE_void || //header type of y BAT not void
	    xBAT->hseqbase != yBAT->hseqbase || //the idxs of the headers of the x and y BATs are not the same
	    BATcount(xBAT) != BATcount(yBAT)) { //the number of valid elements in the x and y BATs are not the same
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", "both arguments must have dense and aligned heads");
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);

	//pointers to the first valid elements of the x and y BATS
	x = (dbl *) Tloc(xBAT, BUNfirst(xBAT));
	y = (dbl *) Tloc(yBAT, BUNfirst(yBAT));
	for (i = 0; i < BATcount(xBAT); i++) { //iterate over all valid elements
		str err = NULL;
		if ((err = geomMakePoint2D(&p, &x[i], &y[i])) != MAL_SUCCEED) { //create point
			str msg;
			BBPreleaseref(xBAT->batCacheid);
			BBPreleaseref(yBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.MakePoint", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,p,TRUE); //add the point to the new BAT
		GDKfree(p);
		p = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(xBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;
}

/* creates a point using the x, y, z coordinates */
str geomMakePoint3D(wkb** out, double* x, double* y, double* z) {
	GEOSGeom geosGeometry = NULL;
	str ret = MAL_SUCCEED;
	GEOSCoordSequence *seq = NULL;

	if (*x == dbl_nil || *y == dbl_nil || *z == dbl_nil) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	//create the point from the coordinates
	seq = GEOSCoordSeq_create(1, 3);
	GEOSCoordSeq_setX(seq, 0, *x);
	GEOSCoordSeq_setY(seq, 0, *y);
	GEOSCoordSeq_setZ(seq, 0, *z);
	geosGeometry = GEOSGeom_createPoint(seq);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geom.MakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	ret = geomMakePoint(out, geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return ret;
}

/* the bat version og geomMakePoint3D */
str geomMakePoint3D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* zBAT_id) {
	BAT *outBAT = NULL, *xBAT = NULL, *yBAT = NULL, *zBAT = NULL;
	dbl *x = NULL, *y = NULL, *z = NULL;
	BUN i;
	wkb *p = NULL;

	//get the descriptors of the input BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((zBAT = BATdescriptor(*zBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ( xBAT->htype != TYPE_void || //header type of x BAT not void
		yBAT->htype != TYPE_void || //header type of y BAT not void
		zBAT->htype != TYPE_void || //header type of z BAT not void
		xBAT->hseqbase != yBAT->hseqbase || 
			xBAT->hseqbase != zBAT->hseqbase || //the idxs of the headers of the BATs are not the same
		BATcount(xBAT) != BATcount(yBAT) || 
			BATcount(xBAT) != BATcount(zBAT)) { //the number of valid elements in the BATs are not the same
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		BBPreleaseref(zBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", "both arguments must have dense and aligned heads");
	}

	//create a new BAT. A BAT can be either PERSISTENT or TRANSIENT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		BBPreleaseref(zBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);

	//pointers to the first valid elements of the x and y BATS
	x = (dbl *) Tloc(xBAT, BUNfirst(xBAT));
	y = (dbl *) Tloc(yBAT, BUNfirst(yBAT));
	z = (dbl *) Tloc(zBAT, BUNfirst(zBAT));
	for (i = 0; i < BATcount(xBAT); i++) { //iterate over all valid elements
		str err = NULL;
		if ((err = geomMakePoint3D(&p, &x[i], &y[i], &z[i])) != MAL_SUCCEED) { //create point
			str msg;
			BBPreleaseref(xBAT->batCacheid);
			BBPreleaseref(yBAT->batCacheid);
			BBPreleaseref(zBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.MakePoint", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,p,TRUE); //add the point to the new BAT
		GDKfree(p);
		p = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(xBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPreleaseref(zBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;
}


/* creates a point using the x, y, z, m coordinates */
str geomMakePoint4D(wkb** out, double* x, double* y, double* z, double* m) {
	GEOSGeom geosGeometry = NULL;
	str ret = MAL_SUCCEED;
	GEOSCoordSequence *seq = NULL;

	if (*x == dbl_nil || *y == dbl_nil || *z == dbl_nil || *m == dbl_nil) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	//create the point from the coordinates
	seq = GEOSCoordSeq_create(1, 4);
	GEOSCoordSeq_setX(seq, 0, *x);
	GEOSCoordSeq_setY(seq, 0, *y);
	GEOSCoordSeq_setZ(seq, 0, *z);
	GEOSCoordSeq_setOrdinate(seq, 0, 3, *m);
	geosGeometry = GEOSGeom_createPoint(seq);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	ret = geomMakePoint(out, geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return ret;
}

/* the bat version og geomMakePoint4D */
str geomMakePoint4D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* zBAT_id, int* mBAT_id) {
	BAT *outBAT = NULL, *xBAT = NULL, *yBAT = NULL, *zBAT = NULL, *mBAT = NULL;
	dbl *x = NULL, *y = NULL, *z = NULL, *m = NULL;
	BUN i;
	wkb *p = NULL;

	//get the descriptors of the input BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((zBAT = BATdescriptor(*zBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((mBAT = BATdescriptor(*mBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		BBPreleaseref(zBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ( xBAT->htype != TYPE_void || //header type of x BAT not void
		yBAT->htype != TYPE_void || //header type of y BAT not void
		zBAT->htype != TYPE_void || //header type of z BAT not void
		mBAT->htype != TYPE_void || //header type of z BAT not void
		xBAT->hseqbase != yBAT->hseqbase || 
			xBAT->hseqbase != zBAT->hseqbase || 
			xBAT->hseqbase != mBAT->hseqbase || //the idxs of the headers of the BATs are not the same
		BATcount(xBAT) != BATcount(yBAT) || 
			BATcount(xBAT) != BATcount(zBAT) ||
			BATcount(xBAT) != BATcount(mBAT)) { //the number of valid elements in the BATs are not the same
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		BBPreleaseref(zBAT->batCacheid);
		BBPreleaseref(mBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", "both arguments must have dense and aligned heads");
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		BBPreleaseref(zBAT->batCacheid);
		BBPreleaseref(mBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);

	//pointers to the first valid elements of the x and y BATS
	x = (dbl *) Tloc(xBAT, BUNfirst(xBAT));
	y = (dbl *) Tloc(yBAT, BUNfirst(yBAT));
	z = (dbl *) Tloc(zBAT, BUNfirst(zBAT));
	m = (dbl *) Tloc(mBAT, BUNfirst(mBAT));
	for (i = 0; i < BATcount(xBAT); i++) { //iterate over all valid elements
		str err = NULL;
		if ((err = geomMakePoint4D(&p, &x[i], &y[i], &z[i], &m[i])) != MAL_SUCCEED) { //create point
			str msg;
			BBPreleaseref(xBAT->batCacheid);
			BBPreleaseref(yBAT->batCacheid);
			BBPreleaseref(zBAT->batCacheid);
			BBPreleaseref(mBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.MakePoint", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,p,TRUE); //add the point to the new BAT
		GDKfree(p);
		p = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(xBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPreleaseref(zBAT->batCacheid);
	BBPreleaseref(mBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;
}

str geomMakePointM_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* mBAT_id) {
	BAT *outBAT = NULL, *xBAT = NULL, *yBAT = NULL, *mBAT = NULL;
	dbl *x = NULL, *y = NULL, *m = NULL;
	BUN i;
	wkb *p = NULL;

	//get the descriptors of the input BATs
	if ((xBAT = BATdescriptor(*xBAT_id)) == NULL) {
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((yBAT = BATdescriptor(*yBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ((mBAT = BATdescriptor(*mBAT_id)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", RUNTIME_OBJECT_MISSING);
	}
	if ( xBAT->htype != TYPE_void || //header type of x BAT not void
		yBAT->htype != TYPE_void || //header type of y BAT not void
		mBAT->htype != TYPE_void || //header type of z BAT not void
		xBAT->hseqbase != yBAT->hseqbase || 
			xBAT->hseqbase != mBAT->hseqbase || //the idxs of the headers of the BATs are not the same
		BATcount(xBAT) != BATcount(yBAT) || 
			BATcount(xBAT) != BATcount(mBAT)) { //the number of valid elements in the BATs are not the same
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		BBPreleaseref(mBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", "the arguments must have dense and aligned heads");
	}

	//create a new BAT
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(xBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(xBAT->batCacheid);
		BBPreleaseref(yBAT->batCacheid);
		BBPreleaseref(mBAT->batCacheid);
		throw(MAL, "batgeom.MakePoint", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the x BAT (which is equal to the y BAT)
	BATseqbase(outBAT, xBAT->hseqbase);

	//pointers to the first valid elements of the x and y BATS
	x = (dbl *) Tloc(xBAT, BUNfirst(xBAT));
	y = (dbl *) Tloc(yBAT, BUNfirst(yBAT));
	m = (dbl *) Tloc(mBAT, BUNfirst(mBAT));
	for (i = 0; i < BATcount(xBAT); i++) { //iterate over all valid elements
		str err = NULL;
		if ((err = geomMakePointM(&p, &x[i], &y[i], &m[i])) != MAL_SUCCEED) { //create point
			str msg;
			BBPreleaseref(xBAT->batCacheid);
			BBPreleaseref(yBAT->batCacheid);
			BBPreleaseref(mBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.MakePoint", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,p,TRUE); //add the point to the new BAT
		GDKfree(p);
		p = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(xBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(xBAT->batCacheid);
	BBPreleaseref(yBAT->batCacheid);
	BBPreleaseref(mBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;
}


/* creates a point using the x, y, m coordinates */
str geomMakePointM(wkb** out, double* x, double* y, double* m) {
	GEOSGeom geosGeometry = NULL;
	str ret = MAL_SUCCEED;
	GEOSCoordSequence *seq = NULL;

	if (*x == dbl_nil || *y == dbl_nil || *m == dbl_nil) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	//create the point from the coordinates
	seq = GEOSCoordSeq_create(1, 3);
	GEOSCoordSeq_setOrdinate(seq, 0, 0, *x);
	GEOSCoordSeq_setOrdinate(seq, 0, 1, *y);
	GEOSCoordSeq_setOrdinate(seq, 0, 2, *m);
	geosGeometry = GEOSGeom_createPoint(seq);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	ret = geomMakePoint(out, geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	return ret;
}

/* common code for functions that return integer */
static str wkbBasicInt(int *out, wkb *geom, int (*func)(const GEOSGeometry *), const char* name) {
	GEOSGeom geosGeometry = wkb2geos(geom);
	str ret = MAL_SUCCEED;

	if (!geosGeometry) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	*out = (*func)(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	//if there was an error returned by geos
	if (GDKerrbuf && GDKerrbuf[0]) {
		//create an exception with this name
		ret = createException(MAL, name, "%s", GDKerrbuf);
		
		//clear the error buffer
		GDKerrbuf[0]='\0'; 
	}
	return ret;

}


/* returns the type of the geometry as a string*/
str wkbGeometryType(char** out, wkb** geomWKB, int* flag) {
	int typeId = 0;
	str ret = MAL_SUCCEED;

	ret = wkbBasicInt(&typeId, *geomWKB, GEOSGeomTypeId, "geom.GeometryType");
	if(ret != MAL_SUCCEED)
		return ret;
	typeId = ((typeId+1) << 2);
	geoGetType(out, &typeId, flag);
	
	return ret;
}

/* returns the number of dimensions of the geometry */
str wkbCoordDim(int *out, wkb **geom) {
	return wkbBasicInt(out, *geom, GEOSGeom_getCoordinateDimension, "geom.CoordDim");
}

/* returns the inherent dimension of the geometry, e.g 0 for point */
str wkbDimension(int *dimension, wkb **geomWKB) {
	return wkbBasicInt(dimension, *geomWKB, GEOSGeom_getDimensions, "geom.Dimension");
}

/* returns the srid of the geometry */
str wkbGetSRID(int *out, wkb **geomWKB) {
	return wkbBasicInt(out, *geomWKB, GEOSGetSRID, "geom.GetSRID");
}

/* sets the srid of the geometry */
str wkbSetSRID(wkb** resultGeomWKB, wkb **geomWKB, int* srid) {
	GEOSGeom geosGeometry = wkb2geos(*geomWKB);

	if (!geosGeometry) {
		*resultGeomWKB = wkb_nil; 
		return MAL_SUCCEED;
	}

	GEOSSetSRID(geosGeometry, *srid);
	*resultGeomWKB = geos2wkb(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if(*resultGeomWKB == NULL)
		throw(MAL, "geom.setSRID", "wkbSetSRID failed");

	return MAL_SUCCEED;
}

/* sets the srid of the geometry - BULK version*/
str wkbSetSRID_bat(int* outBAT_id, int* inBAT_id, int* srid) {
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL, *outWKB = NULL;
	BUN p=0, q=0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.SetSRID", RUNTIME_OBJECT_MISSING);
	}
	
	if ( inBAT->htype != TYPE_void ) { //header type of  BAT not void
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.SetSRID", "the arguments must have dense and aligned heads");
	}

	//create a new BAT for the output
	if ((outBAT = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.SetSRID", MAL_MALLOC_FAIL);
	}
	//set the first idx of the output BAT equal to that of the input BAT
	BATseqbase(outBAT, inBAT->hseqbase);

	//iterator over the BAT	
	inBAT_iter = bat_iterator(inBAT);
	//for (i = 0; i < BATcount(inBAT); i++) { 
	BATloop(inBAT, p, q) { //iterate over all valid elements
		str err = NULL;

		//if for used --> inWKB = (wkb *) BUNtail(inBATi, i + BUNfirst(inBAT));
		inWKB = (wkb*) BUNtail(inBAT_iter, p);
		if ((err = wkbSetSRID(&outWKB, &inWKB, srid)) != MAL_SUCCEED) { //set SRID
			str msg;
			BBPreleaseref(inBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.SetSRID", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,outWKB,TRUE); //add the point to the new BAT
		GDKfree(outWKB);
		outWKB = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;

}

/* depending on the specific function it returns the X,Y or Z coordinate of a point */
static str wkbGetCoord(double *out, wkb **geom, int dimNum, const char *name) {
	//int ret=MAL_SUCCEED;
	GEOSGeom geosGeometry = wkb2geos(*geom);
#if GEOS_CAPI_VERSION_MAJOR >= 1 && GEOS_CAPI_VERSION_MINOR >= 3
	const GEOSCoordSequence *gcs;
#else
	const GEOSCoordSeq gcs;
#endif

	if (!geosGeometry) {
		*out = dbl_nil;
		throw(MAL, name, "wkb2geos failed");
	}

	if((GEOSGeomTypeId(geosGeometry)+1) != wkbPoint)
		throw(MAL, name, "Geometry not a Point"); 

	gcs = GEOSGeom_getCoordSeq(geosGeometry);

	if (!gcs) {
		throw(MAL, name, "GEOSGeom_getCoordSeq failed");
	}
	
	GEOSCoordSeq_getOrdinate(gcs, 0, dimNum, out);
	/* gcs shouldn't be freed, it's internal to the GEOSGeom */
	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

str wkbGetCoordX(double *out, wkb **geom) {
	return wkbGetCoord(out, geom, 0, "geom.X");
}

str wkbGetCoordY(double *out, wkb **geom) {
	return wkbGetCoord(out, geom, 1, "geom.Y");
}

/* geos does not store more than 3 dimensions in wkb so this function
 * will never work unless we change geos */
str wkbGetCoordZ(double *out, wkb **geom) {
	return wkbGetCoord(out, geom, 2, "geom.Z");
}


/*common code for functions that return geometry */
static str wkbBasic(wkb **out, wkb **geom, GEOSGeometry* (*func)(const GEOSGeometry *), const char *name) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		throw(MAL, name, "wkb2geos failed");
	}

	*out = geos2wkb((*func)(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

str wkbBoundary(wkb **boundaryWKB, wkb **geomWKB) {
	return wkbBasic(boundaryWKB, geomWKB, GEOSBoundary, "geom.Boundary");
}

str wkbEnvelope(wkb **out, wkb **geom) {
	return wkbBasic(out, geom, GEOSEnvelope, "geom.Envelope");
}

str wkbEnvelopeFromCoordinates(wkb** out, double* xmin, double* ymin, double* xmax, double* ymax, int* srid) {
	GEOSGeom geosGeometry, linearRingGeometry;
 	
	//create the coordinates sequence
	GEOSCoordSeq coordSeq = GEOSCoordSeq_create(5, 2);
	
	//set the values
	GEOSCoordSeq_setX(coordSeq, 0, *xmin);	
	GEOSCoordSeq_setY(coordSeq, 0, *ymin);
	GEOSCoordSeq_setX(coordSeq, 1, *xmin);	
	GEOSCoordSeq_setY(coordSeq, 1, *ymax);
	GEOSCoordSeq_setX(coordSeq, 2, *xmax);	
	GEOSCoordSeq_setY(coordSeq, 2, *ymax);
	GEOSCoordSeq_setX(coordSeq, 3, *xmax);	
	GEOSCoordSeq_setY(coordSeq, 3, *ymin);
	GEOSCoordSeq_setX(coordSeq, 4, *xmin);	
	GEOSCoordSeq_setY(coordSeq, 4, *ymin);

	linearRingGeometry = GEOSGeom_createLinearRing(coordSeq);

	if(linearRingGeometry == NULL) {
		//Gives segmentation fault GEOSCoordSeq_destroy(coordSeq);
		throw(MAL, "geom.MakeEnvelope", "Error creating LinearRing from coordinates");
	}
	geosGeometry = GEOSGeom_createPolygon(linearRingGeometry, NULL, 0);
	if(geosGeometry == NULL) {
		GEOSGeom_destroy(linearRingGeometry);
		throw(MAL, "geom.MakeEnvelope", "Error creating Polygon from LinearRing");
	}
	GEOSSetSRID(geosGeometry, *srid);

	*out = geos2wkb(geosGeometry);

	return MAL_SUCCEED;
}

str wkbMakePolygon(wkb** out, wkb** external, int* internalBAT_id, int* srid) {
	GEOSGeom geosGeometry, externalGeometry, linearRingGeometry;
	bit closed = 0;
	GEOSCoordSeq coordSeq_copy;

	if(wkb_isnil(*external)){
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	externalGeometry = wkb2geos(*external);
	
	//check the type of the external geometry
	if ((GEOSGeomTypeId(externalGeometry)+1) != wkbLineString) {
		*out = wkb_nil;
		GEOSGeom_destroy(externalGeometry);
		throw(MAL, "geom.Polygon", "Geometries should be LineString");
	}

	//check whether the linestring is closed
	wkbIsClosed(&closed, external);
	if (!closed) {
		*out = wkb_nil;
		GEOSGeom_destroy(externalGeometry);
		throw(MAL, "geom.Polygon", "LineString should be closed");
	}

	//create a copy of the coordinates
	coordSeq_copy = GEOSCoordSeq_clone(GEOSGeom_getCoordSeq(externalGeometry));
	
	//create a linearRing using the copy of the coordinates
	linearRingGeometry = GEOSGeom_createLinearRing(coordSeq_copy);

	//create a polygon using the linearRing
	if(*internalBAT_id == 0) {
		geosGeometry = GEOSGeom_createPolygon(linearRingGeometry, NULL, 0);
		if(geosGeometry == NULL) {
			GEOSGeom_destroy(linearRingGeometry);
			throw(MAL, "geom.Polygon", "Error creating Polygon from LinearRing");
		}
	} else {
		geosGeometry = NULL;
	}

	GEOSSetSRID(geosGeometry, *srid);

	*out = geos2wkb(geosGeometry);

	return MAL_SUCCEED;


}

/* Returns the first or last point of a linestring */
static str wkbBorderPoint(wkb **out, wkb **geom, GEOSGeometry* (*func)(const GEOSGeometry *), const char *name) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		throw(MAL, name, "wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		*out = wkb_nil;
		throw(MAL, name, "GEOSGeomGet%s failed. Geometry not a LineString", name + 5);
	}

	*out = geos2wkb((*func)(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* Returns the first point in a linestring */
str wkbStartPoint(wkb **out, wkb **geom) {
	return wkbBorderPoint(out, geom, GEOSGeomGetStartPoint, "geom.StartPoint");
}

/* Returns the last point in a linestring */
str wkbEndPoint(wkb **out, wkb **geom) {
	return wkbBorderPoint(out, geom, GEOSGeomGetEndPoint, "geom.EndPoint");
}

/* Returns the number of points in a geometry */
str wkbNumPoints(int *out, wkb **geom) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "wkb2geos failed");	
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		*out = int_nil;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.NumPoints", "Geometry not a LineString");
	}

	*out = GEOSGeomGetNumPoints(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (*out == -1) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "GEOSGeomGetNumPoints failed");
	}

	return MAL_SUCCEED;
}

/* Returns the n-th point of the geometry */
str wkbPointN(wkb **out, wkb **geom, int *n) {
	int rN = -1;
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		throw(MAL, "geom.PointN", "wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		*out = wkb_nil;
		throw(MAL, "geom.PointN", "Geometry not a LineString");
	}

	//check number of points
	rN = GEOSGeomGetNumPoints(geosGeometry);
	if (rN == -1)
		throw(MAL, "geom.PointN", "GEOSGeomGetNumPoints failed");	

	if(rN <= *n || *n<0) {
		*out = wkb_nil;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.PointN", "Unable to retrieve point %d (not enough points)", *n);
	}

	*out = geos2wkb(GEOSGeomGetPointN(geosGeometry, *n));
	GEOSGeom_destroy(geosGeometry);

	if (*out != NULL)
		return MAL_SUCCEED;

	throw(MAL, "geom.PointN", "GEOSGeomGetPointN failed");
}

/* Returns the exterior ring of the polygon*/
str wkbExteriorRing(wkb **exteriorRingWKB, wkb **geom) {
	GEOSGeom geosGeometry = NULL;
	const GEOSGeometry* exteriorRingGeometry;

	geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) { 
		*exteriorRingWKB = wkb_nil;
		throw(MAL, "geom.exteriorRing", "wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_POLYGON) {
		*exteriorRingWKB = wkb_nil;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.exteriorRing", "Geometry not a Polygon");

	} 
	/* get the exterior ring of the geometry */	
	exteriorRingGeometry = GEOSGetExteriorRing(geosGeometry);
	/* get the wkb representation of it */
	*exteriorRingWKB = geos2wkb(exteriorRingGeometry);
	
	return MAL_SUCCEED;
}

/* Returns the n-th interior ring of a polygon */
str wkbInteriorRingN(wkb **interiorRingWKB, wkb **geom, short* ringNum) {
	GEOSGeom geosGeometry = NULL;
	const GEOSGeometry* interiorRingGeometry;
	int rN = -1;

	geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) { 
		*interiorRingWKB = wkb_nil;
		throw(MAL, "geom.interiorRing", "wkb2geos failed");
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_POLYGON) {
		*interiorRingWKB = wkb_nil;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.interiorRing", "Geometry not a Polygon");

	}

	//check number of internal rings
	rN = GEOSGetNumInteriorRings(geosGeometry);
	if (rN == -1 ) {
		*interiorRingWKB = wkb_nil;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.interiorRingN", "GEOSGetInteriorRingN failed.");
	}
	if(rN <= *ringNum || *ringNum<0) {
		*interiorRingWKB = wkb_nil;
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.interiorRingN", "GEOSGetInteriorRingN failed. Not enough interior rings");
	}

	/* get the exterior ring of the geometry */	
	interiorRingGeometry = GEOSGetInteriorRingN(geosGeometry, *ringNum);
	/* get the wkb representation of it */
	*interiorRingWKB = geos2wkb(interiorRingGeometry);
	
	return MAL_SUCCEED;

	return MAL_SUCCEED;
}

/* Returns the number of interior rings in the first polygon of the provided geometry 
 * plus the exterior ring depending on the value of exteriorRing*/
str wkbNumRings(int* out, wkb** geom, int* exteriorRing) {
	str ret = MAL_SUCCEED;

	//check the type of the geometry
	GEOSGeom geosGeometry = wkb2geos(*geom);
	if(GEOSGeomTypeId(geosGeometry)+1 == wkbMultiPolygon) {
		//use the first polygon as done by PostGIS
		ret = wkbBasicInt(out, geos2wkb(GEOSGetGeometryN(geosGeometry, 0)), GEOSGetNumInteriorRings, "geom.NumRngs");
	} else {
		ret = wkbBasicInt(out, *geom, GEOSGetNumInteriorRings, "geom.NumRings");
	}

	if(ret != MAL_SUCCEED)
		return ret; 
	
	(*out) += (*exteriorRing);

	return MAL_SUCCEED;
}

/* it handles functions that take as input a single geometry and return boolean */
static int wkbBasicBoolean(wkb **geom, char (*func)(const GEOSGeometry*)) {
	int res = -1;
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry)
		return 3;

	res = (*func)(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	return res;
}

/* the function checks whether the geometry is closed. GEOS works only with
 * linestring geometries but PostGIS returns true in any geometry that is not
 * a linestring. I made it to be like PostGIS */
str wkbIsClosed(bit *out, wkb **geom) {
	int res = -1;
	int geometryType = 0;
	GEOSGeom geosGeometry = wkb2geos(*geom);

	*out = bit_nil;

	if (!geosGeometry)
		throw(MAL, "geom.IsClosed", "wkb2geos failed");

	geometryType = GEOSGeomTypeId(geosGeometry)+1;

	/* if the geometry is point or multipoint it is always closed */
	if(geometryType == wkbPoint || geometryType == wkbMultiPoint) {
		*out = 1;
		GEOSGeom_destroy(geosGeometry);
		return MAL_SUCCEED;	
	}

	/* if the geometry is not a point, multipoint or linestring, it is always not closed */
	if(geometryType != wkbLineString) {
		*out = 0;
		GEOSGeom_destroy(geosGeometry);
		return MAL_SUCCEED;	
	}

	
	res = GEOSisClosed(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if(res == 2)
		throw(MAL, "geom.IsClosed", "GEOSisClosed failed");
	*out = res;
	return MAL_SUCCEED;
}

str wkbIsEmpty(bit *out, wkb **geomWKB) {
	int res = wkbBasicBoolean(geomWKB, GEOSisEmpty);
	
	if(res == 3) {
		*out = bit_nil;
		throw(MAL, "geom.IsEmpty", "wkb2geos failed");
	}
	if(res == 2) {
		*out = bit_nil;
		throw(MAL, "geom.IsEmpty", "GEOSisEmpty failed");
	}

	*out = res;
	return MAL_SUCCEED;
}

str wkbIsRing(bit *out, wkb **geomWKB) {
	int res = wkbBasicBoolean(geomWKB, GEOSisRing);
	
	if(res == 3) {
		*out = bit_nil;
		throw(MAL, "geom.IsRing", "wkb2geos failed");
	}
	if(res == 2) {
		*out = bit_nil;
		throw(MAL, "geom.IsRing", "GEOSisRing failed");
	}
	*out = res;
	return MAL_SUCCEED;
}


str wkbIsSimple(bit *out, wkb **geomWKB) {
	int res = wkbBasicBoolean(geomWKB, GEOSisSimple);
	
	if(res == 3) {
		*out = bit_nil;
		throw(MAL, "geom.IsSimple", "wkb2geos failed");
	}
	if(res == 2) {
		*out = bit_nil;
		throw(MAL, "geom.IsSimple", "GEOSisSimple failed");
	}
	*out = res;

	return MAL_SUCCEED;
}

/*geom prints a message sayig the reasom why the geometry is not valid but
 * since there is also isValidReason I skip this here */
str wkbIsValid(bit *out, wkb **geomWKB) {
	int res = wkbBasicBoolean(geomWKB, GEOSisValid);
	
	if(res == 3) {
		*out = bit_nil;
		throw(MAL, "geom.IsValid", "wkb2geos failed");
	}
	if(res == 2) {
		*out = bit_nil;
		throw(MAL, "geom.IsValid", "GEOSisValid failed");
	}
	*out = res;

	if (GDKerrbuf)
		GDKerrbuf[0] = '\0';

	return MAL_SUCCEED;
}

str wkbIsValidReason(char** reason, wkb **geomWKB) {
	GEOSGeom geosGeometry = wkb2geos(*geomWKB);
	char* GEOSReason = NULL;

	if (!geosGeometry) {
		*reason = NULL;
		throw(MAL, "geom.IsValidReason", "wkb2geos failed");
	}

	GEOSReason = GEOSisValidReason(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if(GEOSReason == NULL)
		throw(MAL, "geom.IsValidReason", "GEOSisValidReason failed");
	
	*reason = GDKmalloc((int)sizeof(GEOSReason)+1);
	strcpy(*reason, GEOSReason);
	GEOSFree(GEOSReason);

	return MAL_SUCCEED;
}

/* I should check it since it does not work */
str wkbIsValidDetail(char** out, wkb **geom) {
	int res = -1;
	void* GEOSreason = NULL;
	GEOSGeom GEOSlocation = NULL;
	
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = NULL;
		throw(MAL, "geom.IsValidDetail", "wkb2geos failed");
	}

	res = GEOSisValidDetail(geosGeometry, 1, (char**)&GEOSreason, &GEOSlocation);

	GEOSGeom_destroy(geosGeometry);

	if(res == 2) {
		if(GEOSreason)
			GEOSFree(GEOSreason);
		if(GEOSlocation)
			GEOSGeom_destroy(GEOSlocation);
		throw(MAL, "geom.IsValidDetail", "GEOSisValidDetail failed");
	}

	*out = GDKmalloc(sizeof(GEOSreason));
	memcpy(*out, GEOSreason, sizeof(out));

	GEOSFree(GEOSreason);
	GEOSGeom_destroy(GEOSlocation);
	
	return MAL_SUCCEED;
}

/* returns the area of the geometry */
str wkbArea(dbl *out, wkb** geomWKB) {
	GEOSGeom geosGeometry = wkb2geos(*geomWKB);

	if (!geosGeometry) {
		*out = dbl_nil;
		throw(MAL, "geom.Area", "wkb2geos failed");
	}

	if (!GEOSArea(geosGeometry, out)) {
		GEOSGeom_destroy(geosGeometry);
		*out = dbl_nil;
		throw(MAL, "geom.Area", "GEOSArea failed");
	}

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* returns the centroid of the geometry */
str wkbCentroid(wkb **out, wkb **geom) {
	GEOSGeom geosGeometry = wkb2geos(*geom);
	GEOSGeom outGeometry;

	if (!geosGeometry) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	outGeometry = GEOSGetCentroid(geosGeometry); 
	GEOSSetSRID(outGeometry, GEOSGetSRID(geosGeometry)); //the centroid has the same SRID with the the input geometry
	*out = geos2wkb(outGeometry);

	GEOSGeom_destroy(geosGeometry);
	GEOSGeom_destroy(outGeometry);

	return MAL_SUCCEED;

}

/*  Returns the 2-dimensional cartesian minimum distance (based on spatial ref) between two geometries in projected units */
str wkbDistance(dbl *out, wkb **a, wkb **b) {
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);
	 

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = dbl_nil;
		return MAL_SUCCEED;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = dbl_nil;
		return MAL_SUCCEED;
	}
	if (!ga && !gb) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	if(GEOSGetSRID(ga) != GEOSGetSRID(gb)) {
		GEOSGeom_destroy(ga);
		GEOSGeom_destroy(gb);
		throw(MAL, "geom.Distance", "Geometries of different SRID");
	}

	if (!GEOSDistance(ga, gb, out)) {
		GEOSGeom_destroy(ga);
		GEOSGeom_destroy(gb);
		throw(MAL, "geom.Distance", "GEOSDistance failed");
	}

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return MAL_SUCCEED;
}

/* Returns the 2d length of the geometry if it is a linestring or multilinestring */
str wkbLength(dbl *out, wkb **a) {
	GEOSGeom geosGeometry = wkb2geos(*a);

	if (!geosGeometry) {
		*out = dbl_nil;
		return MAL_SUCCEED;
	}

	if (!GEOSLength(geosGeometry, out)) {
		GEOSGeom_destroy(geosGeometry);
		throw(MAL, "geom.Length", "GEOSLength failed");
	}

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;
}

/* Returns a geometry that represents the convex hull of this geometry. 
 * The convex hull of a geometry represents the minimum convex geometry 
 * that encloses all geometries within the set. */
str wkbConvexHull(wkb **out, wkb **geom) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSConvexHull(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	return MAL_SUCCEED;

}

/* Gets two geometries and returns a new geometry */
static str wkbanalysis(wkb **out, wkb **a, wkb **b, GEOSGeometry *(*func)(const GEOSGeometry *, const GEOSGeometry *), const char *name) {
	
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = wkb_nil;
		return MAL_SUCCEED;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = wkb_nil;
		return MAL_SUCCEED;
	}
	if (!ga && !gb) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	if(GEOSGetSRID(ga) != GEOSGetSRID(gb)) {
		GEOSGeom_destroy(ga);
		GEOSGeom_destroy(gb);
		throw(MAL, name, "Geometries of different SRID");
	}

	/* in case *func fails returns NULL. geos2wkb returns wkbNULL in case of NULL input */
	*out = geos2wkb((*func)(ga, gb));

//	throw(MAL, "geom.@1", "@2 failed");
	

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return MAL_SUCCEED;

}

str wkbIntersection(wkb **out, wkb **a, wkb **b) {
	str msg = wkbanalysis(out, a, b, GEOSIntersection, "geom.Intesection");

	if(wkb_isnil(*out))
		throw(MAL, "geom.Intersection", "GEOSIntersection failed");
	return msg;
}

str wkbUnion(wkb **out, wkb **a, wkb **b) {
	wkbanalysis(out, a, b, GEOSUnion, "geom.Union");
	
	if(wkb_isnil(*out))
		throw(MAL, "geom.Union", "GEOSUnion failed");
	return MAL_SUCCEED;

}

str wkbDifference(wkb **out, wkb **a, wkb **b) {
	wkbanalysis(out, a, b, GEOSDifference, "geom.Difference");

	if(wkb_isnil(*out))
		throw(MAL, "geom.Difference", "GEOSDifference failed");
	return MAL_SUCCEED;

}

str wkbSymDifference(wkb **out, wkb **a, wkb **b) {
	return wkbanalysis(out, a, b, GEOSSymDifference, "geom.SymDifference");

	if(wkb_isnil(*out))
		throw(MAL, "geom.SymDifference", "GEOSSYmDifference failed");
	return MAL_SUCCEED;

}

/* Returns a geometry that represents all points whose distance from this Geometry is less than or equal to distance. */
str wkbBuffer(wkb **out, wkb **geom, dbl *distance) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSBuffer(geosGeometry, *distance, -1));

	GEOSGeom_destroy(geosGeometry);

	if (wkb_isnil(*out))
		throw(MAL, "geom.Buffer", "GEOSBuffer failed");

	return MAL_SUCCEED;
}

/* Gets two geometries and returns a boolean by comparing them */
static int wkbspatial(wkb **geomWKB_a, wkb **geomWKB_b, char (*func)(const GEOSGeometry *, const GEOSGeometry *)) {
	int res = -1;
	GEOSGeom geosGeometry_a = wkb2geos(*geomWKB_a);
	GEOSGeom geosGeometry_b = wkb2geos(*geomWKB_b);

	if (!geosGeometry_a && geosGeometry_b) {
		GEOSGeom_destroy(geosGeometry_b);
		return 3;
	}
	if (geosGeometry_a && !geosGeometry_b) {
		GEOSGeom_destroy(geosGeometry_a);
		return 3;
	}
	if (!geosGeometry_a && !geosGeometry_b)
		return 3;

	if(GEOSGetSRID(geosGeometry_a) != GEOSGetSRID(geosGeometry_b)) {
		GEOSGeom_destroy(geosGeometry_a);
		GEOSGeom_destroy(geosGeometry_b);
		return 4;
	}

	res = (*func)(geosGeometry_a, geosGeometry_b);

	GEOSGeom_destroy(geosGeometry_a);
	GEOSGeom_destroy(geosGeometry_b);

	return res;
}

str wkbContains(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSContains);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Contains", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Contains", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Contains", "GEOSContains failed");
	*out = res;

	return MAL_SUCCEED;
}
/*
geom_export str wkbContains_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str wkbContains_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	int aType, bType;
	
	(void) cntxt;
	(void) mb;
	
fprintf(stderr, "In wkbContains_bat\n");

	//check the types of the two arguments
	aType = stk->stk[getArg(pci, 1)].vtype;
	bType = stk->stk[getArg(pci, 2)].vtype;

	if ((aType == TYPE_bat || isaBatType(aType)) && (bType == TYPE_bat || isaBatType(bType))) {
		bat *aBAT_id = NULL, *bBAT_id=NULL;
		BAT *aBAT = NULL, *bBAT = NULL;
		fprintf(stderr, "BAT - BAT\n");

		//get the indices of the BATs in the BBP
		aBAT_id = (bat*) getArgReference(stk, pci, 1);
		bBAT_id = (bat*) getArgReference(stk, pci, 2);

		//get the descriptors of the BATS	
		if((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
			throw(MAL, "batgeom.Contains", RUNTIME_OBJECT_MISSING);
		}
		if((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
			BBPreleaseref(bBAT->batCacheid);
			throw(MAL, "batgeom.Contains", RUNTIME_OBJECT_MISSING);
		}

		fprintf(stderr, "aBAT size = %d - bBAT size = %d\n", (unsigned int)BATcount(aBAT), (unsigned int)BATcount(bBAT));

		return wkbContains_bat_bat()
	} else if(aType == TYPE_bat || isaBatType(aType)) {
		fprintf(stderr, "BAT - scalar\n");
	}
	else if(bType == TYPE_bat || isaBatType(bType)) {
		fprintf(stderr, "scalar - BAT\n");
	} else {
		fprintf(stderr, "unknown\n");
	}

	return MAL_SUCCEED;
}*/

/* geometry A is scalar, geometry B is BAT */
/*str wkbContains_firstScalar_bat(int* outBAT_id, wkb** aWKB, int* bBAT_id) {
	BAT *outBAT = NULL, *bBAT = NULL;
	wkb *bWKB = NULL;
	bit outBIT;
	BATiter bBAT_iter;
	BUN p=0, q=0;
	str err = NULL;
	mbr *aMBR = NULL;
fprintf(stderr, "In wkbContains_firstScalar_bat\n");
	//get the descriptor of the BAT
	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		throw(MAL, "batgeom.Contains", RUNTIME_OBJECT_MISSING);
	}
	
	if (bBAT->htype != TYPE_void) { //header type of bBAT not void
		BBPreleaseref(bBAT->batCacheid);
		throw(MAL, "batgeom.Contains", "the arguments must have dense and aligned heads");
	}

	//create a new BAT for the output
	if ((outBAT = BATnew(TYPE_void, ATOMindex("bit"), BATcount(bBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(bBAT->batCacheid);
		throw(MAL, "batgeom.Contains", MAL_MALLOC_FAIL);
	}
	//set the first idx of the output BAT equal to that of the aBAT
	BATseqbase(outBAT, bBAT->hseqbase);

	//get the bounding box of geometry A
	if((err = wkbMBR(&aMBR, aWKB)) != MAL_SUCCEED) {
		str msg;
		BBPreleaseref(bBAT->batCacheid);
		BBPreleaseref(outBAT->batCacheid);
		msg = createException(MAL, "batgeom.Contains", "%s", err);
		GDKfree(err);
		return msg;
	}

	//iterator over the BAT of geometry B	
	bBAT_iter = bat_iterator(bBAT);
	BATloop(bBAT, p, q) { //iterate over all valid elements of geometry B
		mbr *bMBR = NULL;
		bWKB = (wkb*) BUNtail(bBAT_iter, p);

		//get the bounding box of geometry B
		if((err = wkbMBR(&bMBR, &bWKB)) != MAL_SUCCEED) {
			str msg;
			BBPreleaseref(bBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.Contains", "%s", err);
			GDKfree(err);
			GDKfree(aMBR);
			return msg;
		}

		//check first if the bounding box of geometry a contains the bounding box of geometry b
		if((err = mbrContains(&outBIT, &aMBR, &bMBR)) != MAL_SUCCEED) {
			str msg;
			BBPreleaseref(bBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.Contains", "%s", err);
			GDKfree(err);
			GDKfree(aMBR);
			GDKfree(bMBR);
			return msg;
		}

		if(outBIT) {
			if ((err = wkbContains(&outBIT, aWKB, &bWKB)) != MAL_SUCCEED) { //check
				str msg;
				BBPreleaseref(bBAT->batCacheid);
				BBPreleaseref(outBAT->batCacheid);
				msg = createException(MAL, "batgeom.Contains", "%s", err);
				GDKfree(err);
				GDKfree(aMBR);
				GDKfree(bMBR);
				return msg;
			}
		}
		BUNappend(outBAT,&outBIT,TRUE); //add the result to the outBAT
		GDKfree(bMBR);
	}
	GDKfree(aMBR);

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(bBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(bBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;

}*/

str wkbContains_bat_bat(int* outBAT_id, int* aBAT_id, int* bBAT_id) {
	BAT *outBAT = NULL, *aBAT = NULL, *bBAT = NULL;
	wkb *aWKB = NULL, *bWKB = NULL; //, *aWKB_previous = NULL, *bWKB_previous = NULL;
	bit outBIT;
	BATiter aBAT_iter, bBAT_iter;
	BUN i=0;

	//get the descriptor of the BAT
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
		throw(MAL, "batgeom.Contains", RUNTIME_OBJECT_MISSING);
	}
	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPreleaseref(aBAT->batCacheid);
		throw(MAL, "batgeom.Contains", RUNTIME_OBJECT_MISSING);
	}
	
	if ( aBAT->htype != TYPE_void || //header type of aBAT not void
		 bBAT->htype != TYPE_void || //header type of bBAT not void
	    aBAT->hseqbase != bBAT->hseqbase || //the idxs of the headers of the BATs are not the same
	    BATcount(aBAT) != BATcount(bBAT)) { //the number of valid elements in the BATs are not the same
		BBPreleaseref(aBAT->batCacheid);
		BBPreleaseref(bBAT->batCacheid);
		throw(MAL, "batgeom.Contains", "the arguments must have dense and aligned heads");
	}

	//create a new BAT for the output
	if ((outBAT = BATnew(TYPE_void, ATOMindex("bit"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(aBAT->batCacheid);
		BBPreleaseref(bBAT->batCacheid);
		throw(MAL, "batgeom.Contains", MAL_MALLOC_FAIL);
	}
	//set the first idx of the output BAT equal to that of the aBAT
	BATseqbase(outBAT, aBAT->hseqbase);

	//iterator over the BATs	
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) { 
		str err = NULL;
		aWKB = (wkb*) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		bWKB = (wkb*) BUNtail(bBAT_iter, i + BUNfirst(bBAT));

		if ((err = wkbContains(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) { //check
			str msg;
			BBPreleaseref(aBAT->batCacheid);
			BBPreleaseref(bBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.Contains", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,&outBIT,TRUE); //add the result to the outBAT
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(aBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(aBAT->batCacheid);
	BBPreleaseref(bBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	
	return MAL_SUCCEED;

}

/**
 * It filters the geometry in the second BAT with respect to the MBR of the geometry in the first BAT.
 **/
str wkbContainsFilter_bat(int* aBATfiltered_id, int* bBATfiltered_id, int* aBAT_id, int* bBAT_id) {
	BAT *aBATfiltered = NULL, *bBATfiltered = NULL, *aBAT = NULL, *bBAT = NULL;
	wkb *aWKB = NULL, *bWKB = NULL;
	bit outBIT;
	BATiter aBAT_iter, bBAT_iter;
	BUN i=0;
	int remainingElements =0;

	//get the descriptor of the BAT
	if ((aBAT = BATdescriptor(*aBAT_id)) == NULL) {
		throw(MAL, "batgeom.MBRfilter", RUNTIME_OBJECT_MISSING);
	}
	if ((bBAT = BATdescriptor(*bBAT_id)) == NULL) {
		BBPreleaseref(aBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", RUNTIME_OBJECT_MISSING);
	}
	
	if ( aBAT->htype != TYPE_void || //header type of aBAT not void
		 bBAT->htype != TYPE_void || //header type of bBAT not void
	    aBAT->hseqbase != bBAT->hseqbase || //the idxs of the headers of the BATs are not the same
	    BATcount(aBAT) != BATcount(bBAT)) { //the number of valid elements in the BATs are not the same
		BBPreleaseref(aBAT->batCacheid);
		BBPreleaseref(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", "The arguments must have dense and aligned heads");
	}

	//create two new BATs for the output
	if ((aBATfiltered = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(aBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(aBAT->batCacheid);
		BBPreleaseref(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", MAL_MALLOC_FAIL);
	}
	if ((bBATfiltered = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(bBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(aBAT->batCacheid);
		BBPreleaseref(bBAT->batCacheid);
		throw(MAL, "batgeom.MBRfilter", MAL_MALLOC_FAIL);
	}

	//set the first idx of the output BATs equal to that of the aBAT
	BATseqbase(aBATfiltered, aBAT->hseqbase);
	BATseqbase(bBATfiltered, bBAT->hseqbase);

	//iterator over the BATs	
	aBAT_iter = bat_iterator(aBAT);
	bBAT_iter = bat_iterator(bBAT);

	for (i = BUNfirst(aBAT); i < BATcount(aBAT); i++) { 
		str err = NULL;
		aWKB = (wkb*) BUNtail(aBAT_iter, i + BUNfirst(aBAT));
		bWKB = (wkb*) BUNtail(bBAT_iter, i + BUNfirst(bBAT));
		
		//check the containment of the MBRs
		if((err = mbrContains_wkb(&outBIT, &aWKB, &bWKB)) != MAL_SUCCEED) {
			str msg;
			BBPreleaseref(aBAT->batCacheid);
			BBPreleaseref(bBAT->batCacheid);
			BBPreleaseref(aBATfiltered->batCacheid);
			BBPreleaseref(bBATfiltered->batCacheid);
			msg = createException(MAL, "batgeom.wkbFilter", "%s", err);
			GDKfree(err);
			return msg;
		}
		if(outBIT) {
			BUNappend(aBATfiltered,aWKB, TRUE); //add the result to the aBAT
			BUNappend(bBATfiltered,bWKB, TRUE); //add the result to the bBAT
			remainingElements++;
		}
	}

	//set some properties of the new BATs
	BATsetcount(aBATfiltered, remainingElements);
    	BATsettrivprop(aBATfiltered);
    	BATderiveProps(aBATfiltered,FALSE);
	
	BATsetcount(bBATfiltered, remainingElements);
    	BATsettrivprop(bBATfiltered);
    	BATderiveProps(bBATfiltered,FALSE);
	
	BBPreleaseref(aBAT->batCacheid);
	BBPreleaseref(bBAT->batCacheid);
	BBPkeepref(*aBATfiltered_id = aBATfiltered->batCacheid);
	BBPkeepref(*bBATfiltered_id = bBATfiltered->batCacheid);
	
	return MAL_SUCCEED;


}

str wkbCrosses(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSCrosses);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Crosses", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Crosses", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Crosses", "GEOSCrosses failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbDisjoint(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSDisjoint);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Disjoint", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Disjoint", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Disjoint", "GEOSDisjoint failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbEquals(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSEquals);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Equals", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Equals", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Equals", "GEOSEquals failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbIntersects(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSIntersects);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Intersects", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Intersects", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Intersects", "GEOSIntersects failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbOverlaps(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSOverlaps);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Overlaps", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Overlaps", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Overlaps", "GEOSOverlaps failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbRelate(bit *out, wkb **geomWKB_a, wkb **geomWKB_b, str *pattern) {
	int res = -1;
	GEOSGeom geosGeometry_a = wkb2geos(*geomWKB_a);
	GEOSGeom geosGeometry_b = wkb2geos(*geomWKB_b);

	*out = bit_nil;

	if (!geosGeometry_a && geosGeometry_b) {
		GEOSGeom_destroy(geosGeometry_b);
		return MAL_SUCCEED;
	}
	if (geosGeometry_a && !geosGeometry_b) {
		GEOSGeom_destroy(geosGeometry_a);
		return MAL_SUCCEED;
	}
	if (!geosGeometry_a && !geosGeometry_b) 
		return MAL_SUCCEED;
	
	if(GEOSGetSRID(geosGeometry_a) != GEOSGetSRID(geosGeometry_b)) {
		GEOSGeom_destroy(geosGeometry_a);
		GEOSGeom_destroy(geosGeometry_b);
		return MAL_SUCCEED;
	}

	res = GEOSRelatePattern(geosGeometry_a, geosGeometry_b, *pattern);

	GEOSGeom_destroy(geosGeometry_a);
	GEOSGeom_destroy(geosGeometry_b);
	
	if(res == 2)
		throw(MAL, "geom.Relate", "GEOSRelatePattern failed");

	*out = res;
	
	return MAL_SUCCEED;
}

str wkbTouches(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSTouches);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Touches", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Touches", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Touches", "GEOSTouches failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbWithin(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSWithin);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Within", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Within", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Within", "GEOSWithin failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbCovers(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSCovers);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Within", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Within", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Within", "GEOSCovers failed");
	*out = res;

	return MAL_SUCCEED;
}

str wkbCoveredBy(bit *out, wkb **geomWKB_a, wkb **geomWKB_b) {
	int res =  wkbspatial(geomWKB_a, geomWKB_b, GEOSCoveredBy);
	*out = bit_nil;

	if(res == 4)
		throw(MAL, "geom.Within", "Geometries of different SRID");
	if(res == 3)
		throw(MAL, "geom.Within", "wkb2geos failed");
	if(res == 2)
		throw(MAL, "geom.Within", "GEOSCoveredBy failed");
	*out = res;

	return MAL_SUCCEED;
}

/*returns the n-th geometry in a multi-geometry */
str wkbGeometryN(wkb** out, wkb** geom, int* geometryNum) {
	int geometriesNum = -1;
	GEOSGeom geosGeometry = NULL;

	//no geometry at this position
	if(*geometryNum <= 0) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	geosGeometry = wkb2geos(*geom);

	if(!geosGeometry) {
		*out = wkb_nil;
		throw(MAL, "geom.GeometryN", "wkb2geos failed");
	}

	geometriesNum = GEOSGetNumGeometries(geosGeometry);
	if(geometriesNum < 0) {
		*out = wkb_nil;
		throw(MAL, "geom.GeometryN","GEOSGetNumGeometries failed");
	}
	
	//geometry is not a multi geometry
	if(geometriesNum == 1) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	//no geometry at this position
	if(geometriesNum < *geometryNum) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSGetGeometryN(geosGeometry, (*geometryNum-1)));

	return MAL_SUCCEED;
}

/* returns the number of geometries */
str wkbNumGeometries(int* out, wkb** geom) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if(!geosGeometry) {
		*out = int_nil;
		throw(MAL, "geom.NumGeometries", "wkb2geos failed");
	}

	*out = GEOSGetNumGeometries(geosGeometry);
	if(*out < 0) {
		*out = int_nil;
		throw(MAL, "geom.GeometryN","GEOSGetNumGeometries failed");
	}

	return MAL_SUCCEED;
}



/* MBR */

/* Creates the mbr for the given geom_geometry. */
str wkbMBR(mbr **geomMBR, wkb **geomWKB) {
	GEOSGeom geosGeometry = wkb2geos(*geomWKB);
	*geomMBR = mbrFromGeos(geosGeometry);

	if(geosGeometry)
		GEOSGeom_destroy(geosGeometry);
	else if(mbr_isnil(*geomMBR))
		throw(MAL, "wkb.mbr", "Failed to create mbr");

	return MAL_SUCCEED;	
}

/* Creates the BAT with mbrs from the BAT with geometries. */
str wkbMBR_bat(int* outBAT_id, int* inBAT_id) {
	BAT *outBAT = NULL, *inBAT = NULL;
	wkb *inWKB = NULL;
	mbr *outMBR = NULL;
	BUN p =0, q =0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.mbr", RUNTIME_OBJECT_MISSING);
	}
	
	if ( inBAT->htype != TYPE_void ) { //header type of  BAT not void
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.mbr", "the arguments must have dense and aligned heads");
	}

	//create a new BAT for the output
	if ((outBAT = BATnew(TYPE_void, ATOMindex("mbr"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.mbr", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the input BAT
	BATseqbase(outBAT, inBAT->hseqbase);

	//iterator over the BAT	
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) { //iterate over all valid elements
		str err = NULL;

		inWKB = (wkb*) BUNtail(inBAT_iter, p);
		if ((err = wkbMBR(&outMBR, &inWKB)) != MAL_SUCCEED) {
			str msg;
			BBPreleaseref(inBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.mbr", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,outMBR,TRUE); //add the point to the new BAT
		GDKfree(outMBR);
		outMBR = NULL;
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;
}

str wkbBox2D(mbr** box, wkb** point1, wkb** point2) {
	GEOSGeom point1_geom, point2_geom;	
	double xmin=0.0, ymin=0.0, xmax=0.0, ymax=0.0;

	//check null input
	if(wkb_isnil(*point1) || wkb_isnil(*point2)) {
		*box=mbr_nil;
		return MAL_SUCCEED;		
	}
	
	//check input not point geometries
	point1_geom = wkb2geos(*point1);
	if((GEOSGeomTypeId(point1_geom)+1) != wkbPoint) {
		GEOSGeom_destroy(point1_geom);
		*box = mbr_nil;
		throw(MAL, "geom.MakeBox2D", "Geometries should be points");
	}

	point2_geom = wkb2geos(*point2);	
	if((GEOSGeomTypeId(point2_geom)+1) != wkbPoint) {
		GEOSGeom_destroy(point1_geom);
		GEOSGeom_destroy(point2_geom);
		*box = mbr_nil;
		throw(MAL, "geom.MakeBox2D", "Geometries should be points");	
	}

	if(GEOSGeomGetX(point1_geom, &xmin) == -1 ||
		GEOSGeomGetY(point1_geom, &ymin) == -1 ||
		GEOSGeomGetX(point2_geom, &xmax) == -1 ||
		GEOSGeomGetY(point2_geom, &ymax) == -1) {
		
		GEOSGeom_destroy(point1_geom);
		GEOSGeom_destroy(point2_geom);
		*box = mbr_nil;
		throw(MAL, "geom.MakeBox2D", "Error in reading the points' coordinates");	

	}

	*box = (mbr*) GDKmalloc(sizeof(mbr));
	(*box)->xmin = (float) xmin;
	(*box)->ymin = (float) ymin;
	(*box)->xmax = (float) xmax;
	(*box)->ymax = (float) ymax;

	return MAL_SUCCEED;
} 

/*returns true if the two 
 * 	mbrs overlap */
str mbrOverlaps(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else //they cannot overlap if b2 is left, right, above or below b1
		*out = !((*b2)->ymax < (*b1)->ymin || (*b2)->ymin > (*b1)->ymax || (*b2)->xmax < (*b1)->xmin || (*b2)->xmin > (*b1)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of the two geometris overlap */
str mbrOverlaps_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrOverlaps(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if b1 is above b2 */
str mbrAbove(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ((*b1)->ymin >(*b2)->ymax); 
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is above the mbr of geom2 */
str mbrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrAbove(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if b1 is below b2 */
str mbrBelow(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ( (*b1)->ymax < (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is below the mbr of geom2 */
str mbrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrBelow(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if box1 is left of box2 */
str mbrLeft(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ( (*b1)->xmax < (*b2)->xmin );
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the left of the mbr of geom2 */
str mbrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrLeft(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if box1 is right of box2 */
str mbrRight(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else 
		*out = ( (*b1)->xmin > (*b2)->xmax );
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is on the right of the mbr of geom2 */
str mbrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrRight(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if box1 overlaps or is above box2 when only the Y coordinate is considered*/
str mbrOverlapOrAbove(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out =  ((*b1)->ymin >= (*b2)->ymin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is above the mbr of geom2 */
str mbrOverlapOrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrOverlapOrAbove(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if box1 overlaps or is below box2 when only the Y coordinate is considered*/
str mbrOverlapOrBelow(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ((*b1)->ymax <= (*b2)->ymax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is below the mbr of geom2 */
str mbrOverlapOrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrOverlapOrBelow(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if box1 overlaps or is left of box2 when only the X coordinate is considered*/
str mbrOverlapOrLeft(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ((*b1)->xmax <= (*b2)->xmax);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the left of the mbr of geom2 */
str mbrOverlapOrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrOverlapOrLeft(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if box1 overlaps or is right of box2 when only the X coordinate is considered*/
str mbrOverlapOrRight(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ((*b1)->xmin >= (*b2)->xmin);
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 overlaps or is on the right of the mbr of geom2 */
str mbrOverlapOrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrOverlapOrRight(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if b1 is contained in b2 */
str mbrContained(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ( ((*b1)->xmin >= (*b2)->xmin) && ((*b1)->xmax <= (*b2)->xmax) && ((*b1)->ymin >= (*b2)->ymin) && ((*b1)->ymax <= (*b2)->ymax));
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 is contained in the mbr of geom2 */
str mbrContained_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrContained(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/*returns true if b1 contains b2 */
str mbrContains(bit *out, mbr **b1, mbr **b2) {
	return mbrContained(out, b2, b1);
}

/*returns true if the mbrs of geom1 contains the mbr of geom2 */
str mbrContains_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrContains(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns true if the boxes are the same */
str mbrEqual(bit *out, mbr **b1, mbr **b2) {
	if (mbr_isnil(*b1) && mbr_isnil(*b2))
		*out = 1;
	else if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = ( ((*b1)->xmin == (*b2)->xmin) && ((*b1)->xmax == (*b2)->xmax) && ((*b1)->ymin == (*b2)->ymin) && ((*b1)->ymax == (*b2)->ymax));
	return MAL_SUCCEED;
}

/*returns true if the mbrs of geom1 and the mbr of geom2 are the same */
str mbrEqual_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrEqual(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* returns the Euclidean distance of the centroids of the boxes */
str mbrDistance(double *out, mbr **b1, mbr **b2) {
	double b1_Cx = 0.0, b1_Cy = 0.0, b2_Cx =0.0, b2_Cy=0.0;

	if (mbr_isnil(*b1) || mbr_isnil(*b2)) {
		*out = 0;
		return MAL_SUCCEED;
	}

	//compute the centroids of the two polygons
	b1_Cx = ((*b1)->xmin+(*b1)->xmax)/2.0;
	b1_Cy = ((*b1)->ymin+(*b1)->ymax)/2.0;
	b2_Cx = ((*b2)->xmin+(*b2)->xmax)/2.0;
	b2_Cy = ((*b2)->ymin+(*b2)->ymax)/2.0;
	
	//compute the euclidean distance
	*out = sqrt( pow(b1_Cx*b2_Cx, 2.0) + pow(b1_Cy*b2_Cy, 2.0));

	return MAL_SUCCEED;
}

/*returns the Euclidean distance of the centroids of the mbrs of the two geometries */
str mbrDistance_wkb(double *out, wkb **geom1WKB, wkb **geom2WKB) {
	mbr *geom1MBR = NULL, *geom2MBR = NULL;
	str ret = MAL_SUCCEED;

	ret = wkbMBR(&geom1MBR, geom1WKB);
	if(ret != MAL_SUCCEED) {
		return ret;
	}
	
	ret = wkbMBR(&geom2MBR, geom2WKB);
	if(ret != MAL_SUCCEED) {
		GDKfree(geom1MBR);
		return ret;
	}
	
	ret = mbrDistance(out, &geom1MBR, &geom2MBR);

	GDKfree(geom1MBR);
	GDKfree(geom2MBR);
	
	return ret;
}

/* get Xmin, Ymin, Xmax, Ymax coordinates of mbr */
str wkbCoordinateFromMBR(dbl* coordinateValue, mbr** geomMBR, int* coordinateIdx) {
	switch(*coordinateIdx) {
		case 1:
			*coordinateValue = (*geomMBR)->xmin;
			break;
		case 2:
			*coordinateValue = (*geomMBR)->ymin;
			break;
		case 3:
			*coordinateValue = (*geomMBR)->xmax;
			break;
		case 4:
			*coordinateValue = (*geomMBR)->ymax;
			break;
		default:
			throw(MAL, "geom.coordinateFromMBR", "Unrecognised coordinateIdx: %d\n", *coordinateIdx);
	}

	return MAL_SUCCEED;
}

str wkbCoordinateFromMBR_bat(int *outBAT_id, int *inBAT_id, int* coordinateIdx) {
	BAT *outBAT = NULL, *inBAT = NULL;
	mbr *inMBR = NULL;
	double outDbl = 0.0;
	BUN p =0, q =0;
	BATiter inBAT_iter;

	//get the descriptor of the BAT
	if ((inBAT = BATdescriptor(*inBAT_id)) == NULL) {
		throw(MAL, "batgeom.coordinateFromMBR", RUNTIME_OBJECT_MISSING);
	}
	
	if ( inBAT->htype != TYPE_void ) { //header type of  BAT not void
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.coordinateFromMBR", "the arguments must have dense and aligned heads");
	}

	//create a new BAT for the output
	if ((outBAT = BATnew(TYPE_void, ATOMindex("dbl"), BATcount(inBAT), TRANSIENT)) == NULL) {
		BBPreleaseref(inBAT->batCacheid);
		throw(MAL, "batgeom.coordinateFromMBR", MAL_MALLOC_FAIL);
	}
	//set the first idx of the new BAT equal to that of the input BAT
	BATseqbase(outBAT, inBAT->hseqbase);

	//iterator over the BAT	
	inBAT_iter = bat_iterator(inBAT);
	BATloop(inBAT, p, q) { //iterate over all valid elements
		str err = NULL;

		inMBR = (mbr*) BUNtail(inBAT_iter, p);
		if ((err = wkbCoordinateFromMBR(&outDbl, &inMBR, coordinateIdx)) != MAL_SUCCEED) {
			str msg;
			BBPreleaseref(inBAT->batCacheid);
			BBPreleaseref(outBAT->batCacheid);
			msg = createException(MAL, "batgeom.coordinateFromMBR", "%s", err);
			GDKfree(err);
			return msg;
		}
		BUNappend(outBAT,&outDbl,TRUE);
	}

	//set some properties of the new BAT
	BATsetcount(outBAT, BATcount(inBAT));
    	BATsettrivprop(outBAT);
    	BATderiveProps(outBAT,FALSE);
	BBPreleaseref(inBAT->batCacheid);
	BBPkeepref(*outBAT_id = outBAT->batCacheid);
	return MAL_SUCCEED;

}
 

str wkbCoordinateFromWKB(dbl* coordinateValue, wkb** geomWKB, int* coordinateIdx) {
	mbr* geomMBR;
	str ret = MAL_SUCCEED ; 

	wkbMBR(&geomMBR, geomWKB);
	ret = wkbCoordinateFromMBR(coordinateValue, &geomMBR, coordinateIdx);	

	if(geomMBR)
		GDKfree(geomMBR);

	return ret;
}

str wkbCoordinateFromWKB_bat(int *outBAT_id, int *inBAT_id, int* coordinateIdx) {
	str err = NULL;
	int inBAT_mbr_id = 0; //the id of the bat with the mbrs

	if((err = wkbMBR_bat(&inBAT_mbr_id, inBAT_id)) != MAL_SUCCEED) {
		str msg;
		msg = createException(MAL, "batgeom.coordinateFromMBR", "%s", err);
		GDKfree(err);
		return msg;

	}

	//call the bulk version of wkbCoordinateFromMBR
	return wkbCoordinateFromMBR_bat(outBAT_id, &inBAT_mbr_id, coordinateIdx);
}



geom_export BUN mbrHASH(mbr *atom);
geom_export int mbrCOMP(mbr *l, mbr *r);
geom_export str mbrFromString(mbr **w, str *src);
geom_export BUN wkbHASH(wkb *w);
geom_export int wkbCOMP(wkb *l, wkb *r);
geom_export str wkbIsnil(bit *r, wkb **v);
geom_export void wkbDEL(Heap *h, var_t *index);
geom_export int wkbLENGTH(wkb *p);
geom_export void wkbHEAP(Heap *heap, size_t capacity);
geom_export var_t wkbPUT(Heap *h, var_t *bun, wkb *val);
geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);

/* HASH: compute a hash value. */
/* returns a positive integer hash value */

BUN
mbrHASH(mbr *atom)
{
	return (BUN) (((int) atom->xmin * (int)atom->ymin) *((int) atom->xmax * (int)atom->ymax));
}

/* COMP: compare two mbrs. */
/* returns int <0 if l<r, 0 if l==r, >0 else */

int
mbrCOMP(mbr *l, mbr *r)
{
	/* simple lexicographical ordering on (x,y) */
	int res;
	if (l->xmin == r->xmin)
		res = (l->ymin < r->ymin) ? -1 : (l->ymin != r->ymin);
	else
		res = (l->xmin < r->xmin) ? -1 : 1;
	if (res == 0) {
		if (l->xmax == r->xmax)
			res = (l->ymax < r->ymax) ? -1 : (l->ymax != r->ymax);
		else
			res = (l->xmax < r->xmax) ? -1 : 1;
	}
	return res;
}



str
mbrFromString(mbr **w, str *src)
{
	int len = *w ? (int) sizeof(mbr) : 0;
	char *errbuf;
	str ex;

	if (mbrFROMSTR(*src, &len, w))
		return MAL_SUCCEED;
	errbuf = GDKerrbuf;
	if (errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
	} else {
		errbuf = "cannot parse string";
	}

	ex = createException(MAL, "mbr.FromString", "%s", errbuf);

	if (GDKerrbuf)
		GDKerrbuf[0] = '\0';

	return ex;
}




BUN
wkbHASH(wkb *w)
{
	int i;
	BUN h = 0;

	for (i = 0; i < (w->len - 1); i += 2) {
		int a = *(w->data + i), b = *(w->data + i + 1);
		h = (h << 3) ^ (h >> 11) ^ (h >> 17) ^ (b << 8) ^ a;
	}
	return h;
}

int
wkbCOMP(wkb *l, wkb *r)
{
	int len = l->len;

	if (len != r->len)
		return len - r->len;

	if (len == ~(int) 0)
		return (0);

	return memcmp(l->data, r->data, len);
}



str
wkbIsnil(bit *r, wkb **v)
{
	*r = wkb_isnil(*v);
	return MAL_SUCCEED;
}



void
wkbDEL(Heap *h, var_t *index)
{
	HEAP_free(h, *index);
}



int
wkbLENGTH(wkb *p)
{
	var_t len = wkb_size(p->len);
	assert(len <= GDK_int_max);
	return (int) len;
}

void
wkbHEAP(Heap *heap, size_t capacity)
{
	HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

var_t
wkbPUT(Heap *h, var_t *bun, wkb *val)
{
	char *base;

	*bun = HEAP_malloc(h, wkb_size(val->len));
	base = h->base;
	if (*bun)
		memcpy(&base[*bun << GDK_VARSHIFT], (char *) val, wkb_size(val->len));
	return *bun;
}

/* COMMAND mbr
 * Creates the mbr for the given geom_geometry.
 */

str
ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY)
{
	if ((*res = (mbr *) GDKmalloc(sizeof(mbr))) == NULL)
		throw(MAL, "geom.mbr", MAL_MALLOC_FAIL);
	if (*minX == flt_nil || *minY == flt_nil ||
	    *maxX == flt_nil || *maxY == flt_nil)
		**res = *mbrNULL();
	else {
		(*res)->xmin = *minX;
		(*res)->ymin = *minY;
		(*res)->xmax = *maxX;
		(*res)->ymax = *maxY;
	}
	return MAL_SUCCEED;
}
























