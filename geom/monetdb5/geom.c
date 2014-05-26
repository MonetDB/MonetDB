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
#include <mal.h>
#include <mal_atom.h>
#include <mal_exception.h>
#include "libgeom.h"

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


#define GEOMETRY_HAS_Z(info)(info & 0x02)
#define GEOMETRY_HAS_M(info)(info & 0x01)
#define COORDINATES_NUM 3

/* the first argument in the functions is the return variable */

int TYPE_mbr;

geom_export void geoHasZ(int* res, int* info);
geom_export void geoHasM(int* res, int* info);
geom_export void geoGetType(char** res, int* info);

geom_export bat *geom_prelude(void);
geom_export void geom_epilogue(void);

geom_export wkb *wkbNULL(void);
geom_export mbr *mbrNULL(void);

geom_export wkb *geos2wkb(GEOSGeom geosGeometry);

geom_export int mbrFROMSTR(char *src, int *len, mbr **atom);
geom_export int mbrTOSTR(char **dst, int *len, mbr *atom);
geom_export int wkbFROMSTR(char *src, int srid, int *len, wkb **atom);
geom_export int wkbTOSTR(char **dst, int *len, wkb *atom);
geom_export str wkbFromText(wkb **w, str *wkt, int srid, int *tpe);
geom_export str wkbAsText(str *r, wkb **w);

geom_export str geomMakePoint2D(wkb**, double*, double*);
geom_export str geomMakePoint3D(wkb**, double*, double*, double*);
geom_export str geomMakePoint4D(wkb**, double*, double*, double*, double*);
geom_export str geomMakePointM(wkb**, double*, double*, double*);

geom_export str wkbGeometryType(char**, wkb**);
geom_export str wkbBoundary(wkb**, wkb**);
geom_export str wkbCoordDim(int* , wkb**);
geom_export str wkbDimension(int*, wkb**);
geom_export str wkbSRID(int*, wkb**);
geom_export str wkbGetCoordX(double*, wkb**);
geom_export str wkbGetCoordY(double*, wkb**);
geom_export str wkbGetCoordZ(double*, wkb**);
geom_export str wkbStartPoint(wkb **out, wkb **geom);
geom_export str wkbEndPoint(wkb **out, wkb **geom);
geom_export str wkbNumPoints(int *out, wkb **geom);
geom_export str wkbPointN(wkb **out, wkb **geom, int *n);
geom_export str wkbEnvelope(wkb **out, wkb **geom);
geom_export str wkbExteriorRing(wkb**, wkb**);
geom_export str wkbInteriorRingN(wkb**, wkb**, short*);
geom_export str wkbNumInteriorRings(int*, wkb**);

geom_export str wkbIsEmpty(bit *out, wkb **geom);
geom_export str wkbIsSimple(bit *out, wkb **geom);


/*check if the geometry has z coordinate*/
void geoHasZ(int* res, int* info) {
	if(GEOMETRY_HAS_Z(*info)) *res=1;
	else *res=0;

}
/*check if the geometry has m coordinate*/
void geoHasM(int* res, int* info) {
	if(GEOMETRY_HAS_M(*info)) *res=1;
	else *res=0;
}
/*check the geometry subtype*/
/*returns the length of the resulting string*/
void geoGetType(char** res, int* info) {
	int type = (*info >> 2);
	const char* typeStr=geom_type2str(type) ;
	
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

/* FROMSTR: parse string to mbr. */
/* return number of parsed characters. */
int mbrFROMSTR(char *src, int *len, mbr **atom) {
	int nil = 0;
	int nchars = 0;	/* The number of characters parsed; the return value. */
	GEOSGeom geosMbr = NULL; /* The geometry object that is parsed from the src string. */

	if (strcmp(src, str_nil) == 0)
		nil = 1;

	if (!nil && (geosMbr = GEOSGeomFromWKT(src)) == NULL)
		return 0;

	if (*len < (int) sizeof(mbr)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = sizeof(mbr));
	}
	if (nil) {
		nchars = 3;
		**atom = *mbrNULL();
	} else if (getMbrGeos(*atom, geosMbr)) {
		size_t l = strlen(src);
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

/* FROMSTR: parse string to @1. */
/* return number of parsed characters. */
int wkbFROMSTR(char *src, int srid, int *len, wkb **atom) {
	GEOSGeom geometry_FROM_wkt = NULL;	/* The geometry object that is parsed from the src string. */
	unsigned char *geometry_TO_wkb = NULL;	/* The "well known binary" serialization of the geometry object. */
	size_t wkbLen = 0;		/* The length of the wkbSer string. */
	int nil = 0;
	//int coordinateDimensions = 0;

	if (strcmp(src, str_nil) == 0)
		nil = 1;

	if (!nil && (geometry_FROM_wkt = GEOSGeomFromWKT(src)) == NULL) {
		goto return_nil;
	}

	////it returns 2 or 3. How can I get 4??
	//coordinateDimensions =  GEOSGeom_getCoordinateDimension(geosGeometry);

	if (!nil && GEOSGeomTypeId(geometry_FROM_wkt) == -1) {
		GEOSGeom_destroy(geometry_FROM_wkt);
		goto return_nil;
	}

	if (!nil) {
		//add the srid
		if(srid == 0 )
			GEOSSetSRID(geometry_FROM_wkt, 4326);
		else //should we check whether the srid exists in spatial_ref_sys?
			GEOSSetSRID(geometry_FROM_wkt, srid);
		//the srid is lost with the transformation of the GEOSGeom to wkb
		
		//set the number of dimensions
		GEOS_setWKBOutputDims(COORDINATES_NUM);

		geometry_TO_wkb = GEOSGeomToWKB_buf(geometry_FROM_wkt, &wkbLen);
		GEOSGeom_destroy(geometry_FROM_wkt);
	}
	if (*atom == NULL || *len < (int) wkb_size(wkbLen)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = (int) wkb_size(wkbLen));
	}
	if (!geometry_TO_wkb) {
		**atom = *wkbNULL();
	} else {
		assert(wkbLen <= GDK_int_max);
		(*atom)->len = (int) wkbLen;
		memcpy(&(*atom)->data, geometry_TO_wkb, wkbLen);
		GEOSFree(geometry_TO_wkb);
	}
	wkbLen = strlen(src);
	assert(wkbLen <= GDK_int_max);
	return (int) wkbLen;
  return_nil:
	if ((size_t) *len < sizeof(wkb)) {
		if (*atom)
			GDKfree(*atom);
		*atom = GDKmalloc(*len = (int) sizeof(wkb));
	}
	**atom = *wkbNULL();
	return 0;
}

/* TOSTR: print atom in a string. */
/* return length of resulting string. */
int wkbTOSTR(char **dst, int *len, wkb *atom) {
	char *wkt = NULL;
	int dstStrLen = 3;			/* "nil" */
	GEOSGeom geosGeometry = wkb2geos(atom);

	
dstStrLen = -1;
dstStrLen = GEOSGeom_getCoordinateDimension(geosGeometry); 
	if (geosGeometry) {
		size_t l;
		wkt = GEOSGeomToWKT(geosGeometry);
		l = strlen(wkt);
		assert(l < GDK_int_max);
		dstStrLen = (int) l + 2;	/* add quotes */
		GEOSGeom_destroy(geosGeometry);
	}

	if (*len < dstStrLen + 1) {	/* + 1 for the '\0' */
		if (*dst)
			GDKfree(*dst);
		*dst = GDKmalloc(*len = dstStrLen + 1);
	}

	if (wkt) {
		snprintf(*dst, *len, "\"%s\"", wkt);
		GEOSFree(wkt);
	} else {
		strcpy(*dst, "nil");
	}

	return dstStrLen;
}

/* creates a wkb from the given textual representation */
/*int* tpe is needed to verify that the type of the FromText function used is the
 * same with the type of the geometry created from the wkt representation */
str wkbFromText(wkb **w, str *wkt, int srid, int *tpe) {
	int len = 0, te = *tpe;
	char *errbuf;
	str ex;

	*w = NULL;
	if (wkbFROMSTR(*wkt, srid, &len, w) &&
	    (wkb_isnil(*w) || *tpe == wkbGeometryCollection ||
	     (te = *((*w)->data + 1) & 0x0f) == *tpe))
		return MAL_SUCCEED;
	if (*w == NULL)
		*w = (wkb *) GDKmalloc(sizeof(wkb));
	**w = *wkbNULL();
	if (*tpe > 0 && te != *tpe)
		throw(MAL, "wkb.FromText", "Trying to read Geometry type '%s' with function for Geometry type '%s'", geom_type2str(te), geom_type2str(*tpe));
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
str wkbAsText(str *r, wkb **w) {
	int len = 0;

	wkbTOSTR(r, &len, *w);
	if (len)
		return MAL_SUCCEED;
	throw(MAL, "geom.AsText", "Failed to create Text from Well Known Format");
}

static str geomMakePoint(wkb **out, GEOSGeom geosGeometry) {
	unsigned char* wkbSer = NULL;
	size_t wkbLen = 0;

	//creates the wkbSer from the GeosGeometry structure
	wkbSer = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);
	GEOSGeom_destroy(geosGeometry);

	if(!wkbSer) {
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to create wkb from GEOSGeometry");
	}

	assert(wkbLen <= GDK_int_max);
	*out = GDKmalloc((int) wkb_size(wkbLen));

	if(*out == NULL) {
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to reserve memory for *wkb");
	}

	(*out)->len = (int) wkbLen;
	memcpy(&(*out)->data, wkbSer, wkbLen);
	GEOSFree(wkbSer);

	return MAL_SUCCEED;
}

/* creates a point using the x, y coordinates */
str geomMakePoint2D(wkb** out, double* x, double* y) {
	GEOSGeom geosGeometry = NULL;

	//create the point from the coordinates
	GEOSCoordSequence *seq = GEOSCoordSeq_create(1, 2);
	GEOSCoordSeq_setX(seq, 0, *x);
	GEOSCoordSeq_setY(seq, 0, *y);
	geosGeometry = GEOSGeom_createPoint(seq);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	return geomMakePoint(out, geosGeometry);
}

/* creates a point using the x, y, z coordinates */
str geomMakePoint3D(wkb** out, double* x, double* y, double* z) {
	GEOSGeom geosGeometry = NULL;

	//create the point from the coordinates
	GEOSCoordSequence *seq = GEOSCoordSeq_create(1, 3);
	GEOSCoordSeq_setX(seq, 0, *x);
	GEOSCoordSeq_setY(seq, 0, *y);
	GEOSCoordSeq_setZ(seq, 0, *z);
	geosGeometry = GEOSGeom_createPoint(seq);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	return geomMakePoint(out, geosGeometry);
}

/* creates a point using the x, y, z, m coordinates */
str geomMakePoint4D(wkb** out, double* x, double* y, double* z, double* m) {
	GEOSGeom geosGeometry = NULL;

	//create the point from the coordinates
	GEOSCoordSequence *seq = GEOSCoordSeq_create(1, 4);
	GEOSCoordSeq_setX(seq, 0, *x);
	GEOSCoordSeq_setY(seq, 0, *y);
	GEOSCoordSeq_setZ(seq, 0, *z);
	GEOSCoordSeq_setOrdinate(seq, 0, 3, *m);
	geosGeometry = GEOSGeom_createPoint(seq);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	return geomMakePoint(out, geosGeometry);
}

/* creates a point using the x, y, m coordinates */
str geomMakePointM(wkb** out, double* x, double* y, double* m) {
	GEOSGeom geosGeometry = NULL;

	//create the point from the coordinates
	GEOSCoordSequence *seq = GEOSCoordSeq_create(1, 3);
	GEOSCoordSeq_setOrdinate(seq, 0, 0, *x);
	GEOSCoordSeq_setOrdinate(seq, 0, 1, *y);
	GEOSCoordSeq_setOrdinate(seq, 0, 2, *m);
	geosGeometry = GEOSGeom_createPoint(seq);

	if(geosGeometry == NULL){
		*out = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to create GEOSGeometry from the coordiates");
	}

	return geomMakePoint(out, geosGeometry);
}

/* common code for functions that return integer */
static str wkbBasicInt(int *out, wkb **geom, int (*func)(const GEOSGeometry *), const char *name) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = int_nil;
		return MAL_SUCCEED;
	}

	*out = (*func)(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, name, "failed");
	return MAL_SUCCEED;

}


/* returns the type of the geometry as a string*/
str wkbGeometryType(char** out, wkb** geom) {
	int typeId = 0;
	str ret = MAL_SUCCEED;

	ret = wkbBasicInt(&typeId, geom, GEOSGeomTypeId, "geom.GeometryType");
	typeId = ((typeId+1) << 2);
	geoGetType(out, &typeId);
	
	return ret;
}

/* returns the number of dimensions of the geometry */
/* geos does not know the number of dimensions as long as a wkb has been created 
 * more precisely it descards all dimensions but x and y*/
str wkbCoordDim(int *out, wkb **geom) {
	return wkbBasicInt(out, geom, GEOSGeom_getCoordinateDimension, "geom.CoordDim");
}

/* returns the inherent dimension of the geometry, e.g 0 for point */
str wkbDimension(int *out, wkb **geom) {
	return wkbBasicInt(out, geom, GEOSGeom_getDimensions, "geom.Dimensions");
}

/* returns the srid of the geometry */
/* there is no srid information on the wkb representation of the wkb*/
str wkbSRID(int *out, wkb **geom) {
	return wkbBasicInt(out, geom, GEOSGetSRID, "geom.SRID");
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

str wkbBoundary(wkb **out, wkb **geom) {
	return wkbBasic(out, geom, GEOSBoundary, "geom.Boundary");
}

str wkbEnvelope(wkb **out, wkb **geom) {
	return wkbBasic(out, geom, GEOSEnvelope, "geom.Envelope");
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

/* Returns the number of points in the linestring */
str wkbNumPoints(int *out, wkb **geom) {
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = int_nil;
		throw(MAL, "geolib/libgeom.hm.NumPoints", "wkb2geos failed");	
	}

	if (GEOSGeomTypeId(geosGeometry) != GEOS_LINESTRING) {
		*out = int_nil;
		throw(MAL, "geom.NumPoints", "failed. Geometry not a LineString");
	}

	*out = GEOSGeomGetNumPoints(geosGeometry);
	GEOSGeom_destroy(geosGeometry);

	if (*out == -1)
		throw(MAL, "geom.NumPoints", "failed");

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

/* function to handle static geometry returned by GEOSGetExteriorRing */
static const GEOSGeometry* handleConstExteriorRing(GEOSGeom* geosGeometry, wkb** geom) {
	*geosGeometry = wkb2geos(*geom);

	if (!*geosGeometry) 
		return NULL;

	if (GEOSGeomTypeId(*geosGeometry) != GEOS_POLYGON) 
		return NULL;

	return GEOSGetExteriorRing(*geosGeometry);

}

/* Returns the exterior ring of the polygon*/
str wkbExteriorRing(wkb **out, wkb **geom) {
	GEOSGeom geosGeometry = NULL;
	const GEOSGeometry* exteriorRingGeometry = handleConstExteriorRing(&geosGeometry, geom);
	size_t wkbLen = 0;
	unsigned char *w = NULL;

	if (!exteriorRingGeometry) {
		*out = wkb_nil;

		if(!geosGeometry) {
			throw(MAL, "geom.exteriorRing", "wkb2geos failed");
		} else {
			GEOSGeom_destroy(geosGeometry);
			throw(MAL, "geom.exteriorRing", "Geometry not a Polygon");
		}
	}
	w = GEOSGeomToWKB_buf(exteriorRingGeometry, &wkbLen);
	GEOSGeom_destroy(geosGeometry);

	*out = GDKmalloc(wkb_size(wkbLen));
	if (!(*out)) {
		*out = wkb_nil;
		GEOSFree(w);
		throw(MAL, "geom.exteriorRing", "GDKmalloc failed");
	}
		
	assert(wkbLen <= GDK_int_max);
	(*out)->len = (int) wkbLen;
	memcpy(&(*out)->data, w, wkbLen);
	GEOSFree(w);
	
	return MAL_SUCCEED;
}

/* function to handle static geometry returned by GEOSGetInteriorRingN */
static const GEOSGeometry* handleConstInteriorRing(GEOSGeom* geosGeometry, wkb** geom, int ringIdx, int* reason) {
	int rN = -1;
	*geosGeometry = wkb2geos(*geom);

	if (!*geosGeometry) {
		*reason=1;
		return NULL;
	}
	
	//check number of internal
	rN = GEOSGetNumInteriorRings(*geosGeometry);
	if (rN == -1 ) {
		*reason=2;
		return NULL;
	}
	if(rN <= ringIdx || ringIdx<0) {
		*reason=3; 
		return NULL;
	}

	return GEOSGetInteriorRingN(*geosGeometry, ringIdx);
}


/* Returns the n-th interior ring of a polygon */
str wkbInteriorRingN(wkb **out, wkb **geom, short* ringNum) {
	GEOSGeom geosGeometry = NULL;
	int reason =0;
	const GEOSGeometry* interiorRingGeometry = handleConstInteriorRing(&geosGeometry, geom, *ringNum-1, &reason);
	size_t wkbLen = 0;
	unsigned char *w = NULL;

	if (interiorRingGeometry == NULL) { 
		*out = wkb_nil;

		if(!geosGeometry) {
			throw(MAL, "geom.interiorRingN", "wkb2geos failed");
		} else {
			GEOSGeom_destroy(geosGeometry);
			if(reason == 3)
			throw(MAL, "geom.interiorRingN", "GEOSGetInteriorRingN failed. Not enough interior rings");
			else if(reason == 2)
				throw(MAL, "geom.interiorRingN", "GEOSGetInteriorRingN failed.");
		}
	}

	w = GEOSGeomToWKB_buf(interiorRingGeometry, &wkbLen);
	GEOSGeom_destroy(geosGeometry);

	*out = GDKmalloc(wkb_size(wkbLen));
	if (!(*out)) {
		*out = wkb_nil;
		GEOSFree(w);
		throw(MAL, "geom.interiorRing", "GDKmalloc failed");
	}
		
	assert(wkbLen <= GDK_int_max);
	(*out)->len = (int) wkbLen;
	memcpy(&(*out)->data, w, wkbLen);
	GEOSFree(w);
	
	return MAL_SUCCEED;
}

/* Returns the number of interior rings in the first polygon of the provided geometry */
str wkbNumInteriorRings(int* out, wkb** geom) {
	return wkbBasicInt(out, geom, GEOSGetNumInteriorRings, "geom.NumInteriorRings");
}





str wkbIsEmpty(bit *out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = GEOSisEmpty(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.IsEmpty", "GEOSisEmpty failed");
	return MAL_SUCCEED;

}

str
wkbIsSimple(bit *out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = GEOSisSimple(geosGeometry);

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.IsSimple", "GEOSisSimple failed");
	return MAL_SUCCEED;

}



geom_export BUN mbrHASH(mbr *atom);
geom_export int mbrCOMP(mbr *l, mbr *r);
geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
geom_export int mbrWRITE(mbr *c, stream *s, size_t cnt);
geom_export str mbrFromString(mbr **w, str *src);
geom_export str mbrFromMBR(mbr **w, mbr **src);
//geom_export str wkbFromString(wkb **w, str *wkt);
geom_export str wkbFromWKB(wkb **w, wkb **src);
geom_export BUN wkbHASH(wkb *w);
geom_export int wkbCOMP(wkb *l, wkb *r);
geom_export str wkbIsnil(bit *r, wkb **v);
geom_export void wkbDEL(Heap *h, var_t *index);
geom_export wkb *wkbREAD(wkb *a, stream *s, size_t cnt);
geom_export int wkbWRITE(wkb *a, stream *s, size_t cnt);
geom_export int wkbLENGTH(wkb *p);
geom_export void wkbHEAP(Heap *heap, size_t capacity);
geom_export var_t wkbPUT(Heap *h, var_t *bun, wkb *val);
geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);
geom_export str wkbMBR(mbr **res, wkb **geom);


geom_export str wkbcreatepoint(wkb **out, dbl *x, dbl *y);
geom_export str wkbcreatepoint_bat(int *out, int *x, int *y);
geom_export str mbroverlaps(bit *out, mbr **b1, mbr **b2);



geom_export str wkbConvexHull(wkb **out, wkb **geom);
geom_export str wkbEquals(bit *out, wkb **a, wkb **b);
geom_export str wkbDisjoint(bit *out, wkb **a, wkb **b);
geom_export str wkbIntersect(bit *out, wkb **a, wkb **b);
geom_export str wkbTouches(bit *out, wkb **a, wkb **b);
geom_export str wkbCrosses(bit *out, wkb **a, wkb **b);
geom_export str wkbWithin(bit *out, wkb **a, wkb **b);
geom_export str wkbContains(bit *out, wkb **a, wkb **b);
geom_export str wkbOverlaps(bit *out, wkb **a, wkb **b);
geom_export str wkbRelate(bit *out, wkb **a, wkb **b, str *pattern);
geom_export str wkbArea(dbl *out, wkb **a);
geom_export str wkbLength(dbl *out, wkb **a);
geom_export str wkbDistance(dbl *out, wkb **a, wkb **b);
geom_export str wkbIntersection(wkb **out, wkb **a, wkb **b);
geom_export str wkbUnion(wkb **out, wkb **a, wkb **b);
geom_export str wkbDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbSymDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbBuffer(wkb **out, wkb **geom, dbl *distance);


geom_export str wkbCentroid(wkb **out, wkb **geom);




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
		res = (l->ymin < r->ymin) ? -1 : (l->ymin == r->ymin) ? 0 : 1;
	else
		res = (l->xmin < r->xmin) ? -1 : 1;
	if (res == 0) {
		if (l->xmax == r->xmax)
			res = (l->ymax < r->ymax) ? -1 : (l->ymax == r->ymax) ? 0 : 1;
		else
			res = (l->xmax < r->xmax) ? -1 : 1;
	}
	return res;
}

mbr *
mbrREAD(mbr *a, stream *s, size_t cnt)
{
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

int
mbrWRITE(mbr *c, stream *s, size_t cnt)
{
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

str
mbrFromMBR(mbr **w, mbr **src)
{
	*w = (mbr *) GDKmalloc(sizeof(mbr));

	**w = **src;
	return MAL_SUCCEED;
}




/*str
wkbFromString(wkb **w, str *wkt)
{
	int len = 0;
	char *errbuf;
	str ex;

	if (wkbFROMSTR(*wkt, &len, w))
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

str
wkbFromWKB(wkb **w, wkb **src)
{
	*w = (wkb *) GDKmalloc(wkb_size((*src)->len));

	if (wkb_isnil(*src)) {
		**w = *wkbNULL();
	} else {
		(*w)->len = (*src)->len;
		memcpy(&(*w)->data, &(*src)->data, (*src)->len);
	}
	return MAL_SUCCEED;
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

wkb *
wkbREAD(wkb *a, stream *s, size_t cnt)
{
	int len;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1)
		return NULL;
	if ((a = GDKmalloc(wkb_size(len))) == NULL)
		return NULL;
	a->len = len;
	if (len > 0 && mnstr_read(s, (char *) a->data, len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	return a;
}

int
wkbWRITE(wkb *a, stream *s, size_t cnt)
{
	int len = a->len;

	(void) cnt;
	assert(cnt == 1);
	if (!mnstr_writeInt(s, len))	/* 64bit: check for overflow */
		return GDK_FAIL;
	if (len > 0 &&			/* 64bit: check for overflow */
	    mnstr_write(s, (char *) a->data, len, 1) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
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

/* COMMAND mbr
 * Creates the mbr for the given geom_geometry.
 */

str
wkbMBR(mbr **res, wkb **geom)
{
	*res = (mbr *) GDKmalloc(sizeof(mbr));
	if (*res != NULL) {
		if (wkb_isnil(*geom)) {
			**res = *mbrNULL();
			return MAL_SUCCEED;
		} else if (getMbrGeom(*res, *geom))
			return MAL_SUCCEED;
	}
	throw(MAL, "geom.mbr", "Failed to create mbr");
}

wkb *
geos2wkb(GEOSGeom geosGeometry)
{
	size_t wkbLen = 0;
	unsigned char *w = NULL;
	wkb *atom;

	if (geosGeometry != NULL){
		GEOS_setWKBOutputDims(COORDINATES_NUM);
		w = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);
	}

	atom = GDKmalloc(wkb_size(wkbLen));
	if (atom == NULL)
		return NULL;

	if (geosGeometry == NULL || w == NULL) {
		*atom = *wkbNULL();
	} else {
		assert(wkbLen <= GDK_int_max);
		atom->len = (int) wkbLen;
		memcpy(&atom->data, w, wkbLen);
		GEOSFree(w);
	}
	return atom;
}



str
wkbcreatepoint(wkb **out, dbl *x, dbl *y)
{
	GEOSCoordSeq pnt;
	if (*x == dbl_nil || *y == dbl_nil) {
		if ((*out = GDKmalloc(sizeof(wkb))) != NULL)
			**out = *wkbNULL();
	} else {
		pnt = GEOSCoordSeq_create(1, 2);
		GEOSCoordSeq_setX(pnt, 0, *x);
		GEOSCoordSeq_setY(pnt, 0, *y);
		*out = geos2wkb(GEOSGeom_createPoint(pnt));
		GEOSCoordSeq_destroy(pnt);
	}
	if (*out == NULL)
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
wkbcreatepoint_bat(int *out, int *ix, int *iy)
{
	BAT *bo = NULL, *bx = NULL, *by = NULL;
	dbl *x = NULL, *y = NULL;
	BUN i;
	wkb *p = NULL;

	if ((bx = BATdescriptor(*ix)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ((by = BATdescriptor(*iy)) == NULL) {
		BBPreleaseref(bx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ( bx->htype != TYPE_void ||
		 by->htype != TYPE_void ||
	    bx->hseqbase != by->hseqbase ||
	    BATcount(bx) != BATcount(by)) {
		BBPreleaseref(bx->batCacheid);
		BBPreleaseref(by->batCacheid);
		throw(MAL, "geom.point", "both arguments must have dense and aligned heads");
	}

	if ((bo = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(bx))) == NULL) {
		BBPreleaseref(bx->batCacheid);
		BBPreleaseref(by->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}
	BATseqbase(bo, bx->hseqbase);

	x = (dbl *) Tloc(bx, BUNfirst(bx));
	y = (dbl *) Tloc(by, BUNfirst(bx));
	for (i = 0; i < BATcount(bx); i++) {
		str err = NULL;
		if ((err = wkbcreatepoint(&p, &x[i], &y[i])) != MAL_SUCCEED) {
			BBPreleaseref(bx->batCacheid);
			BBPreleaseref(by->batCacheid);
			BBPreleaseref(bo->batCacheid);
			throw(MAL, "geom.point", "%s", err);
		}
		BUNappend(bo,p,TRUE);
		GDKfree(p);
		p = NULL;
	}

	BATsetcount(bo, BATcount(bx));
    	BATsettrivprop(bo);
    	BATderiveProps(bo,FALSE);
	BBPreleaseref(bx->batCacheid);
	BBPreleaseref(by->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;
}

str
mbroverlaps(bit *out, mbr **b1, mbr **b2)
{
	if (mbr_isnil(*b1) || mbr_isnil(*b2))
		*out = 0;
	else
		*out = !((*b2)->ymax < (*b1)->ymin || (*b2)->ymin > (*b1)->ymax || (*b2)->xmax < (*b1)->xmin || (*b2)->xmin > (*b1)->xmax);
	return MAL_SUCCEED;
}








str
wkbConvexHull(wkb **out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSConvexHull(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	if (GDKerrbuf && GDKerrbuf[0])
		throw(MAL, "geom.ConvexHull", "GEOSConvexHull failed");
	return MAL_SUCCEED;

}

static str
wkbspatial(bit *out, wkb **a, wkb **b, char (*func)(const GEOSGeometry *, const GEOSGeometry *))
{
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (!ga && !gb) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = (*func)(ga, gb);

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return MAL_SUCCEED;
}

str
wkbEquals(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSEquals);
}

str
wkbDisjoint(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSDisjoint);
}

str
wkbIntersect(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSIntersects);
}

str
wkbTouches(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSTouches);
}

str
wkbCrosses(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSCrosses);
}

str
wkbWithin(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSWithin);
}

str
wkbContains(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSContains);
}

str
wkbOverlaps(bit *out, wkb **a, wkb **b)
{
	return wkbspatial(out, a, b, GEOSOverlaps);
}

str
wkbRelate(bit *out, wkb **a, wkb **b, str *pattern)
{
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = bit_nil;
		return MAL_SUCCEED;
	}
	if (!ga && !gb) {
		*out = bit_nil;
		return MAL_SUCCEED;
	}

	*out = GEOSRelatePattern(ga, gb, *pattern);

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	return MAL_SUCCEED;
}

str
wkbArea(dbl *out, wkb **a)
{
	str ret = MAL_SUCCEED;
	GEOSGeom ga = wkb2geos(*a);

	if (!ga) {
		*out = dbl_nil;
		return ret;
	}

	if (GEOSArea(ga, out) == 0)
		ret = "GEOSArea failed";

	GEOSGeom_destroy(ga);

	if (ret != MAL_SUCCEED)
		throw(MAL, "geom.Area", "%s", ret);
	return ret;
}

str
wkbLength(dbl *out, wkb **a)
{
	str ret = MAL_SUCCEED;
	GEOSGeom ga = wkb2geos(*a);

	if (!ga) {
		*out = dbl_nil;
		return ret;
	}

	if (GEOSLength(ga, out) == 0)
		ret = "GEOSLength failed";

	GEOSGeom_destroy(ga);

	if (ret != MAL_SUCCEED)
		throw(MAL, "geom.Length", "%s", ret);
	return ret;
}

str
wkbDistance(dbl *out, wkb **a, wkb **b)
{
	str ret = MAL_SUCCEED;
	GEOSGeom ga = wkb2geos(*a);
	GEOSGeom gb = wkb2geos(*b);

	if (!ga && gb) {
		GEOSGeom_destroy(gb);
		*out = dbl_nil;
		return ret;
	}
	if (ga && !gb) {
		GEOSGeom_destroy(ga);
		*out = dbl_nil;
		return ret;
	}
	if (!ga && !gb) {
		*out = dbl_nil;
		return ret;
	}

	if (GEOSDistance(ga, gb, out) == 0)
		ret = "GEOSDistance failed";

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	if (ret != MAL_SUCCEED)
		throw(MAL, "geom.Distance", "%s", ret);
	return ret;
}

static str
 wkbanalysis(wkb **out, wkb **a, wkb **b,
	    GEOSGeometry *(*func)(const GEOSGeometry *, const GEOSGeometry *))
{
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

	*out = geos2wkb((*func)(ga, gb));

	GEOSGeom_destroy(ga);
	GEOSGeom_destroy(gb);

	if (*out != NULL)
		return MAL_SUCCEED;

	throw(MAL, "geom.@1", "@2 failed");
}

str
wkbIntersection(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSIntersection);
}

str
wkbUnion(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSUnion);
}

str
wkbDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSDifference);
}

str
wkbSymDifference(wkb **out, wkb **a, wkb **b)
{
	return wkbanalysis(out, a, b, GEOSSymDifference);
}

str
wkbBuffer(wkb **out, wkb **geom, dbl *distance)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSBuffer(geosGeometry, *distance, -1));

	GEOSGeom_destroy(geosGeometry);

	if (*out != NULL)
		return MAL_SUCCEED;

	throw(MAL, "geom.Buffer", "GEOSBuffer failed");
}



str
wkbCentroid(wkb **out, wkb **geom)
{
	GEOSGeom geosGeometry = wkb2geos(*geom);

	if (!geosGeometry) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSGetCentroid(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

	//if (GDKerrbuf && GDKerrbuf[0])
	//	throw(MAL, "geom.Centroid", "GEOSGetCentroid failed");
	return MAL_SUCCEED;

}




