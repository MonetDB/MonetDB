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

/* the first argument in the functions is the return variable */

int TYPE_mbr;

/* general functions */
geom_export void geoHasZ(int* res, int* info);
geom_export void geoHasM(int* res, int* info);
geom_export void geoGetType(char** res, int* info, int flag);

geom_export bat *geom_prelude(void);
geom_export void geom_epilogue(void);

geom_export wkb *wkbNULL(void);
geom_export mbr *mbrNULL(void);

/* The WKB we use is the EWKB used also in PostGIS 
 * because we decided that it is easire to carry around
 * the SRID */
 
/* gets a GEOSGeometry and creates a WKB */
geom_export wkb* geos2wkb(GEOSGeom geosGeometry);

/* the len argument is needed for correct storage and retrieval */
geom_export int mbrFROMSTR(char *src, int *len, mbr **atom);
geom_export int mbrTOSTR(char **dst, int *len, mbr *atom);
geom_export int wkbFROMSTR(char* geomWKT, int *len, wkb** geomWKB, int srid);
geom_export int wkbTOSTR(char **geomWKT, int *len, wkb *geomWKB);
geom_export str wkbFromText(wkb **geomWKB, str *geomWKT, int* srid, int *tpe);

/* Basic Methods on Geometric objects (OGC) */
geom_export str wkbDimension(int*, wkb**);
geom_export str wkbGeometryType(char**, wkb**, int*);
geom_export str wkbGetSRID(int*, wkb**);
//Envelope
geom_export str wkbAsText(str*, wkb**);
//AsBinary
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
geom_export str wkbOverlaps(bit*, wkb**, wkb**);
geom_export str wkbRelate(bit*, wkb**, wkb**, str*);
//LocateAlong
//LocateBetween

//geom_export str wkbFromString(wkb**, str*); 

geom_export str geomMakePoint2D(wkb**, double*, double*);
geom_export str geomMakePoint3D(wkb**, double*, double*, double*);
geom_export str geomMakePoint4D(wkb**, double*, double*, double*, double*);
geom_export str geomMakePointM(wkb**, double*, double*, double*);

geom_export str wkbCoordDim(int* , wkb**);
geom_export str wkbSetSRID(wkb**, wkb**, int*);
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



geom_export str A_2_B(wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID, int* Type, int* SRID); 

str A_2_B(wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID, int* Type, int* SRID) {
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
		throw(MAL, "geom.A_2_B", "column needs geometry(%d, %d) and value is geometry(%d, %d)\n", *columnType, *columnSRID, valueType, valueSRID);

	*SRID = valueSRID;
	*Type = valueType;

	/* get the wkb from the geosGeometry */
	*resWKB = geos2wkb(geosGeometry);
	
	return MAL_SUCCEED;
}


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
void geoGetType(char** res, int* info, int flag) {
	int type = (*info >> 2);
	const char* typeStr=geom_type2str(type, flag) ;

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
 * It makes sure to make all checks before returning */
wkb* geos2wkb(GEOSGeom geosGeometry) {
	size_t wkbLen = 0;
	unsigned char *w = NULL;
	wkb *geomWKB;

	/* if the geosGeometry is NULL create a NULL WKB */
	if(geosGeometry == NULL) {
		geomWKB = GDKmalloc(sizeof(wkb));
		*geomWKB = *wkbNULL();
		return geomWKB;
	}

	GEOS_setWKBOutputDims(GEOSGeom_getCoordinateDimension(geosGeometry));
	w = GEOSGeomToWKB_buf(geosGeometry, &wkbLen);
	
	if(w == NULL) {
		geomWKB = GDKmalloc(sizeof(wkb));
		*geomWKB = *wkbNULL();
		return geomWKB;
	}

	geomWKB = GDKmalloc(wkb_size(wkbLen));
	if (geomWKB == NULL) {
		GEOSFree(w);
		geomWKB = GDKmalloc(sizeof(wkb));
		*geomWKB = *wkbNULL();
		return geomWKB;
	}

	assert(wkbLen <= GDK_int_max);
	geomWKB->len = (int) wkbLen;
	geomWKB->srid = GEOSGetSRID(geosGeometry);
	memcpy(&geomWKB->data, w, wkbLen);
	GEOSFree(w);
	
	return geomWKB;
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

	return strlen(geomWKT);
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
		*len = dstStrLen+1;
		
		*geomWKT = GDKmalloc(*len);
		snprintf(*geomWKT, *len, "\"%s\"", wkt);
		GEOSFree(wkt);
	} else {
		strcpy(*geomWKT, "nil");
	}

	return *len;
}

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
str wkbAsText(str *txt, wkb **geomWKB) {
	int len =0;
	if(wkbTOSTR(txt, &len, *geomWKB))
		return MAL_SUCCEED;
	throw(MAL, "geom.AsText", "Failed to create Text from Well Known Format");
}

static str geomMakePoint(wkb **geomWKB, GEOSGeom geosGeometry) {
	
	*geomWKB = geos2wkb(geosGeometry);
	
	if(wkb_isnil(*geomWKB)) {
		*geomWKB = wkb_nil;
		throw(MAL, "geomMakePoint", "Failed to crete WKB from GEOSGeometry");
	}
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
	GEOSSetSRID(geosGeometry, 0);

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
str wkbGeometryType(char** out, wkb** geomWKB, int* flag) {
	int typeId = 0;
	str ret = MAL_SUCCEED;

	ret = wkbBasicInt(&typeId, geomWKB, GEOSGeomTypeId, "geom.GeometryType");
	typeId = ((typeId+1) << 2);
	geoGetType(out, &typeId, *flag);
	
	return ret;
}

/* returns the number of dimensions of the geometry */
/* geos does not know the number of dimensions as long as a wkb has been created 
 * more precisely it descards all dimensions but x and y*/
str wkbCoordDim(int *out, wkb **geom) {
	return wkbBasicInt(out, geom, GEOSGeom_getCoordinateDimension, "geom.CoordDim");
}

/* returns the inherent dimension of the geometry, e.g 0 for point */
str wkbDimension(int *dimension, wkb **geomWKB) {
	return wkbBasicInt(dimension, geomWKB, GEOSGeom_getDimensions, "geom.Dimensions");
}

/* returns the srid of the geometry */
str wkbGetSRID(int *out, wkb **geomWKB) {
	return wkbBasicInt(out, geomWKB, GEOSGetSRID, "geom.getSRID");
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

	if (!geosGeometry) {
		*out = wkb_nil;
		return MAL_SUCCEED;
	}

	*out = geos2wkb(GEOSGetCentroid(geosGeometry));

	GEOSGeom_destroy(geosGeometry);

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


geom_export BUN mbrHASH(mbr *atom);
geom_export int mbrCOMP(mbr *l, mbr *r);
geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
geom_export int mbrWRITE(mbr *c, stream *s, size_t cnt);
geom_export str mbrFromString(mbr **w, str *src);
geom_export str mbrFromMBR(mbr **w, mbr **src);
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

int
wkbWRITE(wkb *a, stream *s, size_t cnt)
{
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





str
wkbcreatepoint(wkb **out, dbl *x, dbl *y)
{
	GEOSCoordSeq pnt;
	if (*x == dbl_nil || *y == dbl_nil) {
		*out = wkb_nil;
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





















