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
 * @a Wouter Scherphof, Niels Nes, Foteini Alvanaki
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


/* general functions */
geom_export void geoHasZ(int* res, int* info);
geom_export void geoHasM(int* res, int* info);
geom_export void geoGetType(char** res, int* info, int* flag);

geom_export bat *geom_prelude(void);
geom_export void geom_epilogue(void);

/* the len argument is needed for correct storage and retrieval */
geom_export size_t wkbTOSTR(char **geomWKT, size_t *len, wkb *geomWKB);
geom_export size_t mbrTOSTR(char **dst, size_t *len, mbr *atom);
geom_export size_t wkbaTOSTR(char **toStr, size_t* len, wkba *fromArray);

geom_export size_t wkbFROMSTR(char* geomWKT, size_t *len, wkb** geomWKB, int srid);
geom_export size_t mbrFROMSTR(char *src, size_t *len, mbr **atom);
geom_export size_t wkbaFROMSTR(char *fromStr, size_t *len, wkba **toArray, int srid);

geom_export wkb *wkbNULL(void);
geom_export mbr *mbrNULL(void);
geom_export wkba *wkbaNULL(void);

geom_export BUN wkbHASH(wkb *w);
geom_export BUN mbrHASH(mbr *atom);
geom_export BUN wkbaHASH(wkba *w);

geom_export int wkbCOMP(wkb *l, wkb *r);
geom_export int mbrCOMP(mbr *l, mbr *r);
geom_export int wkbaCOMP(wkba *l, wkba *r);

/* read/write to/from log */
geom_export wkb *wkbREAD(wkb *a, stream *s, size_t cnt);
geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
geom_export wkba* wkbaREAD(wkba *a, stream *s, size_t cnt);

geom_export int wkbWRITE(wkb *a, stream *s, size_t cnt);
geom_export int mbrWRITE(mbr *c, stream *s, size_t cnt);
geom_export int wkbaWRITE(wkba *c, stream *s, size_t cnt);

geom_export var_t wkbPUT(Heap *h, var_t *bun, wkb *val);
geom_export var_t wkbaPUT(Heap *h, var_t *bun, wkba *val);

geom_export void wkbDEL(Heap *h, var_t *index);
geom_export void wkbaDEL(Heap *h, var_t *index);

geom_export int wkbLENGTH(wkb *p);
geom_export int wkbaLENGTH(wkba *p);

geom_export void wkbHEAP(Heap *heap, size_t capacity);
geom_export void wkbaHEAP(Heap *heap, size_t capacity);

//geom_export str mbrFromString(mbr **w, str *src);
geom_export str wkbIsnil(bit *r, wkb **v);

/* functions that are used when a column is added to an existing table */
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


geom_export str wkbFromText(wkb **geomWKB, str *geomWKT, int* srid, int *tpe);
geom_export str wkbMLineStringToPolygon(wkb** geomWKB, str* geomWKT, int* srid, int* flag);


/* Basic Methods on Geometric objects (OGC) */
geom_export str wkbDimension(int*, wkb**);
geom_export str wkbGeometryType(char**, wkb**, int*);
geom_export str wkbGetSRID(int*, wkb**);
//Envelope
geom_export str wkbAsText(char **outTXT, wkb **inWKB, int *withSRID);
geom_export str wkbAsText_bat(bat *inBAT_id, bat *outBAT_id, int *withSRID);

geom_export str wkbAsBinary(char**, wkb**);
geom_export str wkbFromBinary(wkb**, char**);

geom_export str wkbIsEmpty(bit*, wkb**);
geom_export str wkbIsEmpty_bat(bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsSimple(bit*, wkb**);
geom_export str wkbIsSimple_bat(bat *inBAT_id, bat *outBAT_id);
//Is3D
//IsMeasured
geom_export str wkbBoundary(wkb **outWKB, wkb **inWKB);
geom_export str wkbBoundary_bat(bat *inBAT_id, bat *outBAT_id);


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
geom_export str wkbCovers(bit *out, wkb **geomWKB_a, wkb **geomWKB_b);
geom_export str wkbCoveredBy(bit *out, wkb **geomWKB_a, wkb **geomWKB_b);
geom_export str wkbDWithin(bit*, wkb**, wkb**, double*);

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
geom_export str wkbNumPoints(unsigned int *out, wkb **geom, int *check);
geom_export str wkbPointN(wkb **out, wkb **geom, int *n);
geom_export str wkbEnvelope(wkb **out, wkb **geom);
geom_export str wkbEnvelopeFromCoordinates(wkb** out, double* xmin, double* ymin, double* xmax, double* ymax, int* srid);
geom_export str wkbMakePolygon(wkb** out, wkb** external, int* internalBAT_id, int* srid);
geom_export str wkbMakeLine(wkb**, wkb**, wkb**);
geom_export str wkbMakeLineAggr(wkb** outWKB, int* inBAT_id);
geom_export str wkbExteriorRing(wkb**, wkb**);
geom_export str wkbInteriorRingN(wkb**, wkb**, short*);
geom_export str wkbNumRings(int*, wkb**, int*);
geom_export str wkbInteriorRings(wkba**, wkb**);

geom_export str wkbIsClosed(bit *out, wkb **geom);
geom_export str wkbIsClosed_bat(bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsRing(bit *out, wkb **geom);
geom_export str wkbIsRing_bat(bat *inBAT_id, bat *outBAT_id);

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
geom_export str wkbUnionAggr(wkb** outWKB, int* inBAT_id);
geom_export str wkbDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbSymDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbBuffer(wkb **out, wkb **geom, dbl *distance);

geom_export str wkbGeometryN(wkb** out, wkb** geom, int* geometryNum); 
geom_export str wkbNumGeometries(int* out, wkb** geom);

geom_export str wkbTransform(wkb**, wkb**, int*, int*, char**, char**);
geom_export str wkbTranslate(wkb**, wkb**, double*, double*, double*);
geom_export str wkbDelaunayTriangles(wkb**, wkb**, double*, int*);
geom_export str wkbPointOnSurface(wkb**, wkb**);
geom_export str wkbForceDim(wkb**, wkb**, int*);
geom_export str wkbSegmentize(wkb**, wkb**, double*);

geom_export str wkbDump(int* idBAT_id, int* geomBAT_id, wkb**);
geom_export str wkbDumpPoints(int* idBAT_id, int* geomBAT_id, wkb**);

geom_export str geom_2_geom(wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID); 

geom_export str wkbMBR(mbr **res, wkb **geom);
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
geom_export str wkbCoordinateFromMBR(dbl*, mbr**, int*);

geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);

/* BULK */

geom_export str wkbDistance_bat(int* outBAT_id, bat*, bat*);
geom_export str wkbDistance_geom_bat(int* outBAT_id, wkb** geomWKB, int* inBAT_id);
geom_export str wkbDistance_bat_geom(int* outBAT_id, int* inBAT_id, wkb** geomWKB);

geom_export str wkbContains_bat(int* outBAT_id, bat*, bat*);
geom_export str wkbContains_geom_bat(int* outBAT_id, wkb** geomWKB, int* inBAT_id);
geom_export str wkbContains_bat_geom(int* outBAT_id, int* inBAT_id, wkb** geomWKB);

//geom_export str wkbFilter_bat(int* aBATfiltered_id, int* bBATfiltered_id, int* aBAT_id, int* bBAT_id);
geom_export str wkbFilter_geom_bat(int* BATfiltered_id, wkb** geomWKB, int* BAToriginal_id);
geom_export str wkbFilter_bat_geom(int* BATfiltered_id, int* BAToriginal_id, wkb** geomWKB);

geom_export str geomMakePoint2D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id);
geom_export str geomMakePoint3D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* zBAT_id);
geom_export str geomMakePoint4D_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* zBAT_id, int* mBAT_id);
geom_export str geomMakePointM_bat(int* outBAT_id, int* xBAT_id, int* yBAT_id, int* mBAT_id);

geom_export str wkbMakeLine_bat(int* outBAT_id, int* aBAT_id, int* bBAT_id);
geom_export str wkbUnion_bat(int* outBAT_id, int* aBAT_id, int* bBAT_id);

geom_export str wkbSetSRID_bat(int* outBAT_id, int* inBAT_id, int* srid);

geom_export str geom_2_geom_bat(int* outBAT_id, int* inBAT_id, int* columnType, int* columnSRID);

geom_export str wkbMBR_bat(int* outBAT_id, int* inBAT_id);

geom_export str wkbCoordinateFromWKB_bat(int *outBAT_id, int *inBAT_id, int* coordinateIdx);
geom_export str wkbCoordinateFromMBR_bat(int *outBAT_id, int *inBAT_id, int* coordinateIdx);
