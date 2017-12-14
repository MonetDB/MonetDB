/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a Wouter Scherphof, Niels Nes, Foteini Alvanaki
 */

#include "monetdb_config.h"
#include "libgeom.h"

#include "mal.h"
#include "mal_atom.h"
#include "mal_exception.h"
#include "mal_client.h"
#include "stream.h"

#include <string.h>
#include <math.h>
#include <time.h>

#include "gdk_logger.h"

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
geom_export str geoHasZ(int* res, int* info);
geom_export str geoHasM(int* res, int* info);
geom_export str geoGetType(char** res, int* info, int* flag);

geom_export str geom_prelude(void *ret);
geom_export str geom_epilogue(void *ret);

/* the len argument is needed for correct storage and retrieval */
geom_export ssize_t wkbTOSTR(char **geomWKT, size_t *len, const wkb *geomWKB);
geom_export ssize_t mbrTOSTR(char **dst, size_t *len, const mbr *atom);
geom_export ssize_t wkbaTOSTR(char **toStr, size_t* len, const wkba *fromArray);

geom_export ssize_t wkbFROMSTR(const char* geomWKT, size_t *len, wkb** geomWKB);
geom_export ssize_t mbrFROMSTR(const char *src, size_t *len, mbr **atom);
geom_export ssize_t wkbaFROMSTR(const char *fromStr, size_t *len, wkba **toArray);

geom_export const wkb *wkbNULL(void);
geom_export const mbr *mbrNULL(void);
geom_export const wkba *wkbaNULL(void);

geom_export BUN wkbHASH(const wkb *w);
geom_export BUN mbrHASH(const mbr *atom);
geom_export BUN wkbaHASH(const wkba *w);

geom_export int wkbCOMP(const wkb *l, const wkb *r);
geom_export int mbrCOMP(const mbr *l, const mbr *r);
geom_export int wkbaCOMP(const wkba *l, const wkba *r);

/* read/write to/from log */
geom_export wkb *wkbREAD(wkb *a, stream *s, size_t cnt);
geom_export mbr *mbrREAD(mbr *a, stream *s, size_t cnt);
geom_export wkba* wkbaREAD(wkba *a, stream *s, size_t cnt);

geom_export gdk_return wkbWRITE(const wkb *a, stream *s, size_t cnt);
geom_export gdk_return mbrWRITE(const mbr *c, stream *s, size_t cnt);
geom_export gdk_return wkbaWRITE(const wkba *c, stream *s, size_t cnt);

geom_export var_t wkbPUT(Heap *h, var_t *bun, const wkb *val);
geom_export var_t wkbaPUT(Heap *h, var_t *bun, const wkba *val);

geom_export void wkbDEL(Heap *h, var_t *index);
geom_export void wkbaDEL(Heap *h, var_t *index);

geom_export size_t wkbLENGTH(const wkb *p);
geom_export size_t wkbaLENGTH(const wkba *p);

geom_export void wkbHEAP(Heap *heap, size_t capacity);
geom_export void wkbaHEAP(Heap *heap, size_t capacity);

geom_export str mbrFromString(mbr **w, const char **src);
geom_export str wkbIsnil(bit *r, wkb **v);

/* functions that are used when a column is added to an existing table */
geom_export str mbrFromMBR(mbr **w, mbr **src);
geom_export str wkbFromWKB(wkb **w, wkb **src);
//Is it needed?? geom_export str wkbFromWKB_bat(bat* outBAT_id, bat* inBAT_id);

/* The WKB we use is the EWKB used also in PostGIS 
 * because we decided that it is easire to carry around
 * the SRID */
 
/* gets a GEOSGeometry and returns the mbr of it 
 * works only for 2D geometries */
geom_export mbr* mbrFromGeos(const GEOSGeom geosGeometry);


geom_export str wkbFromText(wkb **geomWKB, str *geomWKT, int* srid, int *tpe);
geom_export str wkbFromText_bat(bat *outBAT_id, bat *inBAT_id, int *srid, int *tpe);

geom_export str wkbMLineStringToPolygon(wkb** geomWKB, str *geomWKT, int* srid, int* flag);


/* Basic Methods on Geometric objects (OGC) */
geom_export str wkbDimension(int*, wkb**);
geom_export str wkbDimension_bat(bat *inBAT_id, bat *outBAT_id);

geom_export str wkbGeometryType(char**, wkb**, int*);
geom_export str wkbGeometryType_bat(bat *inBAT_id, bat *outBAT_id, int *flag);

geom_export str wkbGetSRID(int*, wkb**);
//Envelope
geom_export str wkbAsText(char **outTXT, wkb **inWKB, int *withSRID);
geom_export str wkbAsText_bat(bat *inBAT_id, bat *outBAT_id, int *withSRID);

geom_export str wkbAsBinary(char**, wkb**);
geom_export str wkbFromBinary(wkb**, const char**);

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
geom_export str wkbDWithin(bit*, wkb**, wkb**, dbl*);

//LocateAlong
//LocateBetween

//geom_export str wkbFromString(wkb**, str*); 

geom_export str wkbMakePoint(wkb**, dbl*, dbl*, dbl*, dbl*, int*);
geom_export str wkbMakePoint_bat(bat*, bat*, bat*, bat*, bat*, int*);

geom_export str wkbCoordDim(int* , wkb**);
geom_export str wkbSetSRID(wkb**, wkb**, int*);

geom_export str wkbGetCoordinate(dbl *out, wkb **geom, int *dimNum);
geom_export str wkbGetCoordinate_bat(bat *outBAT_id, bat *inBAT_id, int* flag);

geom_export str wkbStartPoint(wkb **out, wkb **geom);
geom_export str wkbEndPoint(wkb **out, wkb **geom);

geom_export str wkbNumPoints(int *out, wkb **geom, int *check);
geom_export str wkbNumPoints_bat(bat *outBAT_id, bat *inBAT_id, int* flag);

geom_export str wkbPointN(wkb **out, wkb **geom, int *n);
geom_export str wkbEnvelope(wkb **out, wkb **geom);
geom_export str wkbEnvelopeFromCoordinates(wkb** out, dbl* xmin, dbl* ymin, dbl* xmax, dbl* ymax, int* srid);
geom_export str wkbMakePolygon(wkb** out, wkb** external, bat* internalBAT_id, int* srid);
geom_export str wkbMakeLine(wkb**, wkb**, wkb**);
geom_export str wkbMakeLineAggr(wkb** outWKB, bat* inBAT_id);
geom_export str wkbExteriorRing(wkb**, wkb**);
geom_export str wkbInteriorRingN(wkb**, wkb**, int*);

geom_export str wkbNumRings(int*, wkb**, int*);
geom_export str wkbNumRings_bat(bat *outBAT_id, bat *inBAT_id, int* flag);

geom_export str wkbInteriorRings(wkba**, wkb**);

geom_export str wkbIsClosed(bit *out, wkb **geom);
geom_export str wkbIsClosed_bat(bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsRing(bit *out, wkb **geom);
geom_export str wkbIsRing_bat(bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsValid(bit *out, wkb **geom);
geom_export str wkbIsValid_bat(bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsValidReason(char** out, wkb **geom);
geom_export str wkbIsValidDetail(char** out, wkb **geom);

geom_export str wkbArea(dbl *out, wkb **a);
geom_export str wkbCentroid(wkb **out, wkb **geom);
geom_export str wkbDistance(dbl *out, wkb **a, wkb **b);
geom_export str wkbLength(dbl *out, wkb **a);
geom_export str wkbConvexHull(wkb **out, wkb **geom);
geom_export str wkbIntersection(wkb **out, wkb **a, wkb **b);
geom_export str wkbUnion(wkb **out, wkb **a, wkb **b);
geom_export str wkbUnionAggr(wkb** outWKB, bat* inBAT_id);
geom_export str wkbDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbSymDifference(wkb **out, wkb **a, wkb **b);
geom_export str wkbBuffer(wkb **out, wkb **geom, dbl *distance);

geom_export str wkbGeometryN(wkb** out, wkb** geom, const int* geometryNum); 
geom_export str wkbGeometryN_bat(bat *outBAT_id, bat *inBAT_id, const int* flag);

geom_export str wkbNumGeometries(int* out, wkb** geom);
geom_export str wkbNumGeometries_bat(bat *outBAT_id, bat *inBAT_id);

geom_export str wkbTransform(wkb**, wkb**, int*, int*, char**, char**);
geom_export str wkbTranslate(wkb**, wkb**, dbl*, dbl*, dbl*);
geom_export str wkbDelaunayTriangles(wkb**, wkb**, dbl*, int*);
geom_export str wkbPointOnSurface(wkb**, wkb**);
geom_export str wkbForceDim(wkb**, wkb**, int*);
geom_export str wkbSegmentize(wkb**, wkb**, dbl*);

geom_export str wkbDump(bat* idBAT_id, bat* geomBAT_id, wkb**);
geom_export str wkbDumpPoints(bat* idBAT_id, bat* geomBAT_id, wkb**);

geom_export str geom_2_geom(wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID); 

geom_export str wkbMBR(mbr **res, wkb **geom);

geom_export str wkbBox2D(mbr** box, wkb** point1, wkb** point2);
geom_export str wkbBox2D_bat(bat* outBAT_id, bat *aBAT_id, bat *bBAT_id);

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
geom_export str mbrDistance(dbl *out, mbr **b1, mbr **b2);
geom_export str mbrDistance_wkb(dbl *out, wkb **geom1WKB, wkb **geom2WKB);

geom_export str wkbCoordinateFromWKB(dbl*, wkb**, int*);
geom_export str wkbCoordinateFromMBR(dbl*, mbr**, int*);

geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);

/* BULK */

geom_export str wkbDistance_bat(bat* outBAT_id, bat*, bat*);
geom_export str wkbDistance_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* inBAT_id);
geom_export str wkbDistance_bat_geom(bat* outBAT_id, bat* inBAT_id, wkb** geomWKB);

geom_export str wkbContains_bat(bat* outBAT_id, bat*, bat*);
geom_export str wkbContains_geom_bat(bat* outBAT_id, wkb** geomWKB, bat* inBAT_id);
geom_export str wkbContains_bat_geom(bat* outBAT_id, bat* inBAT_id, wkb** geomWKB);

//geom_export str wkbFilter_bat(bat* aBATfiltered_id, bat* bBATfiltered_id, bat* aBAT_id, bat* bBAT_id);
geom_export str wkbFilter_geom_bat(bat* BATfiltered_id, wkb** geomWKB, bat* BAToriginal_id);
geom_export str wkbFilter_bat_geom(bat* BATfiltered_id, bat* BAToriginal_id, wkb** geomWKB);

geom_export str wkbMakeLine_bat(bat* outBAT_id, bat* aBAT_id, bat* bBAT_id);
geom_export str wkbUnion_bat(bat* outBAT_id, bat* aBAT_id, bat* bBAT_id);

geom_export str wkbSetSRID_bat(bat* outBAT_id, bat* inBAT_id, int* srid);

geom_export str geom_2_geom_bat(bat* outBAT_id, bat* inBAT_id, int* columnType, int* columnSRID);

geom_export str wkbMBR_bat(bat* outBAT_id, bat* inBAT_id);

geom_export str wkbCoordinateFromWKB_bat(bat *outBAT_id, bat *inBAT_id, int* coordinateIdx);
geom_export str wkbCoordinateFromMBR_bat(bat *outBAT_id, bat *inBAT_id, int* coordinateIdx);

geom_export int geom_catalog_upgrade(void *, int);
geom_export str geom_sql_upgrade(int);
