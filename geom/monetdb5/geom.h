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

wkb * geos2wkb(allocator *, wkb **geomWKB, size_t *len, const GEOSGeometry *geosGeometry);

/* general functions */
geom_export str geoHasZ(Client ctx, int* res, int* info);
geom_export str geoHasM(Client ctx, int* res, int* info);
geom_export str geoGetType(Client ctx, char** res, int* info, int* flag);

/* functions that are used when a column is added to an existing table */
geom_export str mbrFromMBR(Client ctx, mbr **w, mbr **src);
geom_export str wkbFromWKB(Client ctx, wkb **w, wkb **src);
//Is it needed?? geom_export str wkbFromWKB_bat(bat* outBAT_id, bat* inBAT_id);

/* The WKB we use is the EWKB used also in PostGIS
 * because we decided that it is easire to carry around
 * the SRID */

/* gets a GEOSGeometry and returns the mbr of it
 * works only for 2D geometries */
geom_export mbr* mbrFromGeos(const GEOSGeom geosGeometry);


geom_export str wkbFromText(Client ctx, wkb **geomWKB, str *geomWKT, int* srid, int *tpe);
geom_export str wkbFromText_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int *srid, int *tpe);
geom_export str wkbFromText_bat_cand(Client ctx, bat *outBAT_id, bat *inBAT_id, bat *cand, int *srid, int *tpe);

geom_export str wkbMLineStringToPolygon(Client ctx, wkb** geomWKB, str *geomWKT, int* srid, int* flag);


/* Basic Methods on Geometric objects (OGC) */
geom_export str wkbDimension(Client ctx, int*, wkb**);
geom_export str wkbDimension_bat(Client ctx, bat *inBAT_id, bat *outBAT_id);

geom_export str wkbGeometryType(Client ctx, char**, wkb**, int*);
geom_export str wkbGeometryType_bat(Client ctx, bat *inBAT_id, bat *outBAT_id, int *flag);

geom_export str wkbGetSRID(Client ctx, int*, wkb**);
//Envelope
geom_export str wkbAsText(Client ctx, char **outTXT, wkb **inWKB, int *withSRID);
geom_export str wkbAsText_bat(Client ctx, bat *inBAT_id, bat *outBAT_id, int *withSRID);

geom_export str wkbAsBinary(Client ctx, char**, wkb**);
//geom_export str wkbFromBinary(Client ctx, wkb**, const char**);
geom_export str wkbFromBinaryWithBuffer(allocator *ma, wkb **geomWKB, size_t *len, const char **inStr);

geom_export str wkbIsEmpty(Client ctx, bit*, wkb**);
geom_export str wkbIsEmpty_bat(Client ctx, bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsSimple(Client ctx, bit*, wkb**);
geom_export str wkbIsSimple_bat(Client ctx, bat *inBAT_id, bat *outBAT_id);
//Is3D
//IsMeasured
geom_export str wkbBoundary(Client ctx, wkb **outWKB, wkb **inWKB);
geom_export str wkbBoundary_bat(Client ctx, bat *inBAT_id, bat *outBAT_id);


/* Methods for testing spatial relatioships between geometris (OGC) */
geom_export str wkbEquals(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbDisjoint(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbIntersects(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbTouches(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbCrosses(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbWithin(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbContains(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbOverlaps(Client ctx, bit*, wkb**, wkb**);
geom_export str wkbRelate(Client ctx, bit*, wkb**, wkb**, str*);
geom_export str wkbCovers(Client ctx, bit *out, wkb **geomWKB_a, wkb **geomWKB_b);
geom_export str wkbCoveredBy(Client ctx, bit *out, wkb **geomWKB_a, wkb **geomWKB_b);
geom_export str wkbDWithin(Client ctx, bit*, wkb**, wkb**, dbl*);
geom_export str wkbDWithinMbr(Client ctx, bit*, wkb**, wkb**, mbr**, mbr**, dbl*);

//LocateAlong
//LocateBetween

//geom_export str wkbFromString(wkb**, str*);

geom_export str wkbMakePoint(Client ctx, wkb**, dbl*, dbl*, dbl*, dbl*, int*);
geom_export str wkbMakePoint_bat(Client ctx, bat*, bat*, bat*, bat*, bat*, int*);

geom_export str wkbCoordDim(Client ctx, int* , wkb**);
geom_export str wkbSetSRID(Client ctx, wkb**, wkb**, int*);

geom_export str wkbGetCoordinate(Client ctx, dbl *out, wkb **geom, int *dimNum);
geom_export str wkbGetCoordinate_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag);

geom_export str wkbStartPoint(Client ctx, wkb **out, wkb **geom);
geom_export str wkbEndPoint(Client ctx, wkb **out, wkb **geom);

geom_export str wkbNumPoints(Client ctx, int *out, wkb **geom, int *check);
geom_export str wkbNumPoints_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag);

geom_export str wkbPointN(Client ctx, wkb **out, wkb **geom, int *n);
geom_export str wkbEnvelope(Client ctx, wkb **out, wkb **geom);
geom_export str wkbEnvelopeFromCoordinates(Client ctx, wkb** out, dbl* xmin, dbl* ymin, dbl* xmax, dbl* ymax, int* srid);
geom_export str wkbMakePolygon(Client ctx, wkb** out, wkb** external, bat* internalBAT_id, int* srid);
geom_export str wkbMakeLine(Client ctx, wkb**, wkb**, wkb**);
geom_export str wkbMakeLineAggr(Client ctx, wkb **out, bat *bid);
geom_export str wkbMakeLineAggrSubGrouped(Client ctx, bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
geom_export str wkbMakeLineAggrSubGroupedCand(Client ctx, bat* outid, const bat* bid, const bat* gid, const bat* eid, const bat* sid, const bit* skip_nils);
geom_export str wkbExteriorRing(Client ctx, wkb**, wkb**);
geom_export str wkbInteriorRingN(Client ctx, wkb**, wkb**, int*);

geom_export str wkbNumRings(Client ctx, int*, wkb**, int*);
geom_export str wkbNumRings_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag);

geom_export str wkbIsClosed(Client ctx, bit *out, wkb **geom);
geom_export str wkbIsClosed_bat(Client ctx, bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsRing(Client ctx, bit *out, wkb **geom);
geom_export str wkbIsRing_bat(Client ctx, bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsValid(Client ctx, bit *out, wkb **geom);
geom_export str wkbIsValid_bat(Client ctx, bat *inBAT_id, bat *outBAT_id);

geom_export str wkbIsValidReason(Client ctx, char** out, wkb **geom);
geom_export str wkbIsValidDetail(Client ctx, char** out, wkb **geom);

geom_export str wkbArea(Client ctx, dbl *out, wkb **a);
geom_export str wkbCentroid(Client ctx, wkb **out, wkb **geom);
geom_export str wkbDistance(dbl *out, wkb **a, wkb **b);
geom_export str wkbLength(Client ctx, dbl *out, wkb **a);
geom_export str wkbConvexHull(Client ctx, wkb **out, wkb **geom);
geom_export str wkbIntersection(Client ctx, wkb **out, wkb **a, wkb **b);
geom_export str wkbUnion(Client ctx, wkb **out, wkb **a, wkb **b);
geom_export str wkbUnionAggr(Client ctx, wkb** outWKB, bat* inBAT_id);
geom_export str wkbDifference(Client ctx, wkb **out, wkb **a, wkb **b);
geom_export str wkbSymDifference(Client ctx, wkb **out, wkb **a, wkb **b);
geom_export str wkbBuffer(Client ctx, wkb **out, wkb **geom, dbl *distance);

geom_export str wkbGeometryN(Client ctx, wkb** out, wkb** geom, const int* geometryNum);
geom_export str wkbGeometryN_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, const int* flag);

geom_export str wkbNumGeometries(Client ctx, int* out, wkb** geom);
geom_export str wkbNumGeometries_bat(Client ctx, bat *outBAT_id, bat *inBAT_id);

#ifdef HAVE_PROJ
str transformPoint(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P);
str transformLine(GEOSCoordSeq *gcs_new, const GEOSGeometry *geosGeometry, PJ *P);
str transformLineString(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P);
str transformLinearRing(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P);
str transformPolygon(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid);
str transformMultiGeometry(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid, int geometryType);
#endif
geom_export str wkbTransform(Client ctx, wkb**, wkb**, int*, int*, char**, char**);
geom_export str wkbTransform_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str);
geom_export str wkbTransform_bat_cand(bat *outBAT_id, bat *inBAT_id, bat *s_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str);
geom_export str wkbTranslate(Client ctx, wkb**, wkb**, dbl*, dbl*, dbl*);
geom_export str wkbDelaunayTriangles(Client ctx, wkb**, wkb**, dbl*, int*);
geom_export str wkbPointOnSurface(Client ctx, wkb**, wkb**);
geom_export str wkbForceDim(Client ctx, wkb**, wkb**, int*);
geom_export str wkbSegmentize(Client ctx, wkb**, wkb**, dbl*);

geom_export str wkbDump(Client ctx, bat* idBAT_id, bat* geomBAT_id, wkb**);
geom_export str wkbDumpPoints(Client ctx, bat* idBAT_id, bat* geomBAT_id, wkb**);

geom_export str geom_2_geom(Client ctx, wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID);

/* BULK */

geom_export str wkbDistance_bat(Client ctx, bat* outBAT_id, bat*, bat*);
geom_export str wkbDistance_geom_bat(Client ctx, bat* outBAT_id, wkb** geomWKB, bat* inBAT_id);
geom_export str wkbDistance_bat_geom(Client ctx, bat* outBAT_id, bat* inBAT_id, wkb** geomWKB);

geom_export str wkbContains_bat(Client ctx, bat* outBAT_id, bat*, bat*);
geom_export str wkbContains_geom_bat(Client ctx, bat* outBAT_id, wkb** geomWKB, bat* inBAT_id);
geom_export str wkbContains_bat_geom(Client ctx, bat* outBAT_id, bat* inBAT_id, wkb** geomWKB);

//geom_export str wkbFilter_bat(bat* aBATfiltered_id, bat* bBATfiltered_id, bat* aBAT_id, bat* bBAT_id);
geom_export str wkbFilter_geom_bat(Client ctx, bat* BATfiltered_id, wkb** geomWKB, bat* BAToriginal_id);
geom_export str wkbFilter_bat_geom(Client ctx, bat* BATfiltered_id, bat* BAToriginal_id, wkb** geomWKB);

geom_export str wkbMakeLine_bat(Client ctx, bat* outBAT_id, bat* aBAT_id, bat* bBAT_id);
geom_export str wkbUnion_bat(Client ctx, bat* outBAT_id, bat* aBAT_id, bat* bBAT_id);

geom_export str wkbSetSRID_bat(Client ctx, bat* outBAT_id, bat* inBAT_id, int* srid);

geom_export str geom_2_geom_bat(Client ctx, bat* outBAT_id, bat* inBAT_id, bat* cand, int* columnType, int* columnSRID);

geom_export str wkbMBR_bat(Client ctx, bat* outBAT_id, bat* inBAT_id);

geom_export str wkbCoordinateFromWKB_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* coordinateIdx);
geom_export str wkbCoordinateFromMBR_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* coordinateIdx);

geom_export str wkbIntersectsJoinNoIndex(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti);
geom_export str wkbIntersectsSelectNoIndex(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti);

geom_export str wkbIntersectsJoinRTree(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti);
geom_export str wkbIntersectsSelectRTree(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti);

geom_export str wkbDWithinJoinRTree(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, double *distance, bit *nil_matches, lng *estimate, bit *anti);
geom_export str wkbDWithinSelectRTree(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, double *distance, bit *anti);

geom_export str wkbDWithinJoinNoIndex(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, double *distance, bit *nil_matches, lng *estimate, bit *anti);
geom_export str wkbDWithinSelectNoIndex(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, double *distance, bit *anti);

geom_export str mbrIntersects(Client ctx, bit* out, mbr** mbr1, mbr** mbr2);

geom_export str wkbCollectAggr(Client ctx, wkb **out, const bat *bid);
geom_export str wkbCollectAggrSubGrouped(Client ctx, bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);
geom_export str wkbCollectAggrSubGroupedCand(Client ctx, bat* outid, const bat* bid, const bat* gid, const bat* eid, const bat* sid, const bit* skip_nils);
