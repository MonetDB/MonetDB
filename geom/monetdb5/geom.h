/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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

extern wkb *geos2wkb(allocator *, wkb **geomWKB, size_t *len, const GEOSGeometry *geosGeometry)
	ATTR_VISIBILITY_HIDDEN;

/* general functions */
extern str geoHasZ(Client ctx, int* res, int* info)
	ATTR_VISIBILITY_HIDDEN;
extern str geoHasM(Client ctx, int* res, int* info)
	ATTR_VISIBILITY_HIDDEN;
extern str geoGetType(Client ctx, char** res, int* info, int* flag)
	ATTR_VISIBILITY_HIDDEN;

/* functions that are used when a column is added to an existing table */
extern str mbrFromMBR(Client ctx, mbr **w, mbr **src)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbFromWKB(Client ctx, wkb **w, wkb **src)
	ATTR_VISIBILITY_HIDDEN;
//Is it needed?? extern str wkbFromWKB_bat(bat* outBAT_id, bat* inBAT_id);

/* The WKB we use is the EWKB used also in PostGIS
 * because we decided that it is easire to carry around
 * the SRID */

/* gets a GEOSGeometry and returns the mbr of it
 * works only for 2D geometries */
extern mbr* mbrFromGeos(allocator *ma, const GEOSGeom geosGeometry)
	ATTR_VISIBILITY_HIDDEN;


extern str wkbFromText(Client ctx, wkb **geomWKB, str *geomWKT, int* srid, int *tpe)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbFromText_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int *srid, int *tpe)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbFromText_bat_cand(Client ctx, bat *outBAT_id, bat *inBAT_id, bat *cand, int *srid, int *tpe)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbMLineStringToPolygon(Client ctx, wkb** geomWKB, str *geomWKT, int* srid, int* flag)
	ATTR_VISIBILITY_HIDDEN;


/* Basic Methods on Geometric objects (OGC) */
extern str wkbDimension(Client ctx, int*, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDimension_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbGeometryType(Client ctx, char**, wkb**, int*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbGeometryType_bat(Client ctx, bat *inBAT_id, bat *outBAT_id, int *flag)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbGetSRID(Client ctx, int*, wkb**)
	ATTR_VISIBILITY_HIDDEN;
//Envelope
extern str wkbAsText(Client ctx, char **outTXT, wkb **inWKB, int *withSRID)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbAsText_bat(Client ctx, bat *inBAT_id, bat *outBAT_id, int *withSRID)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbAsBinary(Client ctx, char**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
//extern str wkbFromBinary(Client ctx, wkb**, const char**);
extern str wkbFromBinaryWithBuffer(allocator *ma, wkb **geomWKB, size_t *len, const char **inStr)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIsEmpty(Client ctx, bit*, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIsEmpty_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIsSimple(Client ctx, bit*, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIsSimple_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	ATTR_VISIBILITY_HIDDEN;
//Is3D
//IsMeasured
extern str wkbBoundary(Client ctx, wkb **outWKB, wkb **inWKB)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbBoundary_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	ATTR_VISIBILITY_HIDDEN;


/* Methods for testing spatial relatioships between geometris (OGC) */
extern str wkbEquals(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDisjoint(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIntersects(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbTouches(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbCrosses(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbWithin(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbContains(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbOverlaps(Client ctx, bit*, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbRelate(Client ctx, bit*, wkb**, wkb**, str*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbCovers(Client ctx, bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbCoveredBy(Client ctx, bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDWithin(Client ctx, bit*, wkb**, wkb**, dbl*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDWithinMbr(Client ctx, bit*, wkb**, wkb**, mbr**, mbr**, dbl*)
	ATTR_VISIBILITY_HIDDEN;

//LocateAlong
//LocateBetween

//extern str wkbFromString(wkb**, str*);

extern str wkbMakePoint(Client ctx, wkb**, dbl*, dbl*, dbl*, dbl*, int*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbMakePoint_bat(Client ctx, bat*, bat*, bat*, bat*, bat*, int*)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbCoordDim(Client ctx, int* , wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbSetSRID(Client ctx, wkb**, wkb**, int*)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbGetCoordinate(Client ctx, dbl *out, wkb **geom, int *dimNum)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbGetCoordinate_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbStartPoint(Client ctx, wkb **out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbEndPoint(Client ctx, wkb **out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbNumPoints(Client ctx, int *out, wkb **geom, int *check)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbNumPoints_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbPointN(Client ctx, wkb **out, wkb **geom, int *n)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbEnvelope(Client ctx, wkb **out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbEnvelopeFromCoordinates(Client ctx, wkb** out, dbl* xmin, dbl* ymin, dbl* xmax, dbl* ymax, int* srid)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbMakePolygon(Client ctx, wkb** out, wkb** external, bat* internalBAT_id, int* srid)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbMakeLine(Client ctx, wkb**, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbMakeLineAggr(Client ctx, wkb **out, bat *bid)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbMakeLineAggrSubGrouped(Client ctx, bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbMakeLineAggrSubGroupedCand(Client ctx, bat* outid, const bat* bid, const bat* gid, const bat* eid, const bat* sid, const bit* skip_nils)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbExteriorRing(Client ctx, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbInteriorRingN(Client ctx, wkb**, wkb**, int*)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbNumRings(Client ctx, int*, wkb**, int*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbNumRings_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIsClosed(Client ctx, bit *out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIsClosed_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIsRing(Client ctx, bit *out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIsRing_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIsValid(Client ctx, bit *out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIsValid_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIsValidReason(Client ctx, char** out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIsValidDetail(Client ctx, char** out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbArea(Client ctx, dbl *out, wkb **a)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbCentroid(Client ctx, wkb **out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDistance(dbl *out, wkb **a, wkb **b)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbLength(Client ctx, dbl *out, wkb **a)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbConvexHull(Client ctx, wkb **out, wkb **geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIntersection(Client ctx, wkb **out, wkb **a, wkb **b)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbUnion(Client ctx, wkb **out, wkb **a, wkb **b)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbUnionAggr(Client ctx, wkb** outWKB, bat* inBAT_id)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDifference(Client ctx, wkb **out, wkb **a, wkb **b)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbSymDifference(Client ctx, wkb **out, wkb **a, wkb **b)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbBuffer(Client ctx, wkb **out, wkb **geom, dbl *distance)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbGeometryN(Client ctx, wkb** out, wkb** geom, const int* geometryNum)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbGeometryN_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, const int* flag)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbNumGeometries(Client ctx, int* out, wkb** geom)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbNumGeometries_bat(Client ctx, bat *outBAT_id, bat *inBAT_id)
	ATTR_VISIBILITY_HIDDEN;

#ifdef HAVE_PROJ
extern str transformPoint(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
	ATTR_VISIBILITY_HIDDEN;
extern str transformLine(GEOSCoordSeq *gcs_new, const GEOSGeometry *geosGeometry, PJ *P)
	ATTR_VISIBILITY_HIDDEN;
extern str transformLineString(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
	ATTR_VISIBILITY_HIDDEN;
extern str transformLinearRing(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
	ATTR_VISIBILITY_HIDDEN;
extern str transformPolygon(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid)
	ATTR_VISIBILITY_HIDDEN;
extern str transformMultiGeometry(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid, int geometryType)
	ATTR_VISIBILITY_HIDDEN;
#endif
extern str wkbTransform(Client ctx, wkb**, wkb**, int*, int*, char**, char**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbTransform_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbTransform_bat_cand(bat *outBAT_id, bat *inBAT_id, bat *s_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbTranslate(Client ctx, wkb**, wkb**, dbl*, dbl*, dbl*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDelaunayTriangles(Client ctx, wkb**, wkb**, dbl*, int*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbPointOnSurface(Client ctx, wkb**, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbForceDim(Client ctx, wkb**, wkb**, int*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbSegmentize(Client ctx, wkb**, wkb**, dbl*)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbDump(Client ctx, bat* idBAT_id, bat* geomBAT_id, wkb**)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDumpPoints(Client ctx, bat* idBAT_id, bat* geomBAT_id, wkb**)
	ATTR_VISIBILITY_HIDDEN;

extern str geom_2_geom(Client ctx, wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID)
	ATTR_VISIBILITY_HIDDEN;

/* BULK */

extern str wkbDistance_bat(Client ctx, bat* outBAT_id, bat*, bat*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDistance_geom_bat(Client ctx, bat* outBAT_id, wkb** geomWKB, bat* inBAT_id)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDistance_bat_geom(Client ctx, bat* outBAT_id, bat* inBAT_id, wkb** geomWKB)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbContains_bat(Client ctx, bat* outBAT_id, bat*, bat*)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbContains_geom_bat(Client ctx, bat* outBAT_id, wkb** geomWKB, bat* inBAT_id)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbContains_bat_geom(Client ctx, bat* outBAT_id, bat* inBAT_id, wkb** geomWKB)
	ATTR_VISIBILITY_HIDDEN;

//extern str wkbFilter_bat(bat* aBATfiltered_id, bat* bBATfiltered_id, bat* aBAT_id, bat* bBAT_id);
extern str wkbFilter_geom_bat(Client ctx, bat* BATfiltered_id, wkb** geomWKB, bat* BAToriginal_id)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbFilter_bat_geom(Client ctx, bat* BATfiltered_id, bat* BAToriginal_id, wkb** geomWKB)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbMakeLine_bat(Client ctx, bat* outBAT_id, bat* aBAT_id, bat* bBAT_id)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbUnion_bat(Client ctx, bat* outBAT_id, bat* aBAT_id, bat* bBAT_id)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbSetSRID_bat(Client ctx, bat* outBAT_id, bat* inBAT_id, int* srid)
	ATTR_VISIBILITY_HIDDEN;

extern str geom_2_geom_bat(Client ctx, bat* outBAT_id, bat* inBAT_id, bat* cand, int* columnType, int* columnSRID)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbMBR_bat(Client ctx, bat* outBAT_id, bat* inBAT_id)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbCoordinateFromWKB_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* coordinateIdx)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbCoordinateFromMBR_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* coordinateIdx)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIntersectsJoinNoIndex(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIntersectsSelectNoIndex(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbIntersectsJoinRTree(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbIntersectsSelectRTree(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbDWithinJoinRTree(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, double *distance, bit *nil_matches, lng *estimate, bit *anti)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDWithinSelectRTree(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, double *distance, bit *anti)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbDWithinJoinNoIndex(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, double *distance, bit *nil_matches, lng *estimate, bit *anti)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbDWithinSelectNoIndex(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, double *distance, bit *anti)
	ATTR_VISIBILITY_HIDDEN;

extern str mbrIntersects(Client ctx, bit* out, mbr** mbr1, mbr** mbr2)
	ATTR_VISIBILITY_HIDDEN;

extern str wkbCollectAggr(Client ctx, wkb **out, const bat *bid)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbCollectAggrSubGrouped(Client ctx, bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
	ATTR_VISIBILITY_HIDDEN;
extern str wkbCollectAggrSubGroupedCand(Client ctx, bat* outid, const bat* bid, const bat* gid, const bat* eid, const bat* sid, const bit* skip_nils)
	ATTR_VISIBILITY_HIDDEN;
