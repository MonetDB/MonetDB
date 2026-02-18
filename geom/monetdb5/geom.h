/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
	__attribute__((__visibility__("hidden")));

/* general functions */
extern str geoHasZ(Client ctx, int* res, int* info)
	__attribute__((__visibility__("hidden")));
extern str geoHasM(Client ctx, int* res, int* info)
	__attribute__((__visibility__("hidden")));
extern str geoGetType(Client ctx, char** res, int* info, int* flag)
	__attribute__((__visibility__("hidden")));

/* functions that are used when a column is added to an existing table */
extern str mbrFromMBR(Client ctx, mbr **w, mbr **src)
	__attribute__((__visibility__("hidden")));
extern str wkbFromWKB(Client ctx, wkb **w, wkb **src)
	__attribute__((__visibility__("hidden")));
//Is it needed?? geom_export str wkbFromWKB_bat(bat* outBAT_id, bat* inBAT_id);

/* The WKB we use is the EWKB used also in PostGIS
 * because we decided that it is easire to carry around
 * the SRID */

/* gets a GEOSGeometry and returns the mbr of it
 * works only for 2D geometries */
extern mbr* mbrFromGeos(const GEOSGeom geosGeometry)
	__attribute__((__visibility__("hidden")));


extern str wkbFromText(Client ctx, wkb **geomWKB, str *geomWKT, int* srid, int *tpe)
	__attribute__((__visibility__("hidden")));
extern str wkbFromText_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int *srid, int *tpe)
	__attribute__((__visibility__("hidden")));
extern str wkbFromText_bat_cand(Client ctx, bat *outBAT_id, bat *inBAT_id, bat *cand, int *srid, int *tpe)
	__attribute__((__visibility__("hidden")));

extern str wkbMLineStringToPolygon(Client ctx, wkb** geomWKB, str *geomWKT, int* srid, int* flag)
	__attribute__((__visibility__("hidden")));


/* Basic Methods on Geometric objects (OGC) */
extern str wkbDimension(Client ctx, int*, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbDimension_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	__attribute__((__visibility__("hidden")));

extern str wkbGeometryType(Client ctx, char**, wkb**, int*)
	__attribute__((__visibility__("hidden")));
extern str wkbGeometryType_bat(Client ctx, bat *inBAT_id, bat *outBAT_id, int *flag)
	__attribute__((__visibility__("hidden")));

extern str wkbGetSRID(Client ctx, int*, wkb**)
	__attribute__((__visibility__("hidden")));
//Envelope
extern str wkbAsText(Client ctx, char **outTXT, wkb **inWKB, int *withSRID)
	__attribute__((__visibility__("hidden")));
extern str wkbAsText_bat(Client ctx, bat *inBAT_id, bat *outBAT_id, int *withSRID)
	__attribute__((__visibility__("hidden")));

extern str wkbAsBinary(Client ctx, char**, wkb**)
	__attribute__((__visibility__("hidden")));
//geom_export str wkbFromBinary(Client ctx, wkb**, const char**);
extern str wkbFromBinaryWithBuffer(allocator *ma, wkb **geomWKB, size_t *len, const char **inStr)
	__attribute__((__visibility__("hidden")));

extern str wkbIsEmpty(Client ctx, bit*, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbIsEmpty_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	__attribute__((__visibility__("hidden")));

extern str wkbIsSimple(Client ctx, bit*, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbIsSimple_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	__attribute__((__visibility__("hidden")));
//Is3D
//IsMeasured
extern str wkbBoundary(Client ctx, wkb **outWKB, wkb **inWKB)
	__attribute__((__visibility__("hidden")));
extern str wkbBoundary_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	__attribute__((__visibility__("hidden")));


/* Methods for testing spatial relatioships between geometris (OGC) */
extern str wkbEquals(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbDisjoint(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbIntersects(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbTouches(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbCrosses(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbWithin(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbContains(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbOverlaps(Client ctx, bit*, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbRelate(Client ctx, bit*, wkb**, wkb**, str*)
	__attribute__((__visibility__("hidden")));
extern str wkbCovers(Client ctx, bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
	__attribute__((__visibility__("hidden")));
extern str wkbCoveredBy(Client ctx, bit *out, wkb **geomWKB_a, wkb **geomWKB_b)
	__attribute__((__visibility__("hidden")));
extern str wkbDWithin(Client ctx, bit*, wkb**, wkb**, dbl*)
	__attribute__((__visibility__("hidden")));
extern str wkbDWithinMbr(Client ctx, bit*, wkb**, wkb**, mbr**, mbr**, dbl*)
	__attribute__((__visibility__("hidden")));

//LocateAlong
//LocateBetween

//geom_export str wkbFromString(wkb**, str*);

extern str wkbMakePoint(Client ctx, wkb**, dbl*, dbl*, dbl*, dbl*, int*)
	__attribute__((__visibility__("hidden")));
extern str wkbMakePoint_bat(Client ctx, bat*, bat*, bat*, bat*, bat*, int*)
	__attribute__((__visibility__("hidden")));

extern str wkbCoordDim(Client ctx, int* , wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbSetSRID(Client ctx, wkb**, wkb**, int*)
	__attribute__((__visibility__("hidden")));

extern str wkbGetCoordinate(Client ctx, dbl *out, wkb **geom, int *dimNum)
	__attribute__((__visibility__("hidden")));
extern str wkbGetCoordinate_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag)
	__attribute__((__visibility__("hidden")));

extern str wkbStartPoint(Client ctx, wkb **out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbEndPoint(Client ctx, wkb **out, wkb **geom)
	__attribute__((__visibility__("hidden")));

extern str wkbNumPoints(Client ctx, int *out, wkb **geom, int *check)
	__attribute__((__visibility__("hidden")));
extern str wkbNumPoints_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag)
	__attribute__((__visibility__("hidden")));

extern str wkbPointN(Client ctx, wkb **out, wkb **geom, int *n)
	__attribute__((__visibility__("hidden")));
extern str wkbEnvelope(Client ctx, wkb **out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbEnvelopeFromCoordinates(Client ctx, wkb** out, dbl* xmin, dbl* ymin, dbl* xmax, dbl* ymax, int* srid)
	__attribute__((__visibility__("hidden")));
extern str wkbMakePolygon(Client ctx, wkb** out, wkb** external, bat* internalBAT_id, int* srid)
	__attribute__((__visibility__("hidden")));
extern str wkbMakeLine(Client ctx, wkb**, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbMakeLineAggr(Client ctx, wkb **out, bat *bid)
	__attribute__((__visibility__("hidden")));
extern str wkbMakeLineAggrSubGrouped(Client ctx, bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
	__attribute__((__visibility__("hidden")));
extern str wkbMakeLineAggrSubGroupedCand(Client ctx, bat* outid, const bat* bid, const bat* gid, const bat* eid, const bat* sid, const bit* skip_nils)
	__attribute__((__visibility__("hidden")));
extern str wkbExteriorRing(Client ctx, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbInteriorRingN(Client ctx, wkb**, wkb**, int*)
	__attribute__((__visibility__("hidden")));

extern str wkbNumRings(Client ctx, int*, wkb**, int*)
	__attribute__((__visibility__("hidden")));
extern str wkbNumRings_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* flag)
	__attribute__((__visibility__("hidden")));

extern str wkbIsClosed(Client ctx, bit *out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbIsClosed_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	__attribute__((__visibility__("hidden")));

extern str wkbIsRing(Client ctx, bit *out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbIsRing_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	__attribute__((__visibility__("hidden")));

extern str wkbIsValid(Client ctx, bit *out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbIsValid_bat(Client ctx, bat *inBAT_id, bat *outBAT_id)
	__attribute__((__visibility__("hidden")));

extern str wkbIsValidReason(Client ctx, char** out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbIsValidDetail(Client ctx, char** out, wkb **geom)
	__attribute__((__visibility__("hidden")));

extern str wkbArea(Client ctx, dbl *out, wkb **a)
	__attribute__((__visibility__("hidden")));
extern str wkbCentroid(Client ctx, wkb **out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbDistance(dbl *out, wkb **a, wkb **b)
	__attribute__((__visibility__("hidden")));
extern str wkbLength(Client ctx, dbl *out, wkb **a)
	__attribute__((__visibility__("hidden")));
extern str wkbConvexHull(Client ctx, wkb **out, wkb **geom)
	__attribute__((__visibility__("hidden")));
extern str wkbIntersection(Client ctx, wkb **out, wkb **a, wkb **b)
	__attribute__((__visibility__("hidden")));
extern str wkbUnion(Client ctx, wkb **out, wkb **a, wkb **b)
	__attribute__((__visibility__("hidden")));
extern str wkbUnionAggr(Client ctx, wkb** outWKB, bat* inBAT_id)
	__attribute__((__visibility__("hidden")));
extern str wkbDifference(Client ctx, wkb **out, wkb **a, wkb **b)
	__attribute__((__visibility__("hidden")));
extern str wkbSymDifference(Client ctx, wkb **out, wkb **a, wkb **b)
	__attribute__((__visibility__("hidden")));
extern str wkbBuffer(Client ctx, wkb **out, wkb **geom, dbl *distance)
	__attribute__((__visibility__("hidden")));

extern str wkbGeometryN(Client ctx, wkb** out, wkb** geom, const int* geometryNum)
	__attribute__((__visibility__("hidden")));
extern str wkbGeometryN_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, const int* flag)
	__attribute__((__visibility__("hidden")));

extern str wkbNumGeometries(Client ctx, int* out, wkb** geom)
	__attribute__((__visibility__("hidden")));
extern str wkbNumGeometries_bat(Client ctx, bat *outBAT_id, bat *inBAT_id)
	__attribute__((__visibility__("hidden")));

#ifdef HAVE_PROJ
extern str transformPoint(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
	__attribute__((__visibility__("hidden")));
extern str transformLine(GEOSCoordSeq *gcs_new, const GEOSGeometry *geosGeometry, PJ *P)
	__attribute__((__visibility__("hidden")));
extern str transformLineString(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
	__attribute__((__visibility__("hidden")));
extern str transformLinearRing(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P)
	__attribute__((__visibility__("hidden")));
extern str transformPolygon(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid)
	__attribute__((__visibility__("hidden")));
extern str transformMultiGeometry(GEOSGeometry **transformedGeometry, const GEOSGeometry *geosGeometry, PJ *P, int srid, int geometryType)
	__attribute__((__visibility__("hidden")));
#endif
extern str wkbTransform(Client ctx, wkb**, wkb**, int*, int*, char**, char**)
	__attribute__((__visibility__("hidden")));
extern str wkbTransform_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
	__attribute__((__visibility__("hidden")));
extern str wkbTransform_bat_cand(bat *outBAT_id, bat *inBAT_id, bat *s_id, int *srid_src, int *srid_dst, char **proj4_src_str, char **proj4_dst_str)
	__attribute__((__visibility__("hidden")));
extern str wkbTranslate(Client ctx, wkb**, wkb**, dbl*, dbl*, dbl*)
	__attribute__((__visibility__("hidden")));
extern str wkbDelaunayTriangles(Client ctx, wkb**, wkb**, dbl*, int*)
	__attribute__((__visibility__("hidden")));
extern str wkbPointOnSurface(Client ctx, wkb**, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbForceDim(Client ctx, wkb**, wkb**, int*)
	__attribute__((__visibility__("hidden")));
extern str wkbSegmentize(Client ctx, wkb**, wkb**, dbl*)
	__attribute__((__visibility__("hidden")));

extern str wkbDump(Client ctx, bat* idBAT_id, bat* geomBAT_id, wkb**)
	__attribute__((__visibility__("hidden")));
extern str wkbDumpPoints(Client ctx, bat* idBAT_id, bat* geomBAT_id, wkb**)
	__attribute__((__visibility__("hidden")));

extern str geom_2_geom(Client ctx, wkb** resWKB, wkb **valueWKB, int* columnType, int* columnSRID)
	__attribute__((__visibility__("hidden")));

/* BULK */

extern str wkbDistance_bat(Client ctx, bat* outBAT_id, bat*, bat*)
	__attribute__((__visibility__("hidden")));
extern str wkbDistance_geom_bat(Client ctx, bat* outBAT_id, wkb** geomWKB, bat* inBAT_id)
	__attribute__((__visibility__("hidden")));
extern str wkbDistance_bat_geom(Client ctx, bat* outBAT_id, bat* inBAT_id, wkb** geomWKB)
	__attribute__((__visibility__("hidden")));

extern str wkbContains_bat(Client ctx, bat* outBAT_id, bat*, bat*)
	__attribute__((__visibility__("hidden")));
extern str wkbContains_geom_bat(Client ctx, bat* outBAT_id, wkb** geomWKB, bat* inBAT_id)
	__attribute__((__visibility__("hidden")));
extern str wkbContains_bat_geom(Client ctx, bat* outBAT_id, bat* inBAT_id, wkb** geomWKB)
	__attribute__((__visibility__("hidden")));

//geom_export str wkbFilter_bat(bat* aBATfiltered_id, bat* bBATfiltered_id, bat* aBAT_id, bat* bBAT_id);
extern str wkbFilter_geom_bat(Client ctx, bat* BATfiltered_id, wkb** geomWKB, bat* BAToriginal_id)
	__attribute__((__visibility__("hidden")));
extern str wkbFilter_bat_geom(Client ctx, bat* BATfiltered_id, bat* BAToriginal_id, wkb** geomWKB)
	__attribute__((__visibility__("hidden")));

extern str wkbMakeLine_bat(Client ctx, bat* outBAT_id, bat* aBAT_id, bat* bBAT_id)
	__attribute__((__visibility__("hidden")));
extern str wkbUnion_bat(Client ctx, bat* outBAT_id, bat* aBAT_id, bat* bBAT_id)
	__attribute__((__visibility__("hidden")));

extern str wkbSetSRID_bat(Client ctx, bat* outBAT_id, bat* inBAT_id, int* srid)
	__attribute__((__visibility__("hidden")));

extern str geom_2_geom_bat(Client ctx, bat* outBAT_id, bat* inBAT_id, bat* cand, int* columnType, int* columnSRID)
	__attribute__((__visibility__("hidden")));

extern str wkbMBR_bat(Client ctx, bat* outBAT_id, bat* inBAT_id)
	__attribute__((__visibility__("hidden")));

extern str wkbCoordinateFromWKB_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* coordinateIdx)
	__attribute__((__visibility__("hidden")));
extern str wkbCoordinateFromMBR_bat(Client ctx, bat *outBAT_id, bat *inBAT_id, int* coordinateIdx)
	__attribute__((__visibility__("hidden")));

extern str wkbIntersectsJoinNoIndex(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti)
	__attribute__((__visibility__("hidden")));
extern str wkbIntersectsSelectNoIndex(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti)
	__attribute__((__visibility__("hidden")));

extern str wkbIntersectsJoinRTree(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti)
	__attribute__((__visibility__("hidden")));
extern str wkbIntersectsSelectRTree(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti)
	__attribute__((__visibility__("hidden")));

extern str wkbDWithinJoinRTree(Client ctx, bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, double *distance, bit *nil_matches, lng *estimate, bit *anti)
	__attribute__((__visibility__("hidden")));
extern str wkbDWithinSelectRTree(Client ctx, bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, double *distance, bit *anti)
	__attribute__((__visibility__("hidden")));

extern str wkbDWithinJoinNoIndex(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, double *distance, bit *nil_matches, lng *estimate, bit *anti)
	__attribute__((__visibility__("hidden")));
extern str wkbDWithinSelectNoIndex(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, double *distance, bit *anti)
	__attribute__((__visibility__("hidden")));

extern str mbrIntersects(Client ctx, bit* out, mbr** mbr1, mbr** mbr2)
	__attribute__((__visibility__("hidden")));

extern str wkbCollectAggr(Client ctx, wkb **out, const bat *bid)
	__attribute__((__visibility__("hidden")));
extern str wkbCollectAggrSubGrouped(Client ctx, bat *out, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
	__attribute__((__visibility__("hidden")));
extern str wkbCollectAggrSubGroupedCand(Client ctx, bat* outid, const bat* bid, const bat* gid, const bat* eid, const bat* sid, const bit* skip_nils)
	__attribute__((__visibility__("hidden")));
