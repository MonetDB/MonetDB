/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "geom.h"

/* Geographic data types */
//Bounding box of a geographic shape
typedef struct BoundingBox {
    double xmin;
    double ymin;
    double zmin;
    double xmax;
    double ymax;
    double zmax;
} BoundingBox;

//Geographic point
typedef struct GeoPoint
{
    double lat;
    double lon;
} GeoPoint;

//Geographic line
typedef struct GeoLines
{
    GeoPoint *points;
    int pointCount;
    BoundingBox* bbox;
} GeoLines;

//Geographic polygon
typedef struct GeoPolygon
{
    GeoLines exteriorRing;
    GeoLines *interiorRings;
    int interiorRingsCount;
    BoundingBox* bbox;
} GeoPolygon;

//Cartesian representation of a geographic point (converted from Latitude/Longitude)
typedef struct CartPoint3D
{
    double x;
    double y;
    double z;
} CartPoint3D;

typedef struct CartPoint2D
{
    double x;
    double y;
} CartPoint2D;

str wkbGetCompatibleGeometries(wkb * const *a, wkb * const *b, GEOSGeom *ga, GEOSGeom *gb);

/* Geographic functions */
str wkbCoversGeographic(bit* out, wkb * const * a, wkb * const * b);

str wkbDistanceGeographic(dbl* out, wkb * const * a, wkb * const * b);
str wkbDistanceGeographic_bat(bat *outBAT_id, const bat *aBAT_id, const bat *bBAT_id);
str wkbDistanceGeographic_bat_cand(bat *out_id, const bat *a_id, const bat *b_id, const bat *s1_id, const bat *s2_id);

str wkbDWithinGeographic(bit* out, wkb * const * a, wkb * const * b, const dbl *distance);
str wkbDWithinGeographicSelect(bat* outid, const bat *bid , const bat *sid, wkb * const *wkb_const, const dbl *distance_within, const bit *anti);
str wkbDWithinGeographicJoin(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *d_id, const bat *ls_id, const bat *rs_id, const bit *nil_matches, const lng *estimate, const bit *anti);
str wkbIntersectsGeographic(bit* out, wkb * const * a, wkb * const * b);
str wkbIntersectsGeographicSelect(bat* outid, const bat *bid , const bat *sid, wkb * const *wkb_const, const bit *anti);
str wkbIntersectsGeographicJoin(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, const bit *nil_matches, const lng *estimate, const bit *anti);

str geodeticEdgeBoundingBox(const CartPoint3D* p1, const CartPoint3D* p2, BoundingBox* mbox);
