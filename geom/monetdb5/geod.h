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

/* Geographic functions */
str wkbCoversGeographic(bit* out, wkb** a, wkb** b);

str wkbDistanceGeographic(dbl* out, wkb** a, wkb** b);
str wkbDistanceGeographic_bat(bat *outBAT_id, bat *aBAT_id, bat *bBAT_id);
str wkbDistanceGeographic_bat_cand(bat *out_id, bat *a_id, bat *b_id, bat *s1_id, bat *s2_id);

str wkbDWithinGeographic(bit* out, wkb** a, wkb** b, dbl *distance);
str wkbDWithinGeographicSelect(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, dbl *distance_within, bit *anti);
str wkbDWithinGeographicJoin(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *d_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti);
str wkbIntersectsGeographic(bit* out, wkb** a, wkb** b);
str wkbIntersectsGeographicSelect(bat* outid, const bat *bid , const bat *sid, wkb **wkb_const, bit *anti);
str wkbIntersectsGeographicJoin(bat *lres_id, bat *rres_id, const bat *l_id, const bat *r_id, const bat *ls_id, const bat *rs_id, bit *nil_matches, lng *estimate, bit *anti);

str geodeticEdgeBoundingBox(const CartPoint3D* p1, const CartPoint3D* p2, BoundingBox* mbox);
