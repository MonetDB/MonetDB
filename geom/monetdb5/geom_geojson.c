#include "geom.h"

static char *asgeojson_point(GEOSGeom point, char *srs, bbox3D *bbox, int precision);
static char *asgeojson_line(GEOSGeom line, char *srs, bbox3D *bbox, int precision);
static char *asgeojson_poly(GEOSGeom poly, char *srs, bbox3D *bbox, int precision);
static char * asgeojson_multipoint(GEOSGeom mpoint, char *srs, bbox3D *bbox, int precision);
static char * asgeojson_multiline(GEOSGeom mline, char *srs, bbox3D *bbox, int precision);
static char * asgeojson_multipolygon(GEOSGeom mpoly, char *srs, bbox3D *bbox, int precision);
static char * asgeojson_collection(GEOSGeom col, char *srs, bbox3D *bbox, int precision);
static size_t asgeojson_geom_size(GEOSGeom geom, bbox3D *bbox, int precision);
static size_t asgeojson_geom_buf(GEOSGeom geom, char *output, bbox3D *bbox, int precision);

static size_t points_to_geojson(const GEOSGeometry *geom, char *buf, int precision);
static size_t points_geojson_size(const GEOSGeometry *geom, int precision);
static void trim_trailing_zeros(char *str);

#define BUFSIZE OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION

char*
geom_to_geojson(GEOSGeom geom, char *srs, int precision, int has_bbox)
{
    int type = GEOSGeomTypeId(geom)+1;
    bbox3D *bbox;

    if ( precision > OUT_MAX_DOUBLE_PRECISION ) precision = OUT_MAX_DOUBLE_PRECISION;

    if (has_bbox) 
    {
        bbox3DFromGeos(&bbox, geom);
    }		

    switch (type)
    {
        case wkbPoint_mdb:
            return asgeojson_point(geom, srs, bbox, precision);
        case wkbLineString_mdb:
            return asgeojson_line(geom, srs, bbox, precision);
        case wkbPolygon_mdb:
            return asgeojson_poly(geom, srs, bbox, precision);
        case wkbMultiPoint_mdb:
            return asgeojson_multipoint(geom, srs, bbox, precision);
        case wkbMultiLineString_mdb:
            return asgeojson_multiline(geom, srs, bbox, precision);
        case wkbMultiPolygon_mdb:
            return asgeojson_multipolygon(geom, srs, bbox, precision);
        case wkbGeometryCollection_mdb:
            return asgeojson_collection(geom, srs, bbox, precision);
        default:
            assert(0);
            return NULL;
    }

    return NULL;
}

    static size_t
asgeojson_srs_size(char *srs)
{
    int size;

    size = sizeof("'crs':{'type':'name',");
    size += sizeof("'properties':{'name':''}},");
    size += strlen(srs) * sizeof(char);

    return size;
}

    static size_t
asgeojson_srs_buf(char *output, char *srs)
{
    char *ptr = output;

    ptr += sprintf(ptr, "\"crs\":{\"type\":\"name\",");
    ptr += sprintf(ptr, "\"properties\":{\"name\":\"%s\"}},", srs);

    return (ptr-output);
}

    static size_t
asgeojson_bbox_size(int hasz, int precision)
{
    int size;

    if (!hasz)
    {
        size = sizeof("\"bbox\":[,,,],");
        size +=	2 * 2 * (OUT_MAX_DIGS_DOUBLE + precision);
    }
    else
    {
        size = sizeof("\"bbox\":[,,,,,],");
        size +=	2 * 3 * (OUT_MAX_DIGS_DOUBLE + precision);
    }

    return size;
}

    static size_t
asgeojson_bbox_buf(char *output, bbox3D *bbox, int hasz, int precision)
{
    char *ptr = output;

    if (!hasz)
        ptr += sprintf(ptr, "\"bbox\":[%.*f,%.*f,%.*f,%.*f],",
                precision, bbox->xmin, precision, bbox->ymin,
                precision, bbox->xmax, precision, bbox->ymax);
    else
        ptr += sprintf(ptr, "\"bbox\":[%.*f,%.*f,%.*f,%.*f,%.*f,%.*f],",
                precision, bbox->xmin, precision, bbox->ymin, precision, bbox->zmin,
                precision, bbox->xmax, precision, bbox->ymax, precision, bbox->zmax);

    return (ptr-output);
}

    static size_t
asgeojson_point_size(GEOSGeom point, char *srs, bbox3D *bbox, int precision)
{
    int size;

    size = points_geojson_size(point, precision);
    size += sizeof("{'type':'Point',");
    size += sizeof("'coordinates':}");

    if ( GEOSisEmpty(point) == 1 )
        size += 2; /* [] */

    if (srs) size += asgeojson_srs_size(srs);
    if (bbox) size += asgeojson_bbox_size(GEOS_getWKBOutputDims(point) == 3, precision);

    return size;
}

    static size_t
asgeojson_point_buf(GEOSGeom point, char *srs, char *output, bbox3D *bbox, int precision)
{
    char *ptr = output;

    ptr += sprintf(ptr, "{\"type\":\"Point\",");
    if (srs) ptr += asgeojson_srs_buf(ptr, srs);
    if (bbox) ptr += asgeojson_bbox_buf(ptr, bbox, GEOS_getWKBOutputDims(point) == 3, precision);

    ptr += sprintf(ptr, "\"coordinates\":");
    if ( GEOSisEmpty(point) == 1 )
        ptr += sprintf(ptr, "[]");
    ptr += points_to_geojson(point, ptr, precision);
    ptr += sprintf(ptr, "}");

    return (ptr-output);
}

    static char *
asgeojson_point(GEOSGeom point, char *srs, bbox3D *bbox, int precision)
{
    char *output;
    int size;

    size = asgeojson_point_size(point, srs, bbox, precision);
    output = GDKmalloc(size);
    asgeojson_point_buf(point, srs, output, bbox, precision);
    return output;
}

    static size_t
asgeojson_line_size(GEOSGeom line, char *srs, bbox3D *bbox, int precision)
{
    int size;

    size = sizeof("{'type':'LineString',");
    if (srs) size += asgeojson_srs_size(srs);
    if (bbox) size += asgeojson_bbox_size(GEOS_getWKBOutputDims(line) == 3, precision);
    size += sizeof("'coordinates':[]}");
    size += points_geojson_size(line, precision);

    return size;
}

    static size_t
asgeojson_line_buf(GEOSGeom line, char *srs, char *output, bbox3D *bbox, int precision)
{
    char *ptr=output;

    ptr += sprintf(ptr, "{\"type\":\"LineString\",");
    if (srs) ptr += asgeojson_srs_buf(ptr, srs);
    if (bbox) ptr += asgeojson_bbox_buf(ptr, bbox, GEOS_getWKBOutputDims(line) == 3, precision);
    ptr += sprintf(ptr, "\"coordinates\":[");
    ptr += points_to_geojson(line, ptr, precision);
    ptr += sprintf(ptr, "]}");

    return (ptr-output);
}

    static char *
asgeojson_line(GEOSGeom line, char *srs, bbox3D *bbox, int precision)
{
    char *output;
    int size;

    size = asgeojson_line_size(line, srs, bbox, precision);
    output = GDKmalloc(size);
    asgeojson_line_buf(line, srs, output, bbox, precision);

    return output;
}

    static size_t
asgeojson_poly_size(GEOSGeom poly, char *srs, bbox3D *bbox, int precision)
{
    size_t size;
    int i, nrings = GEOSGetNumInteriorRings(poly);

    size = sizeof("{\"type\":\"Polygon\",");
    if (srs) size += asgeojson_srs_size(srs);
    if (bbox) size += asgeojson_bbox_size(GEOS_getWKBOutputDims(poly) == 3, precision);
    size += sizeof("\"coordinates\":[");
    for (i=0; i<nrings; i++)
    {
        size += points_geojson_size(GEOSGetInteriorRingN(poly, i), precision);
        size += sizeof("[]");
    }
    size += sizeof(",") * i;
    size += sizeof("]}");

    return size;
}

    static size_t
asgeojson_poly_buf(GEOSGeom poly, char *srs, char *output, bbox3D *bbox, int precision)
{
    int i, nrings = GEOSGetNumInteriorRings(poly);
    char *ptr=output;

    ptr += sprintf(ptr, "{\"type\":\"Polygon\",");
    if (srs) ptr += asgeojson_srs_buf(ptr, srs);
    if (bbox) ptr += asgeojson_bbox_buf(ptr, bbox, GEOS_getWKBOutputDims(poly) == 3, precision);
    ptr += sprintf(ptr, "\"coordinates\":[");
    for (i=0; i<nrings; i++)
    {
        if (i) ptr += sprintf(ptr, ",");
        ptr += sprintf(ptr, "[");
        ptr += points_to_geojson(GEOSGetInteriorRingN(poly, i), ptr, precision);
        ptr += sprintf(ptr, "]");
    }
    ptr += sprintf(ptr, "]}");

    return (ptr-output);
}

    static char *
asgeojson_poly(GEOSGeom poly, char *srs, bbox3D *bbox, int precision)
{
    char *output;
    int size;

    size = asgeojson_poly_size(poly, srs, bbox, precision);
    output = GDKmalloc(size);
    asgeojson_poly_buf(poly, srs, output, bbox, precision);

    return output;
}

    static size_t
asgeojson_multipoint_size(GEOSGeom mpoint, char *srs, bbox3D *bbox, int precision)
{
    GEOSGeom  point;
    int size;
    int i, ngeoms = GEOSGetNumGeometries(mpoint);

    size = sizeof("{'type':'MultiPoint',");
    if (srs) size += asgeojson_srs_size(srs);
    if (bbox) size += asgeojson_bbox_size(GEOS_getWKBOutputDims(mpoint) == 3, precision);
    size += sizeof("'coordinates':[]}");

    for (i=0; i<ngeoms; i++)
    {
        point = (GEOSGeom ) GEOSGetGeometryN(mpoint, i);
        size += points_geojson_size(point, precision);
    }
    size += sizeof(",") * i;

    return size;
}

    static size_t
asgeojson_multipoint_buf(GEOSGeom mpoint, char *srs, char *output, bbox3D *bbox, int precision)
{
    GEOSGeom point;
    int i, ngeoms = GEOSGetNumGeometries(mpoint);
    char *ptr=output;

    ptr += sprintf(ptr, "{\"type\":\"MultiPoint\",");
    if (srs) ptr += asgeojson_srs_buf(ptr, srs);
    if (bbox) ptr += asgeojson_bbox_buf(ptr, bbox, GEOS_getWKBOutputDims(mpoint) == 3, precision);
    ptr += sprintf(ptr, "\"coordinates\":[");

    for (i=0; i<ngeoms; i++)
    {
        if (i) ptr += sprintf(ptr, ",");
        point = (GEOSGeom ) GEOSGetGeometryN(mpoint, i);
        ptr += points_to_geojson(point, ptr, precision);
    }
    ptr += sprintf(ptr, "]}");

    return (ptr - output);
}

    static char *
asgeojson_multipoint(GEOSGeom mpoint, char *srs, bbox3D *bbox, int precision)
{
    char *output;
    int size;

    size = asgeojson_multipoint_size(mpoint, srs, bbox, precision);
    output = GDKmalloc(size);
    asgeojson_multipoint_buf(mpoint, srs, output, bbox, precision);

    return output;
}

    static size_t
asgeojson_multiline_size(GEOSGeom mline, char *srs, bbox3D *bbox, int precision)
{
    GEOSGeom  line;
    int size;
    int i, ngeoms = GEOSGetNumGeometries(mline);

    size = sizeof("{'type':'MultiLineString',");
    if (srs) size += asgeojson_srs_size(srs);
    if (bbox) size += asgeojson_bbox_size(GEOS_getWKBOutputDims(mline) == 3, precision);
    size += sizeof("'coordinates':[]}");

    for (i=0 ; i<ngeoms; i++)
    {
        line = (GEOSGeom ) GEOSGetGeometryN(mline, i);
        size += points_geojson_size(line, precision);
        size += sizeof("[]");
    }
    size += sizeof(",") * i;

    return size;
}

    static size_t
asgeojson_multiline_buf(GEOSGeom mline, char *srs, char *output, bbox3D *bbox, int precision)
{
    GEOSGeom line;
    int i, ngeoms = GEOSGetNumGeometries(mline);
    char *ptr=output;

    ptr += sprintf(ptr, "{\"type\":\"MultiLineString\",");
    if (srs) ptr += asgeojson_srs_buf(ptr, srs);
    if (bbox) ptr += asgeojson_bbox_buf(ptr, bbox, GEOS_getWKBOutputDims(mline) == 3, precision);
    ptr += sprintf(ptr, "\"coordinates\":[");

    for (i=0; i<ngeoms; i++)
    {
        if (i) ptr += sprintf(ptr, ",");
        ptr += sprintf(ptr, "[");
        line = (GEOSGeom ) GEOSGetGeometryN(mline, i);
        ptr += points_to_geojson(line, ptr, precision);
        ptr += sprintf(ptr, "]");
    }

    ptr += sprintf(ptr, "]}");

    return (ptr - output);
}

    static char *
asgeojson_multiline(GEOSGeom mline, char *srs, bbox3D *bbox, int precision)
{
    char *output;
    int size;

    size = asgeojson_multiline_size(mline, srs, bbox, precision);
    output = GDKmalloc(size);
    asgeojson_multiline_buf(mline, srs, output, bbox, precision);

    return output;
}

    static size_t
asgeojson_multipolygon_size(GEOSGeom mpoly, char *srs, bbox3D *bbox, int precision)
{
    GEOSGeom poly;
    int size;
    int i, j, ngeoms = GEOSGetNumGeometries(mpoly);

    size = sizeof("{'type':'MultiPolygon',");
    if (srs) size += asgeojson_srs_size(srs);
    if (bbox) size += asgeojson_bbox_size(GEOS_getWKBOutputDims(mpoly) == 3, precision);
    size += sizeof("'coordinates':[]}");

    for (i=0; i < ngeoms; i++)
    {
        int nrings;
        poly = (GEOSGeom ) GEOSGetGeometryN(mpoly, i);
        nrings = GEOSGetNumInteriorRings(poly);
        for (j=0 ; j <nrings ; j++)
        {
            size += points_geojson_size(GEOSGetInteriorRingN(poly, j), precision);
            size += sizeof("[]");
        }
        size += sizeof("[]");
    }
    size += sizeof(",") * i;
    size += sizeof("]}");

    return size;
}

    static size_t
asgeojson_multipolygon_buf(GEOSGeom mpoly, char *srs, char *output, bbox3D *bbox, int precision)
{
    GEOSGeom poly;
    int i, j, ngeoms = GEOSGetNumGeometries(mpoly);
    char *ptr=output;

    ptr += sprintf(ptr, "{\"type\":\"MultiPolygon\",");
    if (srs) ptr += asgeojson_srs_buf(ptr, srs);
    if (bbox) ptr += asgeojson_bbox_buf(ptr, bbox, GEOS_getWKBOutputDims(mpoly) == 3, precision);
    ptr += sprintf(ptr, "\"coordinates\":[");
    for (i=0; i<ngeoms; i++)
    {
        int nrings; 
        if (i) ptr += sprintf(ptr, ",");
        ptr += sprintf(ptr, "[");
        poly = (GEOSGeom ) GEOSGetGeometryN(mpoly, i);
        nrings = GEOSGetNumInteriorRings(poly);
        for (j=0 ; j < nrings ; j++)
        {
            if (j) ptr += sprintf(ptr, ",");
            ptr += sprintf(ptr, "[");
            ptr += points_to_geojson(GEOSGetInteriorRingN(poly, j), ptr, precision);
            ptr += sprintf(ptr, "]");
        }
        ptr += sprintf(ptr, "]");
    }
    ptr += sprintf(ptr, "]}");

    return (ptr - output);
}

    static char *
asgeojson_multipolygon(GEOSGeom mpoly, char *srs, bbox3D *bbox, int precision)
{
    char *output;
    int size;

    size = asgeojson_multipolygon_size(mpoly, srs, bbox, precision);
    output = GDKmalloc(size);
    asgeojson_multipolygon_buf(mpoly, srs, output, bbox, precision);

    return output;
}

    static size_t
asgeojson_collection_size(GEOSGeom col, char *srs, bbox3D *bbox, int precision)
{
    int i,ngeoms = GEOSGetNumGeometries(col);
    int size;
    GEOSGeom subgeom;

    size = sizeof("{'type':'GeometryCollection',");
    if (srs) size += asgeojson_srs_size(srs);
    if (bbox) size += asgeojson_bbox_size(GEOS_getWKBOutputDims(col) == 3, precision);
    size += sizeof("'geometries':");

    for (i=0; i<ngeoms; i++)
    {
        subgeom = (GEOSGeom ) GEOSGetGeometryN(col, i);
        size += asgeojson_geom_size(subgeom, NULL, precision);
    }
    size += sizeof(",") * i;
    size += sizeof("]}");

    return size;
}

    static size_t
asgeojson_collection_buf(GEOSGeom col, char *srs, char *output, bbox3D *bbox, int precision)
{
    int i, ngeoms = GEOSGetNumGeometries(col);
    char *ptr=output;
    GEOSGeom subgeom;

    ptr += sprintf(ptr, "{\"type\":\"GeometryCollection\",");
    if (srs) ptr += asgeojson_srs_buf(ptr, srs);
    if (ngeoms && bbox) ptr += asgeojson_bbox_buf(ptr, bbox, GEOS_getWKBOutputDims(col) == 3, precision);
    ptr += sprintf(ptr, "\"geometries\":[");

    for (i=0; i<ngeoms; i++)
    {
        if (i) ptr += sprintf(ptr, ",");
        subgeom = (GEOSGeom ) GEOSGetGeometryN(col, i);
        ptr += asgeojson_geom_buf(subgeom, ptr, NULL, precision);
    }

    ptr += sprintf(ptr, "]}");

    return (ptr - output);
}

    static char *
asgeojson_collection(GEOSGeom col, char *srs, bbox3D *bbox, int precision)
{
    char *output;
    int size;

    size = asgeojson_collection_size(col, srs, bbox, precision);
    output = GDKmalloc(size);
    asgeojson_collection_buf(col, srs, output, bbox, precision);

    return output;
}

    static size_t
asgeojson_geom_size(GEOSGeom geom, bbox3D *bbox, int precision)
{
    int type = GEOSGeomTypeId(geom)+1;
    size_t size = 0;

    switch (type)
    {
        case wkbPoint_mdb:
            size = asgeojson_point_size(geom, NULL, bbox, precision);
            break;

        case wkbLineString_mdb:
            size = asgeojson_line_size(geom, NULL, bbox, precision);
            break;

        case wkbPolygon_mdb:
            size = asgeojson_poly_size(geom, NULL, bbox, precision);
            break;

        case wkbMultiPoint_mdb:
            size = asgeojson_multipoint_size(geom, NULL, bbox, precision);
            break;

        case wkbMultiLineString_mdb:
            size = asgeojson_multiline_size(geom, NULL, bbox, precision);
            break;

        case wkbMultiPolygon_mdb:
            size = asgeojson_multipolygon_size(geom, NULL, bbox, precision);
            break;

        default:
            assert(0);
            size = 0;
    }

    return size;
}

static size_t
asgeojson_geom_buf(GEOSGeom geom, char *output, bbox3D *bbox, int precision)
{
    int type = GEOSGeomTypeId(geom)+1;
    char *ptr=output;

    switch (type)
    {
        case wkbPoint_mdb:
            ptr += asgeojson_point_buf(geom, NULL, ptr, bbox, precision);
            break;

        case wkbLineString_mdb:
            ptr += asgeojson_line_buf(geom, NULL, ptr, bbox, precision);
            break;

        case wkbPolygon_mdb:
            ptr += asgeojson_poly_buf(geom, NULL, ptr, bbox, precision);
            break;

        case wkbMultiPoint_mdb:
            ptr += asgeojson_multipoint_buf(geom, NULL, ptr, bbox, precision);
            break;

        case wkbMultiLineString_mdb:
            ptr += asgeojson_multiline_buf(geom, NULL, ptr, bbox, precision);
            break;

        case wkbMultiPolygon_mdb:
            ptr += asgeojson_multipolygon_buf(geom, NULL, ptr, bbox, precision);
            break;

        default:
            if (bbox)
                GDKfree(bbox);
            assert(0);
    }

    return (ptr-output);
}

    static int
print_double(double d, int maxdd, char *buf, size_t bufsize)
{
    double ad = fabs(d);
    int ndd = ad < 1 ? 0 : floor(log10(ad))+1; /* non-decimal digits */
    if (fabs(d) < OUT_MAX_DOUBLE)
    {
        if ( maxdd > (OUT_MAX_DOUBLE_PRECISION - ndd) )  maxdd -= ndd;
        return snprintf(buf, bufsize, "%.*f", maxdd, d);
    }
    else
    {
        return snprintf(buf, bufsize, "%g", d);
    }
}

static size_t
points_to_geojson(const GEOSGeometry *geom, char *output, int precision)
{
    uint32_t i;
    char *ptr;
    char x[BUFSIZE+1];
    char y[BUFSIZE+1];
    char z[BUFSIZE+1];
    uint32_t npoints = 0;

    numPointsGeometry(&npoints, geom);
    assert ( precision <= OUT_MAX_DOUBLE_PRECISION );

    x[BUFSIZE] = '\0';
    y[BUFSIZE] = '\0';
    z[BUFSIZE] = '\0';

    ptr = output;

    if ( GEOS_getWKBOutputDims(geom) == 2)
    {
        for (i=0; i<npoints; i++)
        {
            GEOSGeom point = (GEOSGeom) GEOSGetGeometryN(geom, i);
            double pt_x, pt_y;
            GEOSGeomGetX(point, &pt_x);
            GEOSGeomGetY(point, &pt_y);

            print_double(pt_x, precision, x, BUFSIZE);
            trim_trailing_zeros(x);
            print_double(pt_y, precision, y, BUFSIZE);
            trim_trailing_zeros(y);

            if ( i ) ptr += sprintf(ptr, ",");
            ptr += sprintf(ptr, "[%s,%s]", x, y);
        }
    }
    else
    {
        for (i=0; i<npoints; i++)
        {
            GEOSGeom point = (GEOSGeom) GEOSGetGeometryN(geom, i);
            double pt_x, pt_y, pt_z;
            GEOSGeomGetX(point, &pt_x);
            GEOSGeomGetY(point, &pt_y);
            GEOSGeomGetZ(point, &pt_z);

            print_double(pt_x, precision, x, BUFSIZE);
            trim_trailing_zeros(x);
            print_double(pt_y, precision, y, BUFSIZE);
            trim_trailing_zeros(y);
            print_double(pt_z, precision, z, BUFSIZE);
            trim_trailing_zeros(z);

            if ( i ) ptr += sprintf(ptr, ",");
            ptr += sprintf(ptr, "[%s,%s,%s]", x, y, z);
        }
    }

    return (ptr-output);
}

    static size_t
points_geojson_size(const GEOSGeometry *geom, int precision)
{
    uint32_t npoints = 0;
    numPointsGeometry(&npoints, geom);

    assert ( precision <= OUT_MAX_DOUBLE_PRECISION );
    if (GEOS_getWKBOutputDims(geom) == 2)
        return (OUT_MAX_DIGS_DOUBLE + precision + sizeof(","))
            * 2 * npoints + sizeof(",[]");

    return (OUT_MAX_DIGS_DOUBLE + precision + sizeof(",,"))
        * 3 * npoints + sizeof(",[]");
}

    static void
trim_trailing_zeros(char *str)
{
    char *ptr, *totrim=NULL;
    int len;
    int i;

    /* no dot, no decimal digits */
    ptr = strchr(str, '.');
    if ( ! ptr ) return;

    len = strlen(ptr);
    for (i=len-1; i; i--)
    {
        if ( ptr[i] != '0' ) break;
        totrim=&ptr[i];
    }
    if ( totrim )
    {
        if ( ptr == totrim-1 ) *ptr = '\0';
        else *totrim = '\0';
    }
}
