#include "geom.h"

static size_t x3d_3_point_size(GEOSGeom point, int precision);
static char *x3d_3_point(GEOSGeom point, int precision, int opts);
static size_t x3d_3_line_size(GEOSGeom line, int precision, int opts, const char *defid);
static char *x3d_3_line(GEOSGeom line, int precision, int opts, const char *defid);
static size_t x3d_3_poly_size(GEOSGeom poly, int precision, const char *defid);
static size_t x3d_3_triangle_size(GEOSGeom triangle, int precision, const char *defid);
static char *x3d_3_triangle(GEOSGeom triangle, int precision, int opts, const char *defid);
static size_t x3d_3_multi_size(GEOSGeom col, int precisioSn, int opts, const char *defid);
static char *x3d_3_multi(GEOSGeom col, int precision, int opts, const char *defid);
static char *x3d_3_psurface(GEOSGeom psur, int precision, int opts, const char *defid);
static char *x3d_3_tin(GEOSGeom tin, int precision, int opts, const char *defid);
/*static size_t x3d_3_collection_size(GEOSGeom col, int precision, int opts, const char *defid);*/
/*static char *x3d_3_collection(GEOSGeom col, int precision, int opts, const char *defid);*/
static size_t geom_toX3D3(const GEOSGeometry *geom, char *buf, int precision, int opts, int is_closed);

static size_t geom_X3Dsize(const GEOSGeometry *geom, int precision);
static void trim_trailing_zeros(char *str);

/*
 * VERSION X3D 3.0.2 http://www.web3d.org/specifications/x3d-3.0.dtd
 */


/* takes a GEOMETRY and returns an X3D representation */
char *
geom_to_x3d_3(GEOSGeom geom, int precision, int opts, const char *defid)
{
    int type = GEOSGeomTypeId(geom)+1;

    switch (type)
    {
        case wkbPoint_mdb:
            return x3d_3_point(geom, precision, opts);

        case wkbLineString_mdb:
            return x3d_3_line(geom, precision, opts, defid);

        case wkbPolygon_mdb:
            {
                /** We might change this later, but putting a polygon in an indexed face set
                 * seems like the simplest way to go so treat just like a mulitpolygon
                 */
                char *ret;
                GEOSGeom tmp, geoms[1];
                geoms[0] = geom;
                tmp = GEOSGeom_createCollection(GEOS_MULTIPOLYGON, geoms, 1);
                ret = x3d_3_multi(tmp, precision, opts, defid);
                //GEOSGeom_destroy(tmp);
                return ret;
            }

        case wkbTriangle_mdb:
            return x3d_3_triangle(geom, precision, opts, defid);

        case wkbMultiPoint_mdb:
        case wkbMultiLineString_mdb:
        case wkbMultiPolygon_mdb:
            return x3d_3_multi(geom, precision, opts, defid);

        case wkbPolyehdralSurface_mdb:
            return x3d_3_psurface(geom, precision, opts, defid);

        case wkbTin_mdb:
            return x3d_3_tin(geom, precision, opts, defid);

        case wkbGeometryCollection_mdb:
            return x3d_3_psurface(geom, precision, opts, defid);
            //return x3d_3_collection(geom, precision, opts, defid);

        default:
            assert(0);
            return NULL;
    }
}

    static size_t
x3d_3_point_size(GEOSGeom point, int precision)
{
    int size;
    size = geom_X3Dsize(point, precision);
    return size;
}

    static size_t
x3d_3_point_buf(GEOSGeom point, char *output, int precision, int opts)
{
    char *ptr = output;
    ptr += geom_toX3D3(point, ptr, precision, opts, 0);
    return (ptr-output);
}

    static char *
x3d_3_point(GEOSGeom point, int precision, int opts)
{
    char *output;
    int size;

    size = x3d_3_point_size(point, precision);
    output = GDKmalloc(size);
    x3d_3_point_buf(point, output, precision, opts);
    return output;
}


    static size_t
x3d_3_line_size(GEOSGeom line, int precision, int opts, const char *defid)
{
    int size;
    size_t defidlen = strlen(defid);

    size = geom_X3Dsize(line, precision)*2;

    if ( X3D_USE_GEOCOORDS(opts) ) {
        size += (
                sizeof("<LineSet vertexCount=''><GeoCoordinate geoSystem='\"GD\" \"WE\" \"longitude_first\"' point='' /></LineSet>")  + defidlen
                ) * 2;
    }
    else {
        size += (
                sizeof("<LineSet vertexCount=''><Coordinate point='' /></LineSet>")  + defidlen
                ) * 2;
    }

    return size;
}

    static size_t
x3d_3_line_buf(GEOSGeom line, char *output, int precision, int opts, const char *defid)
{
    char *ptr=output;
    uint32_t npoints = 0;
    numPointsGeometry(&npoints, line);

    ptr += sprintf(ptr, "<LineSet %s vertexCount='%d'>", defid, npoints);

    if ( X3D_USE_GEOCOORDS(opts) ) ptr += sprintf(ptr, "<GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ( (opts & GEOM_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
    else
        ptr += sprintf(ptr, "<Coordinate point='");
    ptr += geom_toX3D3(line, ptr, precision, opts, GEOSisClosed(line));

    ptr += sprintf(ptr, "' />");

    ptr += sprintf(ptr, "</LineSet>");
    return (ptr-output);
}

    static size_t
x3d_3_line_coords(GEOSGeom line, char *output, int precision, int opts)
{
    char *ptr=output;
    ptr += geom_toX3D3(line, ptr, precision, opts, GEOSisClosed(line));
    return (ptr-output);
}

    static size_t
x3d_3_mline_coordindex(GEOSGeom mgeom, char *output)
{
    char *ptr=output;
    int i, j, si;
    GEOSGeom geom;
    int ngeoms = GEOSGetNumGeometries(mgeom);

    j = 0;
    for (i=0; i < ngeoms; i++)
    {
        uint32_t k, npoints = 0;
        geom = (GEOSGeom ) GEOSGetGeometryN(mgeom, i);
        numPointsGeometry(&npoints, geom);
        si = j;
        for (k=0; k < npoints ; k++)
        {
            if (k)
            {
                ptr += sprintf(ptr, " ");
            }
            if (!GEOSisClosed(geom) || k < (npoints -1) )
            {
                ptr += sprintf(ptr, "%d", j);
                j += 1;
            }
            else
            {
                ptr += sprintf(ptr,"%d", si);
            }
        }
        if (i < (ngeoms - 1) )
        {
            ptr += sprintf(ptr, " -1 ");
        }
    }
    return (ptr-output);
}

static size_t
x3d_3_mpoly_coordindex(GEOSGeom psur, char *output)
{
    char *ptr=output;
    GEOSGeom geom;
    int i, j, l;
    int ngeoms = GEOSGetNumGeometries(psur);
    j = 0;
    for (i=0; i<ngeoms; i++)
    {
        int nrings;
        geom = (GEOSGeom ) GEOSGetGeometryN(psur, i);
        nrings = GEOSGetNumInteriorRings(geom) + 1;
        for (l=0; l < nrings; l++)
        {
            uint32_t k, npoints = 0;
	        const GEOSGeometry* ring;
            if (!l)
                ring = GEOSGetExteriorRing(geom);
            else
                ring = GEOSGetInteriorRingN(geom, l-1);

            numPointsGeometry(&npoints, ring);

            for (k=0; k < npoints-1 ; k++)
            {
                if (k)
                {
                    ptr += sprintf(ptr, " ");
                }
                ptr += sprintf(ptr, "%d", (j + k));
            }
            j += k;
            if (l < (nrings - 1) )
            {
                ptr += sprintf(ptr, " -1 ");
            }
        }
        if (i < (ngeoms - 1) )
        {
            ptr += sprintf(ptr, " -1 ");
        }
    }
    return (ptr-output);
}

static char *
x3d_3_line(GEOSGeom line, int precision, int opts, const char *defid)
{
    char *output;
    int size;

    size = sizeof("<LineSet><CoordIndex ='' /></LineSet>") + x3d_3_line_size(line, precision, opts, defid);
    output = GDKmalloc(size);
    x3d_3_line_buf(line, output, precision, opts, defid);
    return output;
}

static size_t
x3d_3_poly_size(GEOSGeom poly,  int precision, const char *defid)
{
    size_t size;
    size_t defidlen = strlen(defid);
    int i, nrings = GEOSGetNumInteriorRings(poly)+1;

    size = ( sizeof("<IndexedFaceSet></IndexedFaceSet>") + (defidlen*3) ) * 2 + 6 * (nrings - 1);

    size += geom_X3Dsize((GEOSGeom)GEOSGetExteriorRing(poly), precision);
    for (i=0; i<nrings-1; i++)
        size += geom_X3Dsize((GEOSGeom)GEOSGetInteriorRingN(poly, i), precision);

    return size;
}

static size_t
x3d_3_poly_buf(GEOSGeom poly, char *output, int precision, int opts)
{
    int i, nIntRings = GEOSGetNumInteriorRings(poly);
    char *ptr=output;
    const GEOSGeometry* exteriorRing;
    exteriorRing = GEOSGetExteriorRing(poly);

    ptr += geom_toX3D3((GEOSGeom) exteriorRing, ptr, precision, opts, 1);
    for (i=0; i<nIntRings; i++)
    {
        ptr += sprintf(ptr, " ");
        ptr += geom_toX3D3(GEOSGetInteriorRingN(poly, i), ptr, precision, opts,1);
    }
    return (ptr-output);
}

    static size_t
x3d_3_triangle_size(GEOSGeom triangle, int precision, const char *defid)
{
    size_t size;
    size_t defidlen = strlen(defid);

    /** 6 for the 3 sides and space to separate each side **/
    size = sizeof("<IndexedTriangleSet index=''></IndexedTriangleSet>") + defidlen + 6;
    size += geom_X3Dsize(triangle, precision);

    return size;
}

    static size_t
x3d_3_triangle_buf(GEOSGeom triangle, char *output, int precision, int opts)
{
    char *ptr=output;
    ptr += geom_toX3D3(triangle, ptr, precision, opts, 1);

    return (ptr-output);
}

    static char *
x3d_3_triangle(GEOSGeom triangle, int precision, int opts, const char *defid)
{
    char *output;
    int size;

    size = x3d_3_triangle_size(triangle, precision, defid);
    output = GDKmalloc(size);
    x3d_3_triangle_buf(triangle, output, precision, opts);
    return output;
}


static size_t
x3d_3_multi_size(GEOSGeom col, int precision, int opts, const char *defid)
{
    int i, ngeoms = GEOSGetNumGeometries(col);
    size_t size;
    size_t defidlen = strlen(defid);
    GEOSGeom subgeom;

    if ( X3D_USE_GEOCOORDS(opts) )
        size = sizeof("<PointSet><GeoCoordinate geoSystem='\"GD\" \"WE\" \"longitude_first\"' point='' /></PointSet>");
    else
        size = sizeof("<PointSet><Coordinate point='' /></PointSet>") + defidlen;

    for (i=0; i<ngeoms; i++)
    {
        int type;
        subgeom = (GEOSGeom) GEOSGetGeometryN(col, i);
        type = GEOSGeomTypeId(subgeom)+1;
        if (type == wkbPoint_mdb)
        {
            size += x3d_3_point_size(subgeom, precision);
        }
        else if (type == wkbLineString_mdb)
        {
            size += x3d_3_line_size(subgeom, precision, opts, defid);
        }
        else if (type == wkbPolygon_mdb)
        {
            size += x3d_3_poly_size(subgeom, precision, defid);
        }
    }

    return size;
}

static size_t
x3d_3_multi_buf(GEOSGeom col, char *output, int precision, int opts, const char *defid)
{
    char *ptr, *x3dtype;
    int i;
    int ngeoms;
    int dimension= GEOS_getWKBOutputDims(col);
    int type = GEOSGeomTypeId(col)+1;

    GEOSGeom subgeom;
    ptr = output;
    x3dtype="";


    switch (type)
    {
        case wkbMultiPoint_mdb:
            x3dtype = "PointSet";
            if ( dimension == 2 ){
                x3dtype = "Polypoint2D";   
                ptr += sprintf(ptr, "<%s %s point='", x3dtype, defid);
            }
            else {
                ptr += sprintf(ptr, "<%s %s>", x3dtype, defid);
            }
            break;
        case wkbMultiLineString_mdb:
            x3dtype = "IndexedLineSet";
            ptr += sprintf(ptr, "<%s %s coordIndex='", x3dtype, defid);
            ptr += x3d_3_mline_coordindex((GEOSGeom )col, ptr);
            ptr += sprintf(ptr, "'>");
            break;
        case wkbMultiPolygon_mdb:
            x3dtype = "IndexedFaceSet";
            ptr += sprintf(ptr, "<%s %s convex='false' coordIndex='", x3dtype, defid);
            ptr += x3d_3_mpoly_coordindex((GEOSGeom )col, ptr);
            ptr += sprintf(ptr, "'>");
            break;
        default:
            assert(0);
    }
    if (dimension == 3){
        if ( X3D_USE_GEOCOORDS(opts) ) 
            ptr += sprintf(ptr, "<GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ((opts & GEOM_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
        else
            ptr += sprintf(ptr, "<Coordinate point='");
    }
    ngeoms = GEOSGetNumGeometries(col);
    for (i=0; i<ngeoms; i++)
    {
        int type;
        subgeom =  (GEOSGeom ) GEOSGetGeometryN(col, i);
        type = GEOSGeomTypeId(subgeom)+1;
        if (type == wkbPoint_mdb)
        {
            ptr += x3d_3_point_buf(subgeom, ptr, precision, opts);
            ptr += sprintf(ptr, " ");
        }
        else if (type == wkbLineString_mdb)
        {
            ptr += x3d_3_line_coords(subgeom, ptr, precision, opts);
            ptr += sprintf(ptr, " ");
        }
        else if (type == wkbPolygon_mdb)
        {
            ptr += x3d_3_poly_buf(subgeom, ptr, precision, opts);
            ptr += sprintf(ptr, " ");
        }
    }

    /* Close outmost tag */
    if (dimension == 3){
        ptr += sprintf(ptr, "' /></%s>", x3dtype);
    }
    else { ptr += sprintf(ptr, "' />"); }    
    return (ptr-output);
}

static char *
x3d_3_multi(GEOSGeom col, int precision, int opts, const char *defid)
{
    char *x3d;
    size_t size;

    size = x3d_3_multi_size(col, precision, opts, defid);
    x3d = GDKmalloc(size);
    x3d_3_multi_buf(col, x3d, precision, opts, defid);
    return x3d;
}


    static size_t
x3d_3_psurface_size(GEOSGeom psur, int precision, int opts, const char *defid)
{
    int i, ngeoms = GEOSGetNumGeometries(psur);
    size_t size;
    size_t defidlen = strlen(defid);

    if ( X3D_USE_GEOCOORDS(opts) ) size = sizeof("<IndexedFaceSet convex='false' coordIndex=''><GeoCoordinate geoSystem='\"GD\" \"WE\" \"longitude_first\"' point='' />") + defidlen;
    else size = sizeof("<IndexedFaceSet convex='false' coordIndex=''><Coordinate point='' />") + defidlen;


    for (i=0; i<ngeoms; i++)
    {
        size += x3d_3_poly_size((GEOSGeom) GEOSGetGeometryN(psur, i), precision, defid)*5;
    }

    return size;
}


static size_t
x3d_3_psurface_buf(GEOSGeom psur, char *output, int precision, int opts, const char *defid)
{
    char *ptr;
    int i, ngeoms = GEOSGetNumGeometries(psur);
    int j;
    GEOSGeom geom;
    ptr = output;
    ptr += sprintf(ptr, "<IndexedFaceSet convex='false' %s coordIndex='",defid);

    j = 0;
    for (i=0; i<ngeoms; i++)
    {
        uint32_t k, npoints = 0;
	    const GEOSGeometry* ring;
        geom = (GEOSGeom ) GEOSGetGeometryN(psur, i);
        ring = GEOSGetExteriorRing(geom);
        numPointsGeometry(&npoints, ring);

        for (k=0; k < npoints-1 ; k++)
        {
            if (k)
            {
                ptr += sprintf(ptr, " ");
            }
            ptr += sprintf(ptr, "%d", (j + k));
        }
        if (i < (ngeoms - 1) )
        {
            ptr += sprintf(ptr, " -1 ");
        }
        j += k;
    }

    if ( X3D_USE_GEOCOORDS(opts) ) 
        ptr += sprintf(ptr, "'><GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ( (opts & GEOM_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
    else ptr += sprintf(ptr, "'><Coordinate point='");

    for (i=0; i<ngeoms; i++)
    {
        ptr += x3d_3_poly_buf((GEOSGeom ) GEOSGetGeometryN(psur, i), ptr, precision, opts);
        if (i < (ngeoms - 1) )
        {
            ptr += sprintf(ptr, " ");
        }
    }

    ptr += sprintf(ptr, "' /></IndexedFaceSet>");

    return (ptr-output);
}

static char *
x3d_3_psurface(GEOSGeom psur, int precision, int opts, const char *defid)
{
    char *x3d;
    size_t size;

    size = x3d_3_psurface_size(psur, precision, opts, defid);
    x3d = GDKmalloc(size);
    x3d_3_psurface_buf(psur, x3d, precision, opts, defid);
    return x3d;
}


    static size_t
x3d_3_tin_size(GEOSGeom tin, int precision, const char *defid)
{
    int i, ngeoms = GEOSGetNumGeometries(tin);
    size_t size;
    size_t defidlen = strlen(defid);

    size = sizeof("<IndexedTriangleSet coordIndex=''></IndexedTriangleSet>") + defidlen + ngeoms*12;

    for (i=0; i<ngeoms; i++)
    {
        size += (x3d_3_triangle_size((GEOSGeom ) GEOSGetGeometryN(tin, i), precision, defid) * 20);
    }

    return size;
}


static size_t
x3d_3_tin_buf(GEOSGeom tin, char *output, int precision, int opts, const char *defid)
{
    char *ptr;
    int i, ngeoms = GEOSGetNumGeometries(tin);
    int k;

    ptr = output;

    ptr += sprintf(ptr, "<IndexedTriangleSet %s index='",defid);
    k = 0;
    for (i=0; i<ngeoms; i++)
    {
        ptr += sprintf(ptr, "%d %d %d", k, (k+1), (k+2));
        if (i < (ngeoms - 1) )
        {
            ptr += sprintf(ptr, " ");
        }
        k += 3;
    }

    if ( X3D_USE_GEOCOORDS(opts) ) ptr += sprintf(ptr, "'><GeoCoordinate geoSystem='\"GD\" \"WE\" \"%s\"' point='", ( (opts & GEOM_X3D_FLIP_XY) ? "latitude_first" : "longitude_first") );
    else ptr += sprintf(ptr, "'><Coordinate point='");

    for (i=0; i<ngeoms; i++)
    {
        ptr += x3d_3_triangle_buf((GEOSGeom ) GEOSGetGeometryN(tin, i), ptr, precision,
                opts);
        if (i < (ngeoms - 1) )
        {
            ptr += sprintf(ptr, " ");
        }
    }

    ptr += sprintf(ptr, "'/></IndexedTriangleSet>");

    return (ptr-output);
}

static char *
x3d_3_tin(GEOSGeom tin, int precision, int opts, const char *defid)
{
    char *x3d;
    size_t size;

    size = x3d_3_tin_size(tin, precision, defid);
    x3d = GDKmalloc(size);
    x3d_3_tin_buf(tin, x3d, precision, opts, defid);
    return x3d;
}

#if 0
static size_t
x3d_3_collection_size(GEOSGeom col, int precision, int opts, const char *defid)
{
    int i, ngeoms = GEOSGetNumGeometries(col);
    size_t size;
    size_t defidlen = strlen(defid);
    int type = GEOSGeomTypeId(col)+1;

    size = defidlen*2;
    for (i=0; i<ngeoms; i++)
    {
        GEOSGeom subgeom = (GEOSGeom) GEOSGetGeometryN(col, i);
        size += ( sizeof("<Shape />") + defidlen ) * 2;
        switch (type) {
            case ( wkbPoint_mdb ):
                size += x3d_3_point_size(subgeom, precision);
                break;
            case ( wkbLineString_mdb ):
                size += x3d_3_line_size(subgeom, precision, opts, defid);
                break;
            case ( wkbPolygon_mdb ):
                size += x3d_3_poly_size(subgeom, precision, defid);
                break;
            case ( wkbTin_mdb ):
                size += x3d_3_tin_size(subgeom, precision, defid);
                break;
            case ( wkbPolyehdralSurface_mdb ):
                size += x3d_3_psurface_size(subgeom, precision, opts, defid);
                break;
            case ( wkbGeometryCollection_mdb ):
            case ( wkbMultiPolygon_mdb ):
                size += x3d_3_multi_size(subgeom, precision, opts, defid);
                break;
            default:
                assert(0);
                size += 0;
        }
    }

    return size;
}

static size_t
x3d_3_collection_buf(GEOSGeom col, char *output, int precision, int opts, const char *defid)
{
    char *ptr;
    int i, ngeoms = GEOSGetNumGeometries(col);
    GEOSGeom subgeom;
    int type = GEOSGeomTypeId(col)+1;

    ptr = output;

    for (i=0; i<ngeoms; i++)
    {
        subgeom = (GEOSGeom ) GEOSGetGeometryN(col, i);
        ptr += sprintf(ptr, "<Shape%s>", defid);
        switch (type) {
            case ( wkbPoint_mdb ):
                ptr += x3d_3_point_buf(subgeom, ptr, precision, opts);
                break;
            case ( wkbLineString_mdb ):
                ptr += x3d_3_line_buf(subgeom, ptr, precision, opts, defid);
                break;
            case ( wkbPolygon_mdb ):
                ptr += x3d_3_poly_buf(subgeom, ptr, precision, opts);
                break;
            case ( wkbTin_mdb ):
                ptr += x3d_3_tin_buf(subgeom, ptr, precision, opts,  defid);
                break;
            case ( wkbPolyehdralSurface_mdb ):
                ptr += x3d_3_psurface_buf(subgeom, ptr, precision, opts,  defid);
                break;
            case ( wkbGeometryCollection_mdb ):
                ptr += x3d_3_collection_buf(subgeom, ptr, precision, opts, defid);
                break;
            case wkbMultiPolygon_mdb:
                ptr += x3d_3_multi_buf(subgeom, ptr, precision, opts, defid);
                break;
            default:
                assert(0);
        }

        ptr += printf(ptr, "</Shape>");
    }

    return (ptr-output);
}

static char *
x3d_3_collection(GEOSGeom col, int precision, int opts, const char *defid)
{
    char *x3d;
    size_t size;

    size = x3d_3_collection_size(col, precision, opts, defid);
    x3d = GDKmalloc(size);
    x3d_3_collection_buf(col, x3d, precision, opts, defid);
    return x3d;
}
#endif

static size_t
geom_toX3D3(const GEOSGeometry *geom, char *output, int precision, int opts, int is_closed)
{
    uint32_t i;
    char *ptr;
    char x[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
    char y[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
    char z[OUT_MAX_DIGS_DOUBLE+OUT_MAX_DOUBLE_PRECISION+1];
    uint32_t npoints = 0;
    numPointsGeometry(&npoints, geom);

    ptr = output;

    if ( GEOS_getWKBOutputDims(geom) == 2)
    {
        for (i=0; i<npoints; i++)
        {
            if ( !is_closed || i < (npoints - 1) )
            {
                GEOSGeom point = (GEOSGeom) GEOSGeomGetPointN(geom, i);
                double pt_x, pt_y;
                GEOSGeomGetX(point, &pt_x);
                GEOSGeomGetY(point, &pt_y);

                if (fabs(pt_x) < OUT_MAX_DOUBLE)
                    sprintf(x, "%.*f", precision, pt_x);
                else
                    sprintf(x, "%g", pt_x);
                trim_trailing_zeros(x);

                if (fabs(pt_y) < OUT_MAX_DOUBLE)
                    sprintf(y, "%.*f", precision, pt_y);
                else
                    sprintf(y, "%g", pt_y);
                trim_trailing_zeros(y);

                if ( i )
                    ptr += sprintf(ptr, " ");

                if ( ( opts & GEOM_X3D_FLIP_XY) )
                    ptr += sprintf(ptr, "%s %s", y, x);
                else
                    ptr += sprintf(ptr, "%s %s", x, y);
            }
        }
    }
    else
    {
        for (i=0; i<npoints; i++)
        {
            if ( !is_closed || i < (npoints - 1) )
            {
                GEOSGeom point = (GEOSGeom) GEOSGeomGetPointN(geom, i);
                double pt_x, pt_y, pt_z = 0.0;
                GEOSGeomGetX(point, &pt_x);
                GEOSGeomGetY(point, &pt_y);
                if (GEOSHasZ(point) == 1)
                    GEOSGeomGetZ(point, &pt_z);

                if (fabs(pt_x) < OUT_MAX_DOUBLE)
                    sprintf(x, "%.*f", precision, pt_x);
                else
                    sprintf(x, "%g", pt_x);
                trim_trailing_zeros(x);

                if (fabs(pt_y) < OUT_MAX_DOUBLE)
                    sprintf(y, "%.*f", precision, pt_y);
                else
                    sprintf(y, "%g", pt_y);
                trim_trailing_zeros(y);

                if (fabs(pt_z) < OUT_MAX_DOUBLE)
                    sprintf(z, "%.*f", precision, pt_z);
                else
                    sprintf(z, "%g", pt_z);
                trim_trailing_zeros(z);

                if ( i )
                    ptr += sprintf(ptr, " ");

                if ( ( opts & GEOM_X3D_FLIP_XY) )
                    ptr += sprintf(ptr, "%s %s %s", y, x, z);
                else
                    ptr += sprintf(ptr, "%s %s %s", x, y, z);
            }
        }
    }

    return ptr-output;
}

static size_t
geom_X3Dsize(const GEOSGeometry *geom, int precision)
{
    uint32_t npoints = 0;
    numPointsGeometry(&npoints, geom);

    if (GEOS_getWKBOutputDims(geom) == 2)
        return (OUT_MAX_DIGS_DOUBLE + precision + sizeof(" "))
            * 2 * npoints;

    return (OUT_MAX_DIGS_DOUBLE + precision + sizeof(" ")) * 3 * npoints;
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
