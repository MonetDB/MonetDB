/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a Kostis Kyzirakos
 * @* SFCGAL-based functionality for the geom module
 */

#include "sfcgal.h"

static sfcgal_geometry_t* sfcgal_from_geom(str *ret, const GEOSGeometry *geom, int type);
static str geom_to_sfcgal(sfcgal_geometry_t **res, const GEOSGeometry *geosGeometry);
static str sfcgal_to_geom(GEOSGeom *res, const sfcgal_geometry_t* geom, int force3D, int srid, int flag);

static str
sfcgal_type_to_geom_type(int *res, sfcgal_geometry_type_t type)
{
    str ret = MAL_SUCCEED;
    switch (type)
    {
        case SFCGAL_TYPE_POINT:
            *res = wkbPoint_mdb;
            break;

        case SFCGAL_TYPE_LINESTRING:
            *res = wkbLineString_mdb;
            break;

        case SFCGAL_TYPE_POLYGON:
            *res = wkbPolygon_mdb;
            break;

        case SFCGAL_TYPE_MULTIPOINT:
            *res = wkbMultiPoint_mdb;
            break;

        case SFCGAL_TYPE_MULTILINESTRING:
            *res = wkbMultiLineString_mdb;
            break;

        case SFCGAL_TYPE_MULTIPOLYGON:
            *res = wkbMultiPolygon_mdb;
            break;

        case SFCGAL_TYPE_MULTISOLID:
        case SFCGAL_TYPE_GEOMETRYCOLLECTION:
        case SFCGAL_TYPE_POLYHEDRALSURFACE:
        case SFCGAL_TYPE_SOLID:
            //*res = wkbPolyehdralSurface_mdb;
            *res = wkbGeometryCollection_mdb;
            break;

        case SFCGAL_TYPE_TRIANGULATEDSURFACE:
        case SFCGAL_TYPE_TRIANGLE:
            *res = wkbGeometryCollection_mdb;
            //*res = wkbTin_mdb;
            break;

        default:
            *res = -1;
            throw(MAL, "cgal2geom", "Unknown sfcgal geometry type");
    }

    return ret;
}

static str
ring_from_sfcgal(GEOSGeom *res, const sfcgal_geometry_t* geom, int want3d)
{
    uint32_t i, npoints;
    double point_x, point_y, point_z;
    str ret = MAL_SUCCEED;
    GEOSGeom outGeometry;
    GEOSCoordSeq outCoordSeq;

    assert(geom);
    npoints = sfcgal_linestring_num_points(geom);
    //create the coordSeq for the new geometry
    if (!(outCoordSeq = GEOSCoordSeq_create(npoints, want3d ? 3 : 2))) {
        *res = NULL;
        throw(MAL, "ring_from_sfcgal", "GEOSCoordSeq_create failed");
    }

    for (i = 0; i < npoints; i++)
    {
        const sfcgal_geometry_t* pt = sfcgal_linestring_point_n(geom, i);
        point_x = sfcgal_point_x(pt);
        point_y = sfcgal_point_y(pt);
        GEOSCoordSeq_setX(outCoordSeq, i, point_x);
        GEOSCoordSeq_setY(outCoordSeq, i, point_y);

        if (sfcgal_geometry_is_3d(geom)) {
            point_z = sfcgal_point_z(pt);
            GEOSCoordSeq_setZ(outCoordSeq, i, point_z);
        } else if (want3d) {
            point_z = 0.0;
            GEOSCoordSeq_setZ(outCoordSeq, i, point_z);
        }
    }

    if (!(outGeometry = GEOSGeom_createLinearRing(outCoordSeq))) {
        *res = NULL;
        throw(MAL, "ring_from_sfcgal", "GEOSGeom_createLinearRing failed");
    }
    *res = outGeometry;
    return ret;
}

str
sfcgal_to_geom(GEOSGeom *res, const sfcgal_geometry_t* geom, int force3D, int srid, int flag)
{
    uint32_t ngeoms, nshells, npoints;
    double point_x, point_y, point_z;
    uint32_t i, j, k, nrings;
    int type, want3d;
    str ret = MAL_SUCCEED;
    GEOSGeom* geoms = NULL;
    GEOSGeom outGeometry;
    GEOSCoordSeq outCoordSeq;
    GEOSGeom externalRing, *internalRings;

    assert(geom);

    want3d = force3D || sfcgal_geometry_is_3d(geom);

    switch (sfcgal_geometry_type_id(geom))
    {
        case SFCGAL_TYPE_POINT:
            if (sfcgal_geometry_is_empty(geom)) {
                //TODO: How to build an empty GEOSGeom??
                *res = NULL;
                break;
            }

            if (!(outCoordSeq = GEOSCoordSeq_create(1, want3d ? 3 : 2))) {
                *res = NULL;
                throw(MAL, "sfcgal_to_geom", "GEOSCoordSeq_create failed");
            }

            point_x = sfcgal_point_x(geom);
            point_y = sfcgal_point_y(geom);

	        GEOSCoordSeq_setOrdinate(outCoordSeq, 0, 0, point_x);
        	GEOSCoordSeq_setOrdinate(outCoordSeq, 0, 1, point_y);
            
            if (sfcgal_geometry_is_3d(geom)) {
                point_z = sfcgal_point_z(geom);
        	    GEOSCoordSeq_setOrdinate(outCoordSeq, 0, 2, point_z);
            } else if (want3d) {
                point_z = 0.0;
        	    GEOSCoordSeq_setOrdinate(outCoordSeq, 0, 2, point_z);
            }
            
            if (!(*res = GEOSGeom_createPoint(outCoordSeq))) {
                throw(MAL, "sfcgal_to_geom", "Failed to create GEOSGeometry from the coordinates");
            }
            break;
        case SFCGAL_TYPE_LINESTRING:
            if (sfcgal_geometry_is_empty(geom)) {
                //TODO: How to build an empty GEOSGeom??
                *res = NULL;
                break;
            }

            npoints = sfcgal_linestring_num_points(geom);
            //create the coordSeq for the new geometry
            if (!(outCoordSeq = GEOSCoordSeq_create(npoints, want3d ? 3 : 2))) {
                *res = NULL;
                throw(MAL, "sfcgal_to_geom", "GEOSCoordSeq_create failed");
            }

            for (i = 0; i < npoints; i++)
            {
                const sfcgal_geometry_t* pt = sfcgal_linestring_point_n(geom, i);
                point_x = sfcgal_point_x(pt);
                point_y = sfcgal_point_y(pt);
                GEOSCoordSeq_setX(outCoordSeq, i, point_x);
                GEOSCoordSeq_setY(outCoordSeq, i, point_y);

                if (sfcgal_geometry_is_3d(geom)) {
                    point_z = sfcgal_point_z(pt);
                    GEOSCoordSeq_setZ(outCoordSeq, i, point_z);
                } else if (want3d) {
                    point_z = 0.0;
                    GEOSCoordSeq_setZ(outCoordSeq, i, point_z);
                }
            }

            if (!(outGeometry = GEOSGeom_createLineString(outCoordSeq))) {
                *res = NULL;
                throw(MAL, "sfcgal_to_geom", "GEOSGeom_createLineString failed");
            }
            *res = outGeometry;
            break;
        case SFCGAL_TYPE_TRIANGLE:

            if (sfcgal_geometry_is_empty(geom)) {
                //TODO: How to build an empty GEOSGeom??
                *res = NULL;
                break;
            }
            
            npoints = 4;
            //create the coordSeq for the new geometry
            if (!(outCoordSeq = GEOSCoordSeq_create(npoints, want3d ? 3 : 2))) {
                *res = NULL;
                throw(MAL, "sfcgal_to_geom", "GEOSCoordSeq_create failed");
            }

            for (i = 0; i < 4; i++)
            {
                const sfcgal_geometry_t* pt = sfcgal_triangle_vertex(geom, (i%3));
                point_x = sfcgal_point_x(pt);
                point_y = sfcgal_point_y(pt);
	            GEOSCoordSeq_setOrdinate(outCoordSeq, i, 0, point_x);
	            GEOSCoordSeq_setOrdinate(outCoordSeq, i, 1, point_y);

                if ( sfcgal_geometry_is_3d(geom)) {
                    point_z = sfcgal_point_z(pt);
	                GEOSCoordSeq_setOrdinate(outCoordSeq, i, 2, point_z);
                } else if (want3d) {
                    point_z = 0.0;
	                GEOSCoordSeq_setOrdinate(outCoordSeq, i, 2, point_z);
                }

            }

            //Collection of Polygons
            if (flag == 0) {
                if (!(outGeometry = GEOSGeom_createLinearRing(outCoordSeq))) {
                    *res = NULL;
                    throw(MAL, "sfcgal_to_geom", "GEOSGeom_createLineString failed");
                }
                *res = GEOSGeom_createPolygon(outGeometry, NULL, 0);
            }

            //Collection of MultiStrings
            if (flag == 1) {
                if (!(outGeometry = GEOSGeom_createLineString(outCoordSeq))) {
                    *res = NULL;
                    throw(MAL, "sfcgal_to_geom", "GEOSGeom_createLineString failed");
                }
                *res = outGeometry;
            }
            
            //TIN
            if (flag == 2) {
                    *res = NULL;
                    throw(MAL, "sfcgal_to_geom", "TIN format is not yet supported");
            }

            break;
        case SFCGAL_TYPE_POLYGON:
            if (sfcgal_geometry_is_empty(geom)) {
                //TODO: How to build an empty GEOSGeom??
                *res = NULL;
                break;
            }

            nrings = sfcgal_polygon_num_interior_rings(geom) + 1;
            internalRings = (GEOSGeom*) GDKmalloc(sizeof(GEOSGeom) * nrings);
            if ( (ret = ring_from_sfcgal(&externalRing, sfcgal_polygon_exterior_ring(geom), want3d)) != MAL_SUCCEED) {
                //TODO: free stuff
                return ret;
            }

            for (i = 1; i < nrings; i++)
                if ( (ret = ring_from_sfcgal(&(internalRings[i-1]), sfcgal_polygon_interior_ring_n(geom, i-1), want3d)) != MAL_SUCCEED ) {
                    //TODO: free stuff
                    return ret;
                }

            *res = GEOSGeom_createPolygon(externalRing, internalRings, nrings-1);
            break;
        case SFCGAL_TYPE_MULTIPOINT:
        case SFCGAL_TYPE_MULTILINESTRING:
        case SFCGAL_TYPE_MULTIPOLYGON:
        case SFCGAL_TYPE_GEOMETRYCOLLECTION:
            ngeoms = sfcgal_geometry_collection_num_geometries(geom);
            if (ngeoms)
            {
                geoms = (GEOSGeom*) GDKmalloc(sizeof(GEOSGeom) * ngeoms);
                for (i = 0; i < ngeoms; i++)
                {
                    const sfcgal_geometry_t* g = sfcgal_geometry_collection_geometry_n(geom, i);
                    if ( (ret = sfcgal_to_geom(&geoms[i], g, 0, srid, flag)) != MAL_SUCCEED) {
                        //TODO: free what was allocated
                        *res = NULL;
                        return ret;
                    }
                }
            }

            if ( (ret = sfcgal_type_to_geom_type(&type, sfcgal_geometry_type_id(geom))) != MAL_SUCCEED) {
                //TODO: free what was allocated
                *res = NULL;
                return ret;
            }
            *res = GEOSGeom_createCollection(type-1, geoms, ngeoms);
            break;
        case SFCGAL_TYPE_POLYHEDRALSURFACE:
            ngeoms = sfcgal_polyhedral_surface_num_polygons(geom);
            if (ngeoms)
            {
                geoms = (GEOSGeom*) GDKmalloc(sizeof(GEOSGeom) * ngeoms);
                for (i = 0; i < ngeoms; i++)
                {
                    const sfcgal_geometry_t* g = sfcgal_polyhedral_surface_polygon_n( geom, i );
                    if ( (ret = sfcgal_to_geom(&geoms[i], g, 0, srid, flag)) != MAL_SUCCEED) {
                        //TODO: free what was allocated
                        *res = NULL;
                        return ret;
                    }
                }
            }
            if ( (ret = sfcgal_type_to_geom_type(&type, sfcgal_geometry_type_id(geom))) != MAL_SUCCEED) {
                //TODO: free what was allocated
                *res = NULL;
                return ret;
            }
            *res = GEOSGeom_createCollection(type-1, geoms, ngeoms);
            break;
            /* Solid is map as a closed PolyhedralSurface (for now) */
        case SFCGAL_TYPE_SOLID:
            nshells = sfcgal_solid_num_shells(geom);
            for (ngeoms = 0, i = 0; i < nshells; i++)
                ngeoms += sfcgal_polyhedral_surface_num_polygons(sfcgal_solid_shell_n(geom, i));
            if (ngeoms)
            {
                geoms = (GEOSGeom*) GDKmalloc( sizeof(GEOSGeom) * ngeoms);
                for (i = 0, k =0 ; i < nshells; i++)
                {
                    const sfcgal_geometry_t* shell = sfcgal_solid_shell_n(geom, i);
                    ngeoms = sfcgal_polyhedral_surface_num_polygons(shell);

                    for (j = 0; j < ngeoms; j++)
                    {
                        const sfcgal_geometry_t* g = sfcgal_polyhedral_surface_polygon_n(shell, j);
                        if ( (ret = sfcgal_to_geom(&geoms[k], g, 0, srid, flag)) != MAL_SUCCEED) {
                            //TODO: free what was allocated
                            *res = NULL;
                            return ret;
                        }
                        k++;
                    }
                }
            }
            if ( (ret = sfcgal_type_to_geom_type(&type, sfcgal_geometry_type_id(geom))) != MAL_SUCCEED) {
                //TODO: free what was allocated
                *res = NULL;
                return ret;
            }
            *res = GEOSGeom_createCollection(type-1, geoms, ngeoms);
            //TODO: if (ngeoms) FLAGS_SET_SOLID( rgeom->flags, 1);
            break;
        case SFCGAL_TYPE_TRIANGULATEDSURFACE:
            ngeoms = sfcgal_triangulated_surface_num_triangles(geom);
            if (ngeoms)
            {
                geoms = (GEOSGeom*) GDKmalloc(sizeof(GEOSGeom) * ngeoms);
                for (i = 0; i < ngeoms; i++)
                {
                    const sfcgal_geometry_t* g = sfcgal_triangulated_surface_triangle_n(geom, i);
                    if ( (ret = sfcgal_to_geom(&geoms[i], g, 0, srid, flag)) != MAL_SUCCEED) {
                        //TODO: free what was allocated
                        *res = NULL;
                        return ret;
                    }
                }
            }
            if ( (ret = sfcgal_type_to_geom_type(&type, sfcgal_geometry_type_id(geom))) != MAL_SUCCEED) {
                //TODO: free what was allocated
                *res = NULL;
                return ret;
            }
            *res = GEOSGeom_createCollection(type-1, geoms, ngeoms);
            break;
            //Unsupported types.
        case SFCGAL_TYPE_MULTISOLID:
            ret = createException(MAL, "sfcgal_to_geom", "Unsupported sfcgal geometry type");
            *res = NULL;
            break;
        default:
            ret = createException(MAL, "sfcgal_to_geom", "Unknown sfcgal geometry type");
            *res = NULL;
    }

	//GEOSSetSRID(*res, srid);
    return ret;
}

static sfcgal_geometry_t *
sfcgal_from_geom(str *ret, const GEOSGeometry *geom, int type)
{
    int is_3d;
    double point_x = 0.0, point_y = 0.0, point_z = 0.0;
    int i;
    *ret = MAL_SUCCEED;

    is_3d = GEOS_getWKBOutputDims(geom) == 3;

    switch (type)
    {
        case wkbPoint_mdb:
            {
                GEOSGeomGetX(geom, &point_x);
                GEOSGeomGetY(geom, &point_y);
                if (is_3d) {
                    GEOSGeomGetZ(geom, &point_z);
                    return sfcgal_point_create_from_xyz(point_x, point_y, point_z);
                } else
                    return sfcgal_point_create_from_xy(point_x, point_y);
            }
            break;

        case wkbLineString_mdb:
            {
                sfcgal_geometry_t* line = sfcgal_linestring_create();
                int numPoints = GEOSGeomGetNumPoints(geom);
                for (i = 0; i < numPoints; i++)
                {
                    GEOSGeom pointG = GEOSGeomGetPointN(geom, i);
                    GEOSGeomGetX(pointG, &point_x);
                    GEOSGeomGetY(pointG, &point_y);
                    if (is_3d)
                    {
                        GEOSGeomGetZ(pointG, &point_z);
                        sfcgal_linestring_add_point(line,
                                sfcgal_point_create_from_xyz(point_x, point_y, point_z));
                    }
                    else
                    {
                        sfcgal_linestring_add_point(line,
                                sfcgal_point_create_from_xy(point_x, point_y));
                    }
                }

                return line;
            }
            break;
        case wkbPolygon_mdb:
            {
                sfcgal_geometry_t* line = sfcgal_linestring_create();
	            const GEOSCoordSequence *gcs_old;
                uint32_t numPoints, j;

                /* get the coordinates of the points comprising the geometry */
                gcs_old = GEOSGeom_getCoordSeq(geom);

                /* get the number of points in the geometry */
                GEOSCoordSeq_getSize(gcs_old, &numPoints);

                for (j = 0; j < numPoints; j++)
                {
	                GEOSCoordSeq_getX(gcs_old, j, &point_x);
	                GEOSCoordSeq_getY(gcs_old, j, &point_y);
                    if (is_3d)
                    {
	                    GEOSCoordSeq_getZ(gcs_old, j, &point_z);
                        sfcgal_linestring_add_point(line,
                                sfcgal_point_create_from_xyz(point_x, point_y, point_z));
                    }
                    else
                    {
                        sfcgal_linestring_add_point(line,
                                sfcgal_point_create_from_xy(point_x, point_y));
                    }
                }

                return line;
            }
            break;
        case wkbTriangle_mdb:
            {
                GEOSGeometry* pointG;
                sfcgal_geometry_t* triangle = sfcgal_triangle_create();

                pointG = GEOSGeomGetPointN(geom, 0);
                GEOSGeomGetX(pointG, &point_x);
                GEOSGeomGetY(pointG, &point_y);
                if (is_3d){
                    GEOSGeomGetZ(pointG, &point_z);
                    sfcgal_triangle_set_vertex_from_xyz(triangle, 0, point_x, point_y, point_z);
                } else
                    sfcgal_triangle_set_vertex_from_xy (triangle, 0, point_x, point_y);

                pointG = GEOSGeomGetPointN(geom, 1);
                GEOSGeomGetX(pointG, &point_x);
                GEOSGeomGetY(pointG, &point_y);
                if (is_3d){
                    GEOSGeomGetZ(pointG, &point_z);
                    sfcgal_triangle_set_vertex_from_xyz(triangle, 1, point_x, point_y, point_z);
                } else
                    sfcgal_triangle_set_vertex_from_xy (triangle, 1, point_x, point_y);


                pointG = GEOSGeomGetPointN(geom, 2);
                GEOSGeomGetX(pointG, &point_x);
                GEOSGeomGetY(pointG, &point_y);
                if (is_3d){
                    GEOSGeomGetZ(pointG, &point_z);
                    sfcgal_triangle_set_vertex_from_xyz(triangle, 2, point_x, point_y, point_z);
                } else
                    sfcgal_triangle_set_vertex_from_xy (triangle, 2, point_x, point_y);

                return triangle;
            }
            break;

        default:
            *ret = createException(MAL, "geom_to_sfcgal", "Unknown geometry type");
            return NULL;
    }
}






static str
geom_to_sfcgal(sfcgal_geometry_t **res, const GEOSGeometry *geosGeometry)
{
    int i, numGeometries = 0;
    int type = GEOSGeomTypeId(geosGeometry)+1;
    sfcgal_geometry_t* ret_geom = NULL;
    str ret = MAL_SUCCEED;

    switch (type)
    {
        case wkbPoint_mdb:
            {
                if (GEOSisEmpty(geosGeometry) == 1) {
                    *res = sfcgal_point_create();
                    break;
                }
                *res = sfcgal_from_geom(&ret, geosGeometry, wkbPoint_mdb);
            }
            break;

        case wkbLineString_mdb:
            {
                if (GEOSisEmpty(geosGeometry) == 1) {
                    *res = sfcgal_linestring_create();
                    break;
                }
                *res = sfcgal_from_geom(&ret, geosGeometry, wkbLineString_mdb);
            }
            break;

        case wkbTriangle_mdb:
            {
                if (GEOSisEmpty(geosGeometry) == 1) {
                    res = sfcgal_triangle_create();
                    break;
                }
                *res = sfcgal_from_geom(&ret, geosGeometry, wkbTriangle_mdb);
            }
            break;

        case wkbPolygon_mdb:
            {
                int numInteriorRings = GEOSGetNumInteriorRings(geosGeometry);
	            const GEOSGeometry *extRing = NULL;
                sfcgal_geometry_t* exterior_ring;

                if (GEOSisEmpty(geosGeometry) == 1) {
                    *res = sfcgal_polygon_create();
                    break;
                }

                extRing = GEOSGetExteriorRing(geosGeometry);
                exterior_ring = sfcgal_from_geom(&ret, extRing, wkbPolygon_mdb);
                ret_geom = sfcgal_polygon_create_from_exterior_ring(exterior_ring);

                for (i = 0; i < numInteriorRings; i++)
                {
                    sfcgal_geometry_t* ring = sfcgal_from_geom(&ret, *(GEOSGeom*)GEOSGetInteriorRingN(geosGeometry, i), wkbLineString_mdb);
                    sfcgal_polygon_add_interior_ring(ret_geom, ring);
                }
                *res = ret_geom;
            }
            break;

        case wkbMultiPoint_mdb:
        case wkbMultiLineString_mdb:
        case wkbMultiPolygon_mdb:
        case wkbGeometryCollection_mdb:
            {
                if (type == wkbMultiPoint_mdb)
                    ret_geom = sfcgal_multi_point_create();
                else if (type == wkbMultiLineString_mdb)
                    ret_geom = sfcgal_multi_linestring_create();
                else if (type == wkbMultiPolygon_mdb)
                    ret_geom = sfcgal_multi_polygon_create();
                else
                    ret_geom = sfcgal_geometry_collection_create();

                numGeometries = GEOSGetNumGeometries(geosGeometry);
                for (i = 0; i < numGeometries; i++)
                {
                    sfcgal_geometry_t *g;
                    ret = geom_to_sfcgal(&g, GEOSGetGeometryN(geosGeometry, i));
                    sfcgal_geometry_collection_add_geometry(ret_geom, g);
                }
                *res = ret_geom;
            }
            break;

        case wkbPolyehdralSurface_mdb:
            {
                ret_geom = sfcgal_polyhedral_surface_create();
                numGeometries = GEOSGetNumGeometries(geosGeometry);
                for (i = 0; i < numGeometries; i++)
                {
                    sfcgal_geometry_t* g;
                    ret = geom_to_sfcgal(&g, GEOSGetGeometryN(geosGeometry, i));
                    sfcgal_polyhedral_surface_add_polygon(ret_geom, g);
                }
                /*
                 * TODO: Fix this part
                 if (FLAGS_GET_SOLID(lwp->flags))
                 {
                 *res = sfcgal_solid_create_from_exterior_shell(ret_geom);
                 break;
                 }
                 */

                *res = ret_geom;
            }
            break;

        case wkbTin_mdb:
            {
                ret_geom = sfcgal_triangulated_surface_create();

                numGeometries = GEOSGetNumGeometries(geosGeometry);
                for (i = 0; i < numGeometries; i++)
                {
                    sfcgal_geometry_t* g;
                    ret = geom_to_sfcgal(&g, GEOSGetGeometryN(geosGeometry, i));
                    sfcgal_triangulated_surface_add_triangle(ret_geom, g);
                }

                *res = ret_geom;
            }
            break;

        default:
            ret = createException(MAL, "geom2cgal", "Unknown geometry type");
            *res = NULL;
    }

    return ret;
}

char *
geom_sfcgal_version(char **ret)
{
	*ret = GDKstrdup(sfcgal_version());

	return MAL_SUCCEED;
}

/* SFCGDAL functionality */
str
geom_sfcgal_extrude(wkb **res, wkb **geom, double *ex, double *ey, double *ez)
{
	sfcgal_geometry_t *outGeom, *inGeom;
	GEOSGeom inGeos = wkb2geos(*geom), outGeos;
    int srid;

    if (wkbGetSRID(&srid, geom) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Tesselate", "GEOSTesselate failed");
    }

	if (geom_to_sfcgal(&inGeom, inGeos) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Extrude", "GEOSExtrude failed");
	}
	if (!(outGeom = sfcgal_geometry_extrude (inGeom, *ex, *ey, *ez))) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Extrude", "GEOSExtrude failed");
	}

    if ( sfcgal_to_geom(&outGeos, outGeom, 0, srid, 0) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Extrude", "GEOSExtrude failed");
    }

	*res = geos2wkb(outGeos);
	return MAL_SUCCEED;
}

str
geom_sfcgal_straightSkeleton(wkb **res, wkb **geom)
{
	sfcgal_geometry_t *outGeom, *inGeom;
	GEOSGeom inGeos = wkb2geos(*geom), outGeos;
    int srid;

    if (wkbGetSRID(&srid, geom) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Tesselate", "GEOSTesselate failed");
    }

	if (geom_to_sfcgal(&inGeom, inGeos) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.StraightSkeleton", "GEOSStraightSkeleton failed");
	}
	if (!(outGeom = sfcgal_geometry_straight_skeleton(inGeom))) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.StraightSkeleton", "GEOSStraightSkeleton failed");
	}

    if ( sfcgal_to_geom(&outGeos, outGeom, 0, srid, 0) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Extrude", "GEOSExtrude failed");
    }

	*res = geos2wkb(outGeos);
	return MAL_SUCCEED;
}

str
geom_sfcgal_tesselate(wkb **res, wkb **geom)
{
	sfcgal_geometry_t *outGeom, *inGeom;
	GEOSGeom inGeos = wkb2geos(*geom), outGeos;
    int srid;

    if (wkbGetSRID(&srid, geom) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Tesselate", "GEOSTesselate failed");
    }

	if (geom_to_sfcgal(&inGeom, inGeos) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Tesselate", "GEOSTesselate failed");
	}
	if (!(outGeom = sfcgal_geometry_tesselate(inGeom))) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Tesselate", "GEOSTesselate failed");
	}

    if ( sfcgal_to_geom(&outGeos, outGeom, 0, srid, 0) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Tesselate", "GEOSTesselate failed");
    }

	*res = geos2wkb(outGeos);
	return MAL_SUCCEED;
}

str
geom_sfcgal_triangulate2DZ(wkb **res, wkb **geom, int *flag)
{
	sfcgal_geometry_t *outGeom, *inGeom;
	GEOSGeom inGeos = wkb2geos(*geom), outGeos;
    int srid;

    if (wkbGetSRID(&srid, geom) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Triangulate2DZ", "GEOSTriangulate2DZ failed");
    }

	if (geom_to_sfcgal(&inGeom, inGeos) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Triangulate2DZ", "GEOSTriangulate2DZ failed");
	}

    //TODO
	if (!(outGeom = sfcgal_geometry_triangulate_2dz(inGeom))) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Tesselate", "GEOSTesselate failed");
	}


    if ( sfcgal_to_geom(&outGeos, outGeom, 0, srid, *flag) != MAL_SUCCEED) {
		*res = NULL;
        //TODO: free ret
		return createException(MAL, "geom.Triangulate2DZ", "GEOSTriangulate2DZ failed");
    }

	*res = geos2wkb(outGeos);
	return MAL_SUCCEED;
}
