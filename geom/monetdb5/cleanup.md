# geom module cleanup
## Structure
### C functions
- **geod.c** -> Geodetic functions
- **wkb.c** -> WKB atom functions
- **wkba.c** -> WKBA atom functions + functions that use WKBA
- **mbr.c** -> MBR atom functions + functions that use WKBA
- **geom_srid.c** -> Projection functions + SRID functions
- **geomBulk.c** -> Bulk functions
- **geom_io.c** -> Geometry input/output functions

### SQL
- **40_geom.sql** -> Geodetic + mbr funtcions -> TODO Cleanup
- **40_geom_OGC.sql** -> OGC Simple Features functions
- **40_geom_PostGIS.sql** -> PostGIS functions

## Changes
### Removed C functions:
- geometryHasZ/geoHasZ
- geometryHasM/geoHasM
- geoGetType

### Removed MAL functions:
- geom.hasZ
- geom.hasM
- geom.getType

### Removed SQL functions:
- HasZ
- HasM
- get_type
- wkbIsnil (no usages, libgeom.c has *is_wkb_nil*)
- ST_M
- ST_CurveToLine
- Functions that were commented out:
    - ST_GeomFromWKB (and similar geometry-specific functions)
    - ST_GeomFromText (and similar geometry-specific functions)
    - ST_SetInteriorRings

### Moved:
- Moved spatial_ref_sys and geometry_columns tables to **spatial_ref_sys.sql**
- Moved COPY INTO spatial_ref_sys to **spatial_ref_sys.sql**
- Moved Geodetic functions (except bulk versions) to **geod.c** (MAL functions still on geom.c)
- Moved Geodetic headers to **geod.h** 
- Moved MBR functions (including atom functions) to **mbr.c** (MAL functions still on geom.c)
- Moved MBR headers to **mbr.h**
- Moved WKBA functions to **wkba.c** (MAL functions still on geom.c)
- Moved WKBA headers to **wkba.h**
- Merged *geoGetType* into *wkbGeometryType* (geoGetType wasn't being used outside of this func)
- Moved WKB atom functions to **wkb.c**
- OGC sql functions to **40_geom_OGC.sql**
- PostGIS sql functions to **40_geom_PostGIS.sql**
- Moved geos2wkb C function to **libgeom.c** (is used in a lot of modules)


### Other changes:
- **MBR** atom functions are now on the header file (mbr.h) and are *not static*
- **WKBA** atom functions are now on the header file (wkba.h) and are *not static*
- **WKB** atom functions are now on the header file (wkb.h) and are *not static*

## TODO:
- Remove libgeom.c/.h ? (or add a file with the start up and end functions + MAL ones?)
- Clean mbr.c
- Clean wkba.c
- Clean geomBulk.c (and change name?)
- Clean geom_srid.c
- Clean geod.c
- Clean geom_io.c
- Should we allow z and m in constructor functions? (e.g. ST_MakePoint)
- SQL Clean:
    - Functions on Polyhedral Surfaces
    - Management functions
    - PostGIS Geometry Constructors
    - PostGIS Geometry Editors
    - PostGIS SQL functions in general
    - PostGIS from "Operators" onwards (most aren't implemented)

## Check with Stefanos
- Changing atom functions to other files (mbr and wkba)
- Should we keep wkba (and even mbr?)
- Header include standards (everything on the .h, some includes in the .c?)
- How to remove libgeom
- Check the SQL division
