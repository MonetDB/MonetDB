## Clean up of geom module
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

### Moved:
- Moved spatial_ref_sys and geometry_columns tables to **spatial_ref_sys.sql**
- Moved COPY INTO spatial_ref_sys to **spatial_ref_sys.sql**
- Moved Geodetic functions (except bulk versions) to **geod.c** (MAL functions still on geom.c)
- Moved Geodetic headers to **geod.h** 
