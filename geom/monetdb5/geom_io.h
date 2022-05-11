#include "libgeom.h"

/* Input functions (from non-geom type to geom) */
/* FromText
   FromText functions for a particular geometry type are in the MAL functions of geom.c */
//TODO: Either move the declarations of the bulk functions or move the bulk functions to the c file
geom_export str wkbFromText(wkb **geomWKB, str *geomWKT, int* srid, int *tpe);
geom_export str wkbFromText_bat(bat *outBAT_id, bat *inBAT_id, int *srid, int *tpe);
geom_export str wkbFromText_bat_cand(bat *outBAT_id, bat *inBAT_id, bat *cand, int *srid, int *tpe);
geom_export str wkbFROMSTR_withSRID(const char *geomWKT, size_t *len, wkb **geomWKB, int srid, size_t *nread);

/* FromBinary */
geom_export str wkbFromBinary(wkb**, const char**);

/* Output functions (from geom to non-geom type) */
/* AsText */
///TODO: Either move the declarations of the bulk functions or move the bulk functions to the c file
geom_export str wkbAsText(char **outTXT, wkb **inWKB, int *withSRID);
geom_export str wkbAsText_bat(bat *inBAT_id, bat *outBAT_id, int *withSRID);

/* AsBinary */
geom_export str wkbAsBinary(char**, wkb**);
