#include "libgeom.h"

/* NULL: generic nil mbr. */
static mbr mbrNIL;		/* to be filled in */

/* MBR atom type functions */
ssize_t mbrTOSTR(char **dst, size_t *len, const void *ATOM, bool external);
ssize_t mbrFROMSTR(const char *src, size_t *len, void **ATOM, bool external);
BUN mbrHASH(const void *ATOM);
const void * mbrNULL(void);
int mbrCOMP(const void *L, const void *R);
void * mbrREAD(void *A, size_t *dstlen, stream *s, size_t cnt);
gdk_return mbrWRITE(const void *C, stream *s, size_t cnt);

/* functions that are used when a column is added to an existing table */
//TODO: What are these used for?
geom_export str mbrFromString(mbr **w, const char **src);
geom_export str mbrFromMBR(mbr **w, mbr **src);

//TODO: What's the difference between these two?
/* gets a GEOSGeometry and returns the mbr of it (only works only for 2D geometries) */
geom_export mbr* mbrFromGeos(const GEOSGeom geosGeometry);
geom_export str wkbMBR(mbr **res, wkb **geom);

//TODO: Check these funcs
geom_export str wkbCoordinateFromWKB(dbl*, wkb**, int*);
geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);
geom_export str wkbBox2D(mbr** box, wkb** point1, wkb** point2);
geom_export str wkbBox2D_bat(bat* outBAT_id, bat *aBAT_id, bat *bBAT_id);

/* MBR relations */
geom_export str mbrOverlaps(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlaps_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrAbove(bit *out, mbr **b1, mbr **b2);
geom_export str mbrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrBelow(bit *out, mbr **b1, mbr **b2);
geom_export str mbrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrLeft(bit *out, mbr **b1, mbr **b2);
geom_export str mbrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrRight(bit *out, mbr **b1, mbr **b2);
geom_export str mbrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrAbove(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrAbove_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrBelow(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrBelow_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrLeft(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrLeft_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrRight(bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrRight_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrContains(bit *out, mbr **b1, mbr **b2);
geom_export str mbrContains_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrContained(bit *out, mbr **b1, mbr **b2);
geom_export str mbrContained_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrEqual(bit *out, mbr **b1, mbr **b2);
geom_export str mbrEqual_wkb(bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrDistance(dbl *out, mbr **b1, mbr **b2);
geom_export str mbrDistance_wkb(dbl *out, wkb **geom1WKB, wkb **geom2WKB);

/* BULK functions */
geom_export str wkbMBR_bat(bat* outBAT_id, bat* inBAT_id);
geom_export str wkbCoordinateFromWKB_bat(bat *outBAT_id, bat *inBAT_id, int* coordinateIdx);
geom_export str wkbCoordinateFromMBR_bat(bat *outBAT_id, bat *inBAT_id, int* coordinateIdx);
