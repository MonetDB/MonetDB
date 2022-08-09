#ifndef SIZEOF_RTREE_COORD_T
#define SIZEOF_RTREE_COORD_T 4
#endif
#include <rtree.h>

typedef struct mbr_t {
	float xmin;
	float ymin;
	float xmax;
	float ymax;

} mbr_t;
//TODO REMOVE

gdk_export gdk_return BATrtree(BAT *b);
gdk_export gdk_return BATrtree_wkb(BAT *wkb, BAT* mbr);
gdk_export void RTREEdestroy(BAT *b);
gdk_export BUN* RTREEsearch(BAT *b, mbr_t *inMBR, int result_limit);
