#ifndef SIZEOF_RTREE_COORD_T
#define SIZEOF_RTREE_COORD_T 4
#endif
#include <rtree.h>

//TODO REMOVE
typedef struct mbr_t {
	float xmin;
	float ymin;
	float xmax;
	float ymax;

} mbr_t;

gdk_export bool RTREEexists(BAT *b);
gdk_export gdk_return BATrtree(BAT *wkb, BAT* mbr);
gdk_export void RTREEdestroy(BAT *b);
gdk_export BUN* RTREEsearch(BAT *b, mbr_t *inMBR, int result_limit);
