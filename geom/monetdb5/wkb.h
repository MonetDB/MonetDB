#include "libgeom.h"

/* WKB atom type functions */
ssize_t wkbTOSTR(char **geomWKT, size_t *len, const void *GEOMWKB, bool external);
ssize_t wkbFROMSTR(const char *geomWKT, size_t *len, void **GEOMWKB, bool external);
BUN wkbHASH(const void *W);
const void * wkbNULL(void);
int wkbCOMP(const void *L, const void *R);
void * wkbREAD(void *A, size_t *dstlen, stream *s, size_t cnt);
gdk_return wkbWRITE(const void *A, stream *s, size_t cnt);
var_t wkbPUT(BAT *b, var_t *bun, const void *VAL);
void wkbDEL(Heap *h, var_t *index);
size_t wkbLENGTH(const void *P);
gdk_return wkbHEAP(Heap *heap, size_t capacity);
