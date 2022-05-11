#include "libgeom.h"

static const wkba wkba_nil = {.itemsNum = ~0};

/* WKBA atom type functions */
ssize_t wkbaTOSTR(char **toStr, size_t *len, const void *FROMARRAY, bool external);
ssize_t wkbaFROMSTR(const char *fromStr, size_t *len, void **TOARRAY, bool external);
const void * wkbaNULL(void);
BUN wkbaHASH(const void *WARRAY);
int wkbaCOMP(const void *L, const void *R);
void * wkbaREAD(void *A, size_t *dstlen, stream *s, size_t cnt);
gdk_return wkbaWRITE(const void *A, stream *s, size_t cnt);
var_t wkbaPUT(BAT *b, var_t *bun, const void *VAL);
void wkbaDEL(Heap *h, var_t *index);
size_t wkbaLENGTH(const void *P);
gdk_return wkbaHEAP(Heap *heap, size_t capacity);

/* non-atom type functions */
str wkbInteriorRings(wkba **geomArray, wkb **geomWKB);
