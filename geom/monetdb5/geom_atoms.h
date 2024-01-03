/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "geom.h"


/* NULL: generic nil mbr. */
/* returns a pointer to a nil-mbr. */
extern mbr mbrNIL;

static const wkb wkb_nil = { ~0, 0 };
static const wkba wkba_nil = {.itemsNum = ~0};

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
/* Non-atom WKB functions */
wkb * wkbNULLcopy(void);
wkb * wkbCopy(const wkb* src);
var_t wkb_size(size_t len);
str wkbFROMSTR_withSRID(const char *geomWKT, size_t *len, wkb **geomWKB, int srid, size_t *nread);

/* MBR atom type functions */
ssize_t mbrTOSTR(char **dst, size_t *len, const void *ATOM, bool external);
ssize_t mbrFROMSTR(const char *src, size_t *len, void **ATOM, bool external);
BUN mbrHASH(const void *ATOM);
const void * mbrNULL(void);
int mbrCOMP(const void *L, const void *R);
void * mbrREAD(void *A, size_t *dstlen, stream *s, size_t cnt);
gdk_return mbrWRITE(const void *C, stream *s, size_t cnt);
/* Non-atom MBR functions */
bool is_mbr_nil(const mbr *m);
/* MBR FUNCTIONS */
geom_export str mbrFromString(mbr **w, const char **src);
geom_export str wkbMBR(mbr **res, wkb **geom);
geom_export str wkbBox2D(mbr** box, wkb** point1, wkb** point2);
geom_export str wkbBox2D_bat(bat* outBAT_id, bat *aBAT_id, bat *bBAT_id);
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
geom_export str mbrDiagonal(dbl *out, mbr **b);
geom_export str mbrDistance(dbl *out, mbr **b1, mbr **b2);
geom_export str mbrDistance_wkb(dbl *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str wkbCoordinateFromWKB(dbl*, wkb**, int*);
geom_export str wkbCoordinateFromMBR(dbl*, mbr**, int*);
geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);

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
/* Non-atom WKBA functions */
var_t wkba_size(int items);
str wkbInteriorRings(wkba **geomArray, wkb **geomWKB);

