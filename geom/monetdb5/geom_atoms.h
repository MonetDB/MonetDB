/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "geom.h"


/* NULL: generic nil mbr. */
/* returns a pointer to a nil-mbr. */
extern mbr mbrNIL;

static const wkb wkb_nil = { ~0, 0 };

/* WKB atom type functions */
ssize_t wkbTOSTR(allocator *, char **geomWKT, size_t *len, const void *GEOMWKB, bool external);
ssize_t wkbFROMSTR(allocator *, const char *geomWKT, size_t *len, void **GEOMWKB, bool external);
BUN wkbHASH(const void *W);
const void * wkbNULL(void);
int wkbCOMP(const void *L, const void *R);
void * wkbREAD(allocator *ma, void *A, size_t *dstlen, stream *s, size_t cnt);
gdk_return wkbWRITE(const void *A, stream *s, size_t cnt);
var_t wkbPUT(BAT *b, var_t *bun, const void *VAL);
void wkbDEL(Heap *h, var_t *index);
size_t wkbLENGTH(const void *P);
gdk_return wkbHEAP(Heap *heap, size_t capacity);
/* Non-atom WKB functions */
wkb * wkbNULLcopy(allocator *ma);
wkb * wkbCopy(allocator *ma, const wkb* src);
var_t wkb_size(size_t len);
str wkbFROMSTR_withSRID(const char *geomWKT, size_t *len, wkb **geomWKB, int srid, size_t *nread);

/* MBR atom type functions */
ssize_t mbrTOSTR(allocator *, char **dst, size_t *len, const void *ATOM, bool external);
ssize_t mbrFROMSTR(allocator *, const char *src, size_t *len, void **ATOM, bool external);
BUN mbrHASH(const void *ATOM);
const void * mbrNULL(void);
int mbrCOMP(const void *L, const void *R);
void * mbrREAD(allocator *, void *A, size_t *dstlen, stream *s, size_t cnt);
gdk_return mbrWRITE(const void *C, stream *s, size_t cnt);
/* Non-atom MBR functions */
bool is_mbr_nil(const mbr *m);
/* MBR FUNCTIONS */
geom_export str mbrFromString(Client ctx, mbr **w, const char **src);
geom_export str wkbMBR(Client ctx, mbr **res, wkb **geom);
geom_export str wkbBox2D(Client ctx, mbr** box, wkb** point1, wkb** point2);
geom_export str wkbBox2D_bat(Client ctx, bat* outBAT_id, bat *aBAT_id, bat *bBAT_id);
geom_export str mbrOverlaps(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlaps_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrAbove(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrAbove_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrBelow(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrBelow_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrLeft(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrLeft_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrRight(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrRight_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrAbove(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrAbove_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrBelow(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrBelow_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrLeft(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrLeft_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrOverlapOrRight(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrOverlapOrRight_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrContains(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrContains_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrContained(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrContained_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrEqual(Client ctx, bit *out, mbr **b1, mbr **b2);
geom_export str mbrEqual_wkb(Client ctx, bit *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str mbrDiagonal(dbl *out, mbr **b);
geom_export str mbrDistance(Client ctx, dbl *out, mbr **b1, mbr **b2);
geom_export str mbrDistance_wkb(Client ctx, dbl *out, wkb **geom1WKB, wkb **geom2WKB);
geom_export str wkbCoordinateFromWKB(Client ctx, dbl*, wkb**, int*);
geom_export str wkbCoordinateFromMBR(Client ctx, dbl*, mbr**, int*);
geom_export str ordinatesMBR(mbr **res, flt *minX, flt *minY, flt *maxX, flt *maxY);
