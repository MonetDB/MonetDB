/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a Romulo Goncalves
 * @* The simple geom module
 */

#include "gpu.h"

static str
GpnpolyWithHoles(bat *out, int nvert, dbl *vx, dbl *vy, int nholes, dbl **hx, dbl **hy, int *hn, bat *point_x, bat *point_y)
{
	BAT *bo = NULL, *bpx = NULL, *bpy;
	dbl *px = NULL, *py = NULL;
	BUN i = 0, cnt = 0;
	bit *cs = NULL;
#ifdef GEOMBULK_DEBUG
    static struct timeval start, stop;
    unsigned long long t;
#endif

	/*Get the BATs */
	if ((bpx = BATdescriptor(*point_x)) == NULL) {
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}
	if ((bpy = BATdescriptor(*point_y)) == NULL) {
		BBPunfix(bpx->batCacheid);
		throw(MAL, "geom.point", RUNTIME_OBJECT_MISSING);
	}

	/*Check BATs alignment */
	if (bpx->hseqbase != bpy->hseqbase || BATcount(bpx) != BATcount(bpy)) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", "both point bats must have dense and aligned heads");
	}

	/*Create output BAT */
	if ((bo = COLnew(bpx->hseqbase, TYPE_bit, BATcount(bpx), TRANSIENT)) == NULL) {
		BBPunfix(bpx->batCacheid);
		BBPunfix(bpy->batCacheid);
		throw(MAL, "geom.point", MAL_MALLOC_FAIL);
	}

	/*Iterate over the Point BATs and determine if they are in Polygon represented by vertex BATs */
	px = (dbl *) Tloc(bpx, 0);
	py = (dbl *) Tloc(bpy, 0);
	cnt = BATcount(bpx);
	cs = (bit *) Tloc(bo, 0);
#ifdef GEOMBULK_DEBUG
    gettimeofday(&start, NULL);
#endif

	/*Call to the GPU function*/

	/*
	 * Verify if GPU has enough memory to get all the data 
	 * otherwise do it in waves.
	 */

	/*Lock until all the results are available*/

#ifdef GEOMBULK_DEBUG
    gettimeofday(&stop, NULL);
    t = 1000 * (stop.tv_sec - start.tv_sec) + (stop.tv_usec - start.tv_usec) / 1000;
    fprintf(stdout, "pnpolyWithHoles %llu ms\n", t);
#endif

	bo->tsorted = bo->trevsorted = 0;
	bo->tkey = 0;
	BATrmprops(bo)
	BATsetcount(bo, cnt);
	BATsettrivprop(bo);
	BBPunfix(bpx->batCacheid);
	BBPunfix(bpy->batCacheid);
	BBPkeepref(*out = bo->batCacheid);
	return MAL_SUCCEED;
}

str
wkbGContains_point_bat(bat *out, wkb **a, bat *point_x, bat *point_y, bat *point_z, int *srid)
{
    vertexWKB *verts = NULL;
	wkb *geom = NULL;
	str msg = NULL;
	(void) point_z;

	geom = (wkb *) *a;
    if ((msg = getVerts(geom, &verts)) != MAL_SUCCEED) {
        return msg;
    }
         
	msg = GpnpolyWithHoles(out, (int) verts->nvert, verts->vert_x, verts->vert_y, verts->nholes, verts->holes_x, verts->holes_y, verts->holes_n, point_x, point_y);

    if (verts)
        freeVerts(verts);

	return msg;
}
