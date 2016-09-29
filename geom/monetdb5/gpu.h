/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a Romulo Goncalves
 */

#include <monetdb_config.h>
#include "libgeom.h"
#include "geom.h"
#include "g_pnpoly.h"

#ifdef WIN32
#ifndef LIBGEOM
#define geom_export extern __declspec(dllimport)
#else
#define geom_export extern __declspec(dllexport)
#endif
#else
#define geom_export extern
#endif

geom_export str geom_gpu_setup(bit *res, int *flag);
geom_export str geom_gpu_reset(bit *res, int *flag);
geom_export str geom_gpu_gcontains(bit *res, wkb **geom, double *x, double *y, double *z, int *srid);
geom_export str geom_gpu_gcontains_bat(int *res, wkb **geom, int *xBAT_id, int *yBAT_id, int *zBAT_id, int *srid);
