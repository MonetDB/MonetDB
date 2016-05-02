/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @a Kostis Kyzirakos
 */

#include <monetdb_config.h>
#include "libgeom.h"
#include "geom.h"

#include <mal.h>
#include <mal_atom.h>
#include <mal_exception.h>
#include <mal_client.h>
#include <stream.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include <SFCGAL/capi/sfcgal_c.h>

#ifdef WIN32
#ifndef LIBGEOM
#define geom_export extern __declspec(dllimport)
#else
#define geom_export extern __declspec(dllexport)
#endif
#else
#define geom_export extern
#endif

geom_export char * geom_sfcgal_version(char **ret);
geom_export str geom_sfcgal_extrude(wkb **res, wkb **geom, double *ex, double *ey, double *ez);
geom_export str geom_sfcgal_straightSkeleton(wkb **res, wkb **geom);
geom_export str geom_sfcgal_tesselate(wkb **res, wkb **geom);
geom_export str geom_sfcgal_triangulate2DZ(wkb **res, wkb **geom, int *flag);

