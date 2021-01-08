/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _GEOMLOGGER_H_
#define _GEOMLOGGER_H_

typedef int (*geomcatalogfix_fptr)(void *, int);
gdk_export void geomcatalogfix_set(geomcatalogfix_fptr);
gdk_export geomcatalogfix_fptr geomcatalogfix_get(void);

typedef str (*geomsqlfix_fptr)(int);
gdk_export void geomsqlfix_set(geomsqlfix_fptr);
gdk_export geomsqlfix_fptr geomsqlfix_get(void);

gdk_export void geomversion_set(void);
gdk_export bool geomversion_get(void);

#endif /* _GEOMLOGGER_H_ */
