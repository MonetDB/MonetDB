/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
* Pedro Ferreira
* This file contains declarations for SQL subquery implementations.
*/

#ifndef _GDK_SUBQUERY_H_
#define _GDK_SUBQUERY_H_

gdk_export BAT *BATall_grp(BAT *l, BAT *g, BAT *e, BAT *s);
gdk_export BAT *BATnil_grp(BAT *l, BAT *g, BAT *e, BAT *s);

gdk_export BAT *BATanyequal_grp(BAT *l, BAT *r, BAT *g, BAT *e, BAT *s);
gdk_export BAT *BATallnotequal_grp(BAT *l, BAT *r, BAT *g, BAT *e, BAT *s);
gdk_export BAT *BATanyequal_grp2(BAT *l, BAT *r, BAT *rid, BAT *g, BAT *e, BAT *s);
gdk_export BAT *BATallnotequal_grp2(BAT *l, BAT *r, BAT *rid, BAT *g, BAT *e, BAT *s);

gdk_export BAT *BATsubexist(BAT *l, BAT *g, BAT *e, BAT *s);
gdk_export BAT *BATsubnot_exist(BAT *l, BAT *g, BAT *e, BAT *s);

#endif //_GDK_SUBQUERY_H_
