/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * Implementation for the oid order index.
 * Martin Kersten, Lefteris Sidirourgos.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/*
 * Keep the oid list of *order as the OID order index of *b, and also
 * make it persistent if *b is persistent. BAT *order can then be unfixed by
 * the caller function.
 */

gdk_return
ORDERindex(BAT *b, BAT *order) {
	(void) b;
	(void) order;
	return GDK_SUCCEED;
}
